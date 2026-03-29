from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.context import Context
from airflow.decorators import dag, task

from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.base import poke_mode_only
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pathlib import Path
import pandas as pd
import pendulum
import os
import subprocess
from datetime import timedelta
from enum import StrEnum


media_folder_path = Path("/opt/airflow/media")
watch_folder_path = media_folder_path/Path("watchfolder")
proxy_folder_path = media_folder_path/Path("proxy")
main_folder_path = media_folder_path/Path("main")
FILE_TYPES = [".MP4", ".MOV", ".AVI", ".MKV", ".WMV", ".MXF"]
class Status(StrEnum):
    NEW = "new" # увидели впервые
    GROWING = "growing" # размер ещё меняется
    READY = "ready" # размер стабилен, можно обрабатывать
    PROCESSING = "processing" # задача уже взяла файл в работу
    DONE = "done" # успешно обработан
    ERROR = "error" # была ошибка
stable_count_limit = 3



@poke_mode_only
class Wait_file_sensor(BaseSensorOperator):
    temolated_fields = ("path",)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id="38811")
        df = hook.get_pandas_df(sql="SELECT * FROM files_registry")

        for dirpath, _,filenames in os.walk(watch_folder_path):
            for file in filenames:
                full_path = Path(dirpath)/file
                cur_size = full_path.stat().st_size
                mask = df["file_name"].eq(file)
                if mask.any():
                    i = df.index[mask][0]

                    if df.at[i, "status"] != Status.READY.value:
                        prev_size = df.at[i, "last_size"]

                        if prev_size == cur_size:
                            df.at[i, "stable_count"] += 1
                        else:
                            df.at[i, "stable_count"] = 0
                            df.at[i, "last_size"] = cur_size
                            df.at[i, "status"] = Status.GROWING.value

                        if df.at[i, "stable_count"] >= stable_count_limit:
                            df.at[i, "status"] = Status.READY.value
            else:
                df.loc[-1] = {
                    "file_name": file,
                    "status": Status.NEW.value,
                    "last_size": cur_size,
                    "stable_count": 0,
                }

        engine = hook.get_sqlalchemy_engine()
        df.to_sql(
            name="files_registry_stage",
            con=engine,
            if_exists="replace",
            index=False
        )

        hook.run("""
            INSERT INTO files_registry (status, last_size, stable_count, file_name)
            SELECT file_name, status, last_size, stable_count
            FROM files_registry_stage
            ON CONFLICT (file_name)
            DO UPDATE SET
                status = EXCLUDED.status,
                last_size = EXCLUDED.last_size,
                stable_count = EXCLUDED.stable_count
        """)
        hook.run("DROP TABLE files_registry_stage")
        if (df["status"] == Status.READY.value).sum()!=0:
            return True
        else:
            return False

@dag(
    tags=["convertion_dag"],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=timedelta(minutes=5),
    catchup=False
)
def conversion_dag():

    @task()
    def get_ready_files():
        ready_files = []
        hook = PostgresHook(postgres_conn_id="38811")
        df = hook.get_pandas_df(sql="SELECT * FROM files_registry")
        names = df[df["status"] == Status.READY.value, "file_name"]
        for name in names:
            ready_files.append(name)
        return ready_files

    @task()
    def prepare_paths(file, **context: Context):
        dag_id = "prepare_paths_dag"
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        media_file = file
        if media_file=="no file contributet":
            print("error n1")
            return {"i": "0", "o": "0", "m": "0"}
        new_file_type = str(conf.get("file_type", "not type contributet")).upper()

        if new_file_type in FILE_TYPES and str(media_file[-4:]).upper() in FILE_TYPES:
            file_for_proxy = str(media_file).replace(media_file[-4:], new_file_type.lower())
            file_for_copy = str(media_file)
            try:
                input_path = watch_folder_path/Path(media_file)
                proxy_output = proxy_folder_path/Path(file_for_proxy)
                main_output = main_folder_path/Path(file_for_copy)
                return {"i": str(input_path), "o": str(proxy_output), "m": str(main_output)}
            except (TypeError,ValueError) as e:
                if media_file=="no file contributet":
                    print("error n1")
                print(e)  
                return {"i": "0", "o": "0", "m": "0"}
        else:
            print(f"{new_file_type} or {media_file[-4:]} isn`t correct file type")
            return {"i": "0", "o": "0", "m": "0"}
    
    @task()
    def convert_file(paths):
 
        input_path = paths.get("i")
        proxy_output = paths.get("o")
        main_output = paths.get("m")

        if input_path!="0" and proxy_output!="0" and main_output!="0":
            if not Path(proxy_output).exists() or not Path(main_output).exists():
                #копирует файл в main из watchfolder без изменений
                if not Path(input_path).exists():
                    print(f"input file not found: {input_path}")
                    return {"res": "0"} 
                try:
                    result1 = subprocess.run(
                        [
                            "ffmpeg",
                            "-y",
                            "-i", str(input_path),
                            "-c", "copy",
                            str(main_output),
                        ],
                        capture_output=True,
                        text=True,
                        check=False
                    )
                    if result1.returncode == 0:
                        print(f"succes copy. file dir: {main_output}")
                    else:
                        print("ffmpeg main error:")
                        print(result1.stderr)
                    print(f"main created: {main_output}")

                except Exception as error:
                    print(f"main exeption {error}")
                    return {"res": "0"}

                # транскодирует файл в proxy
                try:
                    result2 = subprocess.run(
                        [
                            "ffmpeg",
                            "-y",
                            "-i", str(input_path),
                            "-vf", "scale=1280:720",
                            "-c:v", "libx264",
                            "-preset", "veryfast",
                            "-crf", "26",
                            "-c:a", "aac",
                            "-b:a", "128k",
                            str(proxy_output)
                        ],
                        capture_output=True,
                        text=True,
                        check=False
                    )
                    if result2.returncode == 0:
                        print(f"succes transcode. file dir: {proxy_output}")
                        return {"res": "1"}
                    else:
                        print("ffmpeg proxy error:")
                        print(result2.stderr)
                    print(f"proxy created: {proxy_output}")
                    return {"res": "0"}

                except Exception as error:
                    print(error)
                    return {"res": "0"}
            else:
                print("this file allready converted")
                return {"res": "0"}
        else:
            print("no values have contributed")
            return {"res": "0"}
            

    @task()
    def conversion_confirmation(res, paths):
        proxy_output = paths.get("o")
        main_output = paths.get("m")
        code = res.get("res")

        if code != "1":
            print("conversion was not successful")
            return {"confirm": "0"}

        try:
            proxy_exists = Path(proxy_output).exists()
            main_exists = Path(main_output).exists()

            if not proxy_exists:
                print(f"proxy file not found: {proxy_output}")
                return {"confirm": "0"}

            if not main_exists:
                print(f"main file not found: {main_output}")
                return {"confirm": "0"}

            proxy_size = os.path.getsize(proxy_output)
            main_size = os.path.getsize(main_output)

            if proxy_size <= 0:
                print(f"proxy file is empty: {proxy_output}")
                return {"confirm": "0"}

            if main_size <= 0:
                print(f"main file is empty: {main_output}")
                return {"confirm": "0"}

            print(f"proxy file confirmed: {proxy_output}")
            print(f"main file confirmed: {main_output}")
            return {"confirm": "1"}

        except FileNotFoundError as e:
            print(f"file not found: {e}")
            return {"confirm": "0"}
        except PermissionError as e:
            print(f"permission error: {e}")
            return {"confirm": "0"}
        except Exception as e:
            print(f"unexpected error: {e}")
            return {"confirm": "0"}
        
    wait_file = Wait_file_sensor(
        task_id="wait_file",
        poke_interval=5,
        timeout=60 * 30,
        mode="poke",
    )
    ready_files = get_ready_files()
    for file in ready_files:
        paths = prepare_paths(file=file)
        res = convert_file(paths)
        confirmation = conversion_confirmation(res, paths)

    wait_file >> paths >> res
    res >> confirmation

conversion_dag()
    



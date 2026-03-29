import requests
import json
from CONSTRAINTS import AIRFLOW_API_URL, USERNAME, PASSWORD, TOKEN_URL
from datetime import datetime, timezone


def get_token():
    url = TOKEN_URL
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "username": "airflow",
        "password": "airflow"
    }
    response = requests.post(url=url,
                             data=json.dumps(data),
                             headers=headers,
                             auth=(USERNAME, PASSWORD))
    return (response.status_code, response.json()["access_token"])


def airflow_post_run_dag(dag_id: str, conf: dict | None):
    resp = get_token()
    # print(resp)
    if resp[0] == 201:
        JWT_TOKEN = resp[-1]
    else:
        return "error with JWT token something went wrong"
    url = AIRFLOW_API_URL + f"/dags/{dag_id}/dagRuns"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {JWT_TOKEN}"
    }
    time_now = datetime.now(timezone.utc).isoformat().replace("+03:00", "Z")
    dag_run_id = dag_id + (time_now.replace("+", "_"))

    if conf is not None:
        data = {
            "dag_run_id": dag_run_id,
            "logical_date": time_now,
            "run_after": time_now,
            "conf": conf
        }
    else:
        data = {
            "dag_run_id": dag_run_id,
            "run_after": time_now,
            "logical_date": time_now
        }
    response = requests.post(url=url,
                             data=json.dumps(data),
                             headers=headers,
                             )
    return (response.status_code,
            response.json(),
            dag_run_id)


def airflow_get_dag_status(dag_id: str, dag_run_id: str):
    resp = get_token()
    if resp[0] == 201:
        JWT_TOKEN = resp[-1]
    else:
        return 502, {"error": "error with JWT token something went wrong"}

    url = AIRFLOW_API_URL + f"/dags/{dag_id}/dagRuns/{dag_run_id}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {JWT_TOKEN}",
    }

    response = requests.get(url=url, headers=headers)
    try:
        body = response.json()
    except ValueError:
        body = {"error": "non-json response from airflow", "raw": response.text}

    return response.status_code, body

# print(airflow_get_dag_status("hello_world_dag"))
# print(datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")x)
conf = {
    "file_name": "sample_1280x720_surfing_with_audio.mxf",
    "file_type": ".mp4"
    }
print(airflow_post_run_dag("conversion_dag", conf))

# code, resp = airflow_post_run_dag("convertion_dag", conf=conf)
# print(code, resp)

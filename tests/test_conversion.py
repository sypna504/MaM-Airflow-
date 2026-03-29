import ffmpeg
from pathlib import Path
import os
pwd = Path(os.getcwd())/Path("media")
input_dir = pwd/Path("old")
FILE_TYPES = [".MP4", ".MOV", ".AVI", ".MKV", ".WMV"]
output_dir = pwd/Path("new")


def change_file_type(file_name, new_type) -> None:
    for _, _, files in os.walk(input_dir):
        for file in files:
            if file_name == file:
                file_type = file[-4:]
                if file_type in FILE_TYPES:
                    output_file = str(file).replace(f"old{file_type}",
                                                    f"new{new_type}")
                    output_direc = str(output_dir/Path(output_file))
                    if not Path(output_direc).exists():
                        try:
                            proces = (
                                ffmpeg
                                .input(str(input_dir/Path(file_name)))
                                .output(output_direc, acodec="aac", vcodec="copy")
                                .run()
                            )
                            proces.communicate(input=input_data)   
                            print(f"succes file dir:{output_direc}")
                        except ffmpeg.Error as error:
                            print(error)
                    else:
                        print("this file already conversated")

change_file_type("IMG_2459_old.MP4", ".AVI")

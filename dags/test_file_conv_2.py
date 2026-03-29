import requests
import json


def get_configuration() -> json:
    url="http://localhost:3000"
    response = requests.get(url)
    if response.status_code==200:
        print(response.text)
        data = response.json()
        return data
    return 0

def get_params_from_data(data):
    file_path = data.get("path", "no path")
    dag_id = data.get("dag_id", "no dagid")
    watch_folders = data.get("watchfolders", "no folders")
    main_foldres = data.get("main_folder", "no main folder")
    if file_path=="no path":
        print("no file path")
        return 1
    if dag_id=="no dagid":
        print("no dag id ")
        return 1
    if watch_folders=="no folders":
        print("no folders")
        return 1
    if main_foldres=="no main folder":
        print("no main folder")
        return 1
    
    




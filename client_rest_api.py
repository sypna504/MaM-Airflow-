from flask import Flask, jsonify, request
from airflow_api import airflow_post_run_dag, airflow_get_dag_status
import json
import requests
app = Flask(__name__)
user_status = dict[int, str]


@app.route("/run_dag", methods=["POST"])
def run_dag():
    payload = request.get_json(silent=True) or {}
    if payload != {}:
        dag_id = payload.get("dag_id")
        conf = payload.get("conf") or None
    code, resp, dag_run_id = airflow_post_run_dag(dag_id, conf)
    print(f"dag_run_id: {dag_run_id}\n")
    print(f"dag_id: {dag_id}\n")
    if code == 200:
        return resp, 200
    else:
        return resp, 404


@app.route("/dag_status", methods=["GET"])
def get_dag_status():
    dag_id = request.args.get("dag_id")
    dag_run_id = request.args.get("dag_run_id")

    if not dag_id or not dag_run_id:
        return {"error": "dag_id and dag_run_id are required"}, 400

    code, resp = airflow_get_dag_status(dag_id, dag_run_id)
    print(code, resp.get("state"))
    return resp, int(code)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)

import uuid
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException


import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.pipeline import run_full_snapshot, run_incremental

app = FastAPI(title="Ecommerce ETL")


tasks = {}


def run_etl_task(task_id, mode):
    tasks[task_id]["status"] = "running"

    try:
        if mode == "full":
            result = run_full_snapshot()
        else:
            result = run_incremental()

        tasks[task_id]["status"] = "completed"
        tasks[task_id]["result"] = result

    except Exception as e:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["result"] = {"error": str(e)}

    finally:
        tasks[task_id]["finished_at"] = datetime.now().isoformat()


@app.post("/etl/full", status_code=202)
def start_full(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "task_id": task_id,
        "status": "pending",
        "mode": "full",
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "result": None,
    }
    background_tasks.add_task(run_etl_task, task_id, "full")
    return {"task_id": task_id, "status": "pending"}


@app.post("/etl/incremental", status_code=202)
def start_incremental(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "task_id": task_id,
        "status": "pending",
        "mode": "incremental",
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "result": None,
    }

    background_tasks.add_task(run_etl_task, task_id, "incremental")
    return {"task_id": task_id, "status": "pending"}


@app.get("/etl/status/{task_id}")
def get_status(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks[task_id]


@app.get("/etl/history")
def get_history():
    return list(tasks.values())

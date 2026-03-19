"""Databricks Files API uploader — uploads local files to Unity Catalog Volumes."""

import os
import requests


DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

VOLUME_PATH = "/Volumes/education_pipeline/bronze/raw_files"


def upload_file(local_path: str, remote_filename: str) -> None:
    """Upload a local file to the bronze Volume via Databricks Files API."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{VOLUME_PATH}/{remote_filename}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream",
    }
    with open(local_path, "rb") as f:
        response = requests.put(url, headers=headers, data=f)

    if response.status_code == 204:
        print(f"[OK] Uploaded {remote_filename} → {VOLUME_PATH}/{remote_filename}")
    else:
        raise RuntimeError(
            f"Upload failed for {remote_filename}: "
            f"HTTP {response.status_code} — {response.text}"
        )


def trigger_job(job_id: int) -> None:
    """Trigger a Databricks job via Jobs API."""
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.post(url, headers=headers, json={"job_id": job_id})

    if response.status_code == 200:
        run_id = response.json().get("run_id")
        print(f"[OK] Job {job_id} triggered — run_id={run_id}")
    else:
        raise RuntimeError(
            f"Failed to trigger job {job_id}: "
            f"HTTP {response.status_code} — {response.text}"
        )

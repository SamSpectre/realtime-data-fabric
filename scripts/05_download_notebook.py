"""
05_download_notebook.py

Download the notebook from Databricks workspace to local project.

Usage:
    uv run python -m project_03_realtime.scripts.05_download_notebook
"""

import os
import base64
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

load_dotenv()


def download_notebook():
    client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    
    email = "samdeeshsehgal@gmail.com"
    notebook_path = f"/Users/{email}/01_github_streaming_bronze"
    local_path = "project_03_realtime/notebooks/01_github_streaming_bronze.py"
    
    print(f"Downloading: {notebook_path}")
    
    response = client.workspace.export(
        path=notebook_path,
        format=ExportFormat.SOURCE
    )

    # Decode content
    content = base64.b64decode(response.content).decode("utf-8")
    
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    print(f"Saved to: {local_path}")
    print(f"Size: {len(content):,} characters")


if __name__ == "__main__":
    download_notebook()
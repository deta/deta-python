import os
from typing import Optional


def _get_project_key_id(project_key: Optional[str] = None, project_id: Optional[str] = None):
    project_key = project_key or os.getenv("DETA_PROJECT_KEY")

    if not project_key:
        raise ValueError("no project key defined")

    if not project_id:
        project_id = project_key.split("_")[0]

    if project_id == project_key:
        raise ValueError("bad project key provided")

    return project_key, project_id

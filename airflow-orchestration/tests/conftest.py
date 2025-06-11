import os
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag

# Add the project root directory to the Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)


@pytest.fixture()
def dagbag():
    return DagBag()

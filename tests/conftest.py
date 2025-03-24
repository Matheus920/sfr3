"""
This file configures pytest to properly find modules in the project.
"""
import sys
from pathlib import Path

# Add the parent directory (project root) to sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

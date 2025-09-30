#!/usr/bin/env python3
"""
Analytics Utilities
===================

Helper functions for analytics components.
"""

import sys
from pathlib import Path

def setup_project_imports():
    """Add project src directory to Python path"""
    project_root = Path(__file__).parent.parent
    src_path = project_root / 'src'
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

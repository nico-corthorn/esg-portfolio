
import pytest
import pandas as pd
from esgtools.etl import etl_alpha


@pytest.fixture
def prices_path():
    """Path to prices folder in tests"""
    yield "esgtools/tests/data/prices/"


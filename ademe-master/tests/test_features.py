import pytest
import pandas as pd


import sys

sys.path.append("./src/step_0")

from features import rename_columns


def test_rename_columns():
    original_file = "./data/original/dpe-v2-tertiaire-2.csv"
    data = pd.read_csv(original_file)

    new_columns = rename_columns(data.columns)
    assert data.shape[1] == len(new_columns)

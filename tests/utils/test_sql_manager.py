import pandas as pd
import pytest


@pytest.fixture
def test_table(sql):
    table_name = "test_table"
    sql.query(f"CREATE TABLE {table_name} (col1 INT, col2 TEXT)", {"table_name": table_name})
    yield table_name
    sql.query(f"DROP TABLE {table_name}", {"table_name": table_name})


@pytest.mark.integration
def test_integration_select(sql, test_table):
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 1, "value2": "a"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 2, "value2": "b"},
    )
    result = sql.select(test_table)
    expected_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.integration
def test_integration_select_query(sql, test_table):
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 1, "value2": "a"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 2, "value2": "b"},
    )
    result = sql.select_query(f"SELECT * FROM {test_table}")
    expected_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.integration
def test_integration_select_column_list(sql, test_table):
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 1, "value2": "a"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 2, "value2": "b"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 3, "value2": "c"},
    )
    result = sql.select_column_list("col1", test_table)
    assert result == [1, 2, 3]


@pytest.mark.integration
def test_integration_select_distinct_column_list(sql, test_table):
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 1, "value2": "a"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 2, "value2": "b"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 2, "value2": "c"},
    )
    result = sql.select_distinct_column_list("col1", test_table)
    assert result == [1, 2]


@pytest.mark.integration
def test_integration_select_as_dictionary(sql, test_table):
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 1, "value2": "a"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 2, "value2": "b"},
    )
    result = sql.select_as_dictionary("col1", "col2", test_table)
    assert result == {1: "a", 2: "b"}


@pytest.mark.integration
def test_integration_upload_df(sql, test_table):
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    sql.upload_df(test_table, df)
    result = sql.select(test_table)
    pd.testing.assert_frame_equal(result, df)


@pytest.mark.integration
def test_integration_clean_table(sql, test_table):
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 1, "value2": "a"},
    )
    sql.query(
        f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
        {"value1": 2, "value2": "b"},
    )
    sql.clean_table(test_table)
    result = sql.select(test_table)
    expected_df = pd.DataFrame(columns=["col1", "col2"])
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.integration
def test_integration_query_insert(sql, test_table):
    values = [(1, "a"), (2, "b"), (3, "c")]
    for value1, value2 in values:
        sql.query(
            f"INSERT INTO {test_table} (col1, col2) VALUES (%(value1)s, %(value2)s)",
            {"value1": value1, "value2": value2},
        )
    result = sql.select(test_table)
    expected_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    pd.testing.assert_frame_equal(result, expected_df)

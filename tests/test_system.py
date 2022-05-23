import pytest
from sql_system_transfer.system import SQLSystem

@pytest.mark.parametrize(
    "system, expected_result", [
        (SQLSystem.MSSQL, "MsSQL"),
        (SQLSystem.MYSQL, "MySQL"),
    ])
def test_system_abbreviation(system, expected_result):
    system_abbreviation = system.system_abbreviation()
    assert system_abbreviation == expected_result

@pytest.mark.parametrize(
    "system, expected_result", [
        (SQLSystem.MSSQL, "mssql"),
        (SQLSystem.MYSQL, "mysql"),
    ])
def test_system_abbreviation_lower(system, expected_result):
    system_abbreviation = system.system_abbreviation_lower()
    assert system_abbreviation == expected_result

@pytest.mark.parametrize(
    "system, expected_result", [
        (SQLSystem.MSSQL, "{ODBC Driver 17 for SQL Server}"),
        (SQLSystem.MYSQL, "{MySQL ODBC 8.0 Unicode Driver}"),
    ])
def test_system_driver(system, expected_result):
    driver = system.system_driver()
    assert driver == expected_result
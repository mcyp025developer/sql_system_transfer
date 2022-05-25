import pytest
import unittest
from unittest.mock import patch
from sql_system_transfer.engine import SQLSystem, Engine, Database, MsSQLTable, MySQLTable, Column

"""
functional, integrated test think about how to implement.

Think of testing a function that accesses an external HTTP API.  Rather than ensuring that a test server is available 
to send the correct responses, we can mock the HTTP library and replace all the HTTP calls with mock calls.  This 
reduces test complexity and dependencies, and gives us precise control over what the HTTP library returns, which may be
difficult to accomplish otherwise.

In the Engine class we will try to mock the pyodbc library and replace all the pyodbc calls with mock calls.
"""




class TestDatabase:

    @pytest.fixture()
    def database(self):
        return Database(system=SQLSystem.MSSQL, database="Testing")

    def test_statement_use_database(self, database):
        statement = database.statement_use_database()
        assert statement == "USE Testing;"

    def test_database_table_names(self, database):
        database_table_names = database.database_table_names()
        assert isinstance(database_table_names, list)

    def test_database_table_parameters(self, database):
        database_table_parameters = database.database_table_parameters()
        assert isinstance(database_table_parameters, list)

    def test_database_table_convert_old(self, database):
        database_tables = database.database_table_convert_old(tables=[])
        assert isinstance(database_tables, list)

    @pytest.mark.parametrize(
        "convert_to_system", [SQLSystem.MSSQL, SQLSystem.MYSQL])
    def test_database_table_convert_new(self, database, convert_to_system):
        database_tables = database.database_table_convert_new(tables=[], convert_to_system=convert_to_system)
        assert isinstance(database_tables, list)

    def test_add_table_to_database(self, database):
        table = {'schema': 'dbo', 'table': 'DimCurrency', 'table_type': 'BASE TABLE'}
        table_columns = [
            {'column_name': 'CurrencyKey', 'nullable': 'NO', 'datatype_name': 'int', 'character_size': None,
             'character_set': None, 'numeric_precision': 10, 'numeric_scale': 0, 'datetime_precision': None},
            {'column_name': 'CurrencyAlternateKey', 'nullable': 'NO', 'datatype_name': 'nchar', 'character_size': 3,
             'character_set': 'UNICODE', 'numeric_precision': None, 'numeric_scale': None, 'datetime_precision': None},
            {'column_name': 'CurrencyName', 'nullable': 'NO', 'datatype_name': 'nvarchar', 'character_size': 50,
             'character_set': 'UNICODE', 'numeric_precision': None, 'numeric_scale': None, 'datetime_precision': None},
        ]
        database.add_table_to_database(table=table, columns=table_columns)
        assert len(database.database_tables) == 1
        assert isinstance(database.database_tables[0], MsSQLTable)


class TestMsSQLTable:

    table_columns = [
        {'column_name': 'AccountKey', 'nullable': 'NO', 'datatype_name': 'int', 'character_size': None,
         'character_set': None, 'numeric_precision': 10, 'numeric_scale': 0, 'datetime_precision': None},
        {'column_name': 'ParentAccountKey', 'nullable': 'YES', 'datatype_name': 'int', 'character_size': None,
         'character_set': None, 'numeric_precision': 10, 'numeric_scale': 0, 'datetime_precision': None},
        {'column_name': 'AccountCodeAlternateKey', 'nullable': 'YES', 'datatype_name': 'int', 'character_size': None,
         'character_set': None, 'numeric_precision': 10, 'numeric_scale': 0, 'datetime_precision': None},
    ]

    @pytest.fixture()
    def table(self):
        return MsSQLTable(
            table="DimAccount",
            table_type="BASE TABLE",
            schema="dbo",
            init_table_columns=self.table_columns
        )

    def test_table_parameters(self, table):
        table_parameters = table.table_parameters()
        assert table_parameters == {"table": "DimAccount", "table_type": "BASE TABLE", "schema": "dbo"}

    @pytest.mark.parametrize(
        "convert_to_system, expected_result", [
            (SQLSystem.MSSQL, MsSQLTable),
            (SQLSystem.MYSQL, MySQLTable),
        ])
    def test_table_convert_no_kwargs(self, table, convert_to_system, expected_result):
        convert_table = table.table_convert(convert_to_system=convert_to_system)
        assert isinstance(convert_table, expected_result)

    @pytest.mark.parametrize(
        "convert_to_system, table_name, schema, expected_result", [
            (SQLSystem.MSSQL, "new_name", "new_schema", (MsSQLTable, "new_name", "new_schema")),
            (SQLSystem.MYSQL, "new_name", "new_schema", (MySQLTable, "new_name", None)),
        ])
    def test_table_convert_kwargs(self, table, convert_to_system, table_name, schema, expected_result):
        convert_table = table.table_convert(convert_to_system=convert_to_system, table=table_name, schema=schema)
        assert isinstance(convert_table, expected_result[0])
        assert convert_table.table == expected_result[1]
        assert convert_table.schema == expected_result[2]

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, f"CREATETABLEtesting.dbo.DimAccount(AccountKeyintnotnull,ParentAccountKeyintnull,AccountCodeAlternateKeyintnull);"),
            ("new_name", f"CREATETABLEtesting.dbo.new_name(AccountKeyintnotnull,ParentAccountKeyintnull,AccountCodeAlternateKeyintnull);"),
        ])
    def test_statement_create_table(self, table, alt_table_name, expected_result):
        statement = table.statement_create_table(database="testing", alt_table_name=alt_table_name).replace("\n", "").replace(" ", "")
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, f"INSERTINTOtesting.dbo.DimAccount(AccountKey,ParentAccountKey,AccountCodeAlternateKey)VALUES(?,?,?);"),
            ("new_name", f"INSERTINTOtesting.dbo.new_name(AccountKey,ParentAccountKey,AccountCodeAlternateKey)VALUES(?,?,?);"),
        ])
    def test_statement_insert_table(self, table, alt_table_name, expected_result):
        statement = table.statement_insert_table(database="testing", alt_table_name=alt_table_name).replace("\n", "").replace(" ", "")
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, "SELECT * FROM testing.dbo.DimAccount;"),
            ("new_name", "SELECT * FROM testing.dbo.new_name;"),
        ])
    def test_statement_select_table(self, table, alt_table_name, expected_result):
        statement = table.statement_select_table(database="testing", alt_table_name=alt_table_name)
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, "DROP TABLE IF EXISTS testing.dbo.DimAccount;"),
            ("new_name", "DROP TABLE IF EXISTS testing.dbo.new_name;"),
        ])
    def test_statement_drop_table(self, table, alt_table_name, expected_result):
        statement = table.statement_drop_table(database="testing", alt_table_name=alt_table_name)
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, "testing.dbo.DimAccount"),
            ("new_name", "testing.dbo.new_name"),
        ])
    def test_table_format(self, table, alt_table_name, expected_result):
        table_format = table.table_format(database="testing", alt_table_name=alt_table_name)
        assert table_format == expected_result


class TestMySQLTable:

    table_columns = [
        {'column_name': 'CurrencyKey', 'nullable': 'YES', 'datatype_name': 'int', 'character_size': None,
         'character_set': None, 'numeric_precision': 10, 'numeric_scale': 0, 'datetime_precision': None},
        {'column_name': 'CurrencyAlternateKey', 'nullable': 'YES', 'datatype_name': 'char', 'character_size': 3,
         'character_set': 'utf8mb4', 'numeric_precision': None, 'numeric_scale': None, 'datetime_precision': None},
        {'column_name': 'CurrencyName', 'nullable': 'YES', 'datatype_name': 'varchar', 'character_size': 50,
         'character_set': 'utf8mb4', 'numeric_precision': None, 'numeric_scale': None, 'datetime_precision': None},
    ]

    @pytest.fixture()
    def table(self):
        return MySQLTable(
            table="DimCurrency",
            table_type="BASE TABLE",
            init_table_columns=self.table_columns
        )

    def test_table_parameters(self, table):
        table_parameters = table.table_parameters()
        assert table_parameters == {"table": "DimCurrency", "table_type": "BASE TABLE", "schema": None}

    @pytest.mark.parametrize(
        "convert_to_system, expected_result", [
            (SQLSystem.MSSQL, MsSQLTable),
            (SQLSystem.MYSQL, MySQLTable),
        ])
    def test_table_convert_no_kwargs(self, table, convert_to_system, expected_result):
        convert_table = table.table_convert(convert_to_system=convert_to_system)
        assert isinstance(convert_table, expected_result)

    @pytest.mark.parametrize(
        "convert_to_system, table_name, schema, expected_result", [
            (SQLSystem.MSSQL, "new_name", "new_schema", (MsSQLTable, "new_name", "new_schema")),
            (SQLSystem.MYSQL, "new_name", "new_schema", (MySQLTable, "new_name", None)),
        ])
    def test_table_convert_kwargs(self, table, convert_to_system, table_name, schema, expected_result):
        convert_table = table.table_convert(convert_to_system=convert_to_system, table=table_name, schema=schema)
        assert isinstance(convert_table, expected_result[0])
        assert convert_table.table == expected_result[1]
        assert convert_table.schema == expected_result[2]

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, f"CREATETABLEtesting.DimCurrency(CurrencyKeyintnull,CurrencyAlternateKeychar(3)charactersetutf8mb4null,CurrencyNamevarchar(50)charactersetutf8mb4null);"),

        ])
    def test_statement_create_table(self, table, alt_table_name, expected_result):
        statement = table.statement_create_table(database="testing", alt_table_name=alt_table_name).replace("\n", "").replace(" ", "")
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, f"INSERTINTOtesting.DimCurrency(CurrencyKey,CurrencyAlternateKey,CurrencyName)VALUES(?,?,?);"),
            ("new_name", f"INSERTINTOtesting.new_name(CurrencyKey,CurrencyAlternateKey,CurrencyName)VALUES(?,?,?);"),
        ])
    def test_statement_insert_table(self, table, alt_table_name, expected_result):
        statement = table.statement_insert_table(database="testing", alt_table_name=alt_table_name).replace("\n", "").replace(" ", "")
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, "SELECT * FROM testing.DimCurrency;"),
            ("new_name", "SELECT * FROM testing.new_name;"),
        ])
    def test_statement_select_table(self, table, alt_table_name, expected_result):
        statement = table.statement_select_table(database="testing", alt_table_name=alt_table_name)
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, "DROP TABLE IF EXISTS testing.DimCurrency;"),
            ("new_name", "DROP TABLE IF EXISTS testing.new_name;"),
        ])
    def test_statement_drop_table(self, table, alt_table_name, expected_result):
        statement = table.statement_drop_table(database="testing", alt_table_name=alt_table_name)
        assert statement in expected_result

    @pytest.mark.parametrize(
        "alt_table_name, expected_result", [
            (None, "testing.DimCurrency"),
            ("new_name", "testing.new_name"),
        ])
    def test_table_format(self, table, alt_table_name, expected_result):
        table_format = table.table_format(database="testing", alt_table_name=alt_table_name)
        assert table_format == expected_result


class TestColumn:

    @pytest.fixture()
    def column(self):
        column_parameters = {
            "system": SQLSystem.MSSQL,
            "column_name": "FirstName",
            "datatype_name": "varchar",
            "nullable": "YES",
            "character_size": 100,
            "character_set": "iso1",
            "numeric_precision": None,
            "numeric_scale": None,
            "datetime_precision": None
        }
        return Column(**column_parameters)

    def test_column_parameters(self, column):
        column_parameters = column.column_parameters()
        assert column_parameters == {"column_name": "FirstName", "datatype_name": "varchar", "character_size": 100}

    def test_column_datatype(self, column):
        column_datatype = column.column_datatype()
        assert column_datatype == "varchar"

    def test_column_format(self, column):
        column_format = column.column_format()
        assert column_format == "FirstName varchar(100) null"

    @pytest.mark.parametrize(
        "convert_to_system", [
            SQLSystem.MSSQL,
            SQLSystem.MYSQL
        ])
    def test_column_convert(self, column, convert_to_system):
        convert_column = column.column_convert(convert_to_system=convert_to_system)
        assert isinstance(convert_column, Column)

import pytest
from sql_system_transfer.statements import MsSQLStatements, MySQLStatements
from textwrap import dedent

class TestMsSQLStatements:

    @pytest.fixture()
    def statements(self):
        return MsSQLStatements()

    def test_statement_information_schema_columns(self, statements):
        statement = statements.statement_information_schema_columns(database="Testing", table="DimTable", schema="dbo")
        assert statement == dedent(f"""
            SELECT COLUMN_NAME 
                ,IS_NULLABLE
                ,DATA_TYPE
                ,CHARACTER_MAXIMUM_LENGTH
                ,CHARACTER_SET_NAME
                ,NUMERIC_PRECISION
                ,NUMERIC_SCALE
                ,DATETIME_PRECISION  
            FROM Testing.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
            AND TABLE_NAME = 'DimTable'
            ORDER BY ORDINAL_POSITION;
        """)

    def test_statement_information_schema_tables(self, statements):
        statement = statements.statement_information_schema_tables(database="Testing")
        assert statement == dedent(f"""
            SELECT TABLE_SCHEMA, 
                TABLE_NAME, 
                TABLE_TYPE
            FROM Testing.INFORMATION_SCHEMA.TABLES;
        """)

    def test_statement_table_exists(self, statements):
        statement = statements.statement_table_exists(
            database="Testing", table="DimTable", schema="dbo", table_type="BASE TABLE"
        )
        assert statement == dedent(f"""
            SELECT count(*) as Cnt
            FROM Testing.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'DimTable'
            AND TABLE_TYPE = 'BASE TABLE';
        """)

class TestMySQLStatements:

    @pytest.fixture()
    def statements(self):
        return MySQLStatements()

    def test_statement_information_schema_columns(self, statements):
        statement = statements.statement_information_schema_columns(database="Testing", table="DimTable")
        assert statement == dedent(f"""
            SELECT COLUMN_NAME 
                ,IS_NULLABLE
                ,DATA_TYPE
                ,CHARACTER_MAXIMUM_LENGTH
                ,CHARACTER_SET_NAME
                ,NUMERIC_PRECISION
                ,NUMERIC_SCALE
                ,DATETIME_PRECISION  
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Testing'
            AND TABLE_NAME = 'DimTable'
            ORDER BY ORDINAL_POSITION;
        """)

    def test_statement_information_schema_tables(self, statements):
        statement = statements.statement_information_schema_tables(database="Testing")
        assert statement == dedent(f"""
            SELECT '', 
                TABLE_NAME, 
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'Testing';
        """)

    def test_statement_table_exists(self, statements):
        statement = statements.statement_table_exists(
            database="Testing", table="DimTable", table_type="BASE TABLE"
        )
        assert statement == dedent(f"""
            SELECT count(*) as Cnt
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'Testing'
            AND TABLE_NAME = 'DimTable'
            AND TABLE_TYPE = 'BASE TABLE';
        """)
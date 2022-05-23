from abc import ABC, abstractmethod
from typing import Optional
from textwrap import dedent


class Statements(ABC):

    @abstractmethod
    def statement_information_schema_columns(self, database: str, table: str, schema: Optional[str] = None) -> str:
        pass

    @abstractmethod
    def statement_information_schema_tables(self, database: str) -> str:
        pass

    @abstractmethod
    def statement_table_exists(self, database: str, table: str, table_type: str, schema: Optional[str] = None) -> str:
        pass


class MsSQLStatements(Statements):

    def statement_information_schema_columns(self, database: str, table: str, schema: Optional[str] = None) -> str:
        statement = f"""
            SELECT COLUMN_NAME 
                ,IS_NULLABLE
                ,DATA_TYPE
                ,CHARACTER_MAXIMUM_LENGTH
                ,CHARACTER_SET_NAME
                ,NUMERIC_PRECISION
                ,NUMERIC_SCALE
                ,DATETIME_PRECISION  
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION;
        """
        return dedent(statement)

    def statement_information_schema_tables(self, database: str) -> str:
        statement = f"""
            SELECT TABLE_SCHEMA, 
                TABLE_NAME, 
                TABLE_TYPE
            FROM {database}.INFORMATION_SCHEMA.TABLES;
        """
        return dedent(statement)

    def statement_table_exists(self, database: str, table: str, table_type: str, schema: Optional[str] = None) -> str:
        statement = f"""
            SELECT count(*) as Cnt
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table}'
            AND TABLE_TYPE = '{table_type}';
        """
        return dedent(statement)


class MySQLStatements(Statements):

    def statement_information_schema_columns(self, database: str, table: str, schema: Optional[str] = None) -> str:
        statement = f"""
            SELECT COLUMN_NAME 
                ,IS_NULLABLE
                ,DATA_TYPE
                ,CHARACTER_MAXIMUM_LENGTH
                ,CHARACTER_SET_NAME
                ,NUMERIC_PRECISION
                ,NUMERIC_SCALE
                ,DATETIME_PRECISION  
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{database}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION;
        """
        return dedent(statement)

    def statement_information_schema_tables(self, database: str) -> str:
        statement = f"""
            SELECT '', 
                TABLE_NAME, 
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{database}';
        """
        return dedent(statement)

    def statement_table_exists(self, database: str, table: str, table_type: str, schema: Optional[str] = None) -> str:
        statement = f"""
            SELECT count(*) as Cnt
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{database}'
            AND TABLE_NAME = '{table}'
            AND TABLE_TYPE = '{table_type}';
        """
        return dedent(statement)

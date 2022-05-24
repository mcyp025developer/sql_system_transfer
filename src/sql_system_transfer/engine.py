# type: ignore
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, InitVar
from typing import List, Optional
from sys import modules
import pyodbc
from sql_system_transfer.system import SQLSystem, SQLSystemError
import sql_system_transfer.statements as sql_st
import sql_system_transfer.datatype as sql_dt


dbc = modules[__name__]


class SQLTableError(Exception):
    pass


class SQLDriverError(Exception):
    pass


@dataclass(kw_only=True)
class Engine:
    system: SQLSystem
    server: str
    database: str
    uid: Optional[str] = field(default=None)
    pwd: Optional[str] = field(default=None)
    trusted_connection: str = field(default=None)
    autocommit: bool = field(default=True)
    _system: SQLSystem = field(init=False, repr=False)
    _connection: pyodbc.Connection = field(init=False, repr=False)
    _statements: sql_st.Statements = field(init=False, repr=False)
    _database_object: Database = field(init=False, repr=False)

    def __post_init__(self) -> None:
        pyodbc.pooling = True
        object.__setattr__(self, "_connection", pyodbc.connect(**self._connection_dict()))
        object.__setattr__(self, "_statements", getattr(sql_st, f"{self.system.system_abbreviation()}Statements")())
        object.__setattr__(self, "_database_object", self._initialize_database())

    @property
    def system(self) -> SQLSystem:
        return self._system

    @system.setter
    def system(self, new_system: SQLSystem) -> None:
        if not isinstance(new_system, SQLSystem):
            raise SQLSystemError("Not a valid SQL System.")
        elif new_system.system_driver().replace("{", "").replace("}", "") not in pyodbc.drivers():
            raise SQLDriverError(f"{new_system} must have driver - {new_system.system_driver()} on your system.")
        else:
            self._system = new_system

    def engine_connection(self) -> pyodbc.Connection:
        return self._connection

    def engine_transfer_tables(self, tables: list, engine: Engine) -> None:
        old_cursor = self.engine_connection().cursor()
        new_cursor = engine.engine_connection().cursor()
        old_tables = self._database_object.database_table_convert_old(tables=tables)
        new_tables = self._database_object.database_table_convert_new(tables=tables, convert_to_system=engine.system)
        for old, new in zip(old_tables, new_tables):
            try:
                self._transfer_table(
                    engine=engine,
                    old_table=old,
                    new_table=new,
                    old_cursor=old_cursor,
                    new_cursor=new_cursor
                )
            except SQLTableError:
                print(f"{old.table} must have table type BASE TABLE")
        old_cursor.close()
        new_cursor.close()

    def _initialize_database(self) -> Database:
        database = Database(system=self.system, database=self.database)
        tables = self._table_information_schema()
        for table in tables:
            try:
                columns = self._column_information_schema(table=table.get("table"), schema=table.get("schema"))
                database.add_table_to_database(table=table, columns=columns)
            except sql_dt.SQLDatatypeError:
                print(f"{table} has a unsupported datatype")
        return database

    def _connection_dict(self) -> dict:
        return {
            'server': self.server,
            'driver': self.system.system_driver(),
            'database': self.database,
            'uid': self.uid,
            'pwd': self.pwd,
            'trusted_connection': self.trusted_connection,
            'autocommit': self.autocommit
        }

    def _transfer_table(self, engine: Engine, old_table: Table, new_table: Table, old_cursor: pyodbc.Cursor,
                        new_cursor: pyodbc.Cursor) -> None:
        if old_table.table_type != 'BASE TABLE':
            raise SQLTableError("table_type must be BASE TABLE")

        select_statement = old_table.statement_select_table(database=self.database)
        drop_statement = new_table.statement_drop_table(database=engine.database)
        create_statement = new_table.statement_create_table(database=engine.database)
        insert_statement = new_table.statement_insert_table(database=engine.database)
        old_cursor.execute(select_statement)
        new_cursor.execute(drop_statement)
        new_cursor.execute(create_statement)
        row = old_cursor.fetchone()
        while row:
            new_cursor.execute(insert_statement, *row)
            row = old_cursor.fetchone()

    def _table_information_schema(self) -> list:
        tables = []
        cursor = self.engine_connection().cursor()
        statement = self._statements.statement_information_schema_tables(database=self.database)
        cursor.execute(statement)
        row = cursor.fetchone()
        while row:
            tables.append({
                'schema': None if row[0] == '' else row[0],
                'table': row[1],
                'table_type': row[2],
            })
            row = cursor.fetchone()
        cursor.close()
        return tables

    def _column_information_schema(self, table: str, schema: Optional[str] = None) -> list[dict]:
        columns = []
        cursor = self.engine_connection().cursor()
        statement = self._statements.statement_information_schema_columns(
            database=self.database, table=table, schema=schema
        )
        cursor.execute(statement)
        row = cursor.fetchone()
        while row:
            columns.append({
                "column_name": row[0],
                "nullable": row[1],
                "datatype_name": row[2],
                "character_size": row[3],
                "character_set": row[4],
                "numeric_precision": row[5],
                "numeric_scale": row[6],
                "datetime_precision": row[7]
            })
            row = cursor.fetchone()
        cursor.close()
        return columns


@dataclass(kw_only=True, frozen=True)
class Database:
    system: SQLSystem
    database: str
    database_tables: list = field(init=False, repr=False, default_factory=list)

    def statement_use_database(self) -> str:
        return f"USE {self.database};"

    def database_table_names(self) -> list:
        return [table.table_format(database=self.database) for table in self.database_tables]

    def database_table_parameters(self) -> list:
        return [table.table_parameters() for table in self.database_tables]

    def database_table_convert_old(self, tables: list) -> List[Table]:
        new_tables = self._valid_database_tables(tables=tables)
        return [table for table in new_tables]

    def database_table_convert_new(self, tables: list, convert_to_system: SQLSystem) -> List[Table]:
        new_tables = self._valid_database_tables(tables=tables)
        return [table.table_convert(convert_to_system=convert_to_system) for table in new_tables]

    def add_table_to_database(self, table: dict, columns: list) -> None:
        table_object = getattr(dbc, f"{self.system.system_abbreviation()}Table")
        self.database_tables.append(table_object(**table, init_table_columns=columns))

    def _valid_database_tables(self, tables: list) -> List[Table]:
        return [
            table
            for table in self.database_tables
            if table.table_format(database=self.database) in tables
        ]


@dataclass(kw_only=True, frozen=True)
class Table(ABC):
    table: str
    table_type: str
    schema: Optional[str] = field(default=None)
    init_table_columns: InitVar[list] = field(default=list)
    table_columns: list = field(init=False, repr=False)
    _system: SQLSystem = field(init=False, repr=False)

    def __post_init__(self, init_table_columns: list) -> None:
        object.__setattr__(self, "table_columns", self._initialize_column_objects(init_table_columns))

    def table_parameters(self) -> dict:
        return {
            'table': self.table,
            'table_type': self.table_type,
            'schema': self.schema,
        }

    def table_convert(self, convert_to_system: SQLSystem, **kwargs) -> Table:
        table = kwargs.get("table")
        schema = kwargs.get("schema")
        columns = [column.column_convert(convert_to_system).column_parameters() for column in self.table_columns]
        return getattr(dbc, f"{convert_to_system.system_abbreviation()}Table")(
            table=self.table if table is None else table,
            table_type=self.table_type,
            schema=self.schema if schema is None else schema,
            init_table_columns=columns
        )

    @abstractmethod
    def table_format(self, database: str, alt_table_name: Optional[str] = None) -> str:
        pass

    def statement_create_table(self, database: str, alt_table_name: Optional[str] = None) -> str:
        columns = ',\n'.join([column.column_format() for column in self.table_columns])
        statement = f"CREATE TABLE {self.table_format(database, alt_table_name)} (\n\n{columns}\n\n);"
        return statement

    def statement_insert_table(self, database: str, alt_table_name: Optional[str] = None) -> str:
        q_marks = len(self.table_columns) * "?,"
        columns = ", ".join([column.column_name for column in self.table_columns])
        statement = f"INSERT INTO {self.table_format(database, alt_table_name)} ({columns}) VALUES ({q_marks[0:-1]});"
        return statement

    def statement_select_table(self, database: str, alt_table_name: Optional[str] = None) -> str:
        return f"SELECT * FROM {self.table_format(database, alt_table_name)};"

    def statement_drop_table(self, database: str, alt_table_name: Optional[str] = None) -> str:
        return f"DROP TABLE IF EXISTS {self.table_format(database, alt_table_name)};"

    def _initialize_column_objects(self, columns: list) -> List:
        column_objects = []
        for column in columns:
            column_object = Column(system=self._system, **column)
            column_objects.append(column_object)
        return column_objects


@dataclass(kw_only=True, frozen=True)
class MsSQLTable(Table):
    _system: SQLSystem = field(init=False, repr=False, default=SQLSystem.MSSQL)

    def __post_init__(self, init_table_columns: list) -> None:
        super().__post_init__(init_table_columns)
        object.__setattr__(self, "schema", "dbo" if self.schema is None else self.schema)

    def table_format(self, database: str, alt_table_name: Optional[str] = None) -> str:
        if alt_table_name is None:
            alt_table_name = self.table
        return f"{database}.{self.schema}.{alt_table_name}"


@dataclass(kw_only=True, frozen=True)
class MySQLTable(Table):
    _system: SQLSystem = field(init=False, repr=False, default=SQLSystem.MYSQL)

    def __post_init__(self, init_table_columns: list) -> None:
        super().__post_init__(init_table_columns)
        object.__setattr__(self, "schema", None)

    def table_format(self, database: str, alt_table_name: Optional[str] = None) -> str:
        if alt_table_name is None:
            alt_table_name = self.table
        return f"{database}.{alt_table_name}"


@dataclass(kw_only=True, frozen=True)
class Column:
    system: SQLSystem
    column_name: str
    datatype_name: InitVar[str]
    nullable: str = field(default="YES")
    character_size: InitVar[int] = field(default=None)
    character_set: InitVar[str] = field(default=None)
    numeric_precision: InitVar[int] = field(default=None)
    numeric_scale: InitVar[int] = field(default=None)
    datetime_precision: InitVar[int] = field(default=None)
    _datatype_object: sql_dt.Datatype = field(init=False, repr=False)

    def __post_init__(self, datatype_name: str, character_size: int, character_set: str, numeric_precision: int,
                      numeric_scale: int, datetime_precision: int) -> None:
        datatype = getattr(sql_dt, f"{self.system.system_abbreviation()}DatatypeFactory")()
        datatype = datatype.create_datatype(
            datatype_name, character_size, character_set, numeric_precision, numeric_scale, datetime_precision
        )
        object.__setattr__(self, "_datatype_object", datatype)

    def column_parameters(self) -> dict:
        return {
            "column_name": self.column_name,
            "datatype_name": self._datatype_object.datatype_name,
            **self._datatype_object.datatype_parameters()
        }

    def column_datatype(self) -> str:
        return self._datatype_object.datatype_name

    def column_format(self):
        datatype_format = self._datatype_object.datatype_format()
        return f"{self.column_name} {datatype_format} {'not null' if self.nullable == 'NO' else 'null'}"

    def column_convert(self, convert_to_system: SQLSystem) -> Column:
        datatype_convert = self._datatype_object.datatype_convert(convert_to_system)
        datatype_convert_parameters = datatype_convert.datatype_parameters()
        return Column(
            system=convert_to_system,
            column_name=self.column_name,
            nullable=self.nullable,
            **datatype_convert_parameters
        )

from enum import Enum, unique
from typing import Dict, ClassVar
from frozendict import frozendict  # type: ignore


class SQLSystemError(Exception):
    pass


@unique
class SQLSystem(Enum):
    MSSQL: ClassVar[Dict[str, str]] = frozendict({
        "id": "MsSQL",
        "name": "Microsoft SQL Server",
        "driver": "{ODBC Driver 17 for SQL Server}"
    })
    MYSQL: ClassVar[Dict[str, str]] = frozendict({
        "id": "MySQL",
        "name": "MySQL",
        "driver": "{MySQL ODBC 8.0 Unicode Driver}"
    })

    def system_abbreviation(self) -> str:
        id_value = self.value.get("id")
        return id_value  # type: ignore

    def system_abbreviation_lower(self) -> str:
        id_value = self.value.get("id")
        return id_value.lower()  # type: ignore

    def system_driver(self) -> str:
        driver = self.value.get("driver")
        return driver  # type: ignore

    def __str__(self) -> str:
        system_name = self.value.get("name")
        return f"{system_name}"

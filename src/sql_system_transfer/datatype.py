# type: ignore
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Literal, Optional
from sql_system_transfer.system import SQLSystem
from sql_system_transfer.datatype_dictionary import DatatypeDictionary
from sql_system_transfer.character_set_dictionary import CharacterSetDictionary


class SQLDatatypeError(Exception):
    pass


class DatatypeFactory(ABC):

    def __init__(self, system: SQLSystem) -> None:
        self._system = system

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def create_datatype(self, datatype_name: str,
                        character_size: Optional[int] = None,
                        character_set: Optional[str] = None,
                        numeric_precision: Optional[int] = None,
                        numeric_scale: Optional[int] = None,
                        datetime_precision: Optional[int] = None
                        ) -> Datatype:
        datatype = DatatypeDictionary(self._system).datatype_synonym_to_name(datatype_name)
        return self._datatype_factory(
            datatype, character_size, character_set, numeric_precision, numeric_scale, datetime_precision
        )

    @abstractmethod
    def _datatype_factory(self, datatype_name: str,
                          character_size: int,
                          character_set: str,
                          numeric_precision: int,
                          numeric_scale: int,
                          datetime_precision: int
                          ) -> Datatype:
        pass


class MsSQLDatatypeFactory(DatatypeFactory):

    def __init__(self) -> None:
        super().__init__(system=SQLSystem.MSSQL)

    def _datatype_factory(self, datatype_name: str,
                          character_size: int,
                          character_set: str,
                          numeric_precision: int,
                          numeric_scale: int,
                          datetime_precision: int
                          ) -> Datatype:
        if datatype_name == "varchar":
            return MsSQLVarchar("varchar", character_size)
        elif datatype_name == "nvarchar":
            return MsSQLVarchar("nvarchar", character_size)
        elif datatype_name == "text":
            return MsSQLText("text")
        elif datatype_name == "ntext":
            return MsSQLText("ntext")
        elif datatype_name == "char":
            return MsSQLChar("char", character_size)
        elif datatype_name == "nchar":
            return MsSQLChar("nchar", character_size)
        elif datatype_name == "varbinary":
            return MsSQLVarbinary(character_size)
        elif datatype_name == "binary":
            return MsSQLBinary(character_size)
        elif datatype_name == "numeric":
            return MsSQLNumeric("numeric", numeric_precision, numeric_scale)
        elif datatype_name == "decimal":
            return MsSQLNumeric("decimal", numeric_precision, numeric_scale)
        elif datatype_name == "float":
            return MsSQLFloat("float")
        elif datatype_name == "real":
            return MsSQLFloat("real")
        elif datatype_name == "bit":
            return MsSQLInteger("bit")
        elif datatype_name == "tinyint":
            return MsSQLInteger("tinyint")
        elif datatype_name == "smallint":
            return MsSQLInteger("smallint")
        elif datatype_name == "int":
            return MsSQLInteger("int")
        elif datatype_name == "bigint":
            return MsSQLInteger("bigint")
        elif datatype_name == "smallmoney":
            return MsSQLMoney("smallmoney")
        elif datatype_name == "money":
            return MsSQLMoney("money")
        elif datatype_name == "timestamp":
            return MsSQLTimestamp()
        elif datatype_name == "date":
            return MsSQLDatetimeOne("date")
        elif datatype_name == "datetime":
            return MsSQLDatetimeOne("datetime")
        elif datatype_name == "smalldatetime":
            return MsSQLDatetimeOne("smalldatetime")
        elif datatype_name == "datetime2":
            return MsSQLDatetimeTwo("datetime2", datetime_precision)
        elif datatype_name == "datetimeoffset":
            return MsSQLDatetimeTwo("datetimeoffset", datetime_precision)
        elif datatype_name == "time":
            return MsSQLDatetimeTwo("time", datetime_precision)
        elif datatype_name == "geography":
            return MsSQLOther("geography")
        elif datatype_name == "geometry":
            return MsSQLOther("geometry")
        elif datatype_name == "hierarchyid":
            return MsSQLOther("hierarchyid")
        elif datatype_name == "image":
            return MsSQLOther("image")
        elif datatype_name == "sql_variant":
            return MsSQLOther("sql_variant")
        elif datatype_name == "sysname":
            return MsSQLOther("sysname")
        elif datatype_name == "uniqueidentifier":
            return MsSQLOther("uniqueidentifier")
        elif datatype_name == "xml":
            return MsSQLOther("xml")
        else:
            raise SQLDatatypeError("Not a valid SQL Server Datatype")


class MySQLDatatypeFactory(DatatypeFactory):

    def __init__(self) -> None:
        super().__init__(system=SQLSystem.MYSQL)

    def _datatype_factory(self, datatype_name: str,
                          character_size: int,
                          character_set: str,
                          numeric_precision: int,
                          numeric_scale: int,
                          datetime_precision: int
                          ) -> Datatype:
        if datatype_name == "varchar":
            return MySQLVarchar(character_size, character_set)
        elif datatype_name == "nvarchar":
            return MySQLVarchar(character_size, "utf8mb4")
        elif datatype_name == "text":
            return MySQLText(character_set=character_set, character_size=character_size)
        elif datatype_name == "tinytext":
            return MySQLOtherText("tinytext", character_set)
        elif datatype_name == "mediumtext":
            return MySQLOtherText("mediumtext", character_set)
        elif datatype_name == "longtext":
            return MySQLOtherText("longtext", character_set)
        elif datatype_name == "enum":
            return MySQLVarchar(character_size, character_set)
        elif datatype_name == "set":
            return MySQLVarchar(character_size, character_set)
        elif datatype_name == "char":
            return MySQLChar(character_size, character_set)
        elif datatype_name == "nchar":
            return MySQLChar(character_size, "utf8mb4")
        elif datatype_name == "varbinary":
            return MySQLVarbinary(character_size)
        elif datatype_name == "binary":
            return MySQLBinary(character_size)
        elif datatype_name == "blob":
            return MySQLBlob(character_size)
        elif datatype_name == "tinyblob":
            return MySQLOtherBlob("tinyblob")
        elif datatype_name == "mediumblob":
            return MySQLOtherBlob("mediumblob")
        elif datatype_name == "longblob":
            return MySQLOtherBlob("longblob")
        elif datatype_name == "decimal":
            return MySQLDecimal(numeric_precision, numeric_scale)
        elif datatype_name == "float":
            return MySQLFloat("float")
        elif datatype_name == "double":
            return MySQLFloat("double")
        elif datatype_name == "bit":
            return MySQLBit(numeric_precision)
        elif datatype_name == "tinyint":
            return MySQLInteger("tinyint")
        elif datatype_name == "smallint":
            return MySQLInteger("smallint")
        elif datatype_name == "mediumint":
            return MySQLInteger("mediumint")
        elif datatype_name == "int":
            return MySQLInteger("int")
        elif datatype_name == "bigint":
            return MySQLInteger("bigint")
        elif datatype_name == "serial":
            return MySQLInteger("serial")
        elif datatype_name == "date":
            return MySQLDate()
        elif datatype_name == "datetime":
            return MySQLDatetime("datetime", datetime_precision)
        elif datatype_name == "time":
            return MySQLDatetime("time", datetime_precision)
        elif datatype_name == "timestamp":
            return MySQLDatetime("timestamp", datetime_precision)
        elif datatype_name == "year":
            return MySQLYear()
        else:
            raise SQLDatatypeError("Not a valid MySQL Datatype")


class Datatype(ABC):
    _datatype_name: str

    def __str__(self) -> str:
        return f"sql_dt.{self.__class__.__name__}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    @property
    def datatype_name(self) -> str:
        return self._datatype_name

    @abstractmethod
    def datatype_format(self) -> str:
        pass

    def datatype_parameters(self) -> dict:
        parameters = [
            '_datatype_name', '_character_size', '_character_set', '_numeric_precision', '_numeric_scale',
            '_datetime_precision'
        ]
        return {k.lstrip('_'): v for k, v in self.__dict__.items() if k in parameters}

    def _datatype_convert_to_mssql(self) -> Datatype:
        return self

    def _datatype_convert_to_mysql(self) -> Datatype:
        return self

    def datatype_convert(self, convert_to_system: SQLSystem) -> Datatype:
        sql_system = convert_to_system.system_abbreviation_lower()
        return getattr(self, f"_datatype_convert_to_{sql_system}")()


class MsSQLVarchar(Datatype):

    def __init__(self, datatype_name: Literal["varchar", "nvarchar"], character_size: int = 1) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self.character_size = character_size

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["varchar", "nvarchar"]) -> None:
        if new_datatype_name not in ["varchar", "nvarchar"]:
            self._datatype_name = "varchar"
        else:
            self._datatype_name = new_datatype_name

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_character_size: int) -> None:
        if not isinstance(new_character_size, int) or \
                new_character_size < -1 or new_character_size > 8000 or new_character_size == 0:
            self._character_size = -1
        else:
            self._character_size = new_character_size

    def datatype_format(self) -> str:
        if self.character_size == -1:
            return f"{self.datatype_name}(max)"
        else:
            return f"{self.datatype_name}({self.character_size})"

    def _datatype_convert_to_mysql(self) -> MySQLOtherText | MySQLVarchar:
        if self.datatype_name == "nvarchar":
            if self.character_size == -1:
                return MySQLOtherText(datatype_name="longtext", character_set="utf8mb4")
            else:
                return MySQLVarchar(character_size=self.character_size, character_set="utf8mb4")
        else:
            if self.character_size == -1:
                return MySQLOtherText(datatype_name="longtext")
            else:
                return MySQLVarchar(character_size=self.character_size)


class _MySQLCharacterSetMixin:
    _character_set_dictionary = None
    _character_set = None

    @property
    def character_set(self) -> str:
        return self._character_set

    @character_set.setter
    def character_set(self, new_character_set: str) -> None:
        if not isinstance(new_character_set, str) or new_character_set \
                not in self._character_set_dictionary.character_set_dictionary():
            self._character_set = "latin1"
        else:
            self._character_set = new_character_set


class MySQLVarchar(Datatype, _MySQLCharacterSetMixin):

    def __init__(self, character_size: int, character_set="latin1") -> None:
        super().__init__()
        self._datatype_name = "varchar"
        self._character_set_dictionary = CharacterSetDictionary(system=SQLSystem.MYSQL)
        self.character_set = character_set
        self.character_size = character_size

    @_MySQLCharacterSetMixin.character_set.setter
    def character_set(self, new_character_set: str) -> None:
        if not isinstance(new_character_set, str) or new_character_set \
                not in self._character_set_dictionary.character_set_dictionary():
            self._character_set = "latin1"
        else:
            self._character_set = new_character_set
            max_character_size = self._mysql_max_varchar_size()
            if hasattr(self, 'character_size'):
                if self.character_size > max_character_size:
                    self.character_size = max_character_size

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_size: int) -> None:
        max_character_size = self._mysql_max_varchar_size()
        if not isinstance(new_size, int) or new_size <= 0 or new_size > max_character_size:
            self._character_size = max_character_size
        else:
            self._character_size = new_size

    def datatype_format(self) -> str:
        character_set_format = self._character_set_dictionary.character_set_format(self.character_set)
        return f"{self.datatype_name}({self.character_size}) {character_set_format}"

    def _datatype_convert_to_mssql(self) -> MsSQLVarchar:
        charset_category = self._character_set_dictionary.character_set_category(character_set=self.character_set)
        if self.character_size > 8000:
            if charset_category == "unicode":
                return MsSQLVarchar(datatype_name="nvarchar", character_size=-1)
            else:
                return MsSQLVarchar(datatype_name="varchar", character_size=-1)
        else:
            if charset_category == "unicode":
                return MsSQLVarchar(datatype_name="nvarchar", character_size=self.character_size)
            else:
                return MsSQLVarchar(datatype_name="varchar", character_size=self.character_size)

    def _mysql_max_varchar_size(self) -> int:
        max_length = self._character_set_dictionary.character_set_max_length(character_set=self.character_set)
        if max_length == 1:
            return 65532
        elif max_length == 2:
            return 32766
        elif max_length == 3:
            return 21844
        else:
            return 16383


class MsSQLText(Datatype):

    def __init__(self, datatype_name: Literal["text", "ntext"]) -> None:
        super().__init__()
        self.datatype_name = datatype_name

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["text", "ntext"]) -> None:
        if new_datatype_name not in ["text", "ntext"]:
            self._datatype_name = "text"
        else:
            self._datatype_name = new_datatype_name

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mysql(self) -> MySQLOtherText:
        if self.datatype_name == "ntext":
            return MySQLOtherText(datatype_name="longtext", character_set="utf8mb4")
        else:
            return MySQLOtherText(datatype_name="longtext", character_set="latin1")


class MySQLText(Datatype, _MySQLCharacterSetMixin):

    def __init__(self, character_size: int = 65535, character_set="latin1") -> None:
        super().__init__()
        self._datatype_name = "text"
        self.character_size = character_size
        self._character_set_dictionary = CharacterSetDictionary(system=SQLSystem.MYSQL)
        self.character_set = character_set

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_character_size: int) -> None:
        if not isinstance(new_character_size, int) or new_character_size > 4294967295 or new_character_size < 0:
            self._character_size = 4294967295
        else:
            if new_character_size <= 255:
                self._character_size = 255
            elif new_character_size <= 65535:
                self._character_size = 65535
            elif new_character_size <= 16777215:
                self._character_size = 16777215
            else:
                self._character_size = 4294967295

    def datatype_format(self) -> str:
        character_set_format = self._character_set_dictionary.character_set_format(self.character_set)
        if self.character_size <= 255:
            return f"tinytext {character_set_format}"
        elif self.character_size <= 65535:
            return f"{self.datatype_name} {character_set_format}"
        elif self.character_size <= 16777215:
            return f"mediumtext {character_set_format}"
        else:
            return f"longtext {character_set_format}"

    def _datatype_convert_to_mssql(self) -> MsSQLVarchar:
        charset_category = self._character_set_dictionary.character_set_category(character_set=self.character_set)
        if charset_category == "unicode":
            if self.character_size <= 255:
                return MsSQLVarchar(datatype_name="nvarchar", character_size=255)
            else:
                return MsSQLVarchar(datatype_name="nvarchar", character_size=-1)
        else:
            if self.character_size <= 255:
                return MsSQLVarchar(datatype_name="varchar", character_size=255)
            else:
                return MsSQLVarchar(datatype_name="varchar", character_size=-1)


class MySQLOtherText(Datatype, _MySQLCharacterSetMixin):

    def __init__(self, datatype_name: Literal["tinytext", "mediumtext", "longtext"], character_set="latin1") -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self._character_set_dictionary = CharacterSetDictionary(system=SQLSystem.MYSQL)
        self.character_set = character_set

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["tinytext", "mediumtext", "longtext"]) -> None:
        if new_datatype_name not in ["tinytext", "mediumtext", "longtext"]:
            self._datatype_name = "longtext"
        else:
            self._datatype_name = new_datatype_name

    def datatype_format(self) -> str:
        character_set_format = self._character_set_dictionary.character_set_format(self.character_set)
        return f"{self.datatype_name} {character_set_format}"

    def _datatype_convert_to_mssql(self) -> MsSQLVarchar:
        charset_category = self._character_set_dictionary.character_set_category(character_set=self.character_set)
        if charset_category == "unicode":
            if self.datatype_name == "tinytext":
                return MsSQLVarchar(datatype_name="nvarchar", character_size=255)
            else:
                return MsSQLVarchar(datatype_name="nvarchar", character_size=-1)
        else:
            if self.datatype_name == "tinytext":
                return MsSQLVarchar(datatype_name="varchar", character_size=255)
            else:
                return MsSQLVarchar(datatype_name="varchar", character_size=-1)


class MsSQLChar(Datatype):

    def __init__(self, datatype_name: Literal["char", "nchar"], character_size: int = 1) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self.character_size = character_size

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["char", "nchar"]) -> None:
        if new_datatype_name not in ["char", "nchar"]:
            self._datatype_name = "char"
        else:
            self._datatype_name = new_datatype_name

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_size: int) -> None:
        if not isinstance(new_size, int) or new_size > 8000 or new_size <= 0:
            self._character_size = 8000
        else:
            self._character_size = new_size

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.character_size})"

    def _datatype_convert_to_mysql(self) -> MySQLVarchar | MySQLChar:
        if self.datatype_name == "nchar":
            if self.character_size <= 255:
                return MySQLChar(character_size=self.character_size, character_set="utf8mb4")
            else:
                return MySQLVarchar(character_size=self.character_size, character_set="utf8mb4")
        else:
            if self.character_size <= 255:
                return MySQLChar(character_size=self.character_size, character_set="latin1")
            else:
                return MySQLVarchar(character_size=self.character_size, character_set="latin1")


class MySQLChar(Datatype, _MySQLCharacterSetMixin):

    def __init__(self, character_size: int = 1, character_set: str = "latin1") -> None:
        super().__init__()
        self._datatype_name = "char"
        self._character_set_dictionary = CharacterSetDictionary(system=SQLSystem.MYSQL)
        self.character_set = character_set
        self.character_size = character_size

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_size: int) -> None:
        if not isinstance(new_size, int) or new_size > 255 or new_size <= 0:
            self._character_size = 255
        else:
            self._character_size = new_size

    def datatype_format(self) -> str:
        character_set_format = self._character_set_dictionary.character_set_format(self.character_set)
        return f"{self.datatype_name}({self.character_size}) {character_set_format}"

    def _datatype_convert_to_mssql(self) -> MsSQLChar:
        charset_category = self._character_set_dictionary.character_set_category(character_set=self.character_set)
        if charset_category == "unicode":
            return MsSQLChar(datatype_name="nchar", character_size=self.character_size)
        else:
            return MsSQLChar(datatype_name="char", character_size=self.character_size)


class MsSQLBinary(Datatype):

    def __init__(self, character_size: int = 1) -> None:
        super().__init__()
        self._datatype_name = "binary"
        self.character_size = character_size

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, character_size: int) -> None:
        if not isinstance(character_size, int) or character_size > 8000 or character_size <= 0:
            self._character_size = 8000
        else:
            self._character_size = character_size

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.character_size})"

    def _datatype_convert_to_mysql(self) -> MySQLBinary | MySQLBlob:
        if self.character_size <= 255:
            return MySQLBinary(character_size=self.character_size)
        else:
            return MySQLBlob()


class MySQLBinary(Datatype):

    def __init__(self, character_size: int = 1) -> None:
        super().__init__()
        self._datatype_name = "binary"
        self.character_size = character_size

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_character_size: int) -> None:
        if not isinstance(new_character_size, int) or new_character_size > 255 or new_character_size <= 0:
            self._character_size = 255
        else:
            self._character_size = new_character_size

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.character_size})"

    def _datatype_convert_to_mssql(self) -> MsSQLBinary:
        return MsSQLBinary(character_size=self.character_size)


class MsSQLVarbinary(Datatype):

    def __init__(self, character_size: int = 1) -> None:
        super().__init__()
        self._datatype_name = "varbinary"
        self.character_size = character_size

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_character_size: int) -> None:
        if not isinstance(new_character_size, int) or new_character_size < -1 \
                or new_character_size > 8000 or new_character_size == 0:
            self._character_size = -1
        else:
            self._character_size = new_character_size

    def datatype_format(self) -> str:
        if self.character_size == -1:
            return f"{self.datatype_name}(max)"
        else:
            return f"{self.datatype_name}({self.character_size})"

    def _datatype_convert_to_mysql(self) -> MySQLOtherBlob | MySQLVarbinary:
        if self.character_size == -1:
            return MySQLOtherBlob(datatype_name="longblob")
        else:
            return MySQLVarbinary(character_size=self.character_size)


class MySQLVarbinary(Datatype):

    def __init__(self, character_size: int) -> None:
        super().__init__()
        self._datatype_name = "varbinary"
        self.character_size = character_size

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_size: int) -> None:
        if not isinstance(new_size, int) or new_size < 0 or new_size > 65532:
            self._character_size = 65532
        else:
            self._character_size = new_size

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.character_size})"

    def _datatype_convert_to_mssql(self) -> MsSQLVarbinary:
        return MsSQLVarbinary(character_size=self.character_size)


class MySQLBlob(Datatype):

    def __init__(self, character_size: int = 65535) -> None:
        super().__init__()
        self._datatype_name = "blob"
        self.character_size = character_size

    @property
    def character_size(self) -> int:
        return self._character_size

    @character_size.setter
    def character_size(self, new_character_size: int) -> None:
        if not isinstance(new_character_size, int) or new_character_size > 4294967295 or new_character_size < 0:
            self._character_size = 4294967295
        else:
            if new_character_size <= 255:
                self._character_size = 255
            elif new_character_size <= 65535:
                self._character_size = 65535
            elif new_character_size <= 16777215:
                self._character_size = 16777215
            else:
                self._character_size = 4294967295

    def datatype_format(self) -> str:
        if self.character_size <= 255:
            return "tinyblob"
        elif self.character_size <= 65535:
            return f"{self.datatype_name}"
        elif self.character_size <= 16777215:
            return "mediumblob"
        else:
            return "longblob"

    def _datatype_convert_to_mssql(self) -> MsSQLVarbinary:
        return MsSQLVarbinary(character_size=-1)


class MySQLOtherBlob(Datatype):

    def __init__(self, datatype_name: Literal["tinyblob", "mediumblob", "longblob"]) -> None:
        super().__init__()
        self.datatype_name = datatype_name

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["blob", "tinyblob", "mediumblob", "longblob"]) -> None:
        if new_datatype_name not in ["tinyblob", "mediumblob", "longblob"]:
            self._datatype_name = "longblob"
        else:
            self._datatype_name = new_datatype_name

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mssql(self) -> MsSQLVarbinary:
        if self.datatype_name == "tinyblob":
            return MsSQLVarbinary(character_size=255)
        else:
            return MsSQLVarbinary(character_size=-1)


class MsSQLOther(Datatype):

    def __init__(self, datatype_name: Literal['geography', 'geometry', 'hierarchyid', 'image', 'sql_variant',
                                              'sysname', 'uniqueidentifier', 'xml']) -> None:
        super().__init__()
        self.datatype_name = datatype_name

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal['geography', 'geometry', 'hierarchyid', 'image', 'sql_variant',
                                                       'sysname', 'uniqueidentifier', 'xml']) -> None:
        dt = ['geography', 'geometry', 'hierarchyid', 'image', 'sql_variant', 'sysname', 'uniqueidentifier', 'xml']
        if new_datatype_name not in dt:
            raise SQLDatatypeError("datatype must be one of: " + ", ".join(dt))
        else:
            self._datatype_name = new_datatype_name

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mysql(self) -> MySQLBlob | MySQLText | MySQLVarchar | MySQLBinary:
        if self.datatype_name == "geography":
            return MySQLBlob()
        elif self.datatype_name == "geometry":
            return MySQLBlob()
        elif self.datatype_name == "hierarchyid":
            return MySQLBlob()
        elif self.datatype_name == "image":
            return MySQLBlob()
        elif self.datatype_name == "sql_variant":
            return MySQLText()
        elif self.datatype_name == "sysname":
            return MySQLVarchar(character_size=128, character_set="utf8mb4")
        elif self.datatype_name == "uniqueidentifier":
            return MySQLBinary(character_size=16)
        else:
            return MySQLText()


class MsSQLNumeric(Datatype):

    def __init__(
            self, datatype_name: Literal["decimal", "numeric"], numeric_precision: int = 18, numeric_scale: int = 0
    ) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["decimal", "numeric"]) -> None:
        if new_datatype_name not in ["decimal", "numeric"]:
            self._datatype_name = "numeric"
        else:
            self._datatype_name = new_datatype_name

    @property
    def numeric_precision(self) -> int:
        return self._numeric_precision

    @numeric_precision.setter
    def numeric_precision(self, new_precision: int) -> None:
        if not isinstance(new_precision, int) or new_precision > 38 or new_precision <= 0:
            self._numeric_precision = 38
        else:
            self._numeric_precision = new_precision

    @property
    def numeric_scale(self) -> int:
        return self._numeric_scale

    @numeric_scale.setter
    def numeric_scale(self, new_scale: int) -> None:
        if not isinstance(new_scale, int) or new_scale > self.numeric_precision or new_scale < 0:
            self._numeric_scale = self.numeric_precision
        else:
            self._numeric_scale = new_scale

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.numeric_precision}, {self.numeric_scale})"

    def _datatype_convert_to_mysql(self) -> MySQLDecimal:
        return MySQLDecimal(numeric_precision=self.numeric_precision, numeric_scale=self.numeric_scale)


class MySQLDecimal(Datatype):

    def __init__(self, numeric_precision: int = 10, numeric_scale: int = 0) -> None:
        super().__init__()
        self._datatype_name = "decimal"
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale

    @property
    def numeric_precision(self) -> int:
        return self._numeric_precision

    @numeric_precision.setter
    def numeric_precision(self, new_precision: int) -> None:
        if not isinstance(new_precision, int) or new_precision > 65 or new_precision <= 0:
            self._numeric_precision = 65
        else:
            self._numeric_precision = new_precision

    @property
    def numeric_scale(self) -> int:
        return self._numeric_scale

    @numeric_scale.setter
    def numeric_scale(self, new_scale: int) -> None:
        if not isinstance(new_scale, int) or new_scale > self.numeric_precision or new_scale < 0:
            if self.numeric_precision < 30:
                self._numeric_scale = self.numeric_precision
            else:
                self._numeric_scale = 30
        else:
            if new_scale < 30:
                self._numeric_scale = new_scale
            elif new_scale > 30:
                if self.numeric_precision < 30:
                    self._numeric_scale = self.numeric_precision
                else:
                    self._numeric_scale = 30
            else:
                self._numeric_scale = new_scale

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.numeric_precision}, {self.numeric_scale})"

    def _datatype_convert_to_mssql(self) -> MsSQLNumeric:
        if self.numeric_precision > 38:
            return MsSQLNumeric(datatype_name="numeric", numeric_precision=38, numeric_scale=self.numeric_scale)
        else:
            return MsSQLNumeric(
                datatype_name="numeric",
                numeric_precision=self.numeric_precision,
                numeric_scale=self.numeric_scale
            )


class MsSQLFloat(Datatype):

    def __init__(self, datatype_name: Literal["real", "float"]) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self._numeric_precision = self.__numeric_precision()

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["real", "float"]) -> None:
        if new_datatype_name not in ["real", "float"]:
            self._datatype_name = "float"
            self._numeric_precision = 53
        else:
            self._datatype_name = new_datatype_name
            self._numeric_precision = self.__numeric_precision()

    @property
    def numeric_precision(self) -> int:
        return self._numeric_precision

    def __numeric_precision(self):
        if self.datatype_name == "float":
            return 53
        else:
            return 24

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mysql(self) -> MySQLFloat:
        if self.datatype_name == "real":
            return MySQLFloat(datatype_name="double")
        else:
            return MySQLFloat(datatype_name="float")


class MySQLFloat(Datatype):

    def __init__(self, datatype_name: Literal["double", "float"]) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self._numeric_precision = self.__numeric_precision()

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["double", "float"]) -> None:
        if new_datatype_name not in ["double", "float"]:
            self._datatype_name = "double"
            self._numeric_precision = 22
        else:
            self._datatype_name = new_datatype_name
            self._numeric_precision = self.__numeric_precision()

    @property
    def numeric_precision(self) -> int:
        return self._numeric_precision

    def __numeric_precision(self):
        if self.datatype_name == "float":
            return 12
        else:
            return 22

    def datatype_format(self) -> str:
        if self.datatype_name == "float":
            return f"{self.datatype_name}({self.numeric_precision})"
        else:
            return f"{self.datatype_name}"

    def _datatype_convert_to_mssql(self) -> MsSQLFloat:
        if self.datatype_name == "double":
            return MsSQLFloat(datatype_name="float")
        else:
            return MsSQLFloat(datatype_name="real")


class MsSQLInteger(Datatype):

    def __init__(self, datatype_name: Literal["bit", "tinyint", "smallint", "int", "bigint"]) -> None:
        super().__init__()
        self.datatype_name = datatype_name

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["bit", "tinyint", "smallint", "int", "bigint"]) -> None:
        if new_datatype_name not in ["bit", "tinyint", "smallint", "int", "bigint"]:
            self._datatype_name = "bigint"
        else:
            self._datatype_name = new_datatype_name

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mysql(self) -> MySQLInteger:
        if self.datatype_name == "bit":
            return MySQLInteger(datatype_name="tinyint")
        elif self.datatype_name == "int":
            return MySQLInteger(datatype_name="int")
        elif self.datatype_name in ["tinyint", "smallint"]:
            return MySQLInteger(datatype_name="smallint")
        else:
            return MySQLInteger(datatype_name="bigint")


class MySQLInteger(Datatype):

    def __init__(
            self, datatype_name: Literal["int", "tinyint", "smallint", "mediumint", "bigint", "serial"],
            signed_unsigned: str = "signed"
    ) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self.signed_unsigned = signed_unsigned

    @Datatype.datatype_name.setter
    def datatype_name(
            self, new_datatype_name: Literal["int", "tinyint", "smallint", "mediumint", "bigint", "serial"]
    ) -> None:
        if new_datatype_name not in ["int", "tinyint", "smallint", "mediumint", "bigint", "serial"]:
            self._datatype_name = "bigint"
        else:
            self._datatype_name = new_datatype_name

    @property
    def signed_unsigned(self) -> str:
        return self._signed_unsigned

    @signed_unsigned.setter
    def signed_unsigned(self, new_signed_unsigned: Literal["signed", "unsigned"]) -> None:
        if new_signed_unsigned not in ["signed", "unsigned"]:
            self._signed_unsigned = "signed"
        else:
            self._signed_unsigned = new_signed_unsigned

    def datatype_format(self) -> str:
        if self.datatype_name == "serial":
            return "bigint unsigned"
        else:
            if self.signed_unsigned == 'unsigned':
                return f"{self.datatype_name} {self.signed_unsigned}"
            else:
                return f"{self.datatype_name}"

    def _datatype_convert_to_mssql(self) -> MsSQLInteger | MsSQLNumeric:
        if self.datatype_name == "int":
            return MsSQLInteger(datatype_name="bigint")
        elif self.datatype_name == "tinyint":
            return MsSQLInteger(datatype_name="smallint")
        elif self.datatype_name in ["smallint", "mediumint"]:
            return MsSQLInteger(datatype_name="int")
        else:
            return MsSQLNumeric(datatype_name="numeric", numeric_precision=20, numeric_scale=0)


class MySQLBit(Datatype):

    def __init__(self, numeric_precision: int = 1) -> None:
        super().__init__()
        self._datatype_name = "bit"
        self.numeric_precision = numeric_precision

    @property
    def numeric_precision(self) -> int:
        return self._numeric_precision

    @numeric_precision.setter
    def numeric_precision(self, new_precision: int) -> None:
        if not isinstance(new_precision, int) or new_precision > 64 or new_precision <= 0:
            self._numeric_precision = 64
        else:
            self._numeric_precision = new_precision

    def datatype_format(self) -> str:
        if self.numeric_precision == 1:
            return f"{self.datatype_name}"
        else:
            return f"{self.datatype_name}({self.numeric_precision})"

    def _datatype_convert_to_mssql(self) -> MsSQLNumeric:
        return MsSQLNumeric(datatype_name="numeric", numeric_precision=20, numeric_scale=0)


class MsSQLMoney(Datatype):

    def __init__(self, datatype_name: Literal["money", "smallmoney"]) -> None:
        super().__init__()
        self.datatype_name = datatype_name

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["money", "smallmoney"]) -> None:
        if new_datatype_name not in ["money", "smallmoney"]:
            self._datatype_name = "money"
        else:
            self._datatype_name = new_datatype_name

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mysql(self) -> MySQLDecimal:
        if self.datatype_name == "smallmoney":
            return MySQLDecimal(numeric_precision=10, numeric_scale=4)
        else:
            return MySQLDecimal(numeric_precision=19, numeric_scale=4)


class MsSQLDatetimeOne(Datatype):

    def __init__(self, datatype_name: Literal["date", "datetime", "smalldatetime"]) -> None:
        super().__init__()
        self.datatype_name = datatype_name

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["date", "datetime", "smalldatetime"]) -> None:
        if new_datatype_name not in ["date", "datetime", "smalldatetime"]:
            self._datatype_name = "date"
        else:
            self._datatype_name = new_datatype_name

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mysql(self) -> MySQLDate | MySQLDatetime:
        if self.datatype_name == "date":
            return MySQLDate()
        else:
            return MySQLDatetime(datatype_name="datetime")


class MsSQLDatetimeTwo(Datatype):

    def __init__(self, datatype_name: Literal["datetime2", "datetimeoffset", "time"], datetime_precision: int) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self.datetime_precision = datetime_precision

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["datetime2", "datetimeoffset", "time"]) -> None:
        if new_datatype_name not in ["datetime2", "datetimeoffset", "time"]:
            self._datatype_name = "datetime2"
        else:
            self._datatype_name = new_datatype_name

    @property
    def datetime_precision(self) -> int:
        return self._datetime_precision

    @datetime_precision.setter
    def datetime_precision(self, new_datetime_precision: int) -> None:
        if not isinstance(new_datetime_precision, int) or new_datetime_precision < 0 or new_datetime_precision > 7:
            self._datetime_precision = 7
        else:
            self._datetime_precision = new_datetime_precision

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.datetime_precision})"

    def _datatype_convert_to_mysql(self) -> MySQLDatetime:
        if self.datatype_name == "time":
            return MySQLDatetime(datatype_name="time")
        else:
            return MySQLDatetime(datatype_name="datetime")


class MsSQLTimestamp(Datatype):

    def __init__(self) -> None:
        super().__init__()
        self._datatype_name = "timestamp"

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mysql(self) -> MySQLInteger:
        return MySQLInteger(datatype_name="bigint")


class MySQLDate(Datatype):

    def __init__(self) -> None:
        super().__init__()
        self._datatype_name = "date"

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mssql(self) -> MsSQLDatetimeOne:
        return MsSQLDatetimeOne(datatype_name="date")


class MySQLDatetime(Datatype):

    def __init__(self, datatype_name: Literal["timestamp", "datetime", "time"], datetime_precision: int = 0) -> None:
        super().__init__()
        self.datatype_name = datatype_name
        self.datetime_precision = datetime_precision

    @Datatype.datatype_name.setter
    def datatype_name(self, new_datatype_name: Literal["timestamp", "datetime", "time"]) -> None:
        if new_datatype_name not in ["timestamp", "datetime", "time"]:
            self._datatype_name = "datetime"
        else:
            self._datatype_name = new_datatype_name

    @property
    def datetime_precision(self) -> int:
        return self._datetime_precision

    @datetime_precision.setter
    def datetime_precision(self, new_datetime_precision: int) -> None:
        if not isinstance(new_datetime_precision, int) or new_datetime_precision < 0 or new_datetime_precision > 6:
            self._datetime_precision = 6
        else:
            self._datetime_precision = new_datetime_precision

    def datatype_format(self) -> str:
        return f"{self.datatype_name}({self.datetime_precision})"

    def _datatype_convert_to_mssql(self) -> MsSQLDatetimeTwo:
        if self.datatype_name == "time":
            return MsSQLDatetimeTwo(datatype_name="time", datetime_precision=self.datetime_precision)
        else:
            return MsSQLDatetimeTwo(datatype_name="datetime2", datetime_precision=self.datetime_precision)


class MySQLYear(Datatype):

    def __init__(self) -> None:
        super().__init__()
        self._datatype_name = "year"

    def datatype_format(self) -> str:
        return f"{self.datatype_name}"

    def _datatype_convert_to_mssql(self) -> MsSQLInteger:
        return MsSQLInteger(datatype_name="int")

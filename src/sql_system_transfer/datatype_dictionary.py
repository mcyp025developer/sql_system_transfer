from sql_system_transfer.system import SQLSystem, SQLSystemError
from dataclasses import dataclass, field
from typing import Dict, Union, List, Optional
from sys import modules
from frozendict import frozendict  # type: ignore


sql_dd = modules[__name__]


@dataclass
class DatatypeDictionary:
    system: SQLSystem
    _datatype_dictionary: Dict[str, Dict[str, Union[List[str], str]]] = field(init=False, repr=False)
    _system: SQLSystem = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._set_datatype_dictionary(self.system)

    def _set_datatype_dictionary(self, system: SQLSystem) -> None:
        object.__setattr__(
            self, "_datatype_dictionary", getattr(sql_dd, f"_{system.system_abbreviation_lower()}_datatype_dictionary")
        )

    @property  # type: ignore
    def system(self) -> SQLSystem:
        return self._system

    @system.setter
    def system(self, new_system: SQLSystem) -> None:
        if not isinstance(new_system, SQLSystem):
            raise SQLSystemError("Not a valid SQL System")
        else:
            self._system = new_system
            self._set_datatype_dictionary(new_system)

    def datatype_dictionary(self) -> Dict[str, Dict[str, Union[str, List[str]]]]:
        return self._datatype_dictionary

    def datatype_metadata(self, datatype_name: str) -> Dict[str, List[str] | str]:
        datatype = self.datatype_synonym_to_name(synonym=datatype_name)
        if datatype is not None:
            return self._datatype_dictionary[datatype]
        else:
            return {}

    def datatype_synonym(self, datatype_name: str) -> List[str]:
        datatype = self.datatype_synonym_to_name(synonym=datatype_name)
        if datatype is not None:
            meta_data = self.datatype_metadata(datatype)
            return meta_data["synonyms"]  # type: ignore
        else:
            return []

    def datatype_category(self, datatype_name: str) -> Optional[str]:
        datatype = self.datatype_synonym_to_name(synonym=datatype_name)
        if datatype is not None:
            meta_data = self.datatype_metadata(datatype)
            return meta_data["category"]  # type: ignore
        else:
            return ""

    def datatype_synonym_to_name(self, synonym: str) -> Optional[str]:
        datatype_name = self._all_possible_datatype_names_and_keys()
        if not isinstance(synonym, str) or synonym.lower() not in datatype_name:
            return None
        return datatype_name[synonym.lower()]

    def _all_possible_datatype_names_and_keys(self) -> Dict[str, str]:
        complete_synonym_dict: Dict[str, str] = {}
        for x, y in self._datatype_dictionary.items():
            for z in y.get("synonyms"):   # type: ignore
                complete_synonym_dict[z] = x
        return complete_synonym_dict


_mssql_datatype_dictionary: Dict[str, Dict[str, Union[List[str], str]]] = frozendict({
    'varchar': {
        'synonyms': ['character varying', 'char varying', 'varchar'],
        'category': "CharacterString",
    },
    'nvarchar': {
        'synonyms': ['nvarchar', 'national character varying', 'national char varying'],
        'category': "CharacterString",
    },
    'text': {
        'synonyms': ['text'],
        'category': "CharacterString",
    },
    'ntext': {
        'synonyms': ['national text', 'ntext'],
        'category': "CharacterString",
    },
    'char': {
        'synonyms': ['character', 'char'],
        'category': 'CharacterString',
    },
    'nchar': {
        'synonyms': ['national character', 'national char', 'nchar'],
        'category': 'CharacterString',
    },
    'numeric': {
        'synonyms': ['numeric'],
        'category': "Numeric",
    },
    'decimal': {
        'synonyms': ['dec', 'decimal'],
        'category': "Numeric",
    },
    'float': {
        'synonyms': ['float', 'double precision'],
        'category': "Numeric",
    },
    'real': {
        'synonyms': ['real'],
        'category': "Numeric",
    },
    "bit": {
        'synonyms': ['bit'],
        'category': "Numeric",
    },
    'tinyint': {
        'synonyms': ['tinyint'],
        'category': "Numeric",
    },
    'smallint': {
        'synonyms': ['smallint'],
        'category': "Numeric",
    },
    'int': {
        'synonyms': ['int', 'integer'],
        'category': "Numeric",
    },
    'bigint': {
        'synonyms': ['bigint'],
        'category': "Numeric",
    },
    'smallmoney': {
        'synonyms': ['smallmoney'],
        'category': "Numeric",
    },
    'money': {
        'synonyms': ['money'],
        'category': "Numeric",
    },
    'varbinary': {
        'synonyms': ['varbinary', 'binary varying'],
        'category': "Binary",
    },
    'binary': {
        'synonyms': ['binary'],
        'category': "Binary",
    },
    'geography': {
        'synonyms': ['geography'],
        'category': "Other",
    },
    'geometry': {
        'synonyms': ['geometry'],
        'category': "Other",
    },
    'hierarchyid': {
        'synonyms': ['hierarchyid'],
        'category': "Other",
    },
    'image': {
        'synonyms': ['image'],
        'category': "Other",
    },
    'sql_variant': {
        'synonyms': ['sql_variant'],
        'category': "Other",
    },
    'sysname': {
        'synonyms': ['sysname'],
        'category': "Other",
    },
    'uniqueidentifier': {
        'synonyms': ['uniqueidentifier'],
        'category': "Other",
    },
    'xml': {
        'synonyms': ['xml'],
        'category': "Other",
    },
    'timestamp': {
        'synonyms': ['timestamp', 'rowversion'],
        'category': "Datetime",
    },
    'date': {
        'synonyms': ['date'],
        'category': "Datetime",
    },
    'datetime': {
        'synonyms': ['datetime'],
        'category': "Datetime",
    },
    'datetime2': {
        'synonyms': ['datetime2'],
        'category': "Datetime",
    },
    'datetimeoffset': {
        'synonyms': ['datetimeoffset'],
        'category': "Datetime",
    },
    'smalldatetime': {
        'synonyms': ['smalldatetime'],
        'category': "Datetime",
    },
    'time': {
        'synonyms': ['time'],
        'category': "Datetime",
    },
})

_mysql_datatype_dictionary: Dict[str, Dict[str, Union[List[str], str]]] = frozendict({
    'varchar': {
        'synonyms': ['character varying', 'char varying', 'varchar'],
        'category': "CharacterString",
    },
    'nvarchar': {
        'synonyms': ['nvarchar', 'national character varying', 'national char varying'],
        'category': "CharacterString",
    },
    'text': {
        'synonyms': ["text"],
        'category': "CharacterString",
    },
    'tinytext': {
        'synonyms': ["tinytext"],
        'category': "CharacterString",
    },
    'mediumtext': {
        'synonyms': ["mediumtext", "long", "long varchar"],
        'category': "CharacterString",
    },
    'longtext': {
        'synonyms': ["longtext"],
        'category': "CharacterString",
    },
    'char': {
        'synonyms': ["character", "char"],
        'category': 'CharacterString',
    },
    'nchar': {
        'synonyms': ["national character", "national char", "nchar"],
        'category': 'CharacterString',
    },
    'set': {
        'synonyms': ['set'],
        'category': "CharacterString",
    },
    'enum': {
        'synonyms': ['enum'],
        'category': "CharacterString",
    },
    'decimal': {
        'synonyms': ['numeric', 'dec', 'decimal', 'fixed'],
        'category': "Numeric",
    },
    'float': {
        'synonyms': ['float', 'float4'],
        'category': 'Numeric',
    },
    'double': {
        'synonyms': ['float8', 'double', 'double precision', 'real'],
        'category': 'Numeric',
    },
    'bit': {
        'synonyms': ["bit"],
        'category': "Numeric",
    },
    'tinyint': {
        'synonyms': ['int1', 'tinyint', "bool", "boolean"],
        'category': "Numeric",
    },
    'smallint': {
        'synonyms': ['int2', 'smallint'],
        'category': "Numeric",
    },
    'mediumint': {
        'synonyms': ['int3', 'mediumint', 'middleint'],
        'category': "Numeric",
    },
    'int': {
        'synonyms': ['int4', 'integer', 'int'],
        'category': "Numeric",
    },
    'bigint': {
        'synonyms': ['int8', 'bigint'],
        'category': "Numeric",
    },
    'serial': {
        'synonyms': ['serial'],
        'category': "Numeric",
    },
    'varbinary': {
        'synonyms': ['varbinary'],
        'category': "Binary",
    },
    'binary': {
        'synonyms': ['binary'],
        'category': "Binary",
    },
    'blob': {
        'synonyms': ['blob'],
        'category': "Binary",
    },
    'tinyblob': {
        'synonyms': ['tinyblob'],
        'category': "Binary",
    },
    'mediumblob': {
        'synonyms': ['mediumblob', 'long varbinary'],
        'category': "Binary",
    },
    'longblob': {
        'synonyms': ['longblob'],
        'category': "Binary",
    },
    'date': {
        'synonyms': ['date'],
        'category': "Datetime",
    },
    'datetime': {
        'synonyms': ['datetime'],
        'category': "Datetime",
    },
    'time': {
        'synonyms': ['time'],
        'category': "Datetime",
    },
    'timestamp': {
        'synonyms': ['timestamp'],
        'category': "Datetime",
    },
    'year': {
        'synonyms': ['year'],
        'category': "Datetime",
    },
})

from sql_system_transfer.system import SQLSystem, SQLSystemError
from dataclasses import dataclass, field
from sys import modules
from typing import Dict
from frozendict import frozendict  # type: ignore


sql_cs = modules[__name__]


@dataclass
class CharacterSetDictionary:
    system: SQLSystem
    _character_set_dictionary: Dict[str, Dict[str, str | int]] = field(init=False, repr=False)
    __system: SQLSystem = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.__set_character_set_dictionary(self.system)

    def __set_character_set_dictionary(self, system: SQLSystem) -> None:
        object.__setattr__(
            self,
            "_character_set_dictionary",
            getattr(sql_cs, f"_{system.system_abbreviation_lower()}_character_set_dictionary")
        )

    @property  # type: ignore
    def system(self) -> SQLSystem:
        return self.__system

    @system.setter
    def system(self, new_system: SQLSystem) -> None:
        if not isinstance(new_system, SQLSystem):
            raise SQLSystemError("Not a valid SQL System")
        else:
            self.__system = new_system
            self.__set_character_set_dictionary(new_system)

    def character_set_dictionary(self) -> Dict[str, Dict[str, str | int]]:
        return self._character_set_dictionary

    def character_set_format(self, character_set: str) -> str:
        if character_set not in self.character_set_dictionary():
            return ""
        return f"character set {character_set}"

    def character_set_category(self, character_set: str) -> str:
        if character_set not in self.character_set_dictionary():
            return ""
        return self.character_set_dictionary().get(character_set).get("category")  # type: ignore

    def character_set_max_length(self, character_set: str) -> int:
        if character_set not in self.character_set_dictionary():
            return 0
        return self.character_set_dictionary().get(character_set).get("max_length")  # type: ignore


_mssql_character_set_dictionary: dict = frozendict({})

_mysql_character_set_dictionary: Dict[str, Dict[str, str | int]] = frozendict({
        "armscii8": {
            "description": "ARMSCII-8 Armenian",
            "max_length": 1,
            "category": "character"
        },
        "ascii": {
            "description": "US ASCII",
            "max_length": 1,
            "category": "character"
        },
        "big5": {
            "description": "Big5 Traditional Chinese",
            "max_length": 2,
            "category": "unicode"
        },
        "binary": {
            "description": "Binary pseudo charset",
            "max_length": 1,
            "category": "character"
        },
        "cp1250": {
            "description": "Windows Central European",
            "max_length": 1,
            "category": "character"
        },
        "cp1251": {
            "description": "Windows Cyrillic",
            "max_length": 1,
            "category": "character"
        },
        "cp1256": {
            "description": "Windows Arabic",
            "max_length": 1,
            "category": "character"
        },
        "cp1257": {
            "description": "Windows Baltic",
            "max_length": 1,
            "category": "character"
        },
        "cp850": {
            "description": "DOS West European",
            "max_length": 1,
            "category": "character"
        },
        "cp852": {
            "description": "DOS Central European",
            "max_length": 1,
            "category": "character"
        },
        "cp866": {
            "description": "DOS Russian",
            "max_length": 1,
            "category": "character"
        },
        "cp932": {
            "description": "SJIS for Windows Japanese",
            "max_length": 2,
            "category": "unicode"
        },
        "dec8": {
            "description": "DEC West European",
            "max_length": 1,
            "category": "character"
        },
        "eucjpms": {
            "description": "UJIS for Windows Japanese",
            "max_length": 3,
            "category": "unicode"
        },
        "euckr": {
            "description": "EUC-KR Korean",
            "max_length": 2,
            "category": "unicode"
        },
        "gb18030": {
            "description": "China National Standard GB18030",
            "max_length": 4,
            "category": "character"
        },
        "gb2312": {
            "description": "GB2312 Simplified Chinese",
            "max_length": 2,
            "category": "unicode"
        },
        "gbk": {
            "description": "GBK Simplified Chinese",
            "max_length": 2,
            "category": "unicode"
        },
        "geostd8": {
            "description": "GEOSTD8 Georgian",
            "max_length": 1,
            "category": "character"
        },
        "greek": {
            "description": "ISO 8859-7 Greek",
            "max_length": 1,
            "category": "character"
        },
        "hebrew": {
            "description": "ISO 8859-8 Hebrew",
            "max_length": 1,
            "category": "character"
        },
        "hp8": {
            "description": "HP West European",
            "max_length": 1,
            "category": "character"
        },
        "keybcs2": {
            "description": "DOS Kamenicky Czech-Slovak",
            "max_length": 1,
            "category": "character"
        },
        "koi8r": {
            "description": "KOI8-R Relcom Russian",
            "max_length": 1,
            "category": "character"
        },
        "koi8u": {
            "description": "KOI8-U Ukrainian",
            "max_length": 1,
            "category": "character"
        },
        "latin1": {
            "description": "cp1252 West European",
            "max_length": 1,
            "category": "character"
        },
        "latin2": {
            "description": "ISO 8859-2 Central European",
            "max_length": 1,
            "category": "character"
        },
        "latin5": {
            "description": "ISO 8859-9 Turkish",
            "max_length": 1,
            "category": "character"
        },
        "latin7": {
            "description": "ISO 8859-13 Baltic",
            "max_length": 1,
            "category": "character"
        },
        "macce": {
            "description": "Mac Central European",
            "max_length": 1,
            "category": "character"
        },
        "macroman": {
            "description": "Mac West EuropeanMac West European",
            "max_length": 1,
            "category": "character"
        },
        "sjis": {
            "description": "Shift-JIS Japanese",
            "max_length": 2,
            "category": "unicode"
        },
        "swe7": {
            "description": "7bit Swedish",
            "max_length": 1,
            "category": "character"
        },
        "tis620": {
            "description": "TIS620 Thai",
            "max_length": 1,
            "category": "character"
        },
        "ucs2": {
            "description": "UCS-2 Unicode",
            "max_length": 2,
            "category": "unicode"
        },
        "ujis": {
            "description": "EUC-JP Japanese",
            "max_length": 3,
            "category": "unicode"
        },
        "utf16": {
            "description": "UTF-16 Unicode",
            "max_length": 4,
            "category": "character"
        },
        "utf16le": {
            "description": "UTF-16LE Unicode",
            "max_length": 4,
            "category": "character"
        },
        "utf32": {
            "description": "UTF-32 Unicode",
            "max_length": 4,
            "category": "character"
        },
        "utf8": {
            "description": "UTF-8 Unicode",
            "max_length": 3,
            "category": "unicode"
        },
        "utf8mb4": {
            "description": "UTF-8 Unicode",
            "max_length": 4,
            "category": "unicode"
        },
    })

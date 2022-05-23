import pytest
from sql_system_transfer.character_set_dictionary import CharacterSetDictionary, SQLSystem, SQLSystemError
from frozendict import frozendict

system_parameters = [SQLSystem.MSSQL, SQLSystem.MYSQL, ]
pytest_parameters_sql_system = pytest.mark.parametrize("system", system_parameters)

@pytest_parameters_sql_system
def test_character_set_dictionary_property_get_system(system):
    character_set_dictionary = CharacterSetDictionary(system=system)
    system = character_set_dictionary.system
    assert isinstance(system, SQLSystem)

@pytest_parameters_sql_system
def test_character_set_dictionary_property_set_system(system):
    character_set_dictionary = CharacterSetDictionary(system=system)
    character_set_dictionary.system = system
    assert isinstance(system, SQLSystem)

@pytest_parameters_sql_system
def test_character_set_dictionary_property_set_system_raise_exception(system):
    character_set_dictionary = CharacterSetDictionary(system=system)
    with pytest.raises(SQLSystemError):
        character_set_dictionary.system = "invalid"

@pytest_parameters_sql_system
def test_character_set_dictionary(system):
    character_set = CharacterSetDictionary(system=system)
    character_set = character_set.character_set_dictionary()
    assert isinstance(character_set, frozendict)

class TestMySQLCharacterSetDictionary:

    _mysql_character_set = [
        "armscii8",
        "ascii",
        "big5",
        "binary",
        "cp1250",
        "cp1251",
        "cp1256",
        "cp1257",
        "cp850",
        "cp852",
        "cp866",
        "cp932",
        "dec8",
        "eucjpms",
        "euckr",
        "gb18030",
        "gb2312",
        "gbk",
        "geostd8",
        "greek",
        "hebrew",
        "hp8",
        "keybcs2",
        "koi8r",
        "koi8u",
        "latin1",
        "latin2",
        "latin5",
        "latin7",
        "macce",
        "macroman",
        "sjis",
        "swe7",
        "tis620",
        "ucs2",
        "ujis",
        "utf16",
        "utf16le",
        "utf32",
        "utf8",
        "utf8mb4"
    ]

    _pytest_mysql_parameters = pytest.mark.parametrize("character_set", _mysql_character_set)

    @pytest.fixture()
    def character_set_dictionary(self):
        character_set_dictionary = CharacterSetDictionary(system=SQLSystem.MSSQL)
        return character_set_dictionary

    @_pytest_mysql_parameters
    def test_character_set_format(self, character_set_dictionary, character_set):
        character_set_format = character_set_dictionary.character_set_format(character_set=character_set)
        assert isinstance(character_set_format, str)

    @_pytest_mysql_parameters
    def test_character_set_category(self, character_set_dictionary, character_set):
        character_set_category = character_set_dictionary.character_set_category(character_set=character_set)
        assert isinstance(character_set_category, str)

    @_pytest_mysql_parameters
    def test_character_set_max_length(self, character_set_dictionary, character_set):
        character_set_max_length = character_set_dictionary.character_set_max_length(character_set=character_set)
        assert isinstance(character_set_max_length, int)

class TestMsSQLCharacterSetDictionary:
    pass
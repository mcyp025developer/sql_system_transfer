import pytest
from sql_system_transfer.datatype_dictionary import DatatypeDictionary, SQLSystem, SQLSystemError
from frozendict import frozendict

system_parameters = [SQLSystem.MSSQL, SQLSystem.MYSQL, ]
pytest_parameters_sql_system = pytest.mark.parametrize("system", system_parameters)

@pytest_parameters_sql_system
def test_datatype_dictionary_property_get_system(system):
    datatype_dictionary = DatatypeDictionary(system=system)
    system = datatype_dictionary.system
    assert isinstance(system, SQLSystem)

@pytest_parameters_sql_system
def test_datatype_dictionary_property_set_system(system):
    datatype_dictionary = DatatypeDictionary(system=system)
    datatype_dictionary.system = system
    assert isinstance(system, SQLSystem)

@pytest_parameters_sql_system
def test_datatype_dictionary_property_set_system_raise_exception(system):
    datatype_dictionary = DatatypeDictionary(system=system)
    with pytest.raises(SQLSystemError):
        datatype_dictionary.system = "invalid"

@pytest_parameters_sql_system
def test_datatype_dictionary(system):
    datatype_dictionary = DatatypeDictionary(system=system)
    datatype_dictionary = datatype_dictionary.datatype_dictionary()
    assert isinstance(datatype_dictionary, frozendict)

class TestMsSQLDatatypeDictionary:

    _mssql_datatypes = [
        ('varchar', 'character varying'), ('varchar', 'char varying'), ('varchar', 'varchar'),
        ('nvarchar', 'national character varying'), ('nvarchar', 'national char varying',), ('nvarchar', 'nvarchar'),
        ('text', 'text'),
        ('ntext', 'national text'), ('ntext', 'ntext'),
        ('char', 'character'), ('char', 'char'),
        ('nchar', 'national character'), ('nchar', 'national char'), ('nchar', 'nchar'),
        ('numeric', 'numeric'),
        ('decimal', 'decimal'), ('decimal', 'dec'),
        ('float', 'float'), ('float', 'double precision'),
        ('real', 'real'),
        ('bit', 'bit'),
        ('tinyint', 'tinyint'),
        ('smallint', 'smallint'),
        ('int', 'int'), ('int', 'integer'),
        ('bigint', 'bigint'),
        ('smallmoney', 'smallmoney'),
        ('money', 'money'),
        ('varbinary', 'varbinary'), ('varbinary', 'binary varying'),
        ('binary', 'binary'),
        ('geography', 'geography'),
        ('geometry', 'geometry'),
        ('hierarchyid', 'hierarchyid'),
        ('image', 'image'),
        ('sql_variant', 'sql_variant'),
        ('sysname', 'sysname'),
        ('uniqueidentifier', 'uniqueidentifier'),
        ('xml', 'xml'),
        ('timestamp', 'timestamp'), ('timestamp', 'rowversion'),
        ('date', 'date'),
        ('datetime', 'datetime'),
        ('datetime2', 'datetime2'),
        ('datetimeoffset', 'datetimeoffset'),
        ('smalldatetime', 'smalldatetime'),
        ('time', 'time')
    ]

    _pytest_mssql_parameters = pytest.mark.parametrize("datatype_name", _mssql_datatypes)

    @pytest.fixture()
    def datatype_dictionary(self):
        datatype_dictionary = DatatypeDictionary(system=SQLSystem.MSSQL)
        return datatype_dictionary

    @_pytest_mssql_parameters
    def test_datatype_metadata(self, datatype_dictionary, datatype_name):
        metadata = datatype_dictionary.datatype_metadata(datatype_name=datatype_name[1])
        assert isinstance(metadata, dict)

    @_pytest_mssql_parameters
    def test_datatype_synonym(self, datatype_dictionary, datatype_name):
        synonym = datatype_dictionary.datatype_synonym(datatype_name=datatype_name[1])
        assert isinstance(synonym, list)

    @_pytest_mssql_parameters
    def test_datatype_category(self, datatype_dictionary, datatype_name):
        category = datatype_dictionary.datatype_category(datatype_name=datatype_name[1])
        assert isinstance(category, str)

    @_pytest_mssql_parameters
    def test_datatype_synonym_to_name(self, datatype_dictionary, datatype_name):
        system_name = datatype_dictionary.datatype_synonym_to_name(synonym=datatype_name[1])
        assert system_name == datatype_name[0]

    def test_datatype_synonym_to_name_return_none(self, datatype_dictionary):
        datatype_synonym_to_name = datatype_dictionary.datatype_synonym_to_name("")
        assert datatype_synonym_to_name is None

class TestMySQLDatatypeDictionary:

    _mysql_datatypes = [
        ('varchar', 'character varying'), ('varchar', 'char varying'), ('varchar', 'varchar'),
        ('nvarchar', 'national character varying'), ('nvarchar', 'national char varying',), ('nvarchar', 'nvarchar'),
        ('text', 'text'),
        ('tinytext', 'tinytext'),
        ('mediumtext', 'mediumtext'), ('mediumtext', 'long'), ('mediumtext', 'long varchar'),
        ('longtext', 'longtext'),
        ('char', 'character'), ('char', 'char'),
        ('nchar', 'national character'), ('nchar', 'national char'), ('nchar', 'nchar'),
        ('set', 'set'),
        ('enum', 'enum'),
        ('decimal', 'decimal'), ('decimal', 'dec'), ('decimal', 'fixed'), ('decimal', 'numeric'),
        ('float', 'float4'), ('float', 'float'),
        ('double', 'float8'), ('double', 'double'), ('double', 'double precision'), ('double', 'real'),
        ("bit", "bit"),
        ('tinyint', 'tinyint'), ('tinyint', 'int1'), ("tinyint", "bool"), ("tinyint", "boolean"),
        ('smallint', 'smallint'), ("smallint", 'int2'),
        ('mediumint', 'mediumint'), ('mediumint', 'int3'), ('mediumint', 'middleint'),
        ('int', 'int'), ('int', 'integer'), ('int', 'int4'),
        ('bigint', 'bigint'), ('bigint', 'int8'),
        ('serial', 'serial'),
        ('varbinary', 'varbinary'),
        ('binary', 'binary'),
        ('blob', 'blob'),
        ('tinyblob', 'tinyblob'),
        ('mediumblob', 'mediumblob'), ('mediumblob', 'long varbinary'),
        ('longblob', 'longblob'),
        ('date', 'date'),
        ('datetime', 'datetime'),
        ('time', 'time'),
        ('timestamp', 'timestamp'),
        ('year', 'year'),
    ]

    _pytest_mysql_parameters = pytest.mark.parametrize("datatype_name", _mysql_datatypes)

    @pytest.fixture()
    def datatype_dictionary(self):
        datatype_dictionary = DatatypeDictionary(system=SQLSystem.MYSQL)
        return datatype_dictionary

    @_pytest_mysql_parameters
    def test_datatype_metadata(self, datatype_dictionary, datatype_name):
        metadata = datatype_dictionary.datatype_metadata(datatype_name=datatype_name[1])
        assert isinstance(metadata, dict)

    @_pytest_mysql_parameters
    def test_datatype_synonym(self, datatype_dictionary, datatype_name):
        synonym = datatype_dictionary.datatype_synonym(datatype_name=datatype_name[1])
        assert isinstance(synonym, list)

    @_pytest_mysql_parameters
    def test_datatype_category(self, datatype_dictionary, datatype_name):
        category = datatype_dictionary.datatype_category(datatype_name=datatype_name[1])
        assert isinstance(category, str)

    @_pytest_mysql_parameters
    def test_datatype_synonym_to_name(self, datatype_dictionary, datatype_name):
        system_name = datatype_dictionary.datatype_synonym_to_name(synonym=datatype_name[1])
        assert system_name == datatype_name[0]

    def test_datatype_synonym_to_name_return_none(self, datatype_dictionary):
        datatype_synonym_to_name = datatype_dictionary.datatype_synonym_to_name("invalid")
        assert datatype_synonym_to_name is None
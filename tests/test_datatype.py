import pytest
import sql_system_transfer.datatype as sql_dt
from sql_system_transfer.system import SQLSystem

class TestMsSQLDatatypeFactory:

    @pytest.fixture()
    def datatype_factory(self):
        datatype_factory = sql_dt.MsSQLDatatypeFactory()
        return datatype_factory

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ('varchar', sql_dt.MsSQLVarchar),
            ('char varying', sql_dt.MsSQLVarchar),
            ('character varying', sql_dt.MsSQLVarchar),
            ('nvarchar', sql_dt.MsSQLVarchar),
            ('national char varying', sql_dt.MsSQLVarchar),
            ('national character varying', sql_dt.MsSQLVarchar),
            ('text', sql_dt.MsSQLText),
            ('national text', sql_dt.MsSQLText),
            ('ntext', sql_dt.MsSQLText),
            ('char', sql_dt.MsSQLChar),
            ('character', sql_dt.MsSQLChar),
            ('nchar', sql_dt.MsSQLChar),
            ('national character', sql_dt.MsSQLChar),
            ('national char', sql_dt.MsSQLChar),
            ('varbinary', sql_dt.MsSQLVarbinary),
            ('varbinary', sql_dt.MsSQLVarbinary),
            ('binary', sql_dt.MsSQLBinary),
            ('numeric', sql_dt.MsSQLNumeric),
            ('decimal', sql_dt.MsSQLNumeric),
            ('dec', sql_dt.MsSQLNumeric),
            ('float', sql_dt.MsSQLFloat),
            ('double precision', sql_dt.MsSQLFloat),
            ('real', sql_dt.MsSQLFloat),
            ('bit', sql_dt.MsSQLInteger),
            ('smallint', sql_dt.MsSQLInteger),
            ('int', sql_dt.MsSQLInteger),
            ('integer', sql_dt.MsSQLInteger),
            ('bigint', sql_dt.MsSQLInteger),
            ('money', sql_dt.MsSQLMoney),
            ('smallmoney', sql_dt.MsSQLMoney),
            ('timestamp', sql_dt.MsSQLTimestamp),
            ('rowversion', sql_dt.MsSQLTimestamp),
            ('date', sql_dt.MsSQLDatetimeOne),
            ('datetime', sql_dt.MsSQLDatetimeOne),
            ('smalldatetime', sql_dt.MsSQLDatetimeOne),
            ('datetime2', sql_dt.MsSQLDatetimeTwo),
            ('datetimeoffset', sql_dt.MsSQLDatetimeTwo),
            ('time', sql_dt.MsSQLDatetimeTwo),
            ('geography', sql_dt.MsSQLOther),
            ('geometry', sql_dt.MsSQLOther),
            ('hierarchyid', sql_dt.MsSQLOther),
            ('image', sql_dt.MsSQLOther),
            ('sql_variant', sql_dt.MsSQLOther),
            ('sysname', sql_dt.MsSQLOther),
            ('uniqueidentifier', sql_dt.MsSQLOther),
            ('xml', sql_dt.MsSQLOther),
        ])
    def test_create_datatype(self, datatype_factory, datatype_name, expected_result):
        datatype = datatype_factory.create_datatype(datatype_name)
        assert isinstance(datatype, expected_result)

    def test_create_datatype_raise_exception(self, datatype_factory):
        with pytest.raises(sql_dt.SQLDatatypeError):
            datatype = datatype_factory.create_datatype(datatype_name="invalid")

class TestMySQLDatatypeFactory:

    @pytest.fixture()
    def datatype_factory(self):
        datatype_factory = sql_dt.MySQLDatatypeFactory()
        return datatype_factory

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ('varchar', sql_dt.MySQLVarchar),
            ('char varying', sql_dt.MySQLVarchar),
            ('character varying', sql_dt.MySQLVarchar),
            ('nvarchar', sql_dt.MySQLVarchar),
            ('national char varying', sql_dt.MySQLVarchar),
            ('national character varying', sql_dt.MySQLVarchar),
            ('text', sql_dt.MySQLText),
            ('tinytext', sql_dt.MySQLOtherText),
            ('mediumtext', sql_dt.MySQLOtherText),
            ('long varchar', sql_dt.MySQLOtherText),
            ('long', sql_dt.MySQLOtherText),
            ('longtext', sql_dt.MySQLOtherText),
            ('char', sql_dt.MySQLChar),
            ('character', sql_dt.MySQLChar),
            ('nchar', sql_dt.MySQLChar),
            ('national character', sql_dt.MySQLChar),
            ('national char', sql_dt.MySQLChar),
            ('set', sql_dt.MySQLVarchar),
            ('enum', sql_dt.MySQLVarchar),
            ('varbinary', sql_dt.MySQLVarbinary),
            ('blob', sql_dt.MySQLBlob),
            ('tinyblob', sql_dt.MySQLOtherBlob),
            ('mediumblob', sql_dt.MySQLOtherBlob),
            ('long varbinary', sql_dt.MySQLOtherBlob),
            ('binary', sql_dt.MySQLBinary),
            ('longblob', sql_dt.MySQLOtherBlob),
            ('decimal', sql_dt.MySQLDecimal),
            ('dec', sql_dt.MySQLDecimal),
            ('fixed', sql_dt.MySQLDecimal),
            ('numeric', sql_dt.MySQLDecimal),
            ('float', sql_dt.MySQLFloat),
            ('float4', sql_dt.MySQLFloat),
            ('double', sql_dt.MySQLFloat),
            ('float8', sql_dt.MySQLFloat),
            ('double precision', sql_dt.MySQLFloat),
            ('real', sql_dt.MySQLFloat),
            ('bit', sql_dt.MySQLBit),
            ('tinyint', sql_dt.MySQLInteger),
            ('int1', sql_dt.MySQLInteger),
            ('bool', sql_dt.MySQLInteger),
            ('boolean', sql_dt.MySQLInteger),
            ('smallint', sql_dt.MySQLInteger),
            ('int2', sql_dt.MySQLInteger),
            ('mediumint', sql_dt.MySQLInteger),
            ('int3', sql_dt.MySQLInteger),
            ('middleint', sql_dt.MySQLInteger),
            ('int', sql_dt.MySQLInteger),
            ('integer', sql_dt.MySQLInteger),
            ('int4', sql_dt.MySQLInteger),
            ('bigint', sql_dt.MySQLInteger),
            ('int8', sql_dt.MySQLInteger),
            ('serial', sql_dt.MySQLInteger),
            ('date', sql_dt.MySQLDate),
            ('datetime', sql_dt.MySQLDatetime),
            ('timestamp', sql_dt.MySQLDatetime),
            ('time', sql_dt.MySQLDatetime),
            ('year', sql_dt.MySQLYear),
        ])
    def test_create_datatype(self, datatype_factory, datatype_name, expected_result):
        datatype = datatype_factory.create_datatype(datatype_name)
        assert isinstance(datatype, expected_result)

    def test_create_datatype_raise_exception(self, datatype_factory):
        with pytest.raises(sql_dt.SQLDatatypeError):
            datatype = datatype_factory.create_datatype(datatype_name="invalid")

class TestMsSQLVarchar:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLVarchar(datatype_name="varchar")
        return datatype

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            (-1, "varchar(max)"),
            (100, "varchar(100)"),
        ])
    def test_datatype_format(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        datatype_format = datatype.datatype_format()
        assert datatype_format == expected_result

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "varchar", "character_size": 1}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {'datatype_name': 'varchar', 'character_size': 1}
        assert isinstance(convert, sql_dt.MsSQLVarchar)

    @pytest.mark.parametrize(
        "datatype_name, character_size, expected_result", [
            ("nvarchar", 500, ({'datatype_name': 'varchar', 'character_size': 500, 'character_set': 'utf8mb4'}, sql_dt.MySQLVarchar)),
            ("varchar", 500, ({'datatype_name': 'varchar', 'character_size': 500, 'character_set': 'latin1'}, sql_dt.MySQLVarchar)),
            ("nvarchar", -1, ({'datatype_name': 'longtext', 'character_set': 'utf8mb4'}, sql_dt.MySQLOtherText)),
            ("varchar", -1, ({'datatype_name': 'longtext', 'character_set': 'latin1'}, sql_dt.MySQLOtherText)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, character_size, expected_result):
        datatype.datatype_name = datatype_name
        datatype.character_size = character_size
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "varchar"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "varchar"),
            ("varchar", "varchar"),
            ("nvarchar", "nvarchar"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 1

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", -1),
            (-2, -1),
            (8001, -1),
            (0, -1),
            (100, 100)
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMySQLVarchar:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLVarchar(character_size=100)
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "varchar(100) character set latin1"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "varchar", "character_size": 100, "character_set": "latin1"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {'datatype_name': 'varchar', 'character_size': 100, "character_set": "latin1"}
        assert isinstance(convert, sql_dt.MySQLVarchar)

    @pytest.mark.parametrize(
        "character_size, character_set, expected_result", [
            (8001, "utf8mb4", ({'datatype_name': 'nvarchar', 'character_size': -1}, sql_dt.MsSQLVarchar)),
            (8001, "latin1", ({'datatype_name': 'varchar', 'character_size': -1}, sql_dt.MsSQLVarchar)),
            (500, "utf8mb4", ({'datatype_name': 'nvarchar', 'character_size': 500}, sql_dt.MsSQLVarchar)),
            (500, "latin1", ({'datatype_name': 'varchar', 'character_size': 500}, sql_dt.MsSQLVarchar)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, character_size, character_set, expected_result):
        datatype.character_size = character_size
        datatype.character_set = character_set
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "varchar"

    def test_character_set_property_getter(self, datatype):
        character_set = datatype.character_set
        assert character_set == "latin1"

    @pytest.mark.parametrize(
        "character_set, expected_result", [
            ("invalid", "latin1"),
            ("latin1", "latin1"),
            ("utf8mb4", "utf8mb4")
        ])
    def test_character_set_property_setter(self, datatype, character_set, expected_result):
        datatype.character_set = character_set
        assert datatype.character_set == expected_result

    @pytest.mark.parametrize(
        "character_set, character_size, expected_result", [
            ("latin1", 50000000, 65532),
            ("utf8", 50000000, 21844),
            ("sjis", 50000000, 32766),
            ("utf8mb4", 50000000, 16383),
        ])
    def test_character_set_property_setter_reset_character_size(
            self, datatype, character_set, character_size, expected_result
    ):
        datatype.character_set = character_set
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 100

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 65532),
            (-1, 65532),
            (70000, 65532),
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMsSQLText:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLText(datatype_name="text")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "text"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "text"}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {'datatype_name': 'text'}
        assert isinstance(convert, sql_dt.MsSQLText)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("ntext", ({'datatype_name': 'longtext', 'character_set': "utf8mb4"}, sql_dt.MySQLOtherText)),
            ("text", ({'datatype_name': 'longtext', 'character_set': "latin1"}, sql_dt.MySQLOtherText))
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "text"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "text"),
            ("text", "text"),
            ("ntext", "ntext"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

class TestMySQLText:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLText()
        return datatype

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            (254, "tinytext character set latin1"),
            (65534, "text character set latin1"),
            (16777214, "mediumtext character set latin1"),
            (4294967294, "longtext character set latin1"),
        ]
    )
    def test_datatype_format(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        datatype_format = datatype.datatype_format()
        assert datatype_format == expected_result

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "text", "character_size": 65535,"character_set": "latin1"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "text", "character_size": 65535,"character_set": "latin1"}
        assert isinstance(convert, sql_dt.MySQLText)

    @pytest.mark.parametrize(
        "character_size, character_set, expected_result", [
            (254, "utf8mb4", ({'datatype_name': 'nvarchar', 'character_size': 255}, sql_dt.MsSQLVarchar)),
            (65535, "utf8mb4", ({'datatype_name': 'nvarchar', 'character_size': -1}, sql_dt.MsSQLVarchar)),
            (254, "latin1", ({'datatype_name': 'varchar', 'character_size': 255}, sql_dt.MsSQLVarchar)),
            (65535, "latin1", ({'datatype_name': 'varchar', 'character_size': -1}, sql_dt.MsSQLVarchar)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, character_size, character_set, expected_result):
        datatype.character_size = character_size
        datatype.character_set = character_set
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "text"

    def test_character_set_property_getter(self, datatype):
        character_set = datatype.character_set
        assert character_set == "latin1"

    @pytest.mark.parametrize(
        "character_set, expected_result", [
            ("invalid", "latin1"),
            ("latin1", "latin1"),
            ("utf8mb4", "utf8mb4")
        ])
    def test_character_set_property_setter(self, datatype, character_set, expected_result):
        datatype.character_set = character_set
        assert datatype.character_set == expected_result

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 65535

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 4294967295),
            (-1, 4294967295),
            (65530, 65535),
            (16777210, 16777215),
            (4294967290, 4294967295),
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMySQLOtherText:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLOtherText(datatype_name="longtext")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "longtext character set latin1"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "longtext", "character_set": "latin1"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "longtext", "character_set": "latin1"}
        assert isinstance(convert, sql_dt.MySQLOtherText)

    @pytest.mark.parametrize(
        "datatype_name, character_set, expected_result", [
            ("tinytext", "utf8mb4", ({"datatype_name": "nvarchar", "character_size": 255}, sql_dt.MsSQLVarchar)),
            ("mediumtext", "utf8mb4", ({"datatype_name": "nvarchar", "character_size": -1}, sql_dt.MsSQLVarchar)),
            ("longtext", "utf8mb4", ({"datatype_name": "nvarchar", "character_size": -1}, sql_dt.MsSQLVarchar)),
            ("tinytext", "latin1", ({"datatype_name": "varchar", "character_size": 255}, sql_dt.MsSQLVarchar)),
            ("mediumtext", "latin1", ({"datatype_name": "varchar", "character_size": -1}, sql_dt.MsSQLVarchar)),
            ("longtext", "latin1", ({"datatype_name": "varchar", "character_size": -1}, sql_dt.MsSQLVarchar)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, datatype_name, character_set, expected_result):
        datatype.datatype_name = datatype_name
        datatype.character_set = character_set
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "longtext"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "longtext"),
            ("longtext", "longtext"),
            ("tinytext", "tinytext"),
            ("mediumtext", "mediumtext"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_character_set_property_getter(self, datatype):
        character_set = datatype.character_set
        assert character_set == "latin1"

    @pytest.mark.parametrize(
        "character_set, expected_result", [
            ("invalid", "latin1"),
            ("latin1", "latin1"),
            ("utf8mb4", "utf8mb4")
        ])
    def test_character_set_property_setter(self, datatype, character_set, expected_result):
        datatype.character_set = character_set
        assert datatype.character_set == expected_result

class TestMsSQLChar:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLChar(datatype_name="char")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "char(1)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "char", "character_size": 1}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "char", "character_size": 1}
        assert isinstance(convert, sql_dt.MsSQLChar)

    @pytest.mark.parametrize(
        "datatype_name, character_size, expected_result", [
            ("nchar", 100, ({"datatype_name": "char", "character_size": 100, "character_set": "utf8mb4"}, sql_dt.MySQLChar)),
            ("nchar", 500, ({"datatype_name": "varchar", "character_size": 500, "character_set": "utf8mb4"}, sql_dt.MySQLVarchar)),
            ("char", 100, ({"datatype_name": "char", "character_size": 100, "character_set": "latin1"}, sql_dt.MySQLChar)),
            ("char", 500, ({"datatype_name": "varchar", "character_size": 500, "character_set": "latin1"}, sql_dt.MySQLVarchar)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, character_size, expected_result):
        datatype.datatype_name = datatype_name
        datatype.character_size = character_size
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "char"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "char"),
            ("char", "char"),
            ("nchar", "nchar"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 1

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 8000),
            (8001, 8000),
            (0, 8000),
            (100, 100)
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMySQLChar:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLChar()
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "char(1) character set latin1"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "char", "character_size": 1, "character_set": "latin1"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "char", "character_size": 1, "character_set": "latin1"}
        assert isinstance(convert, sql_dt.MySQLChar)

    @pytest.mark.parametrize(
        "character_set, character_size, expected_result", [
            ("utf8mb4", 200, ({"datatype_name": "nchar", "character_size": 200}, sql_dt.MsSQLChar)),
            ("latin1", 200, ({"datatype_name": "char", "character_size": 200}, sql_dt.MsSQLChar)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, character_set, character_size, expected_result):
        datatype.character_set = character_set
        datatype.character_size = character_size
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "char"

    def test_character_set_property_getter(self, datatype):
        character_set = datatype.character_set
        assert character_set == "latin1"

    @pytest.mark.parametrize(
        "character_set, expected_result", [
            ("invalid", "latin1"),
            ("latin1", "latin1"),
            ("utf8mb4", "utf8mb4")
        ])
    def test_character_set_property_setter(self, datatype, character_set, expected_result):
        datatype.character_set = character_set
        assert datatype.character_set == expected_result

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 1

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 255),
            (300, 255),
            (-1, 255),
            (200, 200),
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMsSQLBinary:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLBinary()
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "binary(1)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "binary", "character_size": 1}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "binary", "character_size": 1}
        assert isinstance(convert, sql_dt.MsSQLBinary)

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            (100, ({"datatype_name": "binary", "character_size": 100}, sql_dt.MySQLBinary)),
            (300, ({"datatype_name": "blob", "character_size": 65535}, sql_dt.MySQLBlob))
        ])
    def test_datatype_convert_to_mysql(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 1

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 8000),
            (8001, 8000),
            (0, 8000),
            (100, 100)
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMySQLBinary:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLBinary()
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "binary(1)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "binary", "character_size": 1}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "binary", "character_size": 1}
        assert isinstance(convert, sql_dt.MySQLBinary)

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "binary", "character_size": 1}
        assert isinstance(convert, sql_dt.MsSQLBinary)

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 1

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 255),
            (256, 255),
            (0, 255),
            (100, 100)
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMsSQLVarbinary:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLVarbinary()
        return datatype

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            (-1, "varbinary(max)"),
            (100, "varbinary(100)"),
        ])
    def test_datatype_format(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        datatype_format = datatype.datatype_format()
        assert datatype_format == expected_result

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "varbinary", "character_size": 1}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "varbinary", "character_size": 1}
        assert isinstance(convert, sql_dt.MsSQLVarbinary)

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            (-1, ({"datatype_name": "longblob"}, sql_dt.MySQLOtherBlob)),
            (300, ({"datatype_name": "varbinary", "character_size": 300}, sql_dt.MySQLVarbinary))
        ])
    def test_datatype_convert_to_mysql(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 1

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", -1),
            (-2, -1),
            (8001, -1),
            (0, -1),
            (100, 100)
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMySQLVarbinary:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLVarbinary(character_size=100)
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "varbinary(100)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "varbinary", "character_size": 100}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "varbinary", "character_size": 100}
        assert isinstance(convert, sql_dt.MySQLVarbinary)

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "varbinary", "character_size": 100}
        assert isinstance(convert, sql_dt.MsSQLVarbinary)

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 100

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 65532),
            (-1, 65532),
            (65533, 65532),
            (100, 100)
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMySQLBlob:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLBlob()
        return datatype

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            (255, "tinyblob"),
            (65535, "blob"),
            (16777215, "mediumblob"),
            (4294967295, "longblob")
        ])
    def test_datatype_format(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        datatype_format = datatype.datatype_format()
        assert datatype_format == expected_result

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "blob", "character_size": 65535}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "blob", "character_size": 65535}
        assert isinstance(convert, sql_dt.MySQLBlob)

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "varbinary", "character_size": -1}
        assert isinstance(convert, sql_dt.MsSQLVarbinary)

    def test_character_size_property_getter(self, datatype):
        character_size = datatype.character_size
        assert character_size == 65535

    @pytest.mark.parametrize(
        "character_size, expected_result", [
            ("invalid", 4294967295),
            (-1, 4294967295),
            (4294967296, 4294967295),
            (100, 255),
            (65530, 65535),
            (16777210, 16777215),
            (4294967290, 4294967295)
        ])
    def test_character_size_property_setter(self, datatype, character_size, expected_result):
        datatype.character_size = character_size
        assert datatype.character_size == expected_result

class TestMySQLOtherBlob:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLOtherBlob(datatype_name="longblob")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "longblob"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "longblob"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "longblob"}
        assert isinstance(convert, sql_dt.MySQLOtherBlob)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("tinyblob", ({"datatype_name": "varbinary", "character_size": 255}, sql_dt.MsSQLVarbinary)),
            ("mediumblob", ({"datatype_name": "varbinary", "character_size": -1}, sql_dt.MsSQLVarbinary)),
            ("longblob", ({"datatype_name": "varbinary", "character_size": -1}, sql_dt.MsSQLVarbinary)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "longblob"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "longblob"),
            ("longblob", "longblob"),
            ("tinyblob", "tinyblob"),
            ("mediumblob", "mediumblob"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

class TestMsSQLOther:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLOther(datatype_name="xml")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "xml"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "xml"}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "xml"}
        assert isinstance(convert, sql_dt.MsSQLOther)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("geography", ({"datatype_name": "blob", 'character_size': 65535}, sql_dt.MySQLBlob)),
            ("geometry", ({"datatype_name": "blob", 'character_size': 65535}, sql_dt.MySQLBlob)),
            ("hierarchyid", ({"datatype_name": "blob", 'character_size': 65535}, sql_dt.MySQLBlob)),
            ("image", ({"datatype_name": "blob", 'character_size': 65535}, sql_dt.MySQLBlob)),
            ("sql_variant", ({"datatype_name": "text", 'character_size': 65535, 'character_set': 'latin1'}, sql_dt.MySQLText)),
            ("sysname", ({"datatype_name": "varchar", 'character_size': 128, 'character_set': 'utf8mb4'}, sql_dt.MySQLVarchar)),
            ("uniqueidentifier", ({"datatype_name": "binary", 'character_size': 16}, sql_dt.MySQLBinary)),
            ("xml", ({"datatype_name": "text", 'character_size': 65535, 'character_set': 'latin1'}, sql_dt.MySQLText)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "xml"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ('geography', 'geography'),
            ('geometry', 'geometry'),
            ('hierarchyid', 'hierarchyid'),
            ('image', 'image'),
            ('sql_variant', 'sql_variant'),
            ('sysname', 'sysname'),
            ('uniqueidentifier', 'uniqueidentifier'),
            ('xml', 'xml'),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_datatype_name_property_setter_raise_exception(self, datatype):
        with pytest.raises(sql_dt.SQLDatatypeError):
            datatype.datatype_name = "invalid"

class TestMsSQLNumeric:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLNumeric(datatype_name="numeric")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "numeric(18, 0)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "numeric", "numeric_precision": 18, "numeric_scale": 0}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "numeric", "numeric_precision": 18, "numeric_scale": 0}
        assert isinstance(convert, sql_dt.MsSQLNumeric)

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "decimal", "numeric_precision": 18, "numeric_scale": 0}
        assert isinstance(convert, sql_dt.MySQLDecimal)

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "numeric"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "numeric"),
            ("numeric", "numeric"),
            ("decimal", "decimal"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_numeric_precision_property_getter(self, datatype):
        numeric_precision = datatype.numeric_precision
        assert numeric_precision == 18

    @pytest.mark.parametrize(
        "numeric_precision, expected_result", [
            ("invalid", 38),
            (-1, 38),
            (30, 30),
        ])
    def test_numeric_precision_property_setter(self, datatype, numeric_precision, expected_result):
        datatype.numeric_precision = numeric_precision
        assert datatype.numeric_precision == expected_result

    def test_numeric_scale_property_getter(self, datatype):
        numeric_scale = datatype.numeric_scale
        assert numeric_scale == 0

    @pytest.mark.parametrize(
        "numeric_precision, numeric_scale, expected_result", [
            (18, "invalid", 18),
            (18, 25, 18),
            (18, -1, 18)
        ])
    def test_numeric_scale_property_setter(self, datatype, numeric_precision, numeric_scale, expected_result):
        datatype.numeric_precision = numeric_precision
        datatype.numeric_scale = numeric_scale
        assert datatype.numeric_scale == expected_result

class TestMySQLDecimal:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLDecimal()
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "decimal(10, 0)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "decimal", "numeric_precision": 10, "numeric_scale": 0}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "decimal", "numeric_precision": 10, "numeric_scale": 0}
        assert isinstance(convert, sql_dt.MySQLDecimal)

    @pytest.mark.parametrize(
        "numeric_precision, expected_result", [
            (50, ({"datatype_name": "numeric", "numeric_precision": 38, "numeric_scale": 0}, sql_dt.MsSQLNumeric)),
            (20, ({"datatype_name": "numeric", "numeric_precision": 20, "numeric_scale": 0}, sql_dt.MsSQLNumeric)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, numeric_precision, expected_result):
        datatype.numeric_precision = numeric_precision
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "decimal"

    def test_numeric_precision_property_getter(self, datatype):
        numeric_precision = datatype.numeric_precision
        assert numeric_precision == 10

    @pytest.mark.parametrize(
        "numeric_precision, expected_result", [
            ("", 65),
            (70, 65),
            (0, 65),
            (10, 10)
        ])
    def test_numeric_precision_property_setter(self, datatype, numeric_precision, expected_result):
        datatype.numeric_precision = numeric_precision
        assert datatype.numeric_precision == expected_result

    def test_numeric_scale_property_getter(self, datatype):
        numeric_scale = datatype.numeric_scale
        assert numeric_scale == 0

    @pytest.mark.parametrize(
        "numeric_scale, numeric_precision, expected_result", [
            ("invalid", 25, 25),
            (-10, 25, 25),
            (-10, 35, 30),
            (25, 25, 25),
            (35, 25, 25),
            (35, 40, 30),
        ])
    def test_numeric_scale_property_setter(self, datatype, numeric_scale, numeric_precision, expected_result):
        datatype.numeric_precision = numeric_precision
        datatype.numeric_scale = numeric_scale
        assert datatype.numeric_scale == expected_result

class TestMsSQLFloat:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLFloat(datatype_name="float")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "float"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "float", "numeric_precision": 53}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "float", "numeric_precision": 53}
        assert isinstance(convert, sql_dt.MsSQLFloat)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("real", ({'datatype_name': 'double', 'numeric_precision': 22}, sql_dt.MySQLFloat)),
            ("float", ({'datatype_name': 'float', 'numeric_precision': 12}, sql_dt.MySQLFloat)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "float"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", ("float", 53)),
            ("real", ("real", 24)),
            ("float", ("float", 53))
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result[0]
        assert datatype.numeric_precision == expected_result[1]

    def test_numeric_precision_property_getter(self, datatype):
        numeric_precision = datatype.numeric_precision
        assert numeric_precision == 53

class TestMySQLFloat:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLFloat(datatype_name="float")
        return datatype

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("float", "float(12)"),
            ("double", "double"),
        ])
    def test_datatype_format(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        datatype_format = datatype.datatype_format()
        assert datatype_format == expected_result

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "float", "numeric_precision": 12}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "float", "numeric_precision": 12}
        assert isinstance(convert, sql_dt.MySQLFloat)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("double", ({'datatype_name': 'float', 'numeric_precision': 53}, sql_dt.MsSQLFloat)),
            ("float", ({'datatype_name': 'real', 'numeric_precision': 24}, sql_dt.MsSQLFloat)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "float"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", ("double", 22)),
            ("double", ("double", 22)),
            ("float", ("float", 12))
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result[0]
        assert datatype.numeric_precision == expected_result[1]

    def test_numeric_precision_property_getter(self, datatype):
        numeric_precision = datatype.numeric_precision
        assert numeric_precision == 12

class TestMsSQLInteger:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLInteger(datatype_name="int")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "int"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "int"}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "int"}
        assert isinstance(convert, sql_dt.MsSQLInteger)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("bit", ({'datatype_name': 'tinyint'}, sql_dt.MySQLInteger)),
            ("int", ({'datatype_name': 'int'}, sql_dt.MySQLInteger)),
            ("tinyint", ({'datatype_name': 'smallint'}, sql_dt.MySQLInteger)),
            ("smallint", ({'datatype_name': 'smallint'}, sql_dt.MySQLInteger)),
            ("bigint", ({'datatype_name': 'bigint'}, sql_dt.MySQLInteger)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "int"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "bigint"),
            ("bit", "bit"),
            ("int", "int"),
            ("tinyint", "tinyint"),
            ("smallint", "smallint"),
            ("bigint", "bigint"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

class TestMySQLInteger:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLInteger(datatype_name="int")
        return datatype

    @pytest.mark.parametrize(
        "datatype_name, signed_unsigned, expected_result", [
            ("serial", "signed", "bigint unsigned"),
            ("int", "signed", "int"),
            ("int", "unsigned", "int unsigned"),
        ])
    def test_datatype_format(self, datatype, datatype_name, signed_unsigned, expected_result):
        datatype.datatype_name = datatype_name
        datatype.signed_unsigned = signed_unsigned
        datatype_format = datatype.datatype_format()
        assert datatype_format == expected_result

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "int"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "int"}
        assert isinstance(convert, sql_dt.MySQLInteger)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("int", ({'datatype_name': 'bigint'}, sql_dt.MsSQLInteger)),
            ("tinyint", ({'datatype_name': 'smallint'}, sql_dt.MsSQLInteger)),
            ("smallint", ({'datatype_name': 'int'}, sql_dt.MsSQLInteger)),
            ("mediumint", ({'datatype_name': 'int'}, sql_dt.MsSQLInteger)),
            ("bigint", ({'datatype_name': 'numeric', 'numeric_precision': 20, 'numeric_scale': 0}, sql_dt.MsSQLNumeric)),
            ("serial", ({'datatype_name': 'numeric', 'numeric_precision': 20, 'numeric_scale': 0}, sql_dt.MsSQLNumeric)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "int"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "bigint"),
            ("int", "int"),
            ("tinyint", "tinyint"),
            ("smallint", "smallint"),
            ("bigint", "bigint"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_signed_unsigned_property_getter(self, datatype):
        signed_unsigned = datatype.signed_unsigned
        assert signed_unsigned == "signed"

    @pytest.mark.parametrize(
        "signed_unsigned, expected_result", [
            ("invalid", "signed"),
            ("signed", "signed"),
            ("unsigned", "unsigned"),
        ])
    def test_signed_unsigned_property_setter(self, datatype, signed_unsigned, expected_result):
        datatype.signed_unsigned = signed_unsigned
        assert datatype.signed_unsigned == expected_result

class TestMySQLBit:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLBit()
        return datatype

    @pytest.mark.parametrize(
        "numeric_precision, expected_result", [
            (1, "bit"),
            (10, "bit(10)"),
        ])
    def test_datatype_format(self, datatype, numeric_precision, expected_result):
        datatype.numeric_precision = numeric_precision
        datatype_format = datatype.datatype_format()
        assert datatype_format == expected_result

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "bit", 'numeric_precision': 1}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "bit", 'numeric_precision': 1}
        assert isinstance(convert, sql_dt.MySQLBit)

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "numeric", "numeric_precision": 20, "numeric_scale": 0}
        assert isinstance(convert, sql_dt.MsSQLNumeric)

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "bit"

    def test_numeric_precision_property_getter(self, datatype):
        numeric_precision = datatype.numeric_precision
        assert numeric_precision == 1

    @pytest.mark.parametrize(
        "numeric_precision, expected_result", [
            ("invalid", 64),
            (-1, 64),
            (30, 30),
        ])
    def test_numeric_precision_property_setter(self, datatype, numeric_precision, expected_result):
        datatype.numeric_precision = numeric_precision
        assert datatype.numeric_precision == expected_result

class TestMsSQLMoney:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLMoney(datatype_name="money")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "money"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "money"}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "money"}
        assert isinstance(convert, sql_dt.MsSQLMoney)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("money", ({"datatype_name": "decimal", "numeric_precision": 19, "numeric_scale": 4}, sql_dt.MySQLDecimal)),
            ("smallmoney", ({"datatype_name": "decimal", "numeric_precision": 10, "numeric_scale": 4}, sql_dt.MySQLDecimal)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "money"

class TestMsSQLDatetimeOne:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLDatetimeOne(datatype_name="date")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "date"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "date"}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "date"}
        assert isinstance(convert, sql_dt.MsSQLDatetimeOne)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("date", ({"datatype_name": "date"}, sql_dt.MySQLDate)),
            ("datetime", ({"datatype_name": "datetime", "datetime_precision": 0}, sql_dt.MySQLDatetime)),
            ("smalldatetime", ({"datatype_name": "datetime", "datetime_precision": 0}, sql_dt.MySQLDatetime)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "date"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "date"),
            ("date", "date"),
            ("datetime", "datetime"),
            ("smalldatetime", "smalldatetime"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

class TestMsSQLDatetimeTwo:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLDatetimeTwo(datatype_name="datetime2", datetime_precision=0)
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "datetime2(0)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "datetime2", "datetime_precision": 0}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "datetime2", "datetime_precision": 0}
        assert isinstance(convert, sql_dt.MsSQLDatetimeTwo)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("time", ({"datatype_name": "time", "datetime_precision": 0}, sql_dt.MySQLDatetime)),
            ("datetimeoffset", ({"datatype_name": "datetime", "datetime_precision": 0}, sql_dt.MySQLDatetime)),
            ("datetime2", ({"datatype_name": "datetime", "datetime_precision": 0}, sql_dt.MySQLDatetime)),
        ])
    def test_datatype_convert_to_mysql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "datetime2"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "datetime2"),
            ("datetime2", "datetime2"),
            ("datetimeoffset", "datetimeoffset"),
            ("time", "time"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_datetime_precision_property_getter(self, datatype):
        datetime_precision = datatype.datetime_precision
        assert datetime_precision == 0

    @pytest.mark.parametrize(
        "datetime_precision, expected_result", [
            ("invalid", 7),
            (-1, 7),
            (8, 7),
            (5, 5),
        ])
    def test_datetime_precision_property_setter(self, datatype, datetime_precision, expected_result):
        datatype.datetime_precision = datetime_precision
        assert datatype.datetime_precision == expected_result

class TestMsSQLTimestamp:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MsSQLTimestamp()
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "timestamp"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "timestamp"}

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "timestamp"}
        assert isinstance(convert, sql_dt.MsSQLTimestamp)

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "bigint"}
        assert isinstance(convert, sql_dt.MySQLInteger)

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "timestamp"

class TestMySQLDate:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLDate()
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "date"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "date"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "date"}
        assert isinstance(convert, sql_dt.MySQLDate)

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "date"}
        assert isinstance(convert, sql_dt.MsSQLDatetimeOne)

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "date"

class TestMySQLDatetime:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLDatetime(datatype_name="datetime")
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "datetime(0)"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "datetime", "datetime_precision": 0}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "datetime", "datetime_precision": 0}
        assert isinstance(convert, sql_dt.MySQLDatetime)

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("time", ({"datatype_name": "time", "datetime_precision": 0}, sql_dt.MsSQLDatetimeTwo)),
            ("timestamp", ({"datatype_name": "datetime2", "datetime_precision": 0}, sql_dt.MsSQLDatetimeTwo)),
            ("datetime", ({"datatype_name": "datetime2", "datetime_precision": 0}, sql_dt.MsSQLDatetimeTwo)),
        ])
    def test_datatype_convert_to_mssql(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == expected_result[0]
        assert isinstance(convert, expected_result[1])

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "datetime"

    @pytest.mark.parametrize(
        "datatype_name, expected_result", [
            ("invalid", "datetime"),
            ("datetime", "datetime"),
            ("timestamp", "timestamp"),
            ("time", "time"),
        ])
    def test_datatype_name_property_setter(self, datatype, datatype_name, expected_result):
        datatype.datatype_name = datatype_name
        assert datatype.datatype_name == expected_result

    def test_datetime_precision_property_getter(self, datatype):
        datetime_precision = datatype.datetime_precision
        assert datetime_precision == 0

    @pytest.mark.parametrize(
        "datetime_precision, expected_result", [
            ("invalid", 6),
            (-1, 6),
            (8, 6),
            (5, 5),
        ])
    def test_datetime_precision_property_setter(self, datatype, datetime_precision, expected_result):
        datatype.datetime_precision = datetime_precision
        assert datatype.datetime_precision == expected_result

class TestMySQLYear:

    @pytest.fixture()
    def datatype(self):
        datatype = sql_dt.MySQLYear()
        return datatype

    def test_datatype_format(self, datatype):
        datatype_format = datatype.datatype_format()
        assert datatype_format == "year"

    def test_datatype_parameters(self, datatype):
        datatype_parameters = datatype.datatype_parameters()
        assert datatype_parameters == {"datatype_name": "year"}

    def test_datatype_convert_to_mysql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MYSQL)
        assert convert.datatype_parameters() == {"datatype_name": "year"}
        assert isinstance(convert, sql_dt.MySQLYear)

    def test_datatype_convert_to_mssql(self, datatype):
        convert = datatype.datatype_convert(SQLSystem.MSSQL)
        assert convert.datatype_parameters() == {"datatype_name": "int"}
        assert isinstance(convert, sql_dt.MsSQLInteger)

    def test_datatype_name_property_getter(self, datatype):
        datatype_name = datatype.datatype_name
        assert datatype_name == "year"
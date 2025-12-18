import types
from typing import Union, Any
from dataclasses import dataclass, field, fields
from enum import Enum

from google.cloud import bigquery
from yaml import safe_load

from src.class_table import DimTable, FakTable
from src.functions import set_and_check_envs


class YamlValueError(BaseException):
    """
    Custom exception raised when yaml value is not valid
    """
    def __init__(self, message: str):
        super().__init__(message)


class Types(Enum):
    """
    Enum class to represent allowed field types
    """
    INTEGER = "INTEGER"
    STRING = "STRING"
    DATE = "DATE"
    DATETIME = "DATETIME"

class ModeTypes(Enum):
    """Enum class to represent allowed field modes"""
    REQUIRED = "REQUIRED"

class TableTypes(Enum):
    """
    Enum class to represent allowed table types
    """
    DIM = "DIM"
    FAK = "FAK"




@dataclass
class BaseModel:
    """
    All models representing an object in a yaml file must inherit from this class.
    Its only method is _verify_field_models. This method verifies that
    the input types are correct, if not correct it raises an YamlValueError.
    """


    def _verify_field_model_types(self):
        invalid_fields = []
        for f in fields(self):
            field_name = f.name
            field_value = getattr(self, field_name)
            expected_field_value_type = f.type
            actual_field_value_type = type(field_value)
            field_default = f.default



            if isinstance(expected_field_value_type, types.GenericAlias): # Compound types
                expected_field_value_type = expected_field_value_type.__origin__

            if field_default and not field_value: # if default != None & value == None
                field_error = f"{field_name}: cannot be empty."
                invalid_fields.append(field_error)

            if not isinstance(field_value, expected_field_value_type) and  field_value: # actual_type != expected type & not None
                field_error_message = (f"{field_name}: "
                                       f"actual_type={actual_field_value_type}, "
                                       f"expected_type= {expected_field_value_type}")

                invalid_fields.append(field_error_message)

        if invalid_fields:
            error_message = (f"Fields must not contain invalid field values:\n"
                             f"{"\n".join(invalid_fields)}")

            raise YamlValueError(error_message)


@dataclass
class FieldModel(BaseModel):
    """
    Class representing a column in the yaml file. It inherits from BaseModel and uses
    the inherited method _check_field_model_types to check that all input types are correct.
    In addition, field_type and mode can only take values contained in FieldTypes and
    ModeTypes respectively.

    - _verify_field_type: checks if value passed into field_type is allowed

    - _verify_mode: checks if value passed into mode is allowed

    -  as_bq_schema_field:  returns a bigquery.SchemaField instance.

    -  create_from_yaml_dict: returns a FieldModel instance from a dictionary
    """
    name: str
    field_type: str
    description: str
    mode: str = field(default=None)
    max_length: int = field(default=None)



    def __post_init__(self):
        self._verify_field_model_types()
        self._verify_field_type()
        self._verify_mode()


    def _verify_mode(self):
        if self.mode and (self.mode.upper() not in ModeTypes):
            error_message = (f"{self.name}.mode: must be one of the following values:"
                             f" {[field_mode.value for field_mode in ModeTypes]}."
                             f" Got {self.mode.upper()}")


            raise YamlValueError(error_message)

    def _verify_field_type(self):
        if self.field_type.upper() not in FieldTypes:
            error_message = (f"{self.name}.field_type: must be one of the following values:"
                             f" {[field_type.value for field_type in FieldTypes]}."
                             f" Got {self.field_type.upper()}.")

            raise YamlValueError(error_message)

    def as_bq_schema_field(self) -> bigquery.SchemaField:
        return bigquery.SchemaField(
            name=self.name,
            description=self.description,
            field_type=self.field_type,
            mode=self.mode,
            max_length=self.max_length)

    @staticmethod
    def create_from_yaml_dict(yaml_dict: dict[str, Any]) -> "FieldModel":
        name = yaml_dict.get("name")
        field_type = yaml_dict.get("field_type")
        description = yaml_dict.get("description")
        mode = yaml_dict.get("mode")
        max_length = yaml_dict.get("max_length")


        return FieldModel(name=name,
                          field_type=field_type,
                          description=description,
                          mode=mode,
                          max_length=max_length)

@dataclass
class TableModel(BaseModel):
    """
    Class representing a table in the yaml file. It inherits from BaseModel and uses
    the inherited method _check_field_model_types to check that all input types are correct.
    In addition, table_type  can only take values contained in TableTypes.
    Furthermore, the elements of cols can only be instances of FieldModel.

    - _verify_table_type:  checks if value passed into table_type is allowed

    - _verify_field_cols_type:  checks that the list is not empty and that all elements are
                                of type FieldModel

    -  as_table: returns either a DimTable or a FakTable instance depending on table_type value.

    -  create_from_yaml_dict: returns a FieldModel instance from a dictionary
    """

    name: str
    table_type: str
    description:str
    cols: list[FieldModel]
    check_col: str = field(default=None)

    def __post_init__(self):
        self._verify_field_model_types()
        self._verify_check_col()
        self._verify_table_type()
        self._verify_field_cols_type()

    def _verify_check_col(self):
        if self.table_type.upper() ==  TableTypes.FAK.value and not self.check_col:
            raise YamlValueError("check_col: must not be empty.")

    def _verify_table_type(self):
        if self.table_type.upper() not in TableTypes:
            error_message = (f"table_type: must be one of the following values:"
                             f" {[table_type.value for table_type in TableTypes]}."
                             f" Got {self.table_type.upper()}.")

            raise YamlValueError(error_message)

    def _verify_field_cols_type(self):
        if not self.cols:
            raise YamlValueError("cols: must contain at least one column.")

        invalid_elements = []
        for col in self.cols:
            if not isinstance(col, FieldModel):
                invalid_elements.append(col)

        error_message = (f"Elements of cols must be valid values. "
                         f"Invalid elements: {invalid_elements}")

        if invalid_elements:
            raise YamlValueError(error_message)

    def as_table(self) -> Union[DimTable, FakTable]:
        cols = [col.as_bq_schema_field() for col in self.cols]

        if self.table_type.upper() == TableTypes.DIM.value:
            table = DimTable(name=self.name,description=self.description,cols=cols)
        else:
            table = FakTable(name=self.name,description=self.description,cols=cols, check_col=self.check_col)

        return table

    @staticmethod
    def create_from_yaml_dict(yaml_dict: dict[str, Any]) -> "TableModel":
       name = yaml_dict.get("name")
       table_type = yaml_dict.get("table_type")
       description = yaml_dict.get("description")
       raw_cols: list[dict[str, Any]] = yaml_dict.get("cols")

       check_col = yaml_dict.get("check_col")

       try:
           cols = [FieldModel.create_from_yaml_dict(yaml_dict=raw_col) for raw_col in raw_cols]

       except YamlValueError as error:
           raise YamlValueError(f"Error in table: {name}!\n"
                                f"{error}")

       try:
           table_model = TableModel(name=name,
                                    table_type=table_type,
                                    description=description,
                                    cols=cols,
                                    check_col=check_col)

       except YamlValueError as error:
            raise YamlValueError(f"Error in table: {name}!\n"
                                 f"{error}")

       return table_model

if __name__ == "__main__":
    set_and_check_envs()

    with open("config_tables.yaml", 'r') as file:
        tables = safe_load(file)

    yaml_table = tables["tables"][0]
    test_table = TableModel.create_from_yaml_dict(yaml_dict=yaml_table)
    print(test_table)
    test_table_table = test_table.as_table()

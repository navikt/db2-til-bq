from typing import Optional, Union
from typing_extensions import Self
from enum import Enum

from google.cloud import bigquery
from pydantic import BaseModel, field_validator, Field, model_validator

from src.class_table import DimTable, FakTable

# TODO feilrapportering på hvilken tabell og hvilken kolonne som ev. er feil i .yaml fila
# TODO vurder å splitte opp fila da den er lang


class YamlValueError(BaseException):
    """
    Custom exception raised when yaml value is not valid
    """

    def __init__(self, message: str):
        super().__init__(message)


class ColDataTypes(Enum):
    """
    Enum class to represent allowed field types.
    Should not be expanded with numeric or decimal. ibm_db returns these as strings.
    """

    INTEGER = "INTEGER"
    STRING = "STRING"
    DATE = "DATE"
    DATETIME = "DATETIME"


class ColMode(Enum):
    """Enum class to represent allowed field modes"""

    REQUIRED = "REQUIRED"


class TableTypes(Enum):
    """
    Enum class to represent allowed table types
    """

    DIM = "DIM"
    FAK = "FAK"


class ColumnModel(BaseModel):
    name: str
    col_data_type: str
    description: str
    mode: Optional[str] = None
    max_length: Optional[int] = Field(default=None, gt=0)

    def to_bq_schema_field(self) -> bigquery.SchemaField:
        return bigquery.SchemaField(
            name=self.name,
            field_type=self.col_data_type,
            description=self.description,
            mode=self.mode,
            max_length=self.max_length,
        )

    @staticmethod
    def from_dict(col_dict: dict) -> "ColumnModel":
        name = col_dict.get("name")
        col_data_type = col_dict.get("col_data_type")
        description = col_dict.get("description")
        mode = col_dict.get("mode")
        max_length = col_dict.get("max_length")

        return ColumnModel(
            name=name,
            col_data_type=col_data_type,
            description=description,
            mode=mode,
            max_length=max_length,
        )

    @field_validator("col_data_type")
    @classmethod
    def validate_col_data_type(cls, col_data_type: str) -> str:
        if col_data_type.upper() not in ColDataTypes:
            raise YamlValueError(
                f"'col_data_type': must be one of the following values: "
                f"{[data_type.value for data_type in ColDataTypes]}. "
                f"Got '{col_data_type}'. (not case sensitive)"
            )
        return col_data_type

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, mode: Optional[str]) -> str:
        if mode and mode.upper() not in ColMode:
            raise YamlValueError(
                f"'mode': must be one of the following values: "
                f"{[mode_type.value for mode_type in ColMode]}. "
                f"Got '{mode}'. (not case sensitive)"
            )
        return mode


class TableModel(BaseModel):
    name: str
    table_type: str
    description: str
    cols: list[ColumnModel]
    check_col: Optional[str] = None
    order_cols: Optional[list[str]] = None

    def to_table_object(self) -> Union[DimTable, FakTable]:
        cols = [col.to_bq_schema_field() for col in self.cols]

        if self.table_type.upper() == TableTypes.DIM.value:
            return DimTable(
                name=self.name,
                description=self.description,
                cols=cols,
            )
        else:
            return FakTable(
                name=self.name,
                description=self.description,
                cols=cols,
                check_col=self.check_col,
                order_cols=self.order_cols,
            )

    @staticmethod
    def from_dict(table_dict: dict) -> "TableModel":
        name = table_dict.get("name")
        table_type = table_dict.get("table_type")
        description = table_dict.get("description")
        cols = [ColumnModel.from_dict(col) for col in table_dict.get("cols")]
        check_col = table_dict.get("check_col")
        order_cols = table_dict.get("order_cols")

        return TableModel(
            name=name,
            table_type=table_type,
            description=description,
            cols=cols,
            check_col=check_col,
            order_cols=order_cols,
        )

    @field_validator("table_type")
    @classmethod
    def validate_table_type(cls, table_type: str) -> str:
        if table_type.upper() not in TableTypes:
            raise YamlValueError(
                f"'table_type': must be one of the following values: "
                f"{[table_type.value for table_type in TableTypes]}. "
                f"Got '{table_type}'. (not case sensitive)"
            )
        return table_type

    @field_validator("cols")
    @classmethod
    def validate_cols(cls, cols: list[ColumnModel]) -> list[ColumnModel]:
        if not cols or len(cols) == 0:
            raise YamlValueError("'cols' must have at least one column defined.")
        return cols

    @model_validator(mode="after")
    def validate_check_col(self) -> Self:
        # TODO: forenkle if (fordi det ser mye ut)
        # sjekk at er FAK
        if self.table_type.upper() == TableTypes.FAK.value:
            # sjekk at check_col er satt (gitt at det er FAK)
            if not self.check_col:
                raise YamlValueError("Must have 'check_col' for FAK tables.")

            # sjekk at check_col er en av kolonnenavnene
            if self.check_col not in [col.name for col in self.cols]:
                raise YamlValueError(
                    f"'check_col' must be one of the column names defined in 'cols'."
                    f" Got '{self.check_col}'."
                )

        # hvis ikke FAK, så DIM (er allerede validert)
        else:
            if self.check_col:
                raise YamlValueError("Don't set 'check_col' for DIM tables.")
        return self

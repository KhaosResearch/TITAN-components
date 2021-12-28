from dataclasses import dataclass

from drama.core.model import _BaseSimpleTabularDataset
from drama.datatype import (
    DataType,
    is_integer,
    is_string,
    is_float,
    is_boolean,
)


@dataclass
class ExcelDataset(DataType):
    resource: str = is_string()
    file_format: str = is_string(".xlsx")


@dataclass
class HTMLFile(DataType):
    resource: str = is_string()


@dataclass
class Png(DataType):
    resource: str = is_string()


@dataclass
class CompressFile(DataType):
    resource: str = is_string()


@dataclass
class XMLFile(DataType):
    resource: str = is_string()


@dataclass
class JSONFile(DataType):
    resource: str = is_string()


@dataclass
class Pdf(DataType):
    resource: str = is_string()
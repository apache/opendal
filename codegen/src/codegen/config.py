import dataclasses
import enum
from typing import List


@enum.unique
class FieldType(enum.Enum):
    Nothing = enum.auto()
    Bool = enum.auto()
    Int = enum.auto()
    Long = enum.auto()
    Float = enum.auto()
    Double = enum.auto()
    String = enum.auto()


@enum.unique
class RefinedFieldType(enum.Enum):
    Nothing = enum.auto()
    RustUsize = enum.auto()


@dataclasses.dataclass
class ConfigField:
    name: str
    desc: str

    ty: FieldType
    refined_ty: RefinedFieldType

    required: bool
    sensitive: bool

    # default value for the config field if not set
    default: str
    # a list of available values for the config field
    available: List[str]
    # a list of example values for the config field
    example: List[str]


@dataclasses.dataclass
class Config:
    name: str
    desc: str
    fields: List[ConfigField]

    # Rust's #[cfg(...)] conditions for the config
    rust_cfg_cond: str

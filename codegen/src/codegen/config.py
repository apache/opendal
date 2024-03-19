import dataclasses
import enum
from typing import List, Optional

import humps


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

    ty: Optional[FieldType] = None
    refined_ty: Optional[RefinedFieldType] = None

    required: bool = False
    sensitive: bool = False

    # default value for the config field if not set
    default: str = ""
    # a list of available values for the config field
    available: List[str] = dataclasses.field(default_factory=list)
    # a list of example values for the config field
    example: List[str] = dataclasses.field(default_factory=list)

    ###########
    # RustGen #
    ###########
    def rust_type(self) -> str:
        ty = self._rust_type()
        if self.required:
            return ty
        return f"Option<{ty}>"

    def _rust_type(self) -> str:
        match self.refined_ty:
            case RefinedFieldType.Nothing:
                pass
            case RefinedFieldType.RustUsize:
                return "usize"
        match self.ty:
            case FieldType.Nothing:
                raise ValueError("Field type is not set")
            case FieldType.Bool:
                return "bool"
            case FieldType.Int:
                return "i32"
            case FieldType.Long:
                return "i64"
            case FieldType.Float:
                return "f32"
            case FieldType.Double:
                return "f64"
            case FieldType.String:
                return "String"
        raise ValueError(f"Unknown field type: ({self.ty}, {self.refined_ty})")

    def rust_comment(self) -> str:
        res = ""

        for line in self.desc.strip().splitlines():
            if line:
                res += f"/// {line}\n"
            else:
                res += "///\n"

        if self.default:
            res += "///\n"
            res += f"/// Default to `{self.default}` if not set.\n"

        if self.required:
            res += "///\n"
            res += "/// Required.\n"

        if self.example:
            res += "///\n"
            res += "/// For examples:\n"
            for example in self.example:
                res += f"/// - `{example}`\n"

        if self.available:
            res += "///\n"
            res += "/// Available values:\n"
            for available in self.available:
                res += f"/// - `{available}`\n"

        return res

    def rust_debug_field(self) -> str:
        if not self.sensitive:
            return f"&self.{self.name}"
        elif self.required:
            return f"desensitize_secret(&self.{self.name})"
        else:
            return f"&self.{self.name}.as_deref().map(desensitize_secret)"


@dataclasses.dataclass
class Config:
    name: str
    desc: str = ""
    fields: List[ConfigField] = dataclasses.field(default_factory=list)

    # Rust's #[cfg(...)] conditions for the config
    rust_cfg_cond: str = ""

    ###########
    # RustGen #
    ###########

    def rust_struct_name(self) -> str:
        return f"{humps.pascalize(self.name)}Config"

from typing import Final, final

from _typeshed import Incomplete

from .operator import AsyncOperator, Operator

__version__: Final[str]

@final
class DeleteOptions:
    """Options for `delete` operations."""

@final
class ListOptions:
    """Options for `list` operations."""

@final
class ReadOptions:
    """Options for `read` operations."""

@final
class StatOptions:
    """Options for `stat` operations."""

@final
class WriteOptions:
    """Options for `write` operations."""

def _reconstruct_async_operator(scheme: str, map: dict[str, str]) -> AsyncOperator:
    """
    Rebuild an [`AsyncOperator`] while unpickling.

    See [`_reconstruct_operator`] for why a dedicated reconstructor is used.
    """

def _reconstruct_operator(scheme: str, map: dict[str, str]) -> Operator:
    """
    Rebuild a blocking [`Operator`] while unpickling.

    Routes through `from_uri`, not the scheme-based `__new__`, whose scheme
    normalization would corrupt a URI held in `__scheme`. Bare schemes work too:
    the core resolves both through the same path.
    """

def __getattr__(name: str) -> Incomplete: ...

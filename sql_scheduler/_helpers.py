import os

from ._constants import _SIMPLE_OUTPUT_ENVVAR

_SIMPLE_OUTPUT = bool(os.environ.get(_SIMPLE_OUTPUT_ENVVAR, False))


def w_print(content: str, end="\n"):
    if not _SIMPLE_OUTPUT:
        print("\x1b[2K\r", end="")
    print(content, end=end)

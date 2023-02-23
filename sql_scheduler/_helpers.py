import os
from typing import List, Tuple

from ._constants import _SIMPLE_OUTPUT_ENVVAR

_SIMPLE_OUTPUT = bool(os.environ.get(_SIMPLE_OUTPUT_ENVVAR, False))


def w_print(content: str, end="\n"):
    if not _SIMPLE_OUTPUT:
        print("\x1b[2K\r", end="")
    print(content, end=end)


def pad_string(s: str, width: int):
    return f'{s}{" "*(width - len(s))}'


def construct_table(headers: List[str], rows: List[tuple], delimiter: str = " | "):
    column_widths = [0 for _ in headers]
    for row in rows + [headers]:
        for column_index in range(len(column_widths)):
            if len(row[column_index]) > column_widths[column_index]:
                column_widths[column_index] = len(row[column_index])

    horizontal_line = "-" * (sum(column_widths) + len(delimiter) * len(headers))
    return "\n".join(
        [
            horizontal_line,
            delimiter.join(
                [
                    pad_string(header, column_widths[column_index])
                    for column_index, header in enumerate(headers)
                ]
            ),
            horizontal_line,
        ]
        + [
            delimiter.join(
                [
                    pad_string(row[column_index], column_widths[column_index])
                    for column_index, _ in enumerate(headers)
                ]
            )
            for row in rows
        ]
        + [horizontal_line]
    )

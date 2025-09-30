import re
from typing import Union
from pathlib import Path, PurePosixPath

TransportPath = Union[str, Path, PurePosixPath]

_MAGIC_CHECK = re.compile('[*?[]')

class InternalError(ValueError):
    """Error raised when there is an internal error of AiiDA."""

def has_magic(string: TransportPath):
    string = str(string)
    """Return True if the given string contains any special shell characters."""
    return _MAGIC_CHECK.search(string) is not None

class TransportInternalError(InternalError):
    """Raised if there is a transport error that is raised to an internal error (e.g.
    a transport method called without opening the channel first).
    """



"""Miscellaneous functions for escaping strings."""

def escape_for_bash(str_to_escape, use_double_quotes=False):
    """This function takes any string and escapes it in a way that
    bash will interpret it as a single string.

    Explanation:

    At the end, in the return statement, the string is put within single
    quotes. Therefore, the only thing that I have to escape in bash is the
    single quote character. To do this, I substitute every single
    quote ' with '"'"' which means:

    First single quote: exit from the enclosing single quotes

    Second, third and fourth character: "'" is a single quote character,
    escaped by double quotes

    Last single quote: reopen the single quote to continue the string

    Finally, note that for python I have to enclose the string '"'"'
    within triple quotes to make it work, getting finally: the complicated
    string found below.

    :param str_to_escape: the string to escape.
    :param use_double_quotes: boolean, if ``True``, use double quotes instead of single quotes.
    :return: the escaped string.
    """
    if str_to_escape is None:
        return ''

    str_to_escape = str(str_to_escape)
    if use_double_quotes:
        escaped_quotes = str_to_escape.replace('"', '''"'"'"''')
        escaped = f'"{escaped_quotes}"'
    else:
        escaped_quotes = str_to_escape.replace("'", """'"'"'""")
        escaped = f"'{escaped_quotes}'"

    return escaped


# Mapping of "SQL" tokens into corresponding regex expressions
SQL_TO_REGEX_TOKENS = [  # Remember in the strings below we have to escape backslashes as well for python...
    # so '\\\\' is actually a string with two backslashes, '\\' a string with a single backslash, ...
    ('\\\\', re.escape('\\')),  # Double slash should be interpreted as a literal single backslash by regex
    ('\\%', re.escape('%')),  # literal '\%' should be interpreted as literal % by regex
    ('\\_', re.escape('_')),  # literal '\_' should be interpreted as literal _ by regex
    ('%', '.*'),
    ('_', '.'),
]


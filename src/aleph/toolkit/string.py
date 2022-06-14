def truncate_log(s: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncates a string down to the specified length. Appends a user-defined suffix
    if the string is truncated. The string is returned unmodified if it is shorter
    than the specified maximum length.

    :param s: String to truncate.
    :param max_length: Maximum size of the returned string.
    :param suffix: String to be appended to the base string if it is truncated.
    :return: A string of len <= max_length where the end of the original string
             is replaced by the suffix if it is longer than max_length.
    """

    if len(s) <= max_length:
        return s

    return s[: max_length - len(suffix)] + suffix

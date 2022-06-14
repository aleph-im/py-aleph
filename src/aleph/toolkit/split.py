from typing import Callable, Iterable, List, Tuple, TypeVar


T = TypeVar("T")


def split_iterable(iterable: Iterable[T], cond: Callable[[T], bool]) -> Tuple[List[T], List[T]]:
    """
    Splits an iterable in two based on the condition and returns the two lists as
    a (matches, others) tuple.
    :param iterable: The iterable to split.
    :param cond: A condition to verify for each element of the iterable.
    """

    matches = []
    others = []

    for x in iterable:
        if cond(x):
            matches.append(x)
        else:
            others.append(x)

    return matches, others

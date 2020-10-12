"""Functions to parse text fields"""


def get_float(str1: str) -> float:
    """
    Get a float from a string
    :param str1: String containing float
    :return: Float value of string or None
    """
    work = str1.strip()
    if work > "":
        try:
            result = float(work)
        except ValueError:
            result = None
    else:
        result = None

    return result


def get_str(str1: str) -> str:
    """
    Return a non-null string or None
    :param str1: Input string
    :return: Non-null string or None
    """
    work = str1.strip()
    return work if work > "" else None


def get_int(str1: str) -> int:
    """
    Return an integer or None.  If it sees a ".", it does not consider it a valid integer.
    :param str1: String containing integer
    :return: Integer or None
    """
    work = str1.strip()
    if work > "":
        if work.find(".") > -1:
            result = None
        else:
            try:
                result = int(work)
            except ValueError:
                result = None
    else:
        result = None

    return result

"""Attribs custom converters."""

from re import sub
from typing import List


def convert_to_dataset_name(dataset: str) -> str:
    """Make a string a valid dataset name.

    Args:
        dataset (str): The string to be converted

    Returns:
        str: A valid dataset name.
    """
    temp = sub(r"[!?',;.-]", "", dataset)
    return sub(r" +", "_", temp).lower()


def version_to_int(version: str) -> int:
    """Remove the leading v from version and convert to int.

    Args:
        version (str): The version number.

    Returns:
        int: The integer version number.
    """
    return int(version.replace("v", ""))


def split_keywords(keywords: str) -> List[str]:
    """Split keywords separated by a | into a list of keywords.

    Args:
        keywords (str): The keywords to be separated.

    Returns:
        List[str]: The list of keywords.
    """
    return keywords.split("|")

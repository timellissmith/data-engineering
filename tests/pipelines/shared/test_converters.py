"""Test converters for attribs."""

import pytest

from pipelines.shared.converters import (convert_to_dataset_name,
                                         split_keywords, version_to_int)


def test_version_to_int_passes():
    """Test that the version to int returns an int when given a v + number."""
    assert version_to_int("v1") == 1


def test_version_to_int_fails_with_non_version():
    """Test that the version to int fails when given an invalid version."""
    with pytest.raises(ValueError):
        version_to_int("vv")


def test_split_keywords_returns_correct():
    """Test that the split_keywords functions."""
    assert split_keywords("a|b|c") == ["a", "b", "c"]


@pytest.mark.parametrize(
    "dataset_name, expected",
    [("Big lebowski", "big_lebowski"), ("Check!", "check"), ("a ; b", "a_b")],
)
def test_convert_to_dataset_name(dataset_name, expected):
    """Test the convert to dataset name handles spaces and special characters correctly."""
    assert convert_to_dataset_name(dataset_name) == expected

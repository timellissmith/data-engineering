"""Tests for attribs custom validators."""


import pytest

from pipelines.shared.validators import (check_for_duplicate_col_names,
                                         is_greater_than_one,
                                         validate_column_name)


def test_is_greater_than_one_returns_passes():
    """Test that the greater than one test passes when given correct values."""
    is_greater_than_one("", "", 2)


def test_is_greater_than_one_returns_fails():
    """Test that the greater than one test fails when given incorrect values."""
    with pytest.raises(AssertionError, match="Value must be positive"):
        is_greater_than_one("", "", 0)


def test_validate_column_name_passes():
    """Test that the validate_column_names test passes when given correct value."""
    validate_column_name("", "", "a_column")


@pytest.mark.parametrize(
    "col_name, match",
    [
        ("&", "column_name must be number, integer or _"),
        ("_TABLE_", "column_name is reserved"),
        ("1TABLE_", "column_name must not start with a number"),
    ],
)
def test_validate_fails_invalid_entries(col_name, match):
    """Test that the validate_column_names test passes when given a non-allowed character."""
    with pytest.raises(AssertionError, match=match):
        validate_column_name("", "", col_name)


def test_check_for_duplicate_col_names():
    """Test the check for duplicate column names correctly picks up duplicates."""

    class Cols:
        def __init__(self, column_name):
            self.column_name = column_name

    cols = [Cols("a"), Cols("a")]
    with pytest.raises(AssertionError, match="No duplicate column names allowed"):
        check_for_duplicate_col_names("", "", cols)
    cols = [Cols("a"), Cols("b")]
    check_for_duplicate_col_names("", "", cols)

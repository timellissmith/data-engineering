"""Custom Validators for attribs."""


def is_greater_than_one(_instance, _attribute, value):
    """Validate that the input is positive.

    Args:
        value (_type_): The value to be evaluated.
    """
    assert value >= 1, "Value must be positive"


def validate_column_name(_instance, _attribute, value):
    """
    Assert that the column name is valid.

    Args:
        value (_type_): The column name to be validated.
    """
    assert value not in [
        "_TABLE_",
        "_FILE_",
        "_PARTITION",
        "_ROW_TIMESTAMP",
        "__ROOT__",
        "_COLIDENTIFIER",
    ], "column_name is reserved"
    assert any(
        c.isalnum() or c == "_" for c in value
    ), "column_name must be number, integer or _"
    assert not value[0].isnumeric(), "column_name must not start with a number"


def check_for_duplicate_col_names(instance, attribute, value):
    """Check that no column names are duplicated."""
    col_names = [col.column_name for col in value]
    assert len(col_names) == len(set(col_names)), "No duplicate column names allowed"

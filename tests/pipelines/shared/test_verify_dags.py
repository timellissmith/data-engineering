"""Test that the DAGS pass inbuilt verification tests."""


from pipelines.shared.dag_loaders import generate_dags


def test_generate_dags_no_errors():
    """Test that the generate_dags function doesn't error."""
    test_path = "metadata/main_dags"
    generate_dags(test_path)

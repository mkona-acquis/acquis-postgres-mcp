"""Basic import tests for temporal module."""


def test_temporal_imports():
    """Test that temporal modules can be imported successfully."""
    from postgres_mcp.temporal import TemporalManager
    from postgres_mcp.temporal import TemporalQuery

    assert TemporalManager is not None
    assert TemporalQuery is not None


def test_temporal_manager_init():
    """Test that TemporalManager can be instantiated."""
    from unittest.mock import Mock

    from postgres_mcp.temporal import TemporalManager

    mock_driver = Mock()
    manager = TemporalManager(mock_driver)

    assert manager is not None
    assert manager.sql_driver == mock_driver


def test_temporal_query_init():
    """Test that TemporalQuery can be instantiated."""
    from unittest.mock import Mock

    from postgres_mcp.temporal import TemporalQuery

    mock_driver = Mock()
    query = TemporalQuery(mock_driver)

    assert query is not None
    assert query.sql_driver == mock_driver

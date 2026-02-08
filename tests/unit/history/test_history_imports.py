"""Basic import tests for history tracking module."""


def test_history_imports():
    """Test that history tracking modules can be imported successfully."""
    from postgres_mcp.history import HistoryManager
    from postgres_mcp.history import HistoryQuery

    assert HistoryManager is not None
    assert HistoryQuery is not None


def test_history_manager_init():
    """Test that HistoryManager can be instantiated."""
    from unittest.mock import Mock

    from postgres_mcp.history import HistoryManager

    mock_driver = Mock()
    manager = HistoryManager(mock_driver)

    assert manager is not None
    assert manager.sql_driver == mock_driver


def test_history_query_init():
    """Test that HistoryQuery can be instantiated."""
    from unittest.mock import Mock

    from postgres_mcp.history import HistoryQuery

    mock_driver = Mock()
    query = HistoryQuery(mock_driver)

    assert query is not None
    assert query.sql_driver == mock_driver

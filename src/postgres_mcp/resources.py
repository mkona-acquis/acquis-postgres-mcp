"""Dummy MCP resources for testing Claude Desktop resource discovery."""

from mcp.server.fastmcp import FastMCP


def register_resources(mcp: FastMCP) -> None:
    """Register all resources with the MCP server."""

    @mcp.resource("dummy://hello")
    def hello_resource() -> str:
        """A dummy resource that returns a greeting. Used for testing resource discovery in Claude Desktop."""
        return "Hello from acquis-postgres-mcp! If you can see this, resource discovery is working."

    @mcp.resource("dummy://server-info")
    def server_info_resource() -> str:
        """Returns basic server info. Used for testing resource discovery in Claude Desktop."""
        return "Server: acquis-postgres-mcp\nStatus: running\nThis is a dummy resource for testing."

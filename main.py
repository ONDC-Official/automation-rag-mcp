from mcp_server.server import mcp

if __name__ == "__main__":
    # In fastmcp 3.2.0+, transport="sse" mounts the MCP app at /mcp by default
    mcp.run(transport="sse", host="0.0.0.0", port=8004)

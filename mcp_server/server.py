import logging
import asyncio
import re
from typing import Optional, List, Dict
from fastmcp import FastMCP

from mcp_server.config import settings, ALL_PARTITIONS
from mcp_server.milvus_client import vector_search, list_all_collections
from mcp_server.neo4j_client import (
    neo4j_fulltext_search,
    neo4j_nodes_for_action,
    neo4j_rules_for_field,
    neo4j_session_chain,
    neo4j_cross_action_conflicts,
    list_actions,
    list_domains,
    list_versions,
)

log = logging.getLogger(__name__)

mcp = FastMCP(
    "ONDC RAG Server",
    instructions="You are an expert search assistant. Use these tools to query ONDC documentation and Graph Rules.",
)


async def get_schema_cache(query_from: str = "all") -> Dict:
    # Issue E Fix: Assign empty defaults BEFORE try-except to prevent UnboundLocalError
    actions, domains, versions = [], [], []
    collections = []

    if query_from in ["neo4j", "all"]:
        try:
            actions, domains, versions = await asyncio.wait_for(
                asyncio.gather(list_actions(), list_domains(), list_versions()),
                timeout=10.0,
            )
        except Exception as e:
            log.error(f"Neo4j schema fetch failed: {e}")

    if query_from in ["milvus", "all"]:
        try:
            collections = await list_all_collections()
        except Exception as e:
            log.error(f"Milvus collections fetch failed: {e}")

    return {
        "milvus_collections": collections,
        "neo4j_domains": domains,
        "neo4j_versions": versions,
        "neo4j_actions": actions,
        "partitions": ALL_PARTITIONS,
    }


# TOOL 1: Schema Discovery
@mcp.tool()
async def discover_schema(query_from: str = "all") -> Dict:
    """Discover searchable domains, versions, actions, or vector collections."""
    log.info(f"TOOL | discover_schema (query_from={query_from})")
    return await get_schema_cache(query_from=query_from)


# TOOL 2: Combined RAG Search
@mcp.tool()
async def smart_search(
    query: str,
    limit: int = settings.retrieval_limit,
    domain: str = "",
    version: str = "",
    action: str = "",
    query_from: str = "all",
) -> Dict:
    """Search vector embeddings and fulltext graph for ONDC specifications."""
    log.info(
        f"TOOL | smart_search: query='{query[:50]}...' domain='{domain}' version='{version}' action='{action}' query_from='{query_from}'"
    )
    results = []
    tasks = []

    # 1. Milvus Vector Task
    if query_from in ["milvus", "all"]:
        coll_name = None
        if domain and version:
            coll_name = re.sub(r"[^a-zA-Z0-9_]", "_", f"{domain}_{version}")

        log.debug(f"SEARCH | Adding Milvus task (coll={coll_name or 'default'})")
        tasks.append(
            vector_search(
                query, collection_name=coll_name, top_k=limit, action_filter=action
            )
        )

    # 2. Neo4j Text Task
    if query_from in ["neo4j", "all"]:
        log.debug("SEARCH | Adding Neo4j fulltext task")
        tasks.append(
            neo4j_fulltext_search(
                query, limit=limit, domain=domain, version=version, action=action
            )
        )

    if not tasks:
        log.warning(f"SEARCH | No tasks to execute for query_from='{query_from}'")
        return {"results": []}

    search_res = await asyncio.gather(*tasks, return_exceptions=True)

    for i, res in enumerate(search_res):
        source = "milvus" if i == 0 and query_from != "neo4j" else "neo4j"
        if isinstance(res, Exception):
            log.error(f"SEARCH | {source} task failed: {res}")
            continue

        log.info(f"SEARCH | {source} returned {len(res)} hits")
        for item in res:
            if hasattr(item, "model_dump"):
                results.append(item.model_dump())
            else:
                results.append(item)

    log.info(f"TOOL | smart_search complete. Total results: {len(results)}")
    return {"results": results[:limit]}


# Deterministic Neo4j Tools
@mcp.tool()
async def get_action_rules(
    action: str,
    domain: str = "",
    version: str = "",
    skip: int = 0,
    limit: int = settings.retrieval_limit,
) -> Dict:
    """Find validation rules belonging to a specific ONDC Action."""
    log.info(f"TOOL | get_action_rules: action='{action}' domain='{domain}'")
    rules = await neo4j_nodes_for_action(action, domain, version, skip, limit)
    return {"action": action, "rules": rules}


@mcp.tool()
async def get_field_rules(
    jsonpath: str, domain: str = "", version: str = "", skip: int = 0, limit: int = 25
) -> Dict:
    """Find validation rules that apply to a specific JSONPath Field."""
    log.info(f"TOOL | get_field_rules: jsonpath='{jsonpath}'")
    rules = await neo4j_rules_for_field(jsonpath, domain, version, skip, limit)
    return {"jsonpath": jsonpath, "rules": rules}


@mcp.tool()
async def get_session_flow() -> Dict:
    """Get tracing for ONDC session tokens (Who saves a key and who reads it)."""
    log.info("TOOL | get_session_flow")
    return {"flow": await neo4j_session_chain()}


@mcp.tool()
async def get_cross_conflicts() -> Dict:
    """Find Enum fields that vary / conflict across different actions."""
    log.info("TOOL | get_cross_conflicts")
    return {"conflicts": await neo4j_cross_action_conflicts()}


if __name__ == "__main__":
    mcp.run()

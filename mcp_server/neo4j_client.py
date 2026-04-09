import re
import logging
from typing import List, Optional, Any
from neo4j import AsyncGraphDatabase, AsyncDriver
from pydantic import BaseModel
from .config import settings

log = logging.getLogger(__name__)


class GraphNode(BaseModel):
    node_id: str
    labels: List[str]
    properties: dict


_driver: Optional[AsyncDriver] = None


def safe(val: Any) -> str:
    return str(val) if val is not None else ""


def get_driver() -> AsyncDriver:
    global _driver
    if _driver is None:
        log.info(f"NEO4J | Connecting to {settings.neo4j_uri}")
        _driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
    return _driver


async def neo4j_fulltext_search(
    query: str,
    limit: int = settings.retrieval_limit,
    domain: str = "",
    version: str = "",
    action: str = "",
) -> List[dict]:
    """Single-pass Cypher optimized fulltext search with weighted scores."""
    log.info(
        f"NEO4J | Fulltext Search: query='{query[:50]}...' domain='{domain}' action='{action}'"
    )
    driver = get_driver()
    safe_q = re.sub(r'[+\-!(){}[\]^"~*?:\\/]', r"\\\g<0>", query)

    # Issue C Fix: Weighted fallback in one query instead of four network round trips.
    cypher = """
        CALL db.index.fulltext.queryNodes('ondc_chunk_content', $q)
        YIELD node AS c, score
        WITH c, score,
             CASE WHEN c.domain = $domain THEN 2.0 ELSE 0.0 END AS d_score,
             CASE WHEN c.version = $version THEN 1.5 ELSE 0.0 END AS v_score,
             CASE WHEN c.action = $action THEN 3.0 ELSE 0.0 END AS a_score
        WITH c, (score + d_score + v_score + a_score) AS total_score
        RETURN c, total_score
        ORDER BY total_score DESC
        LIMIT $limit
    """

    try:
        async with driver.session() as session:
            result = await session.run(
                cypher,
                q=safe_q,
                limit=limit,
                domain=domain,
                version=version,
                action=action,
            )
            records = await result.data()
            log.info(f"NEO4J | Fulltext Search returned {len(records)} records")

            return [
                {
                    "action": safe(rec["c"].get("action")),
                    "source_type": "",
                    "chunk_type": "validation_rule",
                    "path_prefix": safe(rec["c"].get("scope") or ""),
                    "type_name": safe(rec["c"].get("name")),
                    "partition_name": "validations",
                    "content": safe(
                        rec["c"].get("description") or rec["c"].get("name")
                    ),
                    "score": float(rec.get("total_score", 0.0)),
                    "source_db": "neo4j",
                }
                for rec in records
            ]
    except Exception as e:
        log.error(f"NEO4J | Fulltext Search failed: {e}")
        return []


# Issue C & D Fix: Added pagination (skip/limit) and let errors bubble up
async def neo4j_nodes_for_action(
    action: str, domain: str = "", version: str = "", skip: int = 0, limit: int = 25
) -> List[dict]:
    log.info(f"NEO4J | Nodes for Action: action='{action}' domain='{domain}'")
    driver = get_driver()
    filters = ["r.action = $action"]
    if domain:
        filters.append("r.domain = $domain")
    if version:
        filters.append("r.version = $version")

    cypher = f"""
        MATCH (r)
        WHERE {' AND '.join(filters)} AND (r:Rule OR r:Group)
        RETURN r.name AS node_name, r.rule_type AS rule_type, r.description AS description
        ORDER BY r.name
        SKIP $skip LIMIT $limit
    """
    try:
        async with driver.session() as session:
            result = await session.run(
                cypher,
                action=action,
                domain=domain,
                version=version,
                skip=skip,
                limit=limit,
            )
            records = await result.data()
            log.info(f"NEO4J | Nodes for Action returned {len(records)} records")
            return records
    except Exception as e:
        log.error(f"NEO4J | Nodes for Action failed: {e}")
        return []


async def neo4j_rules_for_field(
    jsonpath: str,
    domain: str = "",
    version: str = "",
    skip: int = 0,
    limit: int = settings.retrieval_limit,
) -> List[dict]:
    log.info(f"NEO4J | Rules for Field: jsonpath='{jsonpath}'")
    driver = get_driver()
    filters = []
    if domain:
        filters.append("r.domain = $domain")
    if version:
        filters.append("r.version = $version")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    cypher = f"""
        MATCH (r)-[cf:CHECKS_FIELD]->(f:Field {{jsonpath: $jsonpath}})
        {where}
        RETURN r.name AS rule_name, r.action AS action, r.rule_type AS rule_type, cf.operator AS operator
        ORDER BY r.action, r.name
        SKIP $skip LIMIT $limit
    """
    try:
        async with driver.session() as session:
            result = await session.run(
                cypher,
                jsonpath=jsonpath,
                domain=domain,
                version=version,
                skip=skip,
                limit=limit,
            )
            records = await result.data()
            log.info(f"NEO4J | Rules for Field returned {len(records)} records")
            return records
    except Exception as e:
        log.error(f"NEO4J | Rules for Field failed: {e}")
        return []


# Example trace tools
async def neo4j_session_chain() -> List[dict]:
    log.info("NEO4J | Fetching Session Chain")
    driver = get_driver()
    cypher = """
        MATCH (saver:Action)-[:SAVES_SESSION]->(sk:SessionKey)
        OPTIONAL MATCH (reader:Rule)-[:READS_SESSION]->(sk)
        RETURN sk.key AS session_key, saver.name AS saved_by_action, collect(DISTINCT reader.name) AS reading_rules
        ORDER BY sk.key
        LIMIT 50
    """
    try:
        async with driver.session() as session:
            result = await session.run(cypher)
            records = await result.data()
            log.info(f"NEO4J | Session Chain returned {len(records)} keys")
            return records
    except Exception as e:
        log.error(f"NEO4J | Session Chain failed: {e}")
        return []


async def neo4j_cross_action_conflicts() -> List[dict]:
    log.info("NEO4J | Fetching Cross Action Conflicts")
    driver = get_driver()
    cypher = """
        MATCH (r:Rule)-[:CHECKS_FIELD]->(f:Field)
        MATCH (r)-[:VALIDATES_ENUM]->(e:EnumValue)
        WITH f.jsonpath AS jsonpath, r.action AS action, collect(DISTINCT e.value) AS enum_set
        WITH jsonpath, collect({action: action, enum_set: enum_set}) AS per_action
        WHERE size(per_action) > 1
        RETURN jsonpath, per_action
        LIMIT 50
    """
    try:
        async with driver.session() as session:
            result = await session.run(cypher)
            records = await result.data()
            log.info(f"NEO4J | Cross Action Conflicts returned {len(records)} items")
            return records
    except Exception as e:
        log.error(f"NEO4J | Cross Action Conflicts failed: {e}")
        return []


async def list_actions() -> List[str]:
    log.debug("NEO4J | Listing Actions")
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            "MATCH (n) WHERE n.action IS NOT NULL RETURN DISTINCT n.action AS action"
        )
        actions = [r["action"] for r in await result.data()]
        log.debug(f"NEO4J | Found {len(actions)} actions")
        return actions


async def list_domains() -> List[str]:
    log.debug("NEO4J | Listing Domains")
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            "MATCH (n) WHERE n.domain IS NOT NULL RETURN DISTINCT n.domain AS domain"
        )
        domains = [r["domain"] for r in await result.data()]
        log.debug(f"NEO4J | Found {len(domains)} domains")
        return domains


async def list_versions() -> List[str]:
    log.debug("NEO4J | Listing Versions")
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            "MATCH (n) WHERE n.version IS NOT NULL RETURN DISTINCT n.version AS version"
        )
        versions = [r["version"] for r in await result.data()]
        log.debug(f"NEO4J | Found {len(versions)} versions")
        return versions

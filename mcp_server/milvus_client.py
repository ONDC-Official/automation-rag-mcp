import asyncio
import logging
from typing import List, Optional
from pymilvus import MilvusClient
from pydantic import BaseModel
from langchain_ollama import OllamaEmbeddings
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from .config import settings, ALL_PARTITIONS

log = logging.getLogger(__name__)


class VectorSearchResult(BaseModel):
    id: str
    score: float
    content: str
    action: str
    chunk_type: str
    source_type: str
    path_prefix: str
    partition_name: str
    metadata: dict


_milvus: Optional[MilvusClient] = None
_embeddings: Optional[OllamaEmbeddings] = None
_llm: Optional[ChatOllama | ChatOpenAI] = None


def get_milvus() -> MilvusClient:
    global _milvus
    if _milvus is None:
        log.info(f"Connecting to Milvus at {settings.milvus_uri}")
        _milvus = MilvusClient(uri=settings.milvus_uri)
    return _milvus


def get_embeddings() -> OllamaEmbeddings:
    global _embeddings
    if _embeddings is None:
        _embeddings = OllamaEmbeddings(
            model=settings.embedding_model,
            base_url=settings.ollama_base_url,
        )
    return _embeddings


def get_llm(use_cache: bool = False):
    global _llm
    if _llm is not None:
        return _llm

    log.debug(f"Initializing LLM: {settings.generation_model}")
    if settings.ollama_api_key:
        _llm = ChatOpenAI(
            model=settings.generation_model,
            base_url=settings.ollama_cloud_url,
            api_key=settings.ollama_api_key,
            cache=use_cache,
        )
    else:
        _llm = ChatOllama(
            model=settings.generation_model,
            base_url=settings.ollama_base_url,
        )
    return _llm


async def list_all_collections() -> List[str]:
    """List all Milvus collections off-loaded to a thread to avoid blocking."""
    log.info("MILVUS | Listing all collections")
    client = get_milvus()
    colls = await asyncio.to_thread(client.list_collections)
    log.info(f"MILVUS | Found {len(colls)} collections")
    return colls


async def embed(text: str) -> List[float]:
    log.debug(f"EMBED | Embedding text: {text[:50]}...")
    embeddings = get_embeddings()
    # OllamaEmbeddings standard aembed_query is already async
    vec = await embeddings.aembed_query(f"search_document: {text}")
    log.debug(f"EMBED | Embedding complete. Vector dim: {len(vec)}")
    return vec


async def vector_search(
    query: str,
    collection_name: Optional[str] = None,
    top_k: int = settings.retrieval_limit,
    partitions: Optional[List[str]] = None,
    action_filter: Optional[str] = None,
    chunk_type_filter: Optional[str] = None,
) -> List[VectorSearchResult]:
    """Search Milvus without blocking the Async Event Loop."""
    target_coll = collection_name or settings.dynamic_collection_name
    # target_coll = f"ONDC_{target_coll}"
    log.info(
        f"MILVUS | Search: coll={target_coll} query='{query[:50]}...' top_k={top_k}"
    )

    query_vec = await embed(query)
    client = get_milvus()
    target_parts = partitions or ALL_PARTITIONS

    filters = []
    if action_filter:
        filters.append(f'action == "{action_filter}"')
    if chunk_type_filter:
        filters.append(f'chunk_type == "{chunk_type_filter}"')
    filter_expr = " && ".join(filters) if filters else None

    output_fields = [
        "content",
        "action",
        "chunk_type",
        "source_type",
        "path_prefix",
        "partition_name",
    ]

    # Fix Issue A: PyMilvus client.search is synchronous. Run in a separate thread.
    try:
        log.debug(
            f"MILVUS | Executing search on {target_coll} with filters: {filter_expr}"
        )
        results = await asyncio.to_thread(
            client.search,
            collection_name=target_coll,
            data=[query_vec],
            limit=top_k,
            partition_names=target_parts if target_parts else None,
            output_fields=output_fields,
            filter=filter_expr,
            search_params={"metric_type": "COSINE", "params": {"ef": 128}},
        )
    except Exception as e:
        log.warning(f"MILVUS | Partition search failed. Trying global: {e}")
        # Fallback to search without partitions
        results = await asyncio.to_thread(
            client.search,
            collection_name=target_coll,
            data=[query_vec],
            limit=top_k,
            output_fields=output_fields,
            filter=filter_expr,
            search_params={"metric_type": "COSINE", "params": {"ef": 128}},
        )

    out = []
    if results and len(results) > 0:
        log.info(f"MILVUS | Search returned {len(results[0])} hits")
        for hit in results[0]:
            e = hit["entity"]
            out.append(
                VectorSearchResult(
                    id=str(hit["id"]),
                    score=round(float(hit["distance"]), 5),
                    content=e.get("content", ""),
                    action=e.get("action", ""),
                    chunk_type=e.get("chunk_type", ""),
                    source_type=e.get("source_type", ""),
                    path_prefix=e.get("path_prefix", ""),
                    partition_name=e.get("partition_name", ""),
                    metadata={k: v for k, v in e.items() if k not in output_fields},
                )
            )
    else:
        log.info("MILVUS | Search returned 0 results")
    return out

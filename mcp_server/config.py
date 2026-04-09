import re
import logging
from pathlib import Path
from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

# 1. Setup Base Directory
# Since this file is in mcp_server/config.py, .parent.parent points to the root
BASE_DIR = Path(__file__).resolve().parent.parent


# 2. Define Settings Class first
class Settings(BaseSettings):
    # This config tells Pydantic how to handle the .env file
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Don't crash if .env has extra vars
        case_sensitive=False,  # Allows 'neo4j_uri' to match 'NEO4J_URI'
    )

    # Ollama
    ollama_base_url: str = "http://localhost:11434"
    ollama_cloud_url: str = "https://ollama.com/v1"
    ollama_api_key: str = ""
    embedding_model: str = "nomic-embed-text-v2-moe"
    generation_model: str = "qwen3-coder:480b-cloud"
    embed_dimension: int = 768

    # Milvus
    milvus_host: str = "milvus"
    milvus_port: int = 19530
    collection_name: str = "ondc_api_docs"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"  # "bolt://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"

    # ONDC domain
    default_domain: str = "ONDC:FIS12"
    default_api_version: str = "2.0.2"
    retrieval_limit: int = 60
    query_from: str = "all"

    # LLM Config
    llm_temperature: float = 0.3
    llm_max_tokens: int = 2048
    langchain_cache: bool = False

    # Misc
    ingest_mode: str = "all"
    log_level: str = "INFO"

    @computed_field
    @property
    def milvus_uri(self) -> str:
        return (
            f"http://{self.milvus_host}:{self.milvus_port}"
        )

    @computed_field
    @property
    def dynamic_collection_name(self) -> str:
        raw_name = f"{self.default_domain}_{self.default_api_version}"
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", raw_name)
        if not re.match(r"^[a-zA-Z_]", safe_name):
            safe_name = "col_" + safe_name
        return safe_name


# 3. Instantiate the settings
settings = Settings()

# 4. Configure Logging (Using the settings we just instantiated)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)
log.info(
    f"Loaded config from {BASE_DIR}. Target collection: {settings.dynamic_collection_name}"
)

# 5. Define Constants
PARTITION_MAP: dict[str, str] = {
    "type_definition": "type_definitions",
    "schema_path:api_schema": "api_schema_paths",
    "schema_path:validation": "valid_paths",
    "validation_rule": "validations",
    "x_validation": "x_validations",
}

ALL_PARTITIONS: list[str] = list(PARTITION_MAP.values())

MAX_ACTION = 100
MAX_SOURCE_TYPE = 50
MAX_CHUNK_TYPE = 50
MAX_PATH_PREFIX = 500
MAX_TYPE_NAME = 200
MAX_PARTITION = 50
MAX_CONTENT = 32_000

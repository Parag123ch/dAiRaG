from __future__ import annotations

import importlib.util
import os
import socket
from pathlib import Path
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

BACKEND_DIR = Path(__file__).resolve().parent
SERVICE_ROOT = BACKEND_DIR.parent
REPO_ROOT = SERVICE_ROOT.parent
ENV_PATHS = [
    SERVICE_ROOT / '.env',
    REPO_ROOT / '.env',
]
_LOADED_ENV_PATHS: list[str] = []


def load_runtime_env() -> list[str]:
    global _LOADED_ENV_PATHS
    loaded: list[str] = []
    if load_dotenv is None:
        _LOADED_ENV_PATHS = loaded
        return loaded

    for path in ENV_PATHS:
        if path.exists():
            load_dotenv(path, override=False)
            loaded.append(str(path))
    _LOADED_ENV_PATHS = loaded
    return loaded


def loaded_env_paths() -> list[str]:
    if not _LOADED_ENV_PATHS:
        load_runtime_env()
    return list(_LOADED_ENV_PATHS)


def parse_neo4j_host_port(uri: str) -> tuple[str, int]:
    parsed = urlparse(uri)
    host = parsed.hostname or '127.0.0.1'
    port = parsed.port or 7687
    return host, port


def tcp_reachable(host: str, port: int, timeout_seconds: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def package_installed(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def runtime_status() -> dict[str, object]:
    env_files = load_runtime_env()
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://127.0.0.1:7687')
    neo4j_username = os.getenv('NEO4J_USERNAME', os.getenv('NEO4J_USER', 'neo4j'))
    neo4j_database = os.getenv('NEO4J_DATABASE') or None
    host, port = parse_neo4j_host_port(neo4j_uri)

    neo4j_driver = package_installed('neo4j')
    dotenv_package = load_dotenv is not None
    raw_turing_key = (os.getenv('TURING_API_KEY') or '').strip()
    raw_turing_gw_key = ((os.getenv('TURING_API_GW_KEY') or '0c015800-dcba-448d-94bb-d01a56b0d22c')).strip()
    raw_turing_authorization = ((os.getenv('TURING_AUTHORIZATION') or 'Basic YWRtaW46VHVyaW5nQDEyMw==')).strip()
    raw_langfuse_public_key = (os.getenv('LANGFUSE_PUBLIC_KEY') or '').strip()
    raw_langfuse_secret_key = (os.getenv('LANGFUSE_SECRET_KEY') or '').strip()
    langfuse_base_url = (os.getenv('LANGFUSE_HOST') or os.getenv('LANGFUSE_BASE_URL') or 'https://cloud.langfuse.com').strip()
    langfuse_judge_enabled = os.getenv('LANGFUSE_JUDGE_ENABLED', 'true').strip().lower() not in {'0', 'false', 'no'}
    langfuse_judge_model = os.getenv('LANGFUSE_JUDGE_MODEL') or os.getenv('TURING_ANSWER_MODEL') or os.getenv('TURING_CYPHER_MODEL', os.getenv('TURING_MODEL', 'gpt-4'))
    turing_key = bool(raw_turing_key)
    turing_gw_key = bool(raw_turing_gw_key)
    turing_authorization = bool(raw_turing_authorization)
    langfuse_public_key = bool(raw_langfuse_public_key)
    langfuse_secret_key = bool(raw_langfuse_secret_key)
    neo4j_password = bool(os.getenv('NEO4J_PASSWORD'))
    bolt_reachable = tcp_reachable(host, port)
    cypher_ready = neo4j_driver and neo4j_password and bolt_reachable
    llm_provider = 'turing' if turing_key and turing_gw_key and turing_authorization else None
    llm_cypher_ready = cypher_ready and llm_provider is not None
    langfuse_installed = package_installed('langfuse')
    langfuse_enabled = langfuse_installed and langfuse_public_key and langfuse_secret_key

    missing: list[str] = []
    if not neo4j_driver:
        missing.append('neo4j_driver')
    if not dotenv_package:
        missing.append('python_dotenv')
    if not neo4j_password:
        missing.append('NEO4J_PASSWORD')
    if not bolt_reachable:
        missing.append('neo4j_server')
    if llm_provider is None:
        if not turing_key:
            missing.append('TURING_API_KEY')
        if not turing_gw_key:
            missing.append('TURING_API_GW_KEY')
        if not turing_authorization:
            missing.append('TURING_AUTHORIZATION')

    return {
        'envFilesLoaded': env_files,
        'llmProvider': llm_provider,
        'langfuseSdkInstalled': langfuse_installed,
        'langfusePublicKeyConfigured': langfuse_public_key,
        'langfuseSecretKeyConfigured': langfuse_secret_key,
        'langfuseBaseUrl': langfuse_base_url,
        'langfuseEnabled': langfuse_enabled,
        'langfuseJudgeEnabled': langfuse_judge_enabled,
        'langfuseJudgeModel': langfuse_judge_model,
        'turingApiKeyConfigured': turing_key,
        'turingApiGwKeyConfigured': turing_gw_key,
        'turingAuthorizationConfigured': turing_authorization,
        'turingBaseUrl': (os.getenv('TURING_BASE_URL') or 'https://kong.turing.com/api/v2/chat'),
        'turingProviderName': (os.getenv('TURING_PROVIDER') or 'openai'),
        'turingModel': os.getenv('TURING_CYPHER_MODEL', os.getenv('TURING_MODEL', 'gpt-4')),
        'neo4jDriverInstalled': neo4j_driver,
        'dotenvInstalled': dotenv_package,
        'neo4jPasswordConfigured': neo4j_password,
        'neo4jUri': neo4j_uri,
        'neo4jHost': host,
        'neo4jPort': port,
        'neo4jUsername': neo4j_username,
        'neo4jDatabase': neo4j_database,
        'neo4jBoltReachable': bolt_reachable,
        'cypherRuntimeReady': cypher_ready,
        'llmCypherRuntimeReady': llm_cypher_ready,
        'missing': missing,
    }

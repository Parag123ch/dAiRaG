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

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
ENV_PATHS = [
    SCRIPT_DIR / '.env',
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


def looks_like_nvidia_api_key(value: str | None) -> bool:
    return bool((value or '').strip()) and (value or '').strip().startswith('nvapi-')


def looks_like_openrouter_api_key(value: str | None) -> bool:
    return bool((value or '').strip()) and (value or '').strip().startswith('sk-or-v1-')


def runtime_status() -> dict[str, object]:
    env_files = load_runtime_env()
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://127.0.0.1:7687')
    neo4j_username = os.getenv('NEO4J_USERNAME', os.getenv('NEO4J_USER', 'neo4j'))
    neo4j_database = os.getenv('NEO4J_DATABASE') or None
    host, port = parse_neo4j_host_port(neo4j_uri)

    openai_package = package_installed('openai')
    gemini_package = package_installed('google.genai')
    neo4j_driver = package_installed('neo4j')
    dotenv_package = load_dotenv is not None
    raw_nvidia_key = (os.getenv('NVIDIA_API_KEY') or '').strip()
    raw_openrouter_key = (os.getenv('OPENROUTER_API_KEY') or '').strip()
    nvidia_key = looks_like_nvidia_api_key(raw_nvidia_key)
    openrouter_key = bool(raw_openrouter_key) or looks_like_openrouter_api_key(raw_nvidia_key)
    openai_key = bool(os.getenv('OPENAI_API_KEY'))
    gemini_key = bool(os.getenv('GEMINI_API_KEY'))
    neo4j_password = bool(os.getenv('NEO4J_PASSWORD'))
    bolt_reachable = tcp_reachable(host, port)
    cypher_ready = neo4j_driver and neo4j_password and bolt_reachable
    llm_provider = None
    if openai_package and nvidia_key:
        llm_provider = 'nvidia'
    elif openai_package and openrouter_key:
        llm_provider = 'openrouter'
    elif gemini_package and gemini_key:
        llm_provider = 'gemini'
    elif openai_package and openai_key:
        llm_provider = 'openai'
    llm_cypher_ready = cypher_ready and llm_provider is not None

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
        if not gemini_package:
            missing.append('google_genai_package')
        if not nvidia_key:
            missing.append('NVIDIA_API_KEY')
        if not openrouter_key:
            missing.append('OPENROUTER_API_KEY')
        if not gemini_key:
            missing.append('GEMINI_API_KEY')
        if not openai_package:
            missing.append('openai_package')
        if not openai_key:
            missing.append('OPENAI_API_KEY')

    return {
        'envFilesLoaded': env_files,
        'llmProvider': llm_provider,
        'nvidiaApiKeyConfigured': nvidia_key,
        'nvidiaBaseUrl': (os.getenv('NVIDIA_BASE_URL') or 'https://integrate.api.nvidia.com/v1'),
        'nvidiaModel': os.getenv('NVIDIA_CYPHER_MODEL', os.getenv('NVIDIA_MODEL', 'nvidia/nemotron-3-super-120b-a12b')),
        'openrouterApiKeyConfigured': openrouter_key,
        'openrouterKeySource': 'OPENROUTER_API_KEY' if raw_openrouter_key else ('NVIDIA_API_KEY' if looks_like_openrouter_api_key(raw_nvidia_key) else None),
        'openrouterBaseUrl': (os.getenv('OPENROUTER_BASE_URL') or 'https://openrouter.ai/api/v1'),
        'openrouterModel': os.getenv('OPENROUTER_CYPHER_MODEL', os.getenv('OPENROUTER_MODEL', 'nvidia/nemotron-3-super-120b-a12b:free')),
        'geminiPackageInstalled': gemini_package,
        'geminiApiKeyConfigured': gemini_key,
        'openaiPackageInstalled': openai_package,
        'openaiApiKeyConfigured': openai_key,
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

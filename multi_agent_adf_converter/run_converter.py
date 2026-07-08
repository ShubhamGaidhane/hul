#!/usr/bin/env python3
"""Main entry point for the ADF to Databricks Lakeflow multi-agent converter.

Usage:
    # First, copy and configure .env file:
    cp .env.example .env
    # Edit .env with your API keys and Azure credentials

    # Run with environment variables for Azure credentials
    python run_converter.py --subscription-id <sub> --resource-group <rg> --factory-name <adf>

    # Run with explicit credentials
    python run_converter.py --subscription-id <sub> --resource-group <rg> --factory-name <adf> \\
        --tenant-id <tenant> --client-id <client> --client-secret <secret>

    # Run for specific pipelines only
    python run_converter.py --subscription-id <sub> --resource-group <rg> --factory-name <adf> \\
        --pipelines "PL_Master,PL_Child"

    # Run from a cached JSON file (no Azure connection needed)
    python run_converter.py --cached-json path/to/cached_adf.json --factory-name <adf>
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Load .env file if it exists (before any other imports)
from dotenv import load_dotenv
env_path = Path(__file__).resolve().parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=str(env_path))
    print(f"📄 Loaded configuration from {env_path}")
else:
    # Try parent directory
    parent_env = Path(__file__).resolve().parent.parent / '.env'
    if parent_env.exists():
        load_dotenv(dotenv_path=str(parent_env))

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from langchain_openai import ChatOpenAI

from multi_agent_adf_converter import Orchestrator
from multi_agent_adf_converter.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Multi-Agent ADF to Databricks Lakeflow Converter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Azure configuration
    azure_group = parser.add_argument_group("Azure Configuration")
    azure_group.add_argument(
        "--subscription-id",
        help="Azure subscription ID (or set AZURE_SUBSCRIPTION_ID env var)",
    )
    azure_group.add_argument(
        "--resource-group",
        help="Azure resource group name (or set AZURE_RESOURCE_GROUP env var)",
    )
    azure_group.add_argument(
        "--factory-name",
        help="ADF factory name (or set AZURE_ADF_FACTORY env var)",
    )
    azure_group.add_argument(
        "--tenant-id",
        help="Azure tenant ID (or set AZURE_TENANT_ID env var)",
    )
    azure_group.add_argument(
        "--client-id",
        help="Azure service principal client ID (or set AZURE_CLIENT_ID env var)",
    )
    azure_group.add_argument(
        "--client-secret",
        help="Azure service principal client secret (or set AZURE_CLIENT_SECRET env var)",
    )

    # Pipeline selection
    parser.add_argument(
        "--pipelines",
        help="Comma-separated list of specific pipeline names to convert",
    )
    parser.add_argument(
        "--cached-json",
        help="Path to a pre-existing ADF catalog JSON file (bypasses Azure scan)",
    )

    # LLM configuration
    llm_group = parser.add_argument_group("LLM Configuration")
    llm_group.add_argument(
        "--llm-model",
        default=os.getenv("LLM_MODEL", "openai.gpt-5-mini"),
        help="LLM model name (or set LLM_MODEL env var)",
    )
    llm_group.add_argument(
        "--llm-base-url",
        default=os.getenv("LLM_BASE_URL", "https://openai.generative.engine.capgemini.com/v1"),
        help="LLM API base URL (or set LLM_BASE_URL env var)",
    )
    llm_group.add_argument(
        "--llm-api-key",
        help="LLM API key (or set LLM_API_KEY env var)",
    )

    # Output configuration
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", "output"),
        help="Directory for output files",
    )
    parser.add_argument(
        "--cache-dir",
        default=os.getenv("CACHE_DIR", "cache"),
        help="Directory for cache files",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.getenv("MAX_RETRIES", "3")),
        help="Maximum conversion retries per pipeline",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument(
        "--log-file",
        help="Path to log file (logs to stderr if not specified)",
    )

    return parser.parse_args()


def create_llm(args: argparse.Namespace) -> ChatOpenAI:
    """Create the LLM instance from arguments or environment.

    Args:
        args: Parsed command line arguments.

    Returns:
        Configured ChatOpenAI instance.
    """
    api_key = args.llm_api_key or os.getenv("LLM_API_KEY")

    if not api_key:
        logger.warning(
            "No LLM API key provided. Set --llm-api-key or LLM_API_KEY env var."
            " Using placeholder key - this may fail."
        )
        api_key = "placeholder"

    return ChatOpenAI(
        model=args.llm_model,
        base_url=args.llm_base_url,
        api_key=api_key,
        default_headers={"x-api-key": api_key} if api_key else None,
        temperature=0.1,
    )


def load_cached_json(cached_json_path: str) -> Dict[str, Any]:
    """Load ADF catalog from a cached JSON file.

    Args:
        cached_json_path: Path to JSON file.

    Returns:
        Parsed JSON data.
    """
    path = Path(cached_json_path)
    if not path.exists():
        raise FileNotFoundError(f"Cached JSON file not found: {cached_json_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Handle both raw catalog and cache wrapper format
    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    logger.info("Loaded cached ADF JSON", path=str(path))
    return data


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    args = parse_args()

    # Resolve Azure configuration
    subscription_id = args.subscription_id or os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = args.resource_group or os.getenv("AZURE_RESOURCE_GROUP")
    factory_name = args.factory_name or os.getenv("AZURE_ADF_FACTORY")

    # Parse pipeline names
    pipeline_names: Optional[List[str]] = None
    if args.pipelines:
        pipeline_names = [p.strip() for p in args.pipelines.split(",") if p.strip()]

    # Load cached JSON if provided
    cached_adf_json: Optional[Dict[str, Any]] = None
    if args.cached_json:
        try:
            cached_adf_json = load_cached_json(args.cached_json)
        except FileNotFoundError as e:
            logger.error(str(e))
            return 1

    # Create LLM
    try:
        llm = create_llm(args)
    except Exception as e:
        logger.error("Failed to create LLM", error=str(e))
        return 1

    # Create orchestrator
    orchestrator = Orchestrator(
        llm=llm,
        output_dir=args.output_dir,
        cache_dir=args.cache_dir,
        max_retries=args.max_retries,
        log_level=args.log_level,
        log_file=args.log_file,
    )

    # If using cached JSON, we need to inject it into the state
    # The orchestrator will handle this via the Scanner -> Analyzer flow
    if cached_adf_json:
        # Use the orchestrator's scanner's cache mechanism
        from multi_agent_adf_converter.utils.cache import CacheManager
        cache = CacheManager(cache_dir=args.cache_dir)
        cache.set(
            data=cached_adf_json,
            factory_name=factory_name or "unknown",
            pipeline_names=pipeline_names,
            metadata={"source": "cached_json", "path": args.cached_json},
        )
        logger.info("Loaded cached JSON into cache for scanner to pick up")

    # Run the conversion
    logger.info("=" * 60)
    logger.info("Starting ADF to Lakeflow Conversion")
    logger.info(f"Factory: {factory_name}")
    logger.info(f"Pipelines: {pipeline_names or 'ALL'}")
    logger.info(f"Output: {args.output_dir}")
    logger.info("=" * 60)

    if not factory_name and not cached_adf_json:
        logger.error(
            "No factory name provided. Use --factory-name or set AZURE_ADF_FACTORY env var."
        )
        return 1

    try:
        final_state = orchestrator.run(
            subscription_id=subscription_id or "",
            resource_group=resource_group or "",
            factory_name=factory_name or "unknown",
            pipeline_names=pipeline_names,
            tenant_id=args.tenant_id,
            client_id=args.client_id,
            client_secret=args.client_secret,
        )
    except Exception as e:
        logger.error("Orchestration failed", error=str(e), exc_info=True)
        return 1

    # Print summary
    summary = final_state.get_summary()
    print("\n" + "=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"  Trace ID:      {summary['trace_id']}")
    print(f"  Pipelines:     {summary['pipelines_completed']}/{summary['pipelines_total']}")
    print(f"  Successful:    {summary['success_count']}")
    print(f"  Failed:        {summary['failed_count']}")
    print(f"  Errors:        {summary['error_count']}")
    print(f"  Output path:   {final_state.final_output_path or 'N/A'}")
    print("=" * 60)

    if summary["failed_count"] > 0 or summary["error_count"] > 0:
        print("\n⚠️  Some pipelines had issues. Check the output directory for details.")
        return 1

    print("\n✅ All pipelines converted successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Scanner Agent - Fetches ADF metadata from Azure or loads from cache."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from ..models.state import OrchestrationState
from ..utils.adf_scanner import ADFScanner
from ..utils.cache import CacheManager
from ..utils.logger import get_logger
from .base_agent import BaseAgent

logger = get_logger(__name__)


class ScannerAgent(BaseAgent):
    """Agent responsible for scanning Azure Data Factory metadata.

    Either fetches fresh data from Azure via SDK or loads from
    local cache if available and valid.
    """

    def __init__(
        self,
        llm,
        cache_dir: str = "cache",
        cache_ttl_hours: int = 24,
        name: str = "scanner",
    ):
        self.cache_manager = CacheManager(
            cache_dir=cache_dir,
            cache_ttl_hours=cache_ttl_hours,
        )
        super().__init__(llm=llm, name=name)

    def _default_system_prompt(self) -> str:
        return (
            "You are the Scanner Agent — the FIRST agent in the ADF-to-Databricks Lakeflow migration pipeline. "
            "Your role is CRITICAL: you are responsible for acquiring ALL Azure Data Factory metadata "
            "that every subsequent agent depends on.\n\n"

            "=== YOUR RESPONSIBILITIES ===\n"
            "1. COORDINATE SCANNING: You orchestrate the ADFScanner utility to fetch metadata from Azure.\n"
            "   - You do NOT call Azure APIs directly — the ADFScanner utility handles that.\n"
            "   - You validate the scanner output for completeness and correctness.\n\n"
            "2. CACHE MANAGEMENT: You manage the local cache to avoid repeated Azure API calls.\n"
            "   - Check cache FIRST before scanning Azure.\n"
            "   - Cache has a configurable TTL (default 24 hours).\n"
            "   - Cache key is based on factory name + pipeline names (SHA256 hash).\n"
            "   - If cache is valid, load from cache and skip Azure entirely.\n"
            "   - If cache is expired or missing, trigger a fresh Azure scan.\n\n"
            "3. DATA VALIDATION: After scanning, you must validate the data:\n"
            "   - Ensure pipelines list is not empty (unless specific pipelines were requested).\n"
            "   - Ensure each pipeline has activities_detail populated.\n"
            "   - Ensure datasets, linked_services, and triggers are captured.\n"
            "   - Flag any anomalies (e.g., pipelines with 0 activities).\n\n"
            "4. SUMMARY GENERATION: You produce a concise summary of what was scanned:\n"
            "   - Total pipelines, datasets, linked services, triggers, integration runtimes.\n"
            "   - Pipeline names (first 10-20 for brevity).\n"
            "   - Any warnings about missing data.\n\n"

            "=== WHAT THE SCANNER COLLECTS ===\n"
            "The ADFScanner collects these asset types from Azure:\n"
            "  - pipelines: Full pipeline definitions with activities, parameters, variables, dependencies\n"
            "  - datasets: Dataset definitions with linked service references, schemas, parameters\n"
            "  - linked_services: Connection details including type, connection strings, Key Vault usage\n"
            "  - triggers: Schedule, event, and tumbling window triggers with linked pipelines\n"
            "  - integration_runtimes: IR type and configuration\n\n"

            "=== EDGE CASES TO HANDLE ===\n"
            "1. If specific pipeline names are provided but some don't exist in ADF:\n"
            "   - Log a warning for each missing pipeline.\n"
            "   - Continue with the ones that were found.\n"
            "2. If NO pipelines are found at all:\n"
            "   - Return an error — there's nothing to convert.\n"
            "3. If Azure credentials are missing:\n"
            "   - Return a clear error message telling the user which env vars to set.\n"
            "4. If the cache file is corrupted:\n"
            "   - Delete it and re-scan from Azure.\n\n"

            "=== OUTPUT ===\n"
            "After scanning, you update the state with:\n"
            "  - raw_adf_json: The complete scan result (catalog + lineage)\n"
            "  - current_step: 'scan_complete' on success, 'error' on failure\n"
            "  - total_pipelines: Count of pipelines found\n"
            "  - cache_path: Path to the cache file (if newly created)\n\n"

            "=== IMPORTANT NOTES ===\n"
            "- The ADFScanner uses Azure SDK's DataFactoryManagementClient.\n"
            "- Service Principal credentials come from environment variables (AZURE_TENANT_ID, etc.).\n"
            "- NEVER log or expose credentials, connection strings, or secrets.\n"
            "- The scan can take 1-5 minutes for large factories with hundreds of pipelines.\n"
            "- Cache is your friend — use it to speed up repeated runs."
        )

    def run(self, state: OrchestrationState) -> Dict[str, Any]:
        """Execute scanning: either load from cache or fetch from Azure.

        Args:
            state: Current orchestration state with Azure credentials.

        Returns:
            Dict with updates to apply to the state.
        """
        self.logger.info(
            "Starting scan",
            factory=state.factory_name,
            pipelines=state.pipeline_names,
        )

        # Step 1: Try loading from cache
        cached = self.cache_manager.get(
            factory_name=state.factory_name or "",
            pipeline_names=state.pipeline_names,
        )

        if cached:
            self.logger.info("Loaded ADF data from cache")
            raw_data = cached.get("data") if isinstance(cached, dict) and "data" in cached else cached
            return self._process_scan_result(raw_data, state, from_cache=True)

        # Step 2: Fetch from Azure (requires credentials in state)
        if not all([state.subscription_id, state.resource_group, state.factory_name]):
            self.logger.error("Missing Azure credentials for scanning")
            return {
                "errors": state.errors + ["Missing Azure credentials. Provide subscription_id, resource_group, factory_name."],
                "current_step": "error",
            }

        # We need credentials - they should be set on the state or passed via config
        # The actual ADFScanner needs tenant_id, client_id, client_secret
        # These are sensitive and should come from environment variables or secrets
        tenant_id = os.getenv("AZURE_TENANT_ID") or os.getenv("AZURE_CLIENT_TENANT")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")

        if not all([tenant_id, client_id, client_secret]):
            self.logger.error("Missing Azure service principal credentials in environment")
            return {
                "errors": state.errors + [
                    "Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET environment variables"
                ],
                "current_step": "error",
            }

        try:
            scanner = ADFScanner(
                subscription_id=state.subscription_id,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )

            scan_result = scanner.execute_full_scan(
                rg_name=state.resource_group,
                factory_name=state.factory_name,
                pipeline_names=state.pipeline_names,
            )

            # Cache the result
            metadata = {
                "factory": state.factory_name,
                "pipeline_count": len(scan_result["catalog"].get("pipelines", [])),
                "dataset_count": len(scan_result["catalog"].get("datasets", [])),
                "linked_service_count": len(scan_result["catalog"].get("linked_services", [])),
                "trigger_count": len(scan_result["catalog"].get("triggers", [])),
            }
            cache_path = self.cache_manager.set(
                data=scan_result,
                factory_name=state.factory_name,
                pipeline_names=state.pipeline_names,
                metadata=metadata,
            )

            self.logger.info("Cached ADF data", path=cache_path)

            return self._process_scan_result(scan_result, state, cache_path=cache_path)

        except Exception as e:
            self.logger.error("ADF scan failed", error=str(e))
            return {
                "errors": state.errors + [f"ADF scan failed: {str(e)}"],
                "current_step": "error",
            }

    def _process_scan_result(
        self,
        scan_result: Dict[str, Any],
        state: OrchestrationState,
        from_cache: bool = False,
        cache_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Process scan result and prepare state updates.

        Args:
            scan_result: The scan result dict (with 'catalog' key).
            state: Current state.
            from_cache: Whether this came from cache.
            cache_path: Path to cache file if just created.

        Returns:
            State updates dict.
        """
        catalog = scan_result.get("catalog", {})
        pipelines = catalog.get("pipelines", [])
        datasets = catalog.get("datasets", [])
        linked_services = catalog.get("linked_services", [])
        triggers = catalog.get("triggers", [])

        self.logger.info(
            "Scan summary",
            pipelines=len(pipelines),
            datasets=len(datasets),
            linked_services=len(linked_services),
            triggers=len(triggers),
        )

        # Invoke LLM to generate a scan summary
        summary_prompt = (
            f"Summarize the following ADF scan results:\n\n"
            f"- Factory: {state.factory_name}\n"
            f"- Total pipelines: {len(pipelines)}\n"
            f"- Total datasets: {len(datasets)}\n"
            f"- Total linked services: {len(linked_services)}\n"
            f"- Total triggers: {len(triggers)}\n"
            f"- Pipeline names: {[p.get('pipeline', 'unknown') for p in pipelines[:10]]}\n\n"
            f"Provide a brief summary suitable for logging."
        )

        summary = self._invoke_llm(summary_prompt, temperature=0.1)

        return {
            "raw_adf_json": scan_result,
            "current_step": "scan_complete",
            "cache_path": cache_path or state.cache_path,
            "total_pipelines": len(pipelines),
        }

"""Base agent class providing shared utilities for all agents."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from ..utils.logger import get_logger


class BaseAgent(ABC):
    """Abstract base class for all specialized agents.

    Provides shared LLM invocation, prompt templating, and logging.
    """

    def __init__(
        self,
        llm: BaseChatModel,
        name: str = "base_agent",
        system_prompt: Optional[str] = None,
    ):
        """Initialize the agent.

        Args:
            llm: LangChain chat model instance.
            name: Agent name for logging.
            system_prompt: Optional system prompt override.
        """
        self.llm = llm
        self.name = name
        self._system_prompt = system_prompt or self._default_system_prompt()
        self.logger = get_logger(f"agent.{name}")

    @abstractmethod
    def _default_system_prompt(self) -> str:
        """Return the default system prompt for this agent."""
        ...

    @abstractmethod
    def run(self, state: Any) -> Any:
        """Execute the agent's logic and return updated state.

        Args:
            state: The current OrchestrationState.

        Returns:
            Updated OrchestrationState or a dict of state updates.
        """
        ...

    def _invoke_llm(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        structured_output: Optional[Dict[str, Any]] = None,
        temperature: float = 0.1,
    ) -> str:
        """Invoke the LLM with a prompt.

        Args:
            prompt: User/content prompt.
            system_prompt: Optional override system prompt.
            structured_output: Optional JSON schema for structured output.
            temperature: LLM temperature parameter.

        Returns:
            LLM response text.
        """
        messages = [
            SystemMessage(content=system_prompt or self._system_prompt),
            HumanMessage(content=prompt),
        ]

        self.logger.debug("Invoking LLM", prompt_length=len(prompt))

        # If structured output requested, bind with response format
        if structured_output:
            llm = self.llm.with_structured_output(
                schema=structured_output,
                method="json_mode",
            )
            response = llm.invoke(messages)
        else:
            response = self.llm.invoke(messages, temperature=temperature)

        content = response.content if hasattr(response, "content") else str(response)
        self.logger.debug("LLM response received", response_length=len(content))
        return content

    def _invoke_llm_structured(
        self,
        prompt: str,
        output_schema: type,
        system_prompt: Optional[str] = None,
    ) -> Any:
        """Invoke the LLM and parse structured JSON output.

        Args:
            prompt: User/content prompt.
            output_schema: Pydantic model class for structured output.
            system_prompt: Optional override system prompt.

        Returns:
            Parsed instance of output_schema.
        """
        messages = [
            SystemMessage(content=system_prompt or self._system_prompt),
            HumanMessage(content=prompt),
        ]

        self.logger.debug("Invoking LLM with structured output")
        llm = self.llm.with_structured_output(output_schema, method="json_mode")
        response = llm.invoke(messages)
        self.logger.debug("Structured response received")
        return response

    def _parse_json_response(self, text: str) -> Dict[str, Any]:
        """Safely parse JSON from LLM response, handling markdown fences."""
        # Remove markdown code fences
        text = text.strip()
        if text.startswith("```"):
            # Find the first { or [
            start = text.find("{")
            if start == -1:
                start = text.find("[")
            if start != -1:
                text = text[start:]
            # Remove trailing ```
            end = text.rfind("}")
            if end != -1:
                text = text[: end + 1]
            end = text.rfind("]")
            if end != -1:
                text = text[: end + 1]

        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            self.logger.error("Failed to parse JSON response", error=str(e))
            return {}

    def _truncate_for_prompt(self, data: Any, max_chars: int = 8000) -> str:
        """Truncate a JSON structure for prompt inclusion.

        Args:
            data: Data to stringify.
            max_chars: Maximum characters to include.

        Returns:
            Truncated JSON string.
        """
        text = json.dumps(data, indent=2, default=str)
        if len(text) > max_chars:
            text = text[:max_chars] + f"\n... [truncated, full size: {len(text)} chars]"
        return text

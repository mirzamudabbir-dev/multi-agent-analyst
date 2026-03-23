"""Base class for all reasoning agents."""

from __future__ import annotations

import json
import os
from typing import Any, Type

from pydantic import BaseModel

from analyst.base_agent import BaseAgent
from analyst.models import AgentResult


class BaseReasoningAgent(BaseAgent):
    """A generic agent that offloads decision-making to Gemini strictly returning a Pydantic schema."""

    def generate_structured_response(self, prompt: str, schema: Type[BaseModel]) -> BaseModel | None:
        """Call Gemini and return the structured JSON strictly mapped to the provided schema."""
        try:
            from google import genai
        except ImportError:
            return None

        import time
        import re
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return None

        client = genai.Client(api_key=api_key)

        max_retries = 5  # increased
        backoff_delay = 5  # start higher for free tier

        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=genai.types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=schema,
                        temperature=0.0,
                    ),
                )
                return schema.model_validate_json(response.text)
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "resource_exhausted" in error_str:
                    # Look for retry delay hint in error message
                    # e.g. "please retry in 37s"
                    match = re.search(r"retry in ([\d\.]+)s", error_str)
                    delay = float(match.group(1)) + 1 if match else backoff_delay
                    
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                        backoff_delay *= 2
                        continue
                return None
        return None

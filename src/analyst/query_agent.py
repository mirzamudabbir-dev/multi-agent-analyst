"""LLM Query Agent to answer questions about the analyzed data."""

from __future__ import annotations

import json
import os

from analyst.state import AnalysisState


def ask_question(state: AnalysisState, question: str) -> str:
    """Uses Gemini to answer a question based on the analysis state."""
    try:
        from google import genai
    except ImportError:
        return "Error: google-genai package is not installed."

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return "Error: GEMINI_API_KEY environment variable is not set. Please run `export GEMINI_API_KEY='your-key'`"

    client = genai.Client(api_key=api_key)

    # Use cached digest if available, otherwise build minimal context
    if state.digest:
        context_str = state.digest
    else:
        context_str = f"Dataset: {state.file_path.name} ({state.row_count}×{state.col_count})"

    prompt = f"""You are an expert Data Analyst AI.
Use the following dataset digest to answer the question concisely and accurately.
Do NOT hallucinate information not present in the context.

{context_str}

Question: {question}
Answer:"""

    import time
    max_retries = 3
    backoff_delay = 2

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=genai.types.GenerateContentConfig(temperature=0.2),
            )
            return response.text or "No response generated."
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "resource_exhausted" in error_str:
                if attempt < max_retries - 1:
                    time.sleep(backoff_delay)
                    backoff_delay *= 2
                    continue
            return f"Error communicating with Gemini: {e}"
    return "Error: Max retries exceeded for Gemini API."


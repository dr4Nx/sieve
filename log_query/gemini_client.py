"""Gemini client construction for standalone and Vertex AI routes."""

import os
import sys
from typing import Tuple

from .logging_utils import Logger


def build_gemini_client(args, log: Logger) -> Tuple[object, object]:
    # Import here so the script is still importable even if lib not installed.
    try:
        from google import genai
        from google.genai import types
    except Exception as exc:
        log.error(
            "Failed to import google-genai. Did you run 'pip install --upgrade google-genai'?\n"
            + str(exc)
        )
        sys.exit(2)

    # Vertex AI route
    if args.vertex_ai:
        if not args.project or not args.location:
            log.error("--vertex-ai requires --project and --location")
            sys.exit(2)
        log.info("Initializing Gemini client via Vertex AI settings...")
        client = genai.Client(vertexai=True, project=args.project, location=args.location)
        types_mod = types
        return client, types_mod

    # Standalone Gemini API route
    api_key = args.api_key or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        log.error(
            "No API key found. Set GEMINI_API_KEY in .env, environment, or pass --api-key.\n"
            "Get a key at https://aistudio.google.com/app/apikey"
        )
        sys.exit(2)
    log.info("Initializing Gemini client with provided API key...")
    client = genai.Client(api_key=api_key)
    return client, types

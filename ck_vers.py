#!/usr/bin/env python3
"""
Print the installed revisions of all required dependencies.
Requires Python >= 3.11
"""

from importlib.metadata import version, PackageNotFoundError

packages = [
    "langgraph",
    "langchain",
    "langchain-openai",
    "langchain-anthropic",
    "langchain_community",
    "langchain_tavily",
    "langchain_mcp_adapters",
    "pydantic",
    "rich",
    "jupyter",
    "ipykernel",
    "tavily-python",
    "httpx",
    "markdownify",
    "deepagents",
]

for pkg in packages:
    try:
        print(f"{pkg}: {version(pkg)}")
    except PackageNotFoundError:
        print(f"{pkg}: NOT INSTALLED")

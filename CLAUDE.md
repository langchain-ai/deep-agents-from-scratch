# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains educational materials for building deep agents from scratch using LangGraph. It demonstrates progressive agent architectures through a series of Jupyter notebooks, starting with basic TODO list functionality and advancing to full agents with file systems and subagent spawning.

## Development Commands

### Environment Setup
```bash
# Install dependencies using uv (preferred package manager)
uv sync

# Run Jupyter notebooks
uv run jupyter notebook

# Alternative: activate virtual environment
source .venv/bin/activate
jupyter notebook
```

### Code Quality
```bash
# Run linting with ruff (notebooks and source code)
uv run ruff check
uv run ruff check notebooks/

# Auto-fix linting issues where possible
uv run ruff check --fix
uv run ruff format

# Run type checking with mypy
uv run mypy src/

# Install dev dependencies (includes ruff and mypy)
uv sync --extra dev
```

### LangGraph Studio Integration
```bash
# Start LangGraph Studio (if installed)
langgraph up

# The langgraph.json file defines two agents:
# - studio_react_agent: "./src/deep-agents-from-scratch/studio_react_agent.py:agent"
# - react_agent: "./src/deep-agents-from-scratch/react_agent.py:agent"
```

## Architecture

### Core Components

**State Management (`state.py`)**
- `DeepAgentState`: Extends LangGraph's `AgentState` with todos and files
- `Todo`: TypedDict for task tracking with status (pending/in_progress/completed)
- `file_reducer`: Merges file dictionaries in state updates

**Virtual File System (`file_tools.py`)**
- `ls()`: List files in virtual filesystem stored in agent state
- `read_file()`: Read file content with offset/limit support
- `write_file()`: Create/overwrite files in virtual filesystem
- `edit_file()`: Perform find-and-replace edits with exact string matching

**Task Planning (`todo_tool.py`)**
- `write_todos()`: Creates and updates structured task lists
- Uses LangGraph `Command` type for state updates
- Critical for context management and long-running tasks

**Agent Implementations**
- `react_agent.py`: Basic ReAct agent with internet search via Tavily
- `studio_react_agent.py`: Studio-compatible version with detailed documentation

### Tutorial Progression (Notebooks)

1. **1_todo.ipynb**: TODO list tool for task planning and progress tracking
2. **2_files.ipynb**: Virtual file system tools (read/write/edit/ls)  
3. **3_subagents.ipynb**: Task delegation and context isolation via subagents
4. **4_full_agent.ipynb**: Complete agent combining all tools and capabilities

### Key Patterns

**Context Engineering Techniques:**
- Context offloading to virtual files stored in state
- TODO lists for planning and progress tracking  
- Subagent spawning for context quarantine
- Task-specific prompt engineering

**State Management:**
- All file operations are virtual - files exist only in LangGraph state
- Enables backtracking/restarting by preserving file system state
- Todo list persisted across agent interactions

## Environment Variables

Create `.env` file in project root with required API keys:
```bash
# Required for research agents with external search
TAVILY_API_KEY=your_tavily_api_key_here

# Required for model usage - choose one or more:
# Google GenAI (recommended for getting started)
GOOGLE_API_KEY=your_google_api_key_here

# OR Anthropic
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# OR OpenAI
OPENAI_API_KEY=your_openai_api_key_here

# Optional: For evaluation and tracing
LANGSMITH_API_KEY=your_langsmith_api_key_here
LANGSMITH_TRACING=true
LANGSMITH_PROJECT=deep-agents-from-scratch
```

## Model Usage

The notebooks support multiple LLM providers. The project includes `langchain-google-genai` for Google's Gemini models:

**Google GenAI (Gemini) - Recommended:**
```python
from langchain_google_genai import ChatGoogleGenerativeAI

model = ChatGoogleGenerativeAI(
    model="models/gemini-1.5-flash",  # Fast and cost-effective
    temperature=0.0
)
# Alternative: "models/gemini-1.5-pro" for more capable reasoning
```

**Anthropic (Claude):**
```python
from langchain_anthropic import ChatAnthropic

model = ChatAnthropic(
    model="claude-3-5-sonnet-20241022",
    temperature=0.0
)
```

**OpenAI (GPT):**
```python
from langchain_openai import ChatOpenAI

model = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.0
)
```

## Testing

No specific test framework is configured. Test agents by:
1. Running notebooks interactively
2. Testing via LangGraph Studio interface
3. Direct Python script execution

## Important Notes

- Virtual file system is ephemeral - exists only during agent execution
- TODO tool should limit to ONE task in_progress at a time
- File edit operations require exact string matching
- Agents can use Google Gemini, Claude, or GPT models (examples use Google Gemini for accessibility)
- Rich formatting utilities in `notebooks/utils.py` for message display
- Notebook `0_create_agent.ipynb` demonstrates basic agent setup with Google Gemini models
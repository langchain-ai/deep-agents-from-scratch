# 🧱 Deep Agents from Scratch

<img width="720" height="289" alt="Screenshot 2025-08-12 at 2 13 54 PM" src="https://github.com/user-attachments/assets/90e5a7a3-7e88-4cbe-98f6-5b2581c94036" />

[Deep Research](https://academy.langchain.com/courses/deep-research-with-langgraph) broke out as one of the first major agent use-cases along with coding. Now, we've seeing an emergence of general purpose agents that can be used for a wide range of tasks. For example, [Manus](https://manus.im/blog/Context-Engineering-for-AI-Agents-Lessons-from-Building-Manus) has gained significant attention and popularity for long-horizon tasks; the average Manus task uses ~50 tool calls!. As a second example, Claude Code is being used generally for tasks beyond coding. Careful review of the [context engineering patterns](https://docs.google.com/presentation/d/16aaXLu40GugY-kOpqDU4e-S0hD1FmHcNyF0rRRnb1OU/edit?slide=id.p#slide=id.p) across these popular "deep" agents shows some common approaches:

* **Task planning (e.g., TODO), often with recitation**
* **Context offloading to file systems**
* **Context isolation through sub-agent delegation**

This course will show how to implement these patterns from scratch using LangGraph! 

## 🚀 Quickstart 

### Prerequisites

- Ensure you're using Python 3.11 or later.
- This version is required for optimal compatibility with LangGraph.
```bash
python3 --version
```
- [uv](https://docs.astral.sh/uv/) package manager
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# Update PATH to use the new uv version
export PATH="/Users/$USER/.local/bin:$PATH"
```

### Installation

1. Clone the repository:
```bash
git clone https://github.com/langchain-ai/deep_agents_from_scratch
cd deep_agents_from_scratch
```

2. Install the package and dependencies (this automatically creates and manages the virtual environment):
```bash
uv sync
```

3. Create a `.env` file in the project root with your API keys:
```bash
# Create .env file
touch .env
```

Add your API keys to the `.env` file:
```env
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

**Note on Model Providers:**
- **Google GenAI**: Easiest to get started with. Get a free API key from [Google AI Studio](https://aistudio.google.com/app/apikey). Works with models like `gemini-2.5-flash` and `gemini-2.5-flash-lite`.
- **Anthropic**: Requires API key from [Anthropic Console](https://console.anthropic.com/). Works with Claude models.
- **OpenAI**: Requires API key from [OpenAI Platform](https://platform.openai.com/). Works with GPT models.

4. Using different models in notebooks:

**Google GenAI (Gemini):**
```python
from langchain_google_genai import ChatGoogleGenerativeAI

model = ChatGoogleGenerativeAI(
    model="models/gemini-2.5-flash",  # or "models/gemini-1.5-pro"
    temperature=0.0
)
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

5. Run notebooks or code using uv:
```bash
# Run Jupyter notebooks directly
uv run jupyter notebook

# Or activate the virtual environment if preferred
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
jupyter notebook
```

## 📚 Tutorial Overview

This repository contains five progressive notebooks that teach you to build advanced AI agents:

### `0_create_agent.ipynb` -
Learn how to use the create_agent component. This component,
- implements a ReAct (Reason - Act) loop that forms the foundation for many agents.
- is easy to use and quick to set up.
- serves as the

### `1_todo.ipynb` - Task Planning Foundations
Learn to implement structured task planning using TODO lists. This notebook introduces:
- Task tracking with status management (pending/in_progress/completed)  
- Progress monitoring and context management
- The `write_todos()` tool for organizing complex multi-step workflows
- Best practices for maintaining focus and preventing task drift

### `2_files.ipynb` - Virtual File Systems
Implement a virtual file system stored in agent state for context offloading:
- File operations: `ls()`, `read_file()`, `write_file()`, `edit_file()`
- Context management through information persistence
- Enabling agent "memory" across conversation turns
- Reducing token usage by storing detailed information in files

### `3_subagents.ipynb` - Context Isolation
Master sub-agent delegation for handling complex workflows:
- Creating specialized sub-agents with focused tool sets
- Context isolation to prevent confusion and task interference
- The `task()` delegation tool and agent registry patterns
- Parallel execution capabilities for independent research streams

### `4_full_agent.ipynb` - Complete Research Agent
Combine all techniques into a production-ready research agent:
- Integration of TODOs, files, and sub-agents
- Real web search with intelligent context offloading
- Content summarization and strategic thinking tools
- Complete workflow for complex research tasks with LangGraph Studio integration

Each notebook builds on the previous concepts, culminating in a sophisticated agent architecture capable of handling real-world research and analysis tasks. 
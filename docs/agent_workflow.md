# Deep Agent Workflow

This repository demonstrates how to build increasingly powerful agents using planning, tools, and memory.

## Agent Architecture

The deep agent follows this workflow:

User Query → Planner → Task List → Agent → Tool Calls → Memory → Final Response

## Components

### Planner
The planner breaks a complex task into smaller steps.

### Task List
Tasks are stored in a TODO list that the agent executes sequentially.

### Agent
The agent reasons about which step to execute next.

### Tools
Tools allow the agent to interact with external systems such as files or APIs.

### Memory
Memory allows the agent to store intermediate results and reuse them later.

## Workflow Diagram

```mermaid
graph TD
User --> Planner
Planner --> Todo_List
Todo_List --> Agent
Agent --> Tools
Tools --> Memory
Memory --> Agent
Agent --> Response
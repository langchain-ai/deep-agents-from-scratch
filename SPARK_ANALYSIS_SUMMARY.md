# Spark Exception Analysis - Deep Agent Pattern

## Overview

This example demonstrates using the **Deep Agent pattern** to analyze Spark executor failures and differentiate between GC-caused and OOM-caused executor loss when the driver reports "executor lost".

## Files Created

1. **`spark_deep_agent_example.py`** - Full implementation with subagents
   - Uses TODO tracking for analysis workflow
   - Virtual file system for log storage
   - Specialized subagents (gc_analyzer, oom_analyzer)
   - Three failure scenarios (OOM, GC, Mixed)

2. **`spark_gc_oom_demo.py`** - Simplified working demo
   - Deep Agent pattern without subagents
   - Faster execution, clearer output
   - Three test scenarios demonstrating differentiation

3. **`test_spark_deep_agent.py`** - Single scenario test
4. **`test_gc_scenario.py`** - GC scenario test

## Key Capabilities

### 1. Differentiation Logic

The agent successfully differentiates between three failure types:

#### **Pure OOM (Out of Memory)**
**Indicators:**
- `OutOfMemoryError: Java heap space`
- Container killed by YARN for exceeding memory limits
- Memory usage >100% of limit (e.g., 4.2 GB / 4 GB)
- Free memory dropped to 0 bytes
- NO GC warnings or long pauses

**Root Cause:** Insufficient memory allocation

**Recommendations:**
- Increase `--executor-memory`
- Add `spark.yarn.executor.memoryOverhead`
- Use `MEMORY_AND_DISK` storage instead of `MEMORY_ONLY`
- Increase partitions to reduce per-partition data size

#### **Pure GC (Garbage Collection)**
**Indicators:**
- Multiple Full GC events with long pauses (>30 seconds)
- Heartbeat timeout (executor couldn't communicate)
- "Stuck in GC" messages
- GC time >50% of execution time
- NO OutOfMemoryError, NO container killed

**Root Cause:** GC thrashing preventing heartbeats

**Recommendations:**
- Switch to G1GC: `-XX:+UseG1GC`
- Increase executor memory to reduce GC pressure
- Tune `spark.memory.fraction` (reduce to 0.5)
- Monitor GC with `-XX:+PrintGCDetails`

#### **Mixed (GC Thrashing → OOM)**
**Indicators:**
- `OutOfMemoryError: GC overhead limit exceeded`
- Multiple Full GC events BEFORE OOM
- Container killed AFTER GC warnings
- GC spending >98% time, recovering <2% heap

**Root Cause:** Insufficient memory causing GC thrashing

**Recommendations:**
- Increase executor memory (primary fix)
- Reduce `spark.memory.storageFraction` to limit caching
- Enable G1GC
- Monitor GC metrics

## Deep Agent Pattern Components

### 1. TODO Tracking
```python
write_todos([
    {"content": "Get driver log", "status": "pending"},
    {"content": "Get executor log", "status": "pending"},
    {"content": "Analyze GC vs OOM indicators", "status": "pending"},
    {"content": "Provide diagnosis", "status": "pending"},
])
```

The agent creates and tracks its analysis workflow, marking tasks as completed.

### 2. Virtual File System
```python
write_file("driver_log.txt", driver_log_content)
write_file("executor_log.txt", executor_log_content)
write_file("analysis_results.txt", analysis_summary)
```

Logs are stored in agent state for context management and potential backtracking.

### 3. Specialized Tools
```python
@tool
def search_gc_indicators(log_content: str) -> dict:
    """Search for GC-related indicators."""
    # Detects Full GC events, pause times, heartbeat timeouts

@tool
def search_oom_indicators(log_content: str) -> dict:
    """Search for OOM-related indicators."""
    # Detects heap space errors, container kills, memory exceeded

@tool
def analyze_failure_pattern(driver_log: str, executor_log: str) -> dict:
    """Determine failure type with confidence level."""
    # Returns: pure_oom, pure_gc, or mixed_gc_oom
```

### 4. Subagents (Full Version)
```python
SubAgent(
    name="gc_analyzer",
    description="Specialized in analyzing GC issues",
    prompt="Expert in GC patterns...",
    tools=["get_executor_log", "search_gc_indicators", ...]
)
```

Specialized subagents with isolated contexts for deep analysis.

## Running the Examples

### Simplified Demo (Recommended)
```bash
uv run python spark_gc_oom_demo.py
```

**Output:** Three scenarios with clear differentiation:
1. Pure OOM - Identifies memory exceeded, no GC issues
2. Pure GC - Identifies heartbeat timeout from GC pauses
3. Mixed - Identifies GC overhead limit exceeded

### Full Deep Agent (With Subagents)
```bash
uv run python spark_deep_agent_example.py
```

**Note:** May take 3-5 minutes due to subagent delegation.

### Single Test
```bash
uv run python test_spark_deep_agent.py
```

## Key Insights

### When Driver Says "Executor Lost"

The agent investigates:

1. **Check driver log reason:**
   - "Container killed by YARN" → Likely OOM
   - "Heartbeat timeout" → Likely GC
   - "RPC disassociated" → Likely GC

2. **Check executor log evidence:**
   - OOM: `OutOfMemoryError`, memory exceeded, free=0
   - GC: Full GC events, long pauses, stuck in GC
   - Mixed: Both GC warnings AND OOM error

3. **Timing analysis:**
   - OOM: Sudden failure when memory exceeded
   - GC: Progressive degradation, timeouts
   - Mixed: GC warnings → thrashing → OOM

## Technical Architecture

```
Main Agent
├── TODO Management (write_todos, read_todos)
├── File System (ls, read_file, write_file)
├── Log Access (get_driver_log, get_executor_log)
├── Analysis Tools
│   ├── search_gc_indicators
│   ├── search_oom_indicators
│   └── analyze_failure_pattern
└── Subagents (optional)
    ├── gc_analyzer (isolated context for GC analysis)
    └── oom_analyzer (isolated context for OOM analysis)
```

## Benefits of Deep Agent Pattern

1. **Context Management:** Virtual files prevent context overflow
2. **Task Tracking:** TODOs ensure systematic analysis
3. **Specialization:** Subagents provide focused expertise
4. **Differentiation:** Clear logic separates GC from OOM
5. **Actionable Output:** Specific recommendations with evidence

## Example Output

```
🔍 DIAGNOSIS: PURE GC FAILURE

Root Cause: Garbage Collection Thrashing

Key Evidence:
- 3 Full GC events: 45s, 52s, 48s (avg: 48.3s)
- Total GC time: 145s (58% of interval)
- Heartbeat timeout: 150s exceeds 120s limit
- Executor stuck in GC
- NO OutOfMemoryError

Recommendations:
1. Switch to G1GC: --conf spark.executor.extraJavaOptions="-XX:+UseG1GC"
2. Increase memory: --executor-memory 8g
3. Tune fractions: --conf spark.memory.fraction=0.5
```

## Conclusion

This example demonstrates how the Deep Agent pattern enables sophisticated log analysis with:
- Clear differentiation between GC and OOM failures
- Evidence-based diagnosis
- Actionable recommendations
- Systematic workflow tracking
- Context management through virtual files

The agent successfully answers: **"When the driver says executor lost, was it GC or OOM?"**

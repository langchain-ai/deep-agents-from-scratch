"""
Spark GC vs OOM Analysis - Simplified Deep Agent Demo

Demonstrates using Deep Agent pattern (todos + file system) to differentiate
between GC-caused executor loss and OOM-caused executor loss.
"""

import os
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.tools import tool

from deep_agents_from_scratch.state import DeepAgentState
from deep_agents_from_scratch.todo_tools import write_todos, read_todos
from deep_agents_from_scratch.file_tools import ls, read_file, write_file

from langgraph.prebuilt import create_react_agent

load_dotenv('.env', override=True)

# ============================================================================
# LOG DATA - Three different failure patterns
# ============================================================================

LOGS = {
    "oom_driver": """
2025-10-01 10:24:45 WARN TaskSetManager: Lost task 0.0 in stage 3.0 (TID 150, executor 1): ExecutorLostFailure
2025-10-01 10:24:45 ERROR TaskSchedulerImpl: Lost executor 1: Container killed by YARN for exceeding memory limits
""",
    "oom_executor": """
2025-10-01 10:24:20 INFO MemoryStore: Block rdd_45_1 stored as bytes in memory (estimated size 1024.0 MB, free 0.0 B)
2025-10-01 10:24:30 ERROR Executor: Exception in task 0.0 in stage 3.0 (TID 150)
java.lang.OutOfMemoryError: Java heap space
	at java.util.Arrays.copyOf(Arrays.java:3332)
2025-10-01 10:24:45 FATAL Container: Container killed by YARN. Current usage: 4.2 GB of 4 GB physical memory used
""",
    "gc_driver": """
2025-10-01 11:16:00 WARN TaskSetManager: Lost task 0.0 in stage 5.0 (TID 200, executor 2): ExecutorLostFailure
2025-10-01 11:16:00 ERROR TaskSchedulerImpl: Lost executor 2: Remote RPC client disassociated. Likely due to heartbeat timeout.
2025-10-01 11:16:01 WARN HeartbeatReceiver: Removing executor 2 with no recent heartbeats: 150000 ms exceeds timeout 120000 ms
""",
    "gc_executor": """
2025-10-01 11:15:40 WARN Executor: GC time exceeded 10% threshold (12% of total time)
2025-10-01 11:15:45 WARN Executor: Full GC event took 45000 ms
2025-10-01 11:15:50 WARN Executor: Full GC event took 52000 ms
2025-10-01 11:15:55 WARN Executor: Full GC event took 48000 ms
2025-10-01 11:15:58 INFO Executor: Total GC time in last interval: 145000 ms (58% of interval)
2025-10-01 11:16:00 ERROR CoarseGrainedExecutorBackend: Cannot send heartbeat to driver, executor appears to be stuck in GC
""",
    "mixed_executor": """
2025-10-01 12:00:10 WARN Executor: GC time exceeded 10% threshold (15% of total time)
2025-10-01 12:00:15 WARN Executor: Full GC event took 35000 ms
2025-10-01 12:00:25 WARN Executor: Full GC event took 42000 ms
2025-10-01 12:00:30 ERROR Executor: Exception in task 0.0 in stage 7.0 (TID 300)
java.lang.OutOfMemoryError: GC overhead limit exceeded
	at org.apache.spark.memory.MemoryManager.acquireExecutionMemory
2025-10-01 12:00:35 FATAL Container: Process killed by YARN for exceeding memory limits
""",
}


@tool
def get_log(log_type: str) -> str:
    """Get a specific Spark log.

    Args:
        log_type: One of 'oom_driver', 'oom_executor', 'gc_driver', 'gc_executor', 'mixed_executor'

    Returns:
        The log content
    """
    return LOGS.get(log_type, f"Error: Unknown log type '{log_type}'")


@tool
def analyze_failure_pattern(driver_log: str, executor_log: str) -> dict:
    """Analyze logs to determine failure pattern (GC vs OOM).

    Args:
        driver_log: Driver log content
        executor_log: Executor log content

    Returns:
        Dictionary with analysis results
    """
    analysis = {
        "failure_type": "unknown",
        "confidence": "low",
        "key_evidence": [],
        "recommendations": []
    }

    # Check for OOM indicators
    oom_indicators = []
    if "OutOfMemoryError: Java heap space" in executor_log:
        oom_indicators.append("Java heap space OOM")
    if "Container killed" in driver_log or "Container killed" in executor_log:
        oom_indicators.append("Container killed by YARN")
    if "free 0.0 B" in executor_log or "free 0 B" in executor_log:
        oom_indicators.append("Memory exhausted (0 free)")
    if "exceeding memory limits" in driver_log or "exceeding memory limits" in executor_log:
        oom_indicators.append("Physical memory limit exceeded")

    # Check for GC indicators
    gc_indicators = []
    if "Full GC event took" in executor_log:
        gc_count = executor_log.count("Full GC event")
        gc_indicators.append(f"{gc_count} Full GC events detected")

        # Extract GC times
        gc_times = []
        for line in executor_log.split('\n'):
            if "Full GC event took" in line:
                try:
                    time_ms = int(line.split("took")[1].split("ms")[0].strip())
                    gc_times.append(time_ms)
                except:
                    pass
        if gc_times:
            max_gc = max(gc_times)
            avg_gc = sum(gc_times) / len(gc_times)
            gc_indicators.append(f"Max GC pause: {max_gc}ms, Avg: {avg_gc:.0f}ms")

    if "heartbeat timeout" in driver_log.lower():
        gc_indicators.append("Heartbeat timeout (classic GC symptom)")
    if "stuck in GC" in executor_log:
        gc_indicators.append("Executor stuck in GC")
    if "GC time exceeded" in executor_log:
        gc_indicators.append("GC time threshold exceeded")

    # Check for mixed pattern
    if "GC overhead limit exceeded" in executor_log:
        analysis["failure_type"] = "mixed_gc_oom"
        analysis["confidence"] = "high"
        analysis["key_evidence"] = [
            "GC overhead limit exceeded - GC thrashing due to insufficient memory",
            *gc_indicators,
            *oom_indicators
        ]
        analysis["recommendations"] = [
            "Increase executor memory (primary fix)",
            "Reduce memory.storageFraction to limit caching",
            "Consider GC tuning: -XX:+UseG1GC",
            "Monitor GC logs with -XX:+PrintGCDetails"
        ]
        return analysis

    # Determine primary failure type
    if oom_indicators and not gc_indicators:
        analysis["failure_type"] = "pure_oom"
        analysis["confidence"] = "high"
        analysis["key_evidence"] = oom_indicators
        analysis["recommendations"] = [
            "Increase executor-memory",
            "Add spark.yarn.executor.memoryOverhead",
            "Use MEMORY_AND_DISK storage instead of MEMORY_ONLY",
            "Increase partition count to reduce per-partition data"
        ]
    elif gc_indicators and not oom_indicators:
        analysis["failure_type"] = "pure_gc"
        analysis["confidence"] = "high"
        analysis["key_evidence"] = gc_indicators
        analysis["recommendations"] = [
            "Tune GC: use G1GC instead of ParallelGC",
            "Increase executor memory to reduce GC pressure",
            "Reduce spark.memory.fraction to leave more heap for GC",
            "Monitor with: -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
        ]
    elif gc_indicators and oom_indicators:
        analysis["failure_type"] = "mixed_gc_oom"
        analysis["confidence"] = "medium"
        analysis["key_evidence"] = gc_indicators + oom_indicators
        analysis["recommendations"] = [
            "Increase executor memory (addresses both issues)",
            "Tune GC settings",
            "Reduce caching pressure"
        ]

    return analysis


def main():
    """Run analysis scenarios."""

    model = init_chat_model(model='anthropic:claude-sonnet-4-5-20250929', temperature=0.0)

    tools = [
        get_log,
        analyze_failure_pattern,
        write_todos,
        read_todos,
        ls,
        read_file,
        write_file,
    ]

    PROMPT = """You are a Spark exception analysis expert using the Deep Agent pattern.

When analyzing "executor lost" failures, you must:
1. Create a TODO plan using write_todos
2. Retrieve both driver and executor logs
3. Store logs in virtual files for context management
4. Use analyze_failure_pattern to determine the root cause
5. Clearly distinguish between:
   - **Pure OOM**: Memory exceeded, container killed, NO GC issues
   - **Pure GC**: Long GC pauses → heartbeat timeout, NO OOM
   - **Mixed**: GC overhead limit exceeded (GC thrashing due to low memory)

Provide a clear, definitive diagnosis with evidence."""

    agent = create_react_agent(
        model,
        tools,
        prompt=PROMPT,
        state_schema=DeepAgentState,
    ).with_config({"recursion_limit": 30})

    # Test scenarios
    scenarios = [
        {
            "name": "Pure OOM Scenario",
            "query": """
Analyze this failure: Driver log shows "Lost executor 1 - Container killed by YARN for exceeding memory limits".

Steps:
1. Create TODO plan
2. Get logs: 'oom_driver' and 'oom_executor'
3. Store them in virtual files
4. Use analyze_failure_pattern tool
5. Diagnosis: Is this GC or OOM?
"""
        },
        {
            "name": "Pure GC Scenario",
            "query": """
Analyze this failure: Driver log shows "Lost executor 2 - heartbeat timeout".

Steps:
1. Create TODO plan
2. Get logs: 'gc_driver' and 'gc_executor'
3. Store them in virtual files
4. Use analyze_failure_pattern tool
5. Diagnosis: Is this GC or OOM?
"""
        },
        {
            "name": "Mixed GC/OOM Scenario",
            "query": """
Analyze this failure: Executor log shows "GC overhead limit exceeded" then container killed.

Steps:
1. Create TODO plan
2. Get logs: 'oom_driver' and 'mixed_executor'
3. Store them in virtual files
4. Use analyze_failure_pattern tool
5. Diagnosis: What's the root cause?
"""
        },
    ]

    for scenario in scenarios:
        print("\n" + "=" * 80)
        print(f"SCENARIO: {scenario['name']}")
        print("=" * 80)

        result = agent.invoke({
            "messages": [{"role": "user", "content": scenario["query"]}]
        })

        print(f"\n{result['messages'][-1].content}\n")

        # Show TODOs
        if result.get("todos"):
            print("--- TODOs ---")
            for todo in result["todos"]:
                emoji = {"pending": "⏳", "in_progress": "🔄", "completed": "✅"}[todo["status"]]
                print(f"{emoji} {todo['content']}")

        print("=" * 80)


if __name__ == "__main__":
    main()

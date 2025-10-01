"""
Spark Exception Analysis - Deep Agent Pattern

This demonstrates using the Deep Agent pattern (todos, files, subagents) to analyze
Spark driver and executor logs to differentiate between GC-caused executor loss vs OOM.

Key Features:
- TODO tracking for analysis workflow
- Virtual file system for log storage and context offloading
- Specialized subagents for executor-specific analysis
- Differentiates GC issues from OOM issues
"""

import os
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.tools import tool

from deep_agents_from_scratch.state import DeepAgentState
from deep_agents_from_scratch.todo_tools import write_todos, read_todos
from deep_agents_from_scratch.file_tools import ls, read_file, write_file
from deep_agents_from_scratch.task_tool import _create_task_tool, SubAgent

from langgraph.prebuilt import create_react_agent

# Load environment variables
load_dotenv('.env', override=True)

# ============================================================================
# SIMULATED LOG DATA - Different scenarios for GC vs OOM
# ============================================================================

DRIVER_LOG_OOM = """
2025-10-01 10:23:45 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 150, executor 1, partition 0, PROCESS_LOCAL)
2025-10-01 10:24:12 INFO TaskSetManager: Starting task 1.0 in stage 3.0 (TID 151, executor 1, partition 1, PROCESS_LOCAL)
2025-10-01 10:24:45 WARN TaskSetManager: Lost task 0.0 in stage 3.0 (TID 150, executor 1): ExecutorLostFailure (executor 1 exited caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits
2025-10-01 10:24:45 ERROR TaskSchedulerImpl: Lost executor 1 on worker-node-3: Container killed by YARN for exceeding memory limits
2025-10-01 10:24:46 WARN TaskSetManager: Lost task 1.0 in stage 3.0 (TID 151, executor 1): ExecutorLostFailure (executor 1 exited caused by one of the running tasks)
2025-10-01 10:24:47 INFO DAGScheduler: Executor lost: 1 (epoch 5)
"""

EXECUTOR_1_LOG_OOM = """
2025-10-01 10:23:45 INFO Executor: Running task 0.0 in stage 3.0 (TID 150)
2025-10-01 10:24:00 INFO MemoryStore: Block rdd_45_0 stored as bytes in memory (estimated size 512.0 MB, free 256.0 MB)
2025-10-01 10:24:15 WARN MemoryStore: Not enough space to cache rdd_45_1 in memory! (computed 1024.0 MB so far)
2025-10-01 10:24:20 INFO MemoryStore: Block rdd_45_1 stored as bytes in memory (estimated size 1024.0 MB, free 0.0 B)
2025-10-01 10:24:30 ERROR Executor: Exception in task 0.0 in stage 3.0 (TID 150)
java.lang.OutOfMemoryError: Java heap space
	at java.util.Arrays.copyOf(Arrays.java:3332)
	at java.io.ByteArrayOutputStream.grow(ByteArrayOutputStream.java:118)
	at org.apache.spark.util.ByteBufferOutputStream.write(ByteBufferOutputStream.scala:41)
2025-10-01 10:24:45 FATAL Container: Process killed by YARN: Container [pid=12345] is running beyond physical memory limits. Current usage: 4.2 GB of 4 GB physical memory used; killing container.
"""

DRIVER_LOG_GC = """
2025-10-01 11:15:30 INFO TaskSetManager: Starting task 0.0 in stage 5.0 (TID 200, executor 2, partition 0, PROCESS_LOCAL)
2025-10-01 11:15:35 INFO TaskSetManager: Starting task 1.0 in stage 5.0 (TID 201, executor 2, partition 1, PROCESS_LOCAL)
2025-10-01 11:16:00 WARN TaskSetManager: Lost task 0.0 in stage 5.0 (TID 200, executor 2): ExecutorLostFailure (executor 2 exited unrelated to the running tasks)
2025-10-01 11:16:00 ERROR TaskSchedulerImpl: Lost executor 2 on worker-node-5: Remote RPC client disassociated. Likely due to heartbeat timeout.
2025-10-01 11:16:01 INFO DAGScheduler: Executor lost: 2 (epoch 8)
2025-10-01 11:16:01 WARN HeartbeatReceiver: Removing executor 2 with no recent heartbeats: 150000 ms exceeds timeout 120000 ms
"""

EXECUTOR_2_LOG_GC = """
2025-10-01 11:15:30 INFO Executor: Running task 0.0 in stage 5.0 (TID 200)
2025-10-01 11:15:32 INFO MemoryStore: Block rdd_67_0 stored as bytes in memory (estimated size 800.0 MB, free 1.2 GB)
2025-10-01 11:15:40 WARN Executor: GC time exceeded 10% threshold (12% of total time)
2025-10-01 11:15:45 WARN Executor: Full GC event took 45000 ms
2025-10-01 11:15:50 WARN Executor: Full GC event took 52000 ms
2025-10-01 11:15:55 WARN Executor: Full GC event took 48000 ms
2025-10-01 11:15:58 INFO Executor: Total GC time in last interval: 145000 ms (58% of interval)
2025-10-01 11:16:00 ERROR CoarseGrainedExecutorBackend: Cannot send heartbeat to driver, executor appears to be stuck in GC
2025-10-01 11:16:00 WARN CoarseGrainedExecutorBackend: Executor heartbeat timed out after 150000 ms
"""

EXECUTOR_3_LOG_MIXED = """
2025-10-01 12:00:00 INFO Executor: Running task 0.0 in stage 7.0 (TID 300)
2025-10-01 12:00:05 INFO MemoryStore: Block rdd_89_0 stored as bytes in memory (estimated size 1.5 GB, free 500.0 MB)
2025-10-01 12:00:10 WARN Executor: GC time exceeded 10% threshold (15% of total time)
2025-10-01 12:00:15 WARN Executor: Full GC event took 35000 ms
2025-10-01 12:00:20 WARN MemoryStore: Not enough space to cache rdd_89_1 in memory!
2025-10-01 12:00:25 WARN Executor: Full GC event took 42000 ms
2025-10-01 12:00:30 ERROR Executor: Exception in task 0.0 in stage 7.0 (TID 300)
java.lang.OutOfMemoryError: GC overhead limit exceeded
	at org.apache.spark.memory.MemoryManager.acquireExecutionMemory
2025-10-01 12:00:35 FATAL Container: Process killed by YARN for exceeding memory limits
"""

# ============================================================================
# TOOLS FOR LOG ACCESS
# ============================================================================

@tool
def get_driver_log(scenario: str = "oom") -> str:
    """Get the Spark driver log for a specific failure scenario.

    Args:
        scenario: The scenario type - 'oom', 'gc', or 'mixed' (default: 'oom')

    Returns:
        The driver log content
    """
    if scenario == "gc":
        return DRIVER_LOG_GC
    else:
        return DRIVER_LOG_OOM


@tool
def get_executor_log(executor_id: int, scenario: str = "oom") -> str:
    """Get the Spark executor log for a specific executor and scenario.

    Args:
        executor_id: The executor ID (1, 2, or 3)
        scenario: The scenario type - 'oom', 'gc', or 'mixed'

    Returns:
        The executor log content
    """
    if scenario == "gc" and executor_id == 2:
        return EXECUTOR_2_LOG_GC
    elif scenario == "mixed" and executor_id == 3:
        return EXECUTOR_3_LOG_MIXED
    elif executor_id == 1:
        return EXECUTOR_1_LOG_OOM
    else:
        return f"Error: No log data for executor {executor_id} in scenario {scenario}"


@tool
def search_gc_indicators(log_content: str) -> dict:
    """Search for GC-related indicators in log content.

    Args:
        log_content: The log content to analyze

    Returns:
        Dictionary with GC indicators found
    """
    indicators = {
        "full_gc_events": log_content.count("Full GC event"),
        "gc_warnings": log_content.count("GC time exceeded"),
        "heartbeat_timeout": "heartbeat" in log_content.lower() and "timeout" in log_content.lower(),
        "gc_overhead": "GC overhead limit exceeded" in log_content,
        "stuck_in_gc": "stuck in GC" in log_content,
    }

    # Extract GC times if present
    gc_times = []
    for line in log_content.split('\n'):
        if "Full GC event took" in line:
            try:
                time_ms = int(line.split("took")[1].split("ms")[0].strip())
                gc_times.append(time_ms)
            except:
                pass

    indicators["gc_event_times_ms"] = gc_times
    indicators["max_gc_time_ms"] = max(gc_times) if gc_times else 0
    indicators["avg_gc_time_ms"] = sum(gc_times) / len(gc_times) if gc_times else 0

    return indicators


@tool
def search_oom_indicators(log_content: str) -> dict:
    """Search for OOM-related indicators in log content.

    Args:
        log_content: The log content to analyze

    Returns:
        Dictionary with OOM indicators found
    """
    indicators = {
        "java_heap_space_oom": "OutOfMemoryError: Java heap space" in log_content,
        "container_killed": "Container killed" in log_content or "killed by YARN" in log_content,
        "memory_exceeded": "exceeding memory limits" in log_content,
        "not_enough_space": log_content.count("Not enough space"),
        "free_memory_zero": "free 0.0 B" in log_content or "free 0 B" in log_content,
    }

    # Extract memory usage if present
    if "Current usage:" in log_content:
        try:
            usage_line = [l for l in log_content.split('\n') if "Current usage:" in l][0]
            indicators["memory_usage_info"] = usage_line.strip()
        except:
            pass

    return indicators


# ============================================================================
# MAIN AGENT SETUP
# ============================================================================

def create_spark_analysis_agent():
    """Create a deep agent for Spark exception analysis."""

    model = init_chat_model(model='anthropic:claude-sonnet-4-5-20250929', temperature=0.0)

    # Define basic tools
    basic_tools = [
        get_driver_log,
        get_executor_log,
        search_gc_indicators,
        search_oom_indicators,
        write_todos,
        read_todos,
        ls,
        read_file,
        write_file,
    ]

    # Define specialized subagents
    subagents = [
        SubAgent(
            name="gc_analyzer",
            description="Specialized in analyzing GC (Garbage Collection) issues and excessive GC pauses",
            prompt="""You are a GC (Garbage Collection) analysis expert for Spark executors.

Your job is to:
1. Analyze GC patterns in executor logs
2. Identify excessive GC pauses (>10 seconds is concerning, >30 seconds is critical)
3. Calculate GC overhead percentage
4. Determine if GC caused heartbeat failures
5. Differentiate between excessive GC and actual OOM

Key indicators of GC issues:
- Multiple Full GC events
- GC times >30 seconds
- Total GC time >50% of execution time
- Heartbeat timeout with GC warnings
- "stuck in GC" messages

Provide specific recommendations for GC tuning.""",
            tools=["get_executor_log", "search_gc_indicators", "read_file", "write_file"]
        ),
        SubAgent(
            name="oom_analyzer",
            description="Specialized in analyzing OOM (Out of Memory) issues and memory allocation problems",
            prompt="""You are an OOM (Out of Memory) analysis expert for Spark executors.

Your job is to:
1. Analyze memory allocation patterns
2. Identify the specific type of OOM (heap space, GC overhead limit, etc.)
3. Calculate memory usage vs limits
4. Identify what consumed the memory (caching, serialization, etc.)
5. Differentiate between true OOM and GC-caused issues

Key indicators of OOM issues:
- "OutOfMemoryError: Java heap space"
- Container killed for exceeding memory limits
- Memory usage >100% of limit
- Free memory dropped to 0
- Large RDD caching before failure

Provide specific recommendations for memory configuration.""",
            tools=["get_executor_log", "search_oom_indicators", "read_file", "write_file"]
        ),
    ]

    # Create task tool for subagent delegation
    task_tool = _create_task_tool(basic_tools, subagents, model, DeepAgentState)

    # All tools including task delegation
    all_tools = basic_tools + [task_tool]

    # Main agent prompt
    MAIN_PROMPT = """You are an expert Spark exception analysis agent using the Deep Agent pattern.

When the driver log shows "executor lost", you must systematically determine if it was caused by:
1. **GC Issues**: Excessive garbage collection causing heartbeat timeouts
2. **OOM Issues**: Actual out of memory errors
3. **Mixed Issues**: GC pressure leading to OOM

Your workflow:
1. Use write_todos to create an analysis plan
2. Get and analyze the driver log to identify which executor(s) failed
3. Store logs in virtual files using write_file for context management
4. Delegate detailed analysis to specialized subagents:
   - Use gc_analyzer subagent for GC pattern analysis
   - Use oom_analyzer subagent for memory issue analysis
5. Synthesize findings and provide a definitive diagnosis
6. Provide specific, actionable recommendations

CRITICAL: You must differentiate between:
- **GC-caused loss**: Long GC pauses (>120s) → heartbeat timeout → executor removed (NOT OOM)
- **OOM-caused loss**: Memory exceeded → container killed → executor lost
- **Mixed**: GC overhead limit exceeded (GC thrashing due to low memory)

Use the virtual file system to store intermediate analysis results."""

    agent = create_react_agent(
        model,
        all_tools,
        prompt=MAIN_PROMPT,
        state_schema=DeepAgentState,
    ).with_config({"recursion_limit": 50})

    return agent


# ============================================================================
# EXAMPLE SCENARIOS
# ============================================================================

def run_scenario(scenario_name: str, scenario_type: str, executor_id: int, description: str):
    """Run a specific analysis scenario."""

    print("\n" + "=" * 80)
    print(f"SCENARIO: {scenario_name}")
    print("=" * 80)
    print(f"\n{description}\n")
    print("=" * 80)

    agent = create_spark_analysis_agent()

    query = f"""
    Analyze this Spark executor failure (scenario: {scenario_type}, executor: {executor_id}).

    The driver log shows the executor was lost. I need you to determine:
    1. Was this caused by GC issues or OOM issues?
    2. What is the root cause?
    3. What specific evidence supports your conclusion?
    4. What are the recommended fixes?

    Use the deep agent pattern:
    - Create a TODO plan
    - Store logs in virtual files
    - Delegate to specialized subagents for detailed analysis
    - Provide a definitive diagnosis
    """

    result = agent.invoke({
        "messages": [{"role": "user", "content": query}],
        "files": {"scenario_info": f"Scenario: {scenario_type}, Executor: {executor_id}"}
    })

    print("\n" + "=" * 80)
    print("ANALYSIS RESULT:")
    print("=" * 80)
    print(f"\n{result['messages'][-1].content}\n")

    # Show files created
    if result.get("files"):
        print("\n" + "=" * 80)
        print("FILES CREATED IN VIRTUAL FILESYSTEM:")
        print("=" * 80)
        for filename in result["files"].keys():
            if filename != "scenario_info":
                print(f"  - {filename}")

    print("\n" + "=" * 80)


def main():
    """Run all example scenarios."""

    print("\n" + "=" * 80)
    print("SPARK DEEP AGENT EXCEPTION ANALYSIS")
    print("=" * 80)
    print("\nDemonstrating Deep Agent pattern for differentiating GC vs OOM failures")
    print("=" * 80)

    # Scenario 1: Pure OOM
    run_scenario(
        "Pure OOM Failure",
        "oom",
        1,
        "Driver: 'Lost executor 1 - Container killed by YARN for exceeding memory limits'\n"
        "Expected: Should identify as OOM, not GC issue"
    )

    # Scenario 2: Pure GC
    run_scenario(
        "GC-Induced Failure",
        "gc",
        2,
        "Driver: 'Lost executor 2 - heartbeat timeout'\n"
        "Expected: Should identify as GC issue causing heartbeat failure, not OOM"
    )

    # Scenario 3: Mixed (GC overhead limit)
    run_scenario(
        "Mixed GC/OOM Failure",
        "mixed",
        3,
        "Driver: 'Lost executor 3 - Container killed'\n"
        "Expected: Should identify as GC thrashing leading to OOM"
    )


if __name__ == "__main__":
    main()

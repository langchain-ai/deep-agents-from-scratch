# Quick Start: Spark Exception Analysis

## Run the Demo

```bash
# Make sure environment is set up
export ANTHROPIC_API_KEY="your-key-here"

# Run the simplified demo (recommended)
uv run python spark_gc_oom_demo.py
```

## What It Does

Analyzes Spark executor failures and determines:

- ✅ **Pure OOM**: Memory exceeded → container killed
- ✅ **Pure GC**: Long GC pauses → heartbeat timeout
- ✅ **Mixed**: GC thrashing → OOM

## Three Test Scenarios

### Scenario 1: Pure OOM
**Driver:** "Container killed by YARN for exceeding memory limits"
**Executor:** `OutOfMemoryError: Java heap space`, memory usage 4.2/4 GB
**Diagnosis:** Pure OOM - NOT GC related
**Fix:** Increase executor memory

### Scenario 2: Pure GC
**Driver:** "Heartbeat timeout"
**Executor:** Full GC events (45s, 52s, 48s), "stuck in GC"
**Diagnosis:** Pure GC - NOT OOM
**Fix:** Tune GC settings, use G1GC

### Scenario 3: Mixed
**Driver:** "Container killed"
**Executor:** GC warnings THEN `GC overhead limit exceeded`
**Diagnosis:** Mixed - GC thrashing due to low memory
**Fix:** Increase memory (primary)

## Key Differentiation

| Symptom | OOM | GC | Mixed |
|---------|-----|----|----|
| OutOfMemoryError: Java heap space | ✅ | ❌ | Sometimes |
| OutOfMemoryError: GC overhead limit | ❌ | ❌ | ✅ |
| Container killed | ✅ | ❌ | ✅ |
| Heartbeat timeout | ❌ | ✅ | Sometimes |
| Full GC events >30s | ❌ | ✅ | ✅ |
| Stuck in GC | ❌ | ✅ | Sometimes |
| Free memory = 0 | ✅ | ❌ | ✅ |

## Deep Agent Pattern Features

1. **TODO Tracking** - Agent creates analysis plan
2. **Virtual Files** - Stores logs in agent state
3. **Specialized Tools** - GC/OOM indicator detection
4. **Evidence-Based** - Clear reasoning with quotes
5. **Actionable** - Specific configuration recommendations

## Example Output

```
DIAGNOSIS: Pure GC (High Confidence)

Evidence:
- 3 Full GC events: avg 48.3 seconds
- Heartbeat timeout: 150s > 120s
- "Cannot send heartbeat, stuck in GC"
- NO OutOfMemoryError

Root Cause: GC thrashing prevented heartbeats

Recommendations:
1. --conf spark.executor.extraJavaOptions="-XX:+UseG1GC"
2. --executor-memory 8g
3. --conf spark.memory.fraction=0.5

TODOs:
✅ Get driver log
✅ Get executor log
✅ Analyze GC vs OOM indicators
✅ Provide diagnosis
```

## Files

- `spark_gc_oom_demo.py` - Main demo (fast, clear)
- `spark_deep_agent_example.py` - Full version with subagents (slower)
- `SPARK_ANALYSIS_SUMMARY.md` - Detailed documentation

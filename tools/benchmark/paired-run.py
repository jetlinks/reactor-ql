#!/usr/bin/env python3
"""Explicit formal JMH: alternate base/PR order per case, serial and fail-fast.

Usage: JAVA_HOME=/path/to/jdk python3 tools/benchmark/paired-run.py target/comparison
Run only when unrelated Java/build workloads are quiet. No retry, fallback or
dynamic parameter change is made. All JSON/logs and exact commands are retained.
"""
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys

output = pathlib.Path(sys.argv[1]).resolve()
receipt = json.loads((output / "receipt.json").read_text())
preflight = json.loads((output / "preflight.json").read_text())
java_home = pathlib.Path(os.environ["JAVA_HOME"]).resolve()
assert str(java_home) == receipt["java_home"], "Use the preparation JAVA_HOME"
assert preflight["pass_count"] == preflight["total"] == 30
assert receipt["artifacts"] == preflight["artifacts"]
for name, expected in receipt["artifacts"].items():
    assert hashlib.sha256((output / name).read_bytes()).hexdigest() == expected, name
assert all(row["oracle_verified"] and row["engine_source_verified"] and not row["wrong_class_origins"] for row in preflight["cases"])
cases = [row for row in preflight["cases"] if row["engine"] == "base"]
assert len(cases) == 15
java = str(java_home / "bin/java")
engines = {"base": "base.jar", "pr": "pr-engine.jar"}
plan = []
for index, case in enumerate(cases):
    for engine in (["base", "pr"] if index % 2 == 0 else ["pr", "base"]):
        stem = "formal-" + engine + "-" + case["case"].replace(":", "-")
        log, result = output / (stem + ".log"), output / (stem + ".json")
        if log.exists() or result.exists(): raise SystemExit("Refusing to overwrite " + stem)
        command = [java, "-Xms512m", "-Xmx512m", "-XX:+UseG1GC", "-cp",
                   os.pathsep.join(str(output / name) for name in ["harness.jar", engines[engine], "runtime.jar"]),
                   "org.openjdk.jmh.Main", "^" + re.escape(case["benchmark"]) + "$", "-bm", "thrpt", "-tu", "s",
                   "-t", "1", "-f", "2", "-wi", "3", "-w", "1s", "-i", "5", "-r", "1s", "-prof", "gc", "-foe", "true",
                   "-jvm", java, "-jvmArgs", "-Xms512m -Xmx512m -XX:+UseG1GC", "-rf", "json", "-rff", str(result)]
        if case["valuesPerKey"] is not None: command += ["-p", "valuesPerKey=" + str(case["valuesPerKey"])]
        plan.append({"case": case["case"], "engine": engine, "benchmark": case["benchmark"], "valuesPerKey": case["valuesPerKey"],
                     "operations_per_invocation": case["operations_per_invocation"], "log": str(log), "json": str(result), "command": command})
manifest = output / "formal-run.json"
if manifest.exists(): raise SystemExit("Refusing to overwrite " + str(manifest))
report = {"artifacts": receipt["artifacts"], "configuration": receipt["configuration"], "normalization": receipt["normalization"],
          "planned_runs": plan, "completed": [], "status": "running"}
manifest.write_text(json.dumps(report, indent=2) + "\n")
for number, run in enumerate(plan, 1):
    print("START " + str(number) + "/30 " + run["engine"] + " " + run["case"], flush=True)
    with pathlib.Path(run["log"]).open("w") as stream:
        process = subprocess.run(run["command"], stdout=stream, stderr=subprocess.STDOUT)
    valid = False
    if process.returncode == 0 and pathlib.Path(run["json"]).exists():
        data = json.loads(pathlib.Path(run["json"]).read_text())
        valid = (len(data) == 1 and data[0]["benchmark"] == run["benchmark"] and data[0]["forks"] == 2
                 and len(data[0]["primaryMetric"]["rawData"]) == 2 and all(len(fork) == 5 for fork in data[0]["primaryMetric"]["rawData"])
                 and "gc.alloc.rate.norm" in data[0]["secondaryMetrics"])
        if run["valuesPerKey"] is not None: valid = valid and data[0].get("params", {}).get("valuesPerKey") == str(run["valuesPerKey"])
    report["completed"].append({"case": run["case"], "engine": run["engine"], "exit_code": process.returncode, "complete_measurement": valid,
                                "log": run["log"], "json": run["json"]})
    report["status"] = "running" if valid else "failed"
    manifest.write_text(json.dumps(report, indent=2) + "\n")
    print("FINISH " + str(number) + "/30 " + run["engine"] + " " + run["case"] + " " + ("OK" if valid else "FAILED"), flush=True)
    if not valid: raise SystemExit("Failed measurement; evidence preserved: " + run["log"])
report["status"] = "complete"; manifest.write_text(json.dumps(report, indent=2) + "\n")
print("FORMAL_TERMINAL 30/30", flush=True)

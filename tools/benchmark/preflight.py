#!/usr/bin/env python3
"""
Verify all selected cases serially, each in its own 512 MiB JVM, both engines.

Usage: JAVA_HOME=/path/to/jdk python3 tools/benchmark/preflight.py target/comparison
This is oracle validation, not JMH measurement. Logs and class-load provenance
are retained; existing validation evidence is never overwritten.
"""
import hashlib
import json
import os
import pathlib
import re
import subprocess  # nosec B404 - Fixed argv invokes the receipt-validated local JDK; no shell.
import sys
import urllib.parse

output = pathlib.Path(sys.argv[1]).resolve()
receipt = json.loads((output / "receipt.json").read_text())
java_home = pathlib.Path(os.environ["JAVA_HOME"]).resolve()
if str(java_home) != receipt["java_home"]:
    raise SystemExit("Use the preparation JAVA_HOME")
for name, expected in receipt["artifacts"].items():
    if hashlib.sha256((output / name).read_bytes()).hexdigest() != expected:
        raise SystemExit("Artifact hash mismatch: " + name)
if (output / "preflight.json").exists() or any(output.glob("preflight-*.log")):
    raise SystemExit("Refusing to overwrite validation evidence")
P = "org.jetlinks.reactor.ql."
C = P + "compare.CommonBaselineComparisonBenchmark."
cases = [
    ("wide:wideProjectionWithFunctions", P + "WideSqlWorkloadBenchmark.wideProjectionWithFunctions", 65536, None),
    ("wide:jsonOperatorWideProjectionWithParsedInput", P + "WideSqlWorkloadBenchmark.jsonOperatorWideProjectionWithParsedInput", 65536, None),
    ("wide:operatorMixWideProjection", P + "WideSqlWorkloadBenchmark.operatorMixWideProjection", 65536, None),
    ("numeric:sqlProjection", P + "NumericTextWorkloadBenchmark.sqlProjection", 16384, None),
    ("numeric:sqlNestedProjection", P + "NumericTextWorkloadBenchmark.sqlNestedProjection", 16384, None),
    ("duration:sqlMixedGrouped", P + "DurationIntervalWorkloadBenchmark.sqlMixedGrouped", 8192, None),
    ("globalAggregates", C + "globalAggregates", 1000000, None),
    ("windowAggregates", C + "windowAggregates", 1000000, None),
    ("orderByLimit", C + "orderByLimit", 20000, None),
    ("multiRowInnerJoin", C + "multiRowInnerJoin", 20000, None),
    ("unionRows", C + "unionRows", 20000, None),
    ("high:1", C + "highCardinalityAggregates", 50000, 1),
    ("high:2", C + "highCardinalityAggregates", 50000, 2),
    ("high:50", C + "highCardinalityAggregates", 50000, 50),
    ("layered:sqlLayeredGroupedHavingTopN", P + "LayeredGroupHavingBenchmark.sqlLayeredGroupedHavingTopN", 16384, None),
]

def matches(origin, engine, origins):
    seen = set()
    while origin in origins and origin not in seen:
        seen.add(origin); origin = origins[origin]
    parsed = urllib.parse.urlparse(origin)
    return parsed.scheme == "file" and pathlib.Path(urllib.parse.unquote(parsed.path)).resolve() == engine

results = []
for label, filename in [("base", "base.jar"), ("pr", "pr-engine.jar")]:
    engine = output / filename
    for case, benchmark, inputs, parameter in cases:
        slug = label + "-" + case.replace(":", "-")
        log, loaded = output / ("preflight-" + slug + ".log"), output / ("classes-" + slug + ".log")
        command = [str(java_home / "bin/java"), "-Xms512m", "-Xmx512m", "-XX:+UseG1GC",
                   "-Xlog:class+load=info:file=" + str(loaded), "-cp",
                   os.pathsep.join(str(output / name) for name in ["harness.jar", filename, "runtime.jar"]),
                   P + "compare.SuiteVerifier", case]
        with log.open("w") as stream:
            result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, shell=False)  # nosec B603 - Fixed suite argv and receipt-validated absolute JAVA_HOME.
        lines = log.read_text(errors="replace").splitlines(); origins = {}
        for line in loaded.read_text(errors="replace").splitlines():
            match = re.search(r"\[class,load\] (org\.jetlinks\.reactor\.ql\.\S+) source: (.+)", line)
            if match: origins[match.group(1)] = match.group(2)
        wrong = {name: origin for name, origin in origins.items()
                 if "Benchmark" not in name and ".compare." not in name and not matches(origin, engine, origins)}
        source_ok = any(line.startswith("ENGINE_SOURCE=") and matches(line.split("=", 1)[1], engine, origins) for line in lines)
        verified = (result.returncode == 0 and "COMMON_SUITE_VERIFIED " + case in lines and source_ok and not wrong
                    and matches(origins.get(P + "ReactorQL", ""), engine, origins))
        results.append({"engine": label, "case": case, "benchmark": benchmark, "operations_per_invocation": inputs,
                        "valuesPerKey": parameter, "exit_code": result.returncode, "oracle_verified": verified,
                        "engine_source_verified": source_ok, "wrong_class_origins": wrong,
                        "log": str(log), "class_log": str(loaded), "command": command})
        print("PREFLIGHT " + label + " " + case + " " + ("OK" if verified else "FAILED"), flush=True)
report = {"artifacts": receipt["artifacts"], "cases": results,
          "pass_count": sum(row["oracle_verified"] for row in results), "total": len(results)}
(output / "preflight.json").write_text(json.dumps(report, indent=2) + "\n")
if report["pass_count"] != 30:
    raise SystemExit("Oracle/class provenance failure; see preflight.json and case logs")
print("PREFLIGHT_TERMINAL 30/30", flush=True)

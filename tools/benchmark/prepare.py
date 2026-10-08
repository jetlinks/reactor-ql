#!/usr/bin/env python3
"""
Prepare a master-compatible harness without changing either engine's sources.

Run after the ordinary JMH build, in a fresh target subdirectory. Requires an
explicit JAVA_HOME, Maven and cached JMH 1.37 annotation processor. Dependencies
come only from the PR fat JAR, not a wildcard local Maven classpath. This script
runs compilation, not measurement; do not overlap it with benchmark timing.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess  # nosec B404 - Fixed argv invokes resolved local tools/JDK; no shell.
import tarfile
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURES = ["WideSqlWorkloadBenchmark", "NumericTextWorkloadBenchmark",
            "DurationIntervalWorkloadBenchmark", "LayeredGroupHavingBenchmark"]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", help="New output directory under target; existing directories are refused")
parser.add_argument("--base", required=True, help="Exact baseline Git ref/commit")
parser.add_argument("--fat-jar", default="target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar")
parser.add_argument("--pr-jar", default="target/reactor-ql.jar")
parser.add_argument("--jmh-processor", help="Explicit jmh-generator-annprocess-1.37.jar; defaults to the Maven cache")
args = parser.parse_args()
output = (ROOT / args.output).resolve()
if not output.is_relative_to((ROOT / "target").resolve()) or output == (ROOT / "target").resolve() or output.exists():
    raise SystemExit("Use a fresh subdirectory under target; prior evidence is never overwritten")
java_home = pathlib.Path(os.environ["JAVA_HOME"]).resolve()
fat, thin = (ROOT / args.fat_jar).resolve(), (ROOT / args.pr_jar).resolve()
processor = pathlib.Path(args.jmh_processor).resolve() if args.jmh_processor else pathlib.Path.home() / ".m2/repository/org/openjdk/jmh/jmh-generator-annprocess/1.37/jmh-generator-annprocess-1.37.jar"
for source in [fat, thin, processor, java_home / "bin/java", java_home / "bin/javac"]:
    if not source.is_file(): raise SystemExit("Missing input: " + str(source))
git_path = shutil.which("git")
maven_path = shutil.which("mvn")
if git_path is None or maven_path is None:
    raise SystemExit("Git and Maven must be available on PATH")
git_path = str(pathlib.Path(git_path).resolve())
maven_path = str(pathlib.Path(maven_path).resolve())
output.mkdir(parents=True)

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def run(command, name, cwd=ROOT):
    with (output / name).open("w") as stream:
        subprocess.run(list(map(str, command)), cwd=cwd, stdout=stream, stderr=subprocess.STDOUT, check=True, shell=False)  # nosec B603 - Script-owned argv uses resolved local tools/JDK paths.
def jar(path, entries):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as result:
        for name, content in sorted(entries.items()):
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0)); info.compress_type = zipfile.ZIP_DEFLATED
            result.writestr(info, content)

base_ref = subprocess.check_output([git_path, "rev-parse", "--verify", "--end-of-options", args.base + "^{commit}"], cwd=ROOT, text=True, shell=False).strip()  # nosec B603 - Absolute Git; explicit option boundary and one baseline ref.
archive = output / "base.tar"
run([git_path, "archive", "--format=tar", "--output=" + str(archive), base_ref], "archive.log")
base_source = output / "base-source"; base_source.mkdir()
with tarfile.open(archive) as source:
    for member in source.getmembers():
        destination = (base_source / member.name).resolve()
        if not destination.is_relative_to(base_source) or member.issym() or member.islnk():
            raise SystemExit("Unsupported archive member: " + member.name)
    source.extractall(base_source)
# No baseline source/POM patches and no PR-only settings.
run([maven_path, "-o", "-q", "-Dmaven.test.skip=true", "-Dmaven.javadoc.skip=true", "package"], "base-build.log", base_source)
shutil.copyfile(base_source / "target/reactor-ql.jar", output / "base.jar")

with zipfile.ZipFile(fat) as source:
    original = {entry.filename: source.read(entry) for entry in source.infolist()}
runtime = {name: content for name, content in original.items()
           if not name.startswith("org/jetlinks/reactor/ql/") and name not in ("META-INF/BenchmarkList", "META-INF/CompilerHints")}
if any(name.startswith("ch/qos/logback/") or name == "org/slf4j/impl/StaticLoggerBinder.class" for name in runtime):
    raise SystemExit("Unexpected SLF4J binding")
jar(output / "runtime.jar", runtime)
production = {str(path.relative_to(ROOT / "src/main/java")).replace(".java", "") for path in (ROOT / "src/main/java").rglob("*.java")}
with zipfile.ZipFile(thin) as source:
    engine = {name: source.read(name) for name in source.namelist() if name.endswith(".class") and name[:-6].split("$")[0] in production}
    engine["META-INF/MANIFEST.MF"] = source.read("META-INF/MANIFEST.MF")
if not all(original.get(name) == content for name, content in engine.items() if name.endswith(".class")):
    raise SystemExit("PR thin/fat classes differ")
jar(output / "pr-engine.jar", engine)

source_root = output / "common-source"
for name in FIXTURES:
    relative = pathlib.Path("src/jmh/java/org/jetlinks/reactor/ql/" + name + ".java")
    destination = source_root / relative; destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, destination)
# Setup-only patch preserves all ordinary tracked benchmark methods.
run([git_path, "apply", "-p0", "--unidiff-zero", str(ROOT / "tools/benchmark/common-setup.patch")], "setup-patch.log", source_root)
compare = source_root / "src/jmh/java/org/jetlinks/reactor/ql/compare"; compare.mkdir()
for source in (ROOT / "tools/benchmark/src/main/java/org/jetlinks/reactor/ql/compare").glob("*.java"):
    shutil.copyfile(source, compare / source.name)
sources = sorted(source_root.rglob("*.java"))
classes, generated = output / "classes", output / "generated"; classes.mkdir(); generated.mkdir()
run([java_home / "bin/javac", "--release", "8", "-encoding", "UTF-8",
     "-cp", str(output / "base.jar") + os.pathsep + str(output / "runtime.jar"),
     "-processorpath", str(processor) + os.pathsep + str(output / "runtime.jar"),
     "-processor", "org.openjdk.jmh.generators.BenchmarkProcessor", "-d", classes, "-s", generated] + sources, "harness-build.log")
harness = {str(path.relative_to(classes)): path.read_bytes() for path in classes.rglob("*") if path.is_file()}
with zipfile.ZipFile(output / "base.jar") as source:
    base_classes = {name for name in source.namelist() if name.endswith(".class")}
if (base_classes | set(engine)) & set(harness):
    raise SystemExit("Engine class in harness")
jar(output / "harness.jar", harness)
receipt = {
    "base_ref": base_ref, "head": subprocess.check_output([git_path, "rev-parse", "HEAD"], cwd=ROOT, text=True, shell=False).strip(),  # nosec B603 - Absolute Git and fixed read-only argv.
    "java_home": str(java_home), "java": subprocess.check_output([str(java_home / "bin/java"), "-version"], stderr=subprocess.STDOUT, text=True, shell=False).strip(),  # nosec B603 - Explicit validated JAVA_HOME and fixed version argv.
    "artifacts": {name: sha(output / name) for name in ["base.jar", "pr-engine.jar", "runtime.jar", "harness.jar"]},
    "inputs": {str(path): sha(path) for path in [fat, thin, processor, ROOT / "tools/benchmark/common-setup.patch"]},
    "sources": {str(path.relative_to(source_root)): sha(path) for path in sources},
    "runtime_entry_count": len(runtime), "runtime_copied_entry_bytes_identical": True,
    "engine_class_overlap": [], "pr_classes_identical_to_thin_and_fat": True,
    "configuration": {"threads": 1, "forks": 2, "warmup": "3 x 1s", "measurement": "5 x 1s", "gc": "G1", "heap": "512m", "profiler": "gc"},
    "normalization": "@OperationsPerInvocation already normalizes ops/s and gc.alloc.rate.norm to input rows; do not divide again",
}
(output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print("PREPARED " + str(output), flush=True)

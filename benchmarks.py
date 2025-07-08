#!/usr/bin/env python3
import subprocess
import time
import os
import sys
import psutil 

# ─── CONFIG ────────────────────────────────────────────────────────────────────
RUST_DIR     = "rust-publisher"
SCALA_DIR    = "scala-publisher"
KAFKA_CONTAINER = "kafka"
BROKER       = "localhost:9092"
RUST_TOPIC   = "rust.topic"
SCALA_TOPIC  = "scala.topic"
DURATION     = 60     # how long to run each producer (in seconds)
GRACE        = 5      # wait after shutdown before counting
TERM_WAIT    = 5      # wait after SIGINT before SIGTERM
KILL_WAIT    = 3      # wait after SIGTERM before SIGKILL

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def run(cmd, cwd=None):
    print(f">>> {cmd} (cwd={cwd})")
    result = subprocess.run(cmd, cwd=cwd, shell=True)
    if result.returncode != 0:
        print(f"Command failed: {cmd}")
        sys.exit(result.returncode)

def count_messages(topic):
    cmd = (
        f"docker exec {KAFKA_CONTAINER} kafka-run-class.sh "
        f"kafka.tools.GetOffsetShell --broker-list {BROKER} "
        f"--topic {topic} --time -1 --offsets 1"
    )
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    total = 0
    for line in proc.stdout.splitlines():
        parts = line.split(":")
        if len(parts) >= 3:
            try:
                total += int(parts[2])
            except ValueError:
                pass
    return total

def run_producer(command, cwd):
    proc = subprocess.Popen(
        command,
        cwd=cwd,
        shell=True,
        start_new_session=True  # Better than preexec_fn for Python 3.8+
    )
    return proc

def terminate_process_tree(parent_pid):
    """Terminate entire process tree including child processes"""
    try:
        parent = psutil.Process(parent_pid)
        children = parent.children(recursive=True)
        
        # First try to terminate gracefully
        for child in children:
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass
        
        # Wait for termination
        gone, alive = psutil.wait_procs(children, timeout=TERM_WAIT)
        
        # Force kill any remaining processes
        for proc in alive:
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                pass
        
        # Finally kill the parent
        try:
            parent.terminate()
            parent.wait(timeout=KILL_WAIT)
        except psutil.TimeoutExpired:
            parent.kill()
    except psutil.NoSuchProcess:
        pass

def shutdown(proc, name):
    print(f"⏹  Stopping {name} process tree (PID {proc.pid})...")
    terminate_process_tree(proc.pid)
    proc.wait()  # Ensure process is reaped

# ─── PRE-BUILD ─────────────────────────────────────────────────────────────────
def build_projects():
    print("🔨 Pre-building Rust project…")
    run("cargo build --release", cwd=RUST_DIR)

    print("🔨 Pre-building Scala project (assembly)…")
    run("sbt clean compile assembly", cwd=SCALA_DIR)

    # find the fat JAR
    target_dir = os.path.join(SCALA_DIR, "target")
    jar = None
    for root, _, files in os.walk(target_dir):
        for f in files:
            if "nonsense-producer-assembly" in f and f.endswith(".jar"):
                jar = os.path.join(root, f)
                break
        if jar:
            break

    if not jar:
        print("Error: Scala assembly JAR not found")
        sys.exit(1)

    abs_jar = os.path.abspath(jar)
    print(f"   → Found Scala assembly JAR: {abs_jar}")
    return abs_jar

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    jar = build_projects()

    # Rust
    print(f"\n▶️  Running Rust producer for {DURATION} seconds…")
    rust_proc = run_producer("./target/release/nonsense-publisher-v2", RUST_DIR)
    time.sleep(DURATION)
    shutdown(rust_proc, "Rust")

    time.sleep(GRACE)
    rust_count = count_messages(RUST_TOPIC)
    print(f"   → Rust topic '{RUST_TOPIC}' has {rust_count} messages")

    # Scala
    print(f"\n▶️  Running Scala producer for {DURATION} seconds…")
    # Use the directory containing the JAR as working directory
    jar_dir = os.path.dirname(jar)
    scala_proc = run_producer(f"java -jar \"{jar}\"", cwd=jar_dir)
    time.sleep(DURATION)
    shutdown(scala_proc, "Scala")

    time.sleep(GRACE)
    scala_count = count_messages(SCALA_TOPIC)
    print(f"   → Scala topic '{SCALA_TOPIC}' has {scala_count} messages")

    print("\n✅ Benchmark complete:")
    print(f"    • Rust  → {rust_count} messages")
    print(f"    • Scala → {scala_count} messages")

if __name__ == "__main__":
    main()
import os
import subprocess
import sys
import time


ROOT = os.path.dirname(os.path.abspath(__file__))
JP_CLI = os.path.join(ROOT, "APAC_PowerDB_and_Aurora_IO_JP_cli.py")
CN_CLI = os.path.join(ROOT, "APAC_PowerDB_and_Aurora_IO_CN_cli.py")


def _menu_input(debug_value, force_value):
    # Menu:
    # 2 -> set debug
    # 3 -> set force update
    # r -> run all modules
    # q -> quit
    return "\n".join(
        [
            "2",
            "true" if debug_value else "false",
            "3",
            "true" if force_value else "false",
            "r",
            "q",
        ]
    ) + "\n"


def _run_one(label, script_path, debug_value, force_value, out_dir):
    combo = f"debug={str(debug_value).lower()}_force={str(force_value).lower()}"
    name = f"{label}_{combo}"
    log_path = os.path.join(out_dir, f"{name}.log")

    input_text = _menu_input(debug_value, force_value)
    cmd = [sys.executable, script_path]

    start = time.time()
    proc = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        cwd=ROOT,
    )
    elapsed = time.time() - start

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"cmd: {' '.join(cmd)}\n")
        f.write(f"elapsed_sec: {elapsed:.2f}\n")
        f.write(f"returncode: {proc.returncode}\n\n")
        f.write("=== STDOUT ===\n")
        f.write(proc.stdout)
        f.write("\n=== STDERR ===\n")
        f.write(proc.stderr)

    ok = proc.returncode == 0
    return name, ok, log_path


def main():
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(ROOT, "cli_test_logs", ts)
    os.makedirs(out_dir, exist_ok=True)

    scripts = [
        ("jp", JP_CLI),
        ("cn", CN_CLI),
    ]
    combos = [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ]

    results = []
    for label, path in scripts:
        for debug_value, force_value in combos:
            results.append(_run_one(label, path, debug_value, force_value, out_dir))

    failed = [r for r in results if not r[1]]
    print(f"Logs: {out_dir}")
    if failed:
        print("FAILED:")
        for name, _, log_path in failed:
            print(f"  {name} -> {log_path}")
        sys.exit(1)
    print("All runs completed successfully.")


if __name__ == "__main__":
    main()

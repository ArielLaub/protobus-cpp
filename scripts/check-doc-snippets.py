#!/usr/bin/env python3
"""
Compile the C++ snippets of the documentation, so the docs cannot drift from
the API.

A ```cpp block is checked when the line before it is

    <!-- doc-check: compile proto=<directory> -->

The schemas in <directory> (relative to the repository root) are generated
with the protobus-cpp CLI, and the snippet is compiled against them with
-fsyntax-only. Unmarked blocks are fragments and are not compiled.

    check-doc-snippets.py --cli PATH --protoc PATH --cxx PATH --include DIR...
"""

import argparse
import pathlib
import re
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
MARK = re.compile(r"<!-- doc-check: compile proto=(\S+) -->\n```cpp\n(.*?)```", re.S)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--cli", required=True)
    p.add_argument("--protoc", required=True)
    p.add_argument("--cxx", required=True)
    p.add_argument("--include", action="append", default=[])
    args = p.parse_args()

    files = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
    generated = {}
    failures = 0
    checked = 0
    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        for md in files:
            text = md.read_text()
            for m in MARK.finditer(text):
                proto, code = m.group(1), m.group(2)
                line = text[: m.start()].count("\n") + 2
                where = f"{md.relative_to(ROOT)}:{line}"
                if proto not in generated:
                    out = tmp / f"gen{len(generated)}"
                    r = subprocess.run([args.cli, "generate", "--proto-dir", str(ROOT / proto), "--out", str(out),
                                        "--protoc", args.protoc], capture_output=True, text=True)
                    if r.returncode != 0:
                        print(f"{where}: cannot generate {proto}:\n{r.stdout}{r.stderr}")
                        failures += 1
                        continue
                    generated[proto] = out
                src = tmp / f"snippet{checked}.cc"
                # A header snippet is compiled as a main file.
                src.write_text(code.replace("#pragma once\n", ""))
                cmd = [args.cxx, "-std=c++20", "-fsyntax-only", "-Wall", "-Wextra", "-Werror", "-Wno-unused-parameter",
                       "-Wno-unused-variable", f"-I{ROOT / 'include'}", f"-I{generated[proto]}"]
                cmd += [f"-I{d}" for d in args.include]
                if "clang" in args.cxx or sys.platform == "darwin":
                    cmd.append("-Wno-nullability-extension")  # protoc's output uses _Nullable
                cmd.append(str(src))
                r = subprocess.run(cmd, capture_output=True, text=True)
                checked += 1
                if r.returncode != 0:
                    failures += 1
                    print(f"{where}: snippet does not compile:\n{r.stderr}")
    print(f"{checked} snippet(s) checked, {failures} failure(s)")
    return 1 if failures or checked == 0 else 0


if __name__ == "__main__":
    sys.exit(main())

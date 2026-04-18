#!/usr/bin/env python3
"""Update the line-count and test-count badges in README.md.

Usage:
    python scripts/update_badge_counts.py [--dry-run]

The script counts:
  - Lines of source code  (src/gfal/**/*.py)
  - Lines of test code    (tests/**/*.py)
  - Number of test functions (def test_* in tests/)

and rewrites the corresponding shields.io badge URLs in README.md.

Run this before cutting a release or whenever counts change significantly.
"""

import argparse
import pathlib
import re
import sys

TEST_COUNT_ROUNDING = 100


def _fmt(n: int) -> str:
    """Return a compact human-readable number label used in badge URLs.

    Examples:
        1234  -> "1.2k"
        12345 -> "12k"
        999   -> "999"
    """
    if n >= 10_000:
        return f"{round(n / 1000)}k"
    if n >= 1_000:
        return f"{n / 1000:.1f}k"
    return str(n)


def count_lines(path: pathlib.Path) -> int:
    """Count lines in all .py files under *path*."""
    return sum(
        len(f.read_text(encoding="utf-8").splitlines())
        for f in sorted(path.rglob("*.py"))
    )


def count_tests(path: pathlib.Path) -> int:
    """Count test functions (lines matching ``def test_``) under *path*."""
    return sum(
        line.strip().startswith("def test_")
        for f in sorted(path.rglob("*.py"))
        for line in f.read_text(encoding="utf-8").splitlines()
    )


def update_readme(
    readme: pathlib.Path,
    src_lines: int,
    test_lines: int,
    test_count: int,
    *,
    dry_run: bool = False,
) -> bool:
    """Rewrite badge URLs in *readme*.

    Returns True when the file was changed (or would be changed in dry-run).
    """
    text = readme.read_text(encoding="utf-8")
    original = text

    src_label = _fmt(src_lines)
    test_label = _fmt(test_lines)
    # Round test count down to the nearest hundred and suffix with "+"
    tests_label = f"{(test_count // TEST_COUNT_ROUNDING) * TEST_COUNT_ROUNDING}%2B"

    # lines of code badge
    text = re.sub(
        r"(lines%20of%20code-)[\w.%+]+(-blue)",
        rf"\g<1>{src_label}\2",
        text,
    )
    # lines of tests badge
    text = re.sub(
        r"(lines%20of%20tests-)[\w.%+]+(-blue)",
        rf"\g<1>{test_label}\2",
        text,
    )
    # tests badge
    text = re.sub(
        r"(tests-)[\w.%+]+(-green)",
        rf"\g<1>{tests_label}\2",
        text,
    )

    changed = text != original
    if changed:
        if dry_run:
            print("Would update README.md with:")
        else:
            readme.write_text(text, encoding="utf-8")
            print("Updated README.md with:")
        print(f"  lines of code  : {src_label}  ({src_lines:,})")
        print(f"  lines of tests : {test_label}  ({test_lines:,})")
        print(
            f"  tests          : {(test_count // TEST_COUNT_ROUNDING) * TEST_COUNT_ROUNDING}+  ({test_count:,} found)"
        )
    else:
        print("README.md badge counts are already up to date.")

    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing the file.",
    )
    args = parser.parse_args()

    repo_root = pathlib.Path(__file__).parent.parent
    src_dir = repo_root / "src" / "gfal"
    tests_dir = repo_root / "tests"
    readme = repo_root / "README.md"

    for path in (src_dir, tests_dir, readme):
        if not path.exists():
            print(f"ERROR: expected path not found: {path}", file=sys.stderr)
            sys.exit(1)

    src_lines = count_lines(src_dir)
    test_lines = count_lines(tests_dir)
    test_count = count_tests(tests_dir)

    print(
        f"Counted {src_lines:,} source lines, {test_lines:,} test lines, {test_count:,} test functions."
    )

    update_readme(
        readme,
        src_lines,
        test_lines,
        test_count,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

import os
import re
from pathlib import Path

# this script returns the procedures and functions that are missing in repo
# or are not compiled on schema using the folder from the previous step

REPO_ROOT = Path(r"C:\Users\ADMIN\MyProjects\Snowflake_task")
SAVED_DIR = REPO_ROOT / "save_from_snowflake"


def is_target_sql(path: Path) -> bool:
    if path.suffix.lower() != ".sql":
        return False
    name = path.stem.upper()
    return name.startswith("SP_") or name.startswith("FN_")


def normalize_name(path: Path) -> str:
    """
    SP_REFRESH_FCT.sql -> SP_REFRESH_FCT
    SP_REFRESH_FCT__abcd.sql -> SP_REFRESH_FCT
    """
    name = path.stem.upper()
    name = re.sub(r"__.*$", "", name)
    return name


def collect_names(root: Path, exclude: Path | None = None) -> set[str]:
    names = set()
    for p in root.rglob("*.sql"):
        if exclude and exclude in p.parents:
            continue
        if is_target_sql(p):
            names.add(normalize_name(p))
    return names


def print_table(title: str, rows: list[str]):
    print(f"\n{title}")
    print("-" * len(title))
    if not rows:
        print("(empty)")
        return
    for r in rows:
        print(r)


def main():
    saved_names = collect_names(SAVED_DIR)
    repo_names = collect_names(REPO_ROOT, exclude=SAVED_DIR)

    in_both = sorted(saved_names & repo_names)
    only_in_saved = sorted(saved_names - repo_names)
    only_in_repo = sorted(repo_names - saved_names)

    print_table("✅ In both (repo & saved)", in_both)
    print_table("❄️ Only in saved (not in repo)", only_in_saved)
    print_table("❌ Only in repo (not in saved)", only_in_repo)

    print("\nSummary")
    print("-------")
    print(f"Saved: {len(saved_names)}")
    print(f"Repo:  {len(repo_names)}")
    print(f"Both:  {len(in_both)}")


if __name__ == "__main__":
    main()

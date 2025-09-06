
#!/usr/bin/env python3
import argparse
import os
import re
import shutil
from pathlib import Path

KEEP_IDS = set([3, 4, 5, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 56, 62, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112])

def parse_args():
    p = argparse.ArgumentParser(description="Prune CowDatabase2 folders based on allowed frame numbers")
    p.add_argument("--root", required=True, help="Path to the CowDatabase2 root that contains numbered folders")
    p.add_argument("--dry-run", action="store_true", help="Only print actions; do not delete anything")
    p.add_argument("--pattern", default=r"^\d+$", help="Regex to match folder names that are pure numbers (default: ^\\d+$)")
    p.add_argument("--map-leading-zeros", action="store_true", help="Treat folder names like 000123 as 123 for matching")
    p.add_argument("--trash", default="", help="Optional path to move deleted folders instead of removing permanently")
    return p.parse_args()

def normalize_folder_name(name, map_leading_zeros=False):
    if map_leading_zeros:
        try:
            return str(int(name))
        except Exception:
            return name
    return name

def main():
    args = parse_args()
    root = Path(args.root)
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Root path '{{root}}' does not exist or is not a directory")

    pat = re.compile(args.pattern)

    keep_set_str = set(str(i) for i in KEEP_IDS)
    ok, to_delete = [], []

    for entry in sorted([p for p in root.iterdir() if p.is_dir()]):
        name = entry.name
        if not pat.match(name):
            # Non-numeric folders are kept by default (log them as 'skip')
            ok.append((name, "skip_non_numeric"))
            continue

        name_norm = normalize_folder_name(name, args.map_leading_zeros)
        if name_norm in keep_set_str:
            ok.append((name, "keep"))
        else:
            to_delete.append(entry)

    print(f"Found {{len(ok)}} folders to keep/skip, {{len(to_delete)}} folders to delete")

    for name, reason in ok:
        print(f"[KEEP] {{name}}  ({{reason}})")

    if not to_delete:
        print("Nothing to delete. Done.")
        return

    if args.dry_run:
        print("\n[DRY-RUN] Folders that would be deleted:")
        for p in to_delete:
            print("  -", p.name)
        return

    if args.trash:
        trash_dir = Path(args.trash)
        trash_dir.mkdir(parents=True, exist_ok=True)
        for p in to_delete:
            dest = trash_dir / p.name
            print(f"[MOVE] {{p}} -> {{dest}}")
            if dest.exists():
                print(f"  [WARN] Destination exists, appending '_del'")
                dest = Path(str(dest) + "_del")
            shutil.move(str(p), str(dest))
    else:
        for p in to_delete:
            print(f"[DELETE] {{p}}")
            shutil.rmtree(p)

    print("Done.")

if __name__ == "__main__":
    main()

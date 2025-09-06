from pathlib import Path
import csv
import shutil

BASE = Path("/Users/oreo/Desktop/My Folder/UC Berkeley/Projects/coWeight")
RAW_DIR = BASE / "data" / "raw"
UNIFIED_DIR = BASE / "data" / "unified"
INVENTORY_CSV = BASE / "data" / "metadata" / "inventory.csv"
META_OUT = BASE / "data" / "metadata" / "unified_manifest.csv"

VIEWS = ["left", "right", "top"]

def case_insensitive_dir(parent: Path, name: str):
    if parent is None or not parent.exists():
        return None
    for p in parent.iterdir():
        if p.is_dir() and p.name.lower() == name.lower():
            return p
    return None

def find_contains_dir(parent: Path, needle: str):
    if parent is None or not parent.exists():
        return None
    needle = needle.lower()
    for p in parent.iterdir():
        if p.is_dir() and needle in p.name.lower():
            return p
    return None

def pick_rgb_png(view_dir: Path):
    """Pick an RGB png in view_dir (filename NOT containing 'depth')."""
    if not view_dir or not view_dir.exists():
        return None
    cands = [p for p in view_dir.glob("*.png") if "depth" not in p.name.lower()]
    # if multiple, choose lexicographically first (stable & deterministic)
    return sorted(cands)[0] if cands else None

def pick_depth_png(view_dir: Path):
    if not view_dir or not view_dir.exists():
        return None
    cands = [p for p in view_dir.glob("*.png") if "depth" in p.name.lower()]
    return sorted(cands)[0] if cands else None

def pick_view_ply(view_dir: Path):
    if not view_dir or not view_dir.exists():
        return None
    cands = sorted([p for p in view_dir.glob("*.ply")])
    return cands[0] if cands else None

def ensure_symlink(src: Path, dst: Path):
    if src is None or not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    # If exists and is correct, keep; otherwise replace
    if dst.exists() or dst.is_symlink():
        try:
            if dst.is_symlink() and dst.resolve() == src.resolve():
                return True
            dst.unlink()
        except FileNotFoundError:
            pass
    dst.symlink_to(src)
    return True

def unify_cowdb(cow_dir: Path, out_cow_dir: Path):
    """
    CowDB:
      cow_dir/
        <left>.ply, <right>.ply, <top>.ply (top-level)
        raw/
          left/ {rgb.png, depth.png, maybe .ply}
          right/ ...
          top/   ...
    """
    raw_dir = case_insensitive_dir(cow_dir, "raw")

    # top-level ply files sometimes exist in CowDB
    top_level_plys = {v: None for v in VIEWS}
    for ply in sorted(cow_dir.glob("*.ply")):
        name = ply.name.lower()
        for v in VIEWS:
            if v in name:
                top_level_plys[v] = ply

    # Create per-view links
    rows = []
    for v in VIEWS:
        view_src = case_insensitive_dir(raw_dir, v) if raw_dir else None
        rgb = pick_rgb_png(view_src)
        depth = pick_depth_png(view_src)
        vply = pick_view_ply(view_src) or top_level_plys.get(v)

        view_out = out_cow_dir / v
        linked_rgb = ensure_symlink(rgb, view_out / "rgb.png")
        linked_depth = ensure_symlink(depth, view_out / "depth.png")
        linked_vply = ensure_symlink(vply, view_out / "view.ply")

        rows.append({
            "dataset": "CowDB",
            "cow_id": cow_dir.name,
            "view": v,
            "rgb_path": str((view_out / "rgb.png").resolve()) if linked_rgb else "",
            "depth_path": str((view_out / "depth.png").resolve()) if linked_depth else "",
            "view_ply_path": str((view_out / "view.ply").resolve()) if linked_vply else "",
            "aligned_ply_path": "",  # not applicable here by description
        })
    return rows

def unify_cowdatabase2(cow_dir: Path, out_cow_dir: Path):
    """
    CowDatabase2:
      cow_dir/
        rawData/ (or similar)
          left/ {rgb.png, depth.png, view .ply}
          right/ ...
          top/   ...
        aligned.ply
    """
    rawdata_dir = case_insensitive_dir(cow_dir, "rawData")
    if rawdata_dir is None:
        rawdata_dir = find_contains_dir(cow_dir, "raw")

    aligned = cow_dir / "aligned.ply"
    linked_aligned = ensure_symlink(aligned if aligned.exists() else None, out_cow_dir / "aligned.ply")

    rows = []
    for v in VIEWS:
        view_src = case_insensitive_dir(rawdata_dir, v) if rawdata_dir else None
        rgb = pick_rgb_png(view_src)
        depth = pick_depth_png(view_src)
        vply = pick_view_ply(view_src)

        view_out = out_cow_dir / v
        linked_rgb = ensure_symlink(rgb, view_out / "rgb.png")
        linked_depth = ensure_symlink(depth, view_out / "depth.png")
        linked_vply = ensure_symlink(vply, view_out / "view.ply")

        rows.append({
            "dataset": "CowDatabase2",
            "cow_id": cow_dir.name,
            "view": v,
            "rgb_path": str((view_out / "rgb.png").resolve()) if linked_rgb else "",
            "depth_path": str((view_out / "depth.png").resolve()) if linked_depth else "",
            "view_ply_path": str((view_out / "view.ply").resolve()) if linked_vply else "",
            "aligned_ply_path": str((out_cow_dir / "aligned.ply").resolve()) if linked_aligned else "",
        })
    return rows

def main():
    rows = []

    cowdb_root = RAW_DIR / "CowDB"
    if cowdb_root.exists():
        for cow_dir in sorted([p for p in cowdb_root.iterdir() if p.is_dir()]):
            out_cow_dir = UNIFIED_DIR / "CowDB" / cow_dir.name
            rows.extend(unify_cowdb(cow_dir, out_cow_dir))

    cowdb2_root = RAW_DIR / "CowDatabase2"
    if cowdb2_root.exists():
        for cow_dir in sorted([p for p in cowdb2_root.iterdir() if p.is_dir()]):
            out_cow_dir = UNIFIED_DIR / "CowDatabase2" / cow_dir.name
            rows.extend(unify_cowdatabase2(cow_dir, out_cow_dir))

    # Write manifest
    META_OUT.parent.mkdir(parents=True, exist_ok=True)
    with META_OUT.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "dataset","cow_id","view",
            "rgb_path","depth_path","view_ply_path","aligned_ply_path"
        ])
        writer.writeheader()
        writer.writerows(rows)

    # Print summary
    total = len(rows)
    has_rgb = sum(1 for r in rows if r["rgb_path"])
    has_depth = sum(1 for r in rows if r["depth_path"])
    has_vply = sum(1 for r in rows if r["view_ply_path"])
    has_aligned = sum(1 for r in rows if r["aligned_ply_path"])

    print(f"Symlinked rows: {total}")
    print(f"  RGB:    {has_rgb}/{total}")
    print(f"  Depth:  {has_depth}/{total}")
    print(f"  ViewPLY:{has_vply}/{total}")
    print(f"  Aligned:{has_aligned} (counting cow-level, may repeat per-view rows)")

if __name__ == "__main__":
    main()

from pathlib import Path
import csv
import re
import sys

RAW_DIR = Path("/Users/oreo/Desktop/My Folder/UC Berkeley/Projects/coWeight/data/raw")
OUT_CSV = Path("/Users/oreo/Desktop/My Folder/UC Berkeley/Projects/coWeight/data/metadata/inventory.csv")

VIEWS = ["left", "right", "top"]

def find_subdir_case_insensitive(parent: Path, name: str):
    """Return a subdirectory of `parent` whose name (lowercased) equals `name`."""
    for p in parent.iterdir():
        if p.is_dir() and p.name.lower() == name.lower():
            return p
    return None

def find_any_subdir_contains(parent: Path, needle: str):
    """Return first subdirectory containing `needle` in its lowercased name."""
    for p in parent.iterdir():
        if p.is_dir() and needle in p.name.lower():
            return p
    return None

def first_match(globpaths):
    for g in globpaths:
        matches = list(g)
        if matches:
            return matches[0]
    return None

def has_pngs(view_dir: Path):
    """Return (has_rgb, has_depth) heuristically by filename."""
    if not view_dir or not view_dir.exists():
        return (False, False)
    pngs = list(view_dir.glob("*.png"))
    has_depth = any("depth" in p.name.lower() for p in pngs)
    # treat non-depth PNGs as RGB
    has_rgb = any(("depth" not in p.name.lower()) for p in pngs)
    return (has_rgb, has_depth)

def has_view_ply(view_dir: Path):
    if not view_dir or not view_dir.exists():
        return False
    return any(p.suffix.lower()==".ply" for p in view_dir.iterdir())

def label_view_from_name(path: Path):
    n = path.name.lower()
    for v in VIEWS:
        if v in n:
            return v
    return None

def scan_cowdb(cow_dir: Path):
    """CowDB structure:
    cow_dir/
      <left>.ply, <right>.ply, <top>.ply
      raw/
        left/  (rgb.png, depth.png, maybe .ply)
        right/
        top/
    """
    rows = []
    raw_dir = find_subdir_case_insensitive(cow_dir, "raw")
    top_level_plys = [p for p in cow_dir.glob("*.ply")]
    top_ply_by_view = {label_view_from_name(p): p for p in top_level_plys}

    for view in VIEWS:
        view_dir = find_subdir_case_insensitive(raw_dir, view) if raw_dir else None
        has_rgb, has_depth = has_pngs(view_dir)
        row = {
            "dataset": "CowDB",
            "cow_id": cow_dir.name,
            "view": view,
            "has_rgb_png": has_rgb,
            "has_depth_png": has_depth,
            "has_view_ply": has_view_ply(view_dir),
            "has_top_level_ply": top_ply_by_view.get(view) is not None,
            "has_aligned_ply": False,  # not present in CowDB by your description
        }
        rows.append(row)
    return rows

def scan_cowdatabase2(cow_dir: Path):
    """CowDatabase2 structure:
    cow_dir/
      rawData/
        left/  (rgb.png, depth.png, view .ply)
        right/
        top/
      aligned.ply
    """
    rows = []
    # rawData could be 'rawData' or similar; try exact then contains 'raw'
    rawdata_dir = find_subdir_case_insensitive(cow_dir, "rawData")
    if rawdata_dir is None:
        rawdata_dir = find_any_subdir_contains(cow_dir, "raw")  # fallback

    aligned_ply = (cow_dir / "aligned.ply").exists()

    for view in VIEWS:
        view_dir = find_subdir_case_insensitive(rawdata_dir, view) if rawdata_dir else None
        has_rgb, has_depth = has_pngs(view_dir)
        row = {
            "dataset": "CowDatabase2",
            "cow_id": cow_dir.name,
            "view": view,
            "has_rgb_png": has_rgb,
            "has_depth_png": has_depth,
            "has_view_ply": has_view_ply(view_dir),
            "has_top_level_ply": False,   # not used here
            "has_aligned_ply": aligned_ply,
        }
        rows.append(row)
    return rows

def main():
    datasets = [d for d in RAW_DIR.iterdir() if d.is_dir()]
    out_rows = []
    for ds in datasets:
        if ds.name == "CowDB":
            for cow_dir in sorted([p for p in ds.iterdir() if p.is_dir()]):
                out_rows.extend(scan_cowdb(cow_dir))
        elif ds.name == "CowDatabase2":
            for cow_dir in sorted([p for p in ds.iterdir() if p.is_dir()]):
                out_rows.extend(scan_cowdatabase2(cow_dir))
        else:
            # Ignore other folders
            pass

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "dataset","cow_id","view",
            "has_rgb_png","has_depth_png",
            "has_view_ply","has_top_level_ply","has_aligned_ply"
        ])
        writer.writeheader()
        writer.writerows(out_rows)

    # Print a tiny summary
    total = len(out_rows)
    print(f"Wrote {total} rows to {OUT_CSV}")
    by_ds = {}
    for r in out_rows:
        by_ds.setdefault(r["dataset"], 0)
        by_ds[r["dataset"]] += 1
    for k,v in by_ds.items():
        print(f"  {k}: {v} rows")

if __name__ == "__main__":
    main()

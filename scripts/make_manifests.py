# scripts/make_manifests_v3.py
from pathlib import Path
import csv
import pandas as pd

# ---------- CONFIG ----------
BASE = Path("/Users/oreo/Desktop/My Folder/UC Berkeley/Projects/coWeight")
RAW_DIR = BASE / "data" / "raw"
UNIFIED_DIR = BASE / "data" / "unified"
META_DIR = BASE / "data" / "metadata"

# Labels
COWDB_LABELS = META_DIR /   "numeric_cowdb.csv"             # adjust if yours is elsewhere
COWDB2_LABELS = META_DIR /  "numeric_cowdatabase2.csv"    # we created earlier (cow_id, weight, ...)

# Outputs
COWDB_OUT = META_DIR / "cowdb_manifest.csv"
COWDB2_OUT = META_DIR / "cowdatabase2_manifest.csv"

# Views (multi-view support)
VIEWS = ["left", "right", "top"]
IMG_EXTS = {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"}

# ---------- HELPERS ----------
def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def normalize_id(x) -> str:
    try:
        return str(int(str(x)))
    except Exception:
        return str(x)

def is_rgb_file(p: Path) -> bool:
    """RGB only: exclude files whose names contain 'depth'."""
    n = p.name.lower()
    return p.is_file() and (p.suffix.lower() in IMG_EXTS) and ("depth" not in n)

def first_rgb_in(d: Path) -> Path | None:
    if not d or not d.exists(): return None
    for p in sorted(d.iterdir()):
        if is_rgb_file(p):
            return p
    return None

def first_rgb_recursive(d: Path) -> Path | None:
    if not d or not d.exists(): return None
    for p in sorted(d.rglob("*")):
        if is_rgb_file(p):
            return p
    return None

def ci_child_dir(parent: Path, name: str) -> Path | None:
    if not parent or not parent.exists(): return None
    for p in parent.iterdir():
        if p.is_dir() and p.name.lower() == name.lower():
            return p
    return None

def any_child_dir_contains(parent: Path, needle: str) -> Path | None:
    if not parent or not parent.exists(): return None
    needle = needle.lower()
    for p in parent.iterdir():
        if p.is_dir() and needle in p.name.lower():
            return p
    return None

def find_weight_col(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if "weight" in str(c).strip().lower().replace(" ", ""):
            return c
    return None

# ---------- COWDATABASE2 (UNIFIED, RGB ONLY, MULTI-VIEW) ----------
def build_cowdatabase2_manifest():
    root = UNIFIED_DIR / "CowDatabase2"
    if not root.exists():
        raise SystemExit(f"[CowDatabase2] Unified root not found: {root}")

    if not COWDB2_LABELS.exists():
        raise SystemExit(f"[CowDatabase2] Missing labels file: {COWDB2_LABELS}")

    df = pd.read_csv(COWDB2_LABELS)
    wc = find_weight_col(df)
    if wc is None:
        raise SystemExit("[CowDatabase2] Could not find a weight column (must contain 'weight').")

    df["cow_id"] = df["cow_id"].apply(normalize_id)

    rows = []
    missing_ids = []
    per_view = {v: 0 for v in VIEWS}
    cows_with_any = 0

    for _, r in df.iterrows():
        cid = r["cow_id"]
        cow_dir = root / cid
        if not cow_dir.exists():
            missing_ids.append(cid)
            continue

        found_any = False

        # Prefer strict per-view dirs (left/right/top). Fallback: any subdir containing the view name.
        for v in VIEWS:
            view_dir = ci_child_dir(cow_dir, v)
            img = first_rgb_in(view_dir) if view_dir else None
            if not img:
                alt = any_child_dir_contains(cow_dir, v)
                if alt:
                    img = first_rgb_recursive(alt)
            if img:
                found_any = True
                per_view[v] += 1
                rows.append({
                    "dataset": "CowDatabase2",
                    "cow_id": cid,
                    "view": v,
                    "image_path": str(img.resolve()),
                    "weight_kg": float(r[wc]),
                })

        # If no view-specific image found, but any RGB exists, write a generic row
        if not found_any:
            any_img = first_rgb_recursive(cow_dir)
            if any_img:
                found_any = True
                rows.append({
                    "dataset": "CowDatabase2",
                    "cow_id": cid,
                    "view": "",
                    "image_path": str(any_img.resolve()),
                    "weight_kg": float(r[wc]),
                })

        if found_any:
            cows_with_any += 1
        else:
            missing_ids.append(cid)

    ensure_parent(COWDB2_OUT)
    with COWDB2_OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["dataset","cow_id","view","image_path","weight_kg"])
        w.writeheader()
        w.writerows(rows)

    print(f"[CowDatabase2] labels: {len(df)} | cows with >=1 RGB: {cows_with_any} | rows: {len(rows)} -> {COWDB2_OUT}")
    for v in VIEWS:
        print(f"[CowDatabase2]   rows view={v}: {per_view[v]}")
    if missing_ids:
        print(f"[CowDatabase2]   WARNING: no RGB found for {len(missing_ids)} ids (e.g., {missing_ids[:10]})")

# ---------- COWDB (COVER ALL LABEL IDS, RGB ONLY, MULTI-VIEW) ----------
def build_cowdb_manifest():
    unified_root = UNIFIED_DIR / "CowDB"
    raw_root = RAW_DIR / "CowDB"

    if not COWDB_LABELS.exists():
        raise SystemExit(f"[CowDB] Missing labels file: {COWDB_LABELS}")
    df = pd.read_csv(COWDB_LABELS)

    # Detect columns
    cow_col = None
    for c in df.columns:
        if str(c).strip().lower().replace(" ", "") in {"cowid","cow_id","id"}:
            cow_col = c
            break
    if cow_col is None:
        raise SystemExit("[CowDB] Could not find 'cow_id' column in labels.csv")
    wc = find_weight_col(df)
    if wc is None:
        raise SystemExit("[CowDB] Could not find a weight column (e.g., 'weight').")

    df["cow_id_norm"] = df[cow_col].apply(normalize_id)

    rows = []
    seen_cows = set()
    per_view = {v: 0 for v in VIEWS}
    cows_with_any = 0
    missing_ids = []

    for _, r in df.iterrows():
        cid = r["cow_id_norm"]
        weight = float(r[wc])

        # Prefer unified/<cid>, else raw/<cid>, else raw/images/<cid>.* (single-view fallback)
        cow_dir = None
        if (unified_root / cid).exists():
            cow_dir = unified_root / cid
        elif (raw_root / cid).exists():
            cow_dir = raw_root / cid

        found_any = False

        if cow_dir and cow_dir.exists():
            # Try strict per-view dirs; fallback to any subdir containing the view name
            for v in VIEWS:
                view_dir = ci_child_dir(cow_dir, v)
                img = first_rgb_in(view_dir) if view_dir else None
                if not img:
                    alt = any_child_dir_contains(cow_dir, v)
                    if alt:
                        img = first_rgb_recursive(alt)
                if img:
                    found_any = True
                    per_view[v] += 1
                    rows.append({
                        "dataset": "CowDB",
                        "cow_id": cid,
                        "view": v,
                        "image_path": str(img.resolve()),
                        "weight_kg": weight,
                    })

            # If still nothing per-view, take any RGB underneath this cow_dir
            if not found_any:
                any_img = first_rgb_recursive(cow_dir)
                if any_img:
                    found_any = True
                    rows.append({
                        "dataset": "CowDB",
                        "cow_id": cid,
                        "view": "",
                        "image_path": str(any_img.resolve()),
                        "weight_kg": weight,
                    })

        # If we couldn't locate a cow directory, try raw/images/<cid>.* as a last resort
        if not found_any:
            images_dir = raw_root / "images"
            if images_dir.exists():
                cands = sorted(p for p in images_dir.glob(f"{cid}*") if is_rgb_file(p))
                if cands:
                    found_any = True
                    rows.append({
                        "dataset": "CowDB",
                        "cow_id": cid,
                        "view": "",
                        "image_path": str(cands[0].resolve()),
                        "weight_kg": weight,
                    })

        if found_any:
            cows_with_any += 1
            seen_cows.add(cid)
        else:
            missing_ids.append(cid)

    ensure_parent(COWDB_OUT)
    with COWDB_OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["dataset","cow_id","view","image_path","weight_kg"])
        w.writeheader()
        w.writerows(rows)

    print(f"[CowDB] labels: {len(df)} | cows with >=1 RGB: {cows_with_any} | rows: {len(rows)} -> {COWDB_OUT}")
    for v in VIEWS:
        print(f"[CowDB]   rows view={v}: {per_view[v]}")
    if missing_ids:
        print(f"[CowDB]   WARNING: no RGB found for {len(missing_ids)} ids (e.g., {missing_ids[:10]})")

def main():
    build_cowdatabase2_manifest()  # RGB-only, multi-view, from UNIFIED (pruned)
    build_cowdb_manifest()         # RGB-only, multi-view, cover all label IDs

if __name__ == "__main__":
    main()

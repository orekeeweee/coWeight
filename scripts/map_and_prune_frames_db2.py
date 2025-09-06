from pathlib import Path
import pandas as pd
import numpy as np
import re
import shutil
import sys
from datetime import datetime

# -------------- CONFIG --------------
BASE = Path("/Users/oreo/Desktop/My Folder/UC Berkeley/Projects/coWeight")
RAW_DB2 = BASE / "data" / "raw" / "CowDatabase2"
LABELS_XLSX = BASE / "data" / "labels" / "Database2022v2.xlsx"  # your cleaned Excel
SHEET_NAME = None  # e.g., "Лист1"; set to None to use the first sheet

# REQUIRED column names in the Excel:
EXCEL_COL_COWID   = "cow_id"        # must match folder number (string/int)
EXCEL_COL_FRAMENO = "frame_number"  # integers of frames you want to keep

# Where to put outputs and quarantined files
OUT_DIR = BASE / "data" / "metadata"
QUARANTINE_DIR = BASE / "data" / "quarantine_db2"  # unmatched files will be moved here (safe)

# Safety: do a dry run first!
DRY_RUN = True     # True = print what would happen; False = actually move/delete
DELETE_INSTEAD_OF_MOVE = False  # if True (and DRY_RUN False), delete instead of move
# ------------------------------------

def load_label_frames():
    xl = pd.ExcelFile(LABELS_XLSX)
    sheet = SHEET_NAME or xl.sheet_names[0]
    df = xl.parse(sheet)
    # normalize expected columns
    cols = {c.lower().strip(): c for c in df.columns}
    def pick(name):
        # accept exact or common variants
        for k, v in cols.items():
            if k == name.lower():
                return v
        # loose matches
        for k, v in cols.items():
            if name.replace("_","") in k.replace("_",""):
                return v
        return None

    c_id = pick(EXCEL_COL_COWID)
    c_fr = pick(EXCEL_COL_FRAMENO)
    if not c_id or not c_fr:
        raise ValueError(f"Could not find columns '{EXCEL_COL_COWID}' and '{EXCEL_COL_FRAMENO}' in {LABELS_XLSX}. Found: {list(df.columns)}")

    # clean
    def clean_id(x):
        if pd.isna(x): return np.nan
        s = str(x).strip().replace(" ","")
        if re.fullmatch(r"\d+", s): return str(int(s))
        s = re.sub(r"[^\d]", "", s)
        return str(int(s)) if s else np.nan

    def to_int(x):
        try:
            return int(str(x).strip().split('.')[0])
        except:
            return np.nan

    out = df[[c_id, c_fr]].rename(columns={c_id:"cow_id", c_fr:"frame_number"})
    out["cow_id"] = out["cow_id"].apply(clean_id)
    out["frame_number"] = out["frame_number"].apply(to_int)
    out = out.dropna(subset=["cow_id","frame_number"]).copy()
    out["frame_number"] = out["frame_number"].astype(int)

    # group to sets per cow
    keep_map = {cid: set(g["frame_number"].tolist()) for cid, g in out.groupby("cow_id")}
    return keep_map

FRAME_PATTERNS = [
    r"frame[_\- ]?(\d+)",
    r"\bf[_\- ]?(\d+)\b",
    r"\bfn[_\- ]?(\d+)\b",
    r"(\d+)"  # last resort: any number group
]

def parse_frame_from_name(name: str):
    """
    Try multiple patterns. Return the LAST match of the first pattern that hits,
    so 'left_rgb_00012.png' -> 12
    """
    nm = name.lower()
    for pat in FRAME_PATTERNS:
        m = re.findall(pat, nm)
        if m:
            try:
                return int(m[-1])
            except:
                # strip non-digits and attempt
                s = re.sub(r"[^\d]", "", m[-1])
                if s.isdigit():
                    return int(s)
    return None

def list_files_for_view(view_dir: Path):
    files = []
    for f in sorted(view_dir.iterdir()):
        if not f.is_file(): 
            continue
        ext = f.suffix.lower()
        if ext not in [".png",".jpg",".jpeg",".tiff",".tif",".ply"]:
            continue
        files.append(f)
    return files

def align_by_order(files, keep_frames_set):
    """
    Fallback: map sorted files to sorted frame_numbers by index.
    Return mapping {file_path: frame_number} and a list of leftovers (if counts differ).
    """
    want = sorted(list(keep_frames_set))
    n = min(len(files), len(want))
    mapping = {files[i]: want[i] for i in range(n)}
    leftovers_files = files[n:]
    leftovers_frames = want[n:]
    return mapping, leftovers_files, leftovers_frames

def ensure_quarantine(dst: Path):
    dst.mkdir(parents=True, exist_ok=True)

def move_or_delete(src: Path, dst_dir: Path):
    if DRY_RUN:
        print(f"[DRY_RUN] Would move {src} -> {dst_dir / src.name}")
        return
    if DELETE_INSTEAD_OF_MOVE:
        try:
            src.unlink()
            print(f"Deleted {src}")
        except Exception as e:
            print(f"ERROR deleting {src}: {e}")
    else:
        try:
            dst_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst_dir / src.name))
            print(f"Moved {src} -> {dst_dir / src.name}")
        except Exception as e:
            print(f"ERROR moving {src}: {e}")

def main():
    if not RAW_DB2.exists():
        print(f"RAW folder not found: {RAW_DB2}")
        sys.exit(1)

    keep_map = load_label_frames()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    kept_rows = []
    quarantined = 0
    total_files = 0

    for cow_dir in sorted([p for p in RAW_DB2.iterdir() if p.is_dir()]):
        cow_id = cow_dir.name
        # find rawData folder
        raw_dirs = [d for d in cow_dir.iterdir() if d.is_dir() and "raw" in d.name.lower()]
        if not raw_dirs:
            continue
        raw_dir = raw_dirs[0]

        keep_frames = keep_map.get(cow_id, set())  # frames we want for this cow
        for view in ["left","right","top"]:
            view_dir = next((d for d in raw_dir.iterdir() if d.is_dir() and d.name.lower()==view), None)
            if view_dir is None:
                continue
            files = list_files_for_view(view_dir)
            total_files += len(files)

            # First try filename parsing
            parsed = {f: parse_frame_from_name(f.name) for f in files}
            exact_kept = [f for f, fr in parsed.items() if fr is not None and fr in keep_frames]
            needs_decision = [f for f, fr in parsed.items() if fr is None or fr not in keep_frames]

            # If we parsed nothing at all, try order-based mapping for all files in this view
            if len(exact_kept) == 0 and len(keep_frames) > 0:
                mapping, leftovers_files, leftovers_frames = align_by_order(files, keep_frames)
                # files mapped by order are kept; leftovers are quarantined
                for f, fr in mapping.items():
                    kept_rows.append({
                        "cow_id": cow_id, "view": view, "file": str(f), "frame_number": fr, "method": "order_align"
                    })
                for f in leftovers_files:
                    move_or_delete(f, QUARANTINE_DIR / cow_id / view)
                    quarantined += 1
                continue  # go next view

            # Keep the ones we matched by name
            for f in exact_kept:
                kept_rows.append({
                    "cow_id": cow_id, "view": view, "file": str(f),
                    "frame_number": parse_frame_from_name(f.name), "method": "filename_parse"
                })

            # Quarantine the rest
            for f in needs_decision:
                move_or_delete(f, QUARANTINE_DIR / cow_id / view)
                quarantined += 1

    kept_df = pd.DataFrame(kept_rows)
    out_csv = OUT_DIR / f"db2_kept_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    kept_df.to_csv(out_csv, index=False)

    print("\n===== SUMMARY =====")
    print(f"Total files scanned: {total_files}")
    print(f"Kept (rows in CSV):  {len(kept_df)}")
    print(f"Quarantined:         {quarantined}")
    print(f"Kept files manifest: {out_csv}")
    print(f"Quarantine folder:   {QUARANTINE_DIR}")
    print(f"DRY_RUN = {DRY_RUN} | DELETE = {DELETE_INSTEAD_OF_MOVE}")

if __name__ == "__main__":
    main()

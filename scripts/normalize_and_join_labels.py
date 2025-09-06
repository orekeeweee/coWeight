from pathlib import Path
import pandas as pd
import numpy as np
import re
import sys

BASE = Path("/Users/oreo/Desktop/My Folder/UC Berkeley/Projects/coWeight")
LABELS_DIR = BASE / "data" / "labels"          # put the two .xlsx here
RAW_LABEL_COWDB = LABELS_DIR / "Manual_measurements.xlsx"
RAW_LABEL_DB2   = LABELS_DIR / "Database2022v2.xlsx"

MANIFEST_CSV = BASE / "data" / "metadata" / "unified_manifest.csv"
LABELS_OUT   = BASE / "data" / "metadata" / "labels_normalized.csv"
TRAIN_OUT    = BASE / "data" / "metadata" / "training_table.csv"

# --------- EDIT THIS CONFIG IF NEEDED ----------
# For each dataset, define how to extract/rename columns per sheet.
# Use any of: 'cow_id', 'weight_kg', 'date_measured', 'sex', 'breed', 'age_months', 'notes'
# If your file stores pounds, map to 'weight_lb' and the script will convert to kg.

# --------- EDIT THIS CONFIG IF NEEDED ----------
CONFIG = {
    "CowDB": {
        "file": BASE / "data" / "labels" / "Manual_measurements.xlsx",
        "sheets": {
            # Sheet name seen in your log was 'result'
            # Columns present: ['N','live weithg', 'withers height', ...]
            "result": {
                "cow_id": "N",
                "weight_kg": "live weithg",  # note the exact misspelling
                # add more if useful, e.g. "notes": "..." etc.
            },
        },
        "autodetect": {  # keep as fallback
            "cow_id":   ["n","cow","id","animal","number","name"],
            "weight_kg":["live weithg","weight_kg","weight (kg)","kg","weight"],
            "weight_lb":["weight_lb","weight (lb)","lbs","pounds"],
            "date":     ["date","measured","date_measured","measurement_date"],
            "sex":      ["sex","gender"],
            "breed":    ["breed"],
            "age_months":["age_months","age (months)","months"],
            "notes":    ["notes","comment","remarks"],
        }
    },
    "CowDatabase2": {
        "file": BASE / "data" / "labels" / "Database2022v2.xlsx",
        "sheets": {
            # Your sheet is 'Лист1' (Russian for 'Sheet1')
            # Likely cow id is 'Tag number' (adjust if your folder names match another id)
            "Лист1": {
                "cow_id": "Tag number",
                "weight_kg": "Live weigth",  # exact misspelling
                # OPTIONAL: if you want age/sex/etc and you see the columns:
                # "sex": "Sex",
                # "breed": "Breed",
                # "date_measured": "Birthday",  # not ideal, but available
            },
        },
        "autodetect": {
            "cow_id":   ["tag number","chip number","cow","id","animal","number","name","№"],
            "weight_kg":["live weigth","weight_kg","weight (kg)","kg","weight"],
            "weight_lb":["weight_lb","weight (lb)","lbs","pounds"],
            "date":     ["date","measured","date_measured","measurement_date","birthday"],
            "sex":      ["sex","gender"],
            "breed":    ["breed"],
            "age_months":["age_months","age (months)","months"],
            "notes":    ["notes","comment","remarks"],
        }
    }
}
# ------------------------------------------------

# ------------------------------------------------

def normalize_whitespace_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def pick_first_present(cols_lower, options):
    # Return first col in df whose lowercase matches any target option
    for opt in options:
        for c in cols_lower:
            if c == opt.lower():
                return c
    return None

def detect_columns(df: pd.DataFrame, rules: dict):
    cols = list(df.columns)
    cols_lower = [c.lower() for c in cols]

    mapping = {}
    # cow_id
    c = pick_first_present(cols_lower, rules["cow_id"])
    if c: mapping["cow_id"] = cols[cols_lower.index(c)]
    # weights (kg or lb)
    ckg = pick_first_present(cols_lower, rules["weight_kg"])
    clb = pick_first_present(cols_lower, rules["weight_lb"])
    if ckg: mapping["weight_kg"] = cols[cols_lower.index(ckg)]
    elif clb: mapping["weight_lb"] = cols[cols_lower.index(clb)]
    # other optional
    c = pick_first_present(cols_lower, rules["date"])
    if c: mapping["date_measured"] = cols[cols_lower.index(c)]
    c = pick_first_present(cols_lower, rules["sex"])
    if c: mapping["sex"] = cols[cols_lower.index(c)]
    c = pick_first_present(cols_lower, rules["breed"])
    if c: mapping["breed"] = cols[cols_lower.index(c)]
    c = pick_first_present(cols_lower, rules["age_months"])
    if c: mapping["age_months"] = cols[cols_lower.index(c)]
    c = pick_first_present(cols_lower, rules["notes"])
    if c: mapping["notes"] = cols[cols_lower.index(c)]

    return mapping

def coerce_cow_id(x):
    # make it string & keep digits/letters/underscore only; common case is numeric string
    if pd.isna(x):
        return None
    s = str(x).strip()
    # drop leading zeros only if purely numeric (to match folder names like "7" not "007")
    if re.fullmatch(r"\d+", s):
        return str(int(s))
    # otherwise normalize lightly
    s = re.sub(r"[^A-Za-z0-9_-]+", "", s)
    return s or None

def to_kg(val, src="kg"):
    if pd.isna(val):
        return np.nan
    try:
        v = float(val)
    except:
        return np.nan
    if src == "kg":
        return v
    if src == "lb":
        return v * 0.45359237
    return np.nan

def parse_age_months(val):
    if pd.isna(val): return np.nan
    try:
        return float(val)
    except:
        # handle strings like "24 m", "2y", "2 years"
        s = str(val).lower().strip()
        m = re.findall(r"[\d\.]+", s)
        if not m: return np.nan
        num = float(m[0])
        if "y" in s:  # years
            return num * 12.0
        return num

def load_and_normalize_one(dataset_name, file_path: Path, config: dict) -> pd.DataFrame:
    if not file_path.exists():
        print(f"[WARN] File missing for {dataset_name}: {file_path}")
        return pd.DataFrame()

    xls = pd.ExcelFile(file_path)
    target_rows = []

    # Decide which sheets to parse
    sheets_cfg = config.get("sheets", {})
    sheets_to_try = list(xls.sheet_names) if not sheets_cfg else list(sheets_cfg.keys())

    for sheet in sheets_to_try:
        try:
            raw = pd.read_excel(file_path, sheet_name=sheet)
        except Exception as e:
            print(f"[WARN] Cannot read {dataset_name}/{sheet}: {e}")
            continue

        df = normalize_whitespace_cols(raw)

        # Use explicit mapping if provided; otherwise autodetect
        explicit_map = sheets_cfg.get(sheet, None)
        if explicit_map is None:
            mapping = detect_columns(df, config["autodetect"])
        else:
            # Convert provided column names to found columns case-insensitively
            mapping = {}
            df_cols_lower = {c.lower(): c for c in df.columns}
            for k, v in explicit_map.items():
                if v is None: 
                    continue
                vv = v.lower()
                if vv in df_cols_lower:
                    mapping[k] = df_cols_lower[vv]
                else:
                    print(f"[WARN] {dataset_name}/{sheet}: mapping for '{k}' -> '{v}' not found in columns.")
        
        # Minimal requirement: cow_id + some weight
        if "cow_id" not in mapping or ("weight_kg" not in mapping and "weight_lb" not in mapping):
            print(f"[INFO] {dataset_name}/{sheet}: could not find required columns.")
            print("       Columns in sheet:", list(df.columns))
            print("       Adjust CONFIG['",dataset_name,"']['sheets'] or autodetect keywords.", sep="")
            continue

        # Build normalized rows
        for idx, row in df.iterrows():
            cow = coerce_cow_id(row.get(mapping["cow_id"], np.nan))
            if not cow:
                continue

            if "weight_kg" in mapping:
                w = to_kg(row.get(mapping["weight_kg"], np.nan), "kg")
            else:
                w = to_kg(row.get(mapping["weight_lb"], np.nan), "lb")
            if pd.isna(w):
                continue

            out = {
                "dataset": dataset_name,
                "cow_id": cow,
                "view": "",  # labels are cow-level; will be broadcast to views later
                "weight_kg": w,
                "date_measured": pd.to_datetime(row.get(mapping.get("date_measured",""), pd.NaT), errors="coerce").date() if mapping.get("date_measured") else pd.NaT,
                "sex": str(row.get(mapping.get("sex",""), "")) if mapping.get("sex") else "",
                "breed": str(row.get(mapping.get("breed",""), "")) if mapping.get("breed") else "",
                "age_months": parse_age_months(row.get(mapping.get("age_months",""), np.nan)) if mapping.get("age_months") else np.nan,
                "notes": str(row.get(mapping.get("notes",""), "")) if mapping.get("notes") else "",
                "source_file": file_path.name,
                "source_sheet": sheet,
                "source_row_index": idx,
            }
            target_rows.append(out)

    return pd.DataFrame(target_rows)

def main():
    # Load labels for both datasets
    parts = []
    parts.append(load_and_normalize_one("CowDB", CONFIG["CowDB"]["file"], CONFIG["CowDB"]))
    parts.append(load_and_normalize_one("CowDatabase2", CONFIG["CowDatabase2"]["file"], CONFIG["CowDatabase2"]))
    labels = pd.concat([p for p in parts if not p.empty], ignore_index=True) if any([not p.empty for p in parts]) else pd.DataFrame(columns=[
        "dataset","cow_id","view","weight_kg","date_measured","sex","breed","age_months","notes","source_file","source_sheet","source_row_index"
    ])

    LABELS_OUT.parent.mkdir(parents=True, exist_ok=True)
    labels.to_csv(LABELS_OUT, index=False)
    print(f"[OK] Wrote normalized labels -> {LABELS_OUT} ({len(labels)} rows)")

    # Join with manifest
    if not MANIFEST_CSV.exists():
        print(f"[ERROR] Manifest not found: {MANIFEST_CSV}")
        sys.exit(1)
    manifest = pd.read_csv(MANIFEST_CSV)

    # Basic cleanups
    for c in ["dataset","cow_id","view"]:
        if c in manifest.columns:
            manifest[c] = manifest[c].astype(str)

    # Some manifests may have missing views (should not, but safe)
    if "view" not in manifest.columns:
        manifest["view"] = ""

    # normalize cow_id the same way
    manifest["cow_id_norm"] = manifest["cow_id"].apply(coerce_cow_id)
    labels["cow_id_norm"] = labels["cow_id"].apply(coerce_cow_id)

    # Prefer only rows that have an RGB path and a weight label
    merged = manifest.merge(
        labels.drop(columns=["cow_id","view"]).rename(columns={"cow_id_norm":"cow_id_key"}),
        left_on=["dataset","cow_id_norm"],
        right_on=["dataset","cow_id_key"],
        how="left"
    )

    # Keep only rows with both rgb and weight_kg
    def nonempty(s): 
        return pd.notna(s) and str(s).strip() != ""
    has_rgb = merged["rgb_path"].apply(nonempty) if "rgb_path" in merged.columns else False
    has_w = merged["weight_kg"].notna()
    training = merged[has_rgb & has_w].copy()

    # Tidy columns
    keep_cols = [
        "dataset","cow_id","view","rgb_path","depth_path","view_ply_path","aligned_ply_path",
        "weight_kg","date_measured","sex","breed","age_months","notes",
        "source_file","source_sheet","source_row_index"
    ]
    for c in keep_cols:
        if c not in training.columns:
            training[c] = np.nan

    training = training[keep_cols].sort_values(["dataset","cow_id","view"])
    training.to_csv(TRAIN_OUT, index=False)
    print(f"[OK] Wrote training table -> {TRAIN_OUT} ({len(training)} rows)")
    print("Sample counts by view:")
    print(training.groupby(["dataset","view"]).size())

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Download the full NESO TEC register via CKAN API, save to a date-stamped file
(Format: tec-register-YYYY-MM-DD), and generate a change report in a multi-sheet
Excel file for Wind and Solar projects.
"""

import csv
import datetime as dt
import json
import sys
import time
from pathlib import Path

# NOTE: You must have 'pandas' and 'openpyxl' installed for this version to work:
# pip install pandas openpyxl
try:
    import pandas as pd
except ImportError:
    print("[ERROR] Pandas library not found. Please run: pip install pandas openpyxl", file=sys.stderr)
    sys.exit(1)


import requests
from requests.adapters import HTTPAdapter, Retry

# --- CONFIG (YOUR FOLDER) ---
OUTPUT_DIR = Path(__file__).parent.parent / "TEC files"
RESOURCE_ID = "17becbab-e3e8-473f-b303-3806f43a6a10"  # TEC register resource on CKAN
BASE_URL = "https://api.neso.energy/api/3/action/datastore_search"
PAGE_LIMIT = 50000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 4

# --- COMPARISON SETTINGS ---
# Define the project categories for the two pots
POT_GROUPS = {
    # Pot 3: Offshore Wind
    "Pot 3 (Offshore Wind)": ["Wind Offshore"],
    # Pot 1: Onshore Wind and Solar
    "Pot 1 (Onshore/Solar)": ["Wind Onshore", "PV Array (Photo Voltaic/solar)"]
}

# The unique ID for a project row
KEY_COL = "Project Number"

# Columns to check for changes (Report Label : CSV Column Name)
COMPARE_COLS = {
    "Capacity": "Cumulative Total Capacity (MW)",
    "Connection Date": "MW Effective From",
    "Gate Status": "Gate"
}
# ---------------------------

# --- CORE FETCH/SAVE/LOAD FUNCTIONS (UNCHANGED) ---

def session_with_retries() -> requests.Session:
    """HTTP session with basic retry/backoff."""
    s = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

def fetch_all_records(resource_id: str, limit: int = PAGE_LIMIT):
    """Paginate CKAN datastore_search to fetch all rows."""
    s = session_with_retries()
    offset = 0
    all_records = []
    fields = None
    meta = {}

    while True:
        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        r = s.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        payload = r.json()

        if not payload.get("success"):
            raise RuntimeError(f"CKAN returned success=False at offset {offset}")

        result = payload.get("result", {})
        if fields is None:
            fields = result.get("fields", [])
        if not meta:
            meta = {
                "total_estimate": result.get("total"),
                "resource_id": result.get("resource_id"),
                "fetched_at_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
                "source": r.url,
            }

        chunk = result.get("records", [])
        all_records.extend(chunk)

        if len(chunk) < limit:
            break

        offset += limit
        time.sleep(0.4)

    return all_records, fields, meta

def write_atomic(path: Path, data: bytes):
    """Write to temp then atomically replace to avoid partial files."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)

def save_json_and_csv(records, fields, meta, outdir: Path) -> tuple[Path, Path]:
    """Save raw JSON and a CSV with the new date-only filename format (tec-register-YYYY-MM-DD)."""
    outdir.mkdir(parents=True, exist_ok=True)
    
    # New filename base: tec-register-YYYY-MM-DD
    ts = dt.datetime.now().strftime("%Y-%m-%d")
    base = outdir / f"tec-register-{ts}"

    # JSON
    json_obj = {"meta": meta, "fields": fields, "records": records}
    json_path = base.with_suffix(".json")
    write_atomic(
        json_path,
        json.dumps(json_obj, ensure_ascii=False, indent=2).encode("utf-8"),
    )

    # CSV - exclude the _id field
    headers = [f["id"] for f in fields if f["id"] != "_id"] if fields else (sorted(records[0].keys()) if records else [])
    csv_path = base.with_suffix(".csv")
    tmp_csv = csv_path.with_suffix(".csv.tmp")
    
    with tmp_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in records:
            writer.writerow(row)
    tmp_csv.replace(csv_path)

    return csv_path, json_path

def load_csv_as_dict(path: Path) -> dict:
    """Helper to load a CSV into a dictionary keyed by Project Number, filtered by target Plant Types."""
    data = {}
    if not path.exists():
        return data
    
    # Combine all target types for initial filtering
    all_target_types = {t.lower() for types in POT_GROUPS.values() for t in types}
    
    with path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            p_type = row.get("Plant Type", "").strip()
            # Only keep rows containing ANY of the target types
            if any(t in p_type.lower() for t in all_target_types):
                key = row.get(KEY_COL)
                if key:
                    data[key] = row
    return data

def normalize_date(date_str: str) -> str:
    """Attempts to parse and return a date string in YYYY-MM-DD format."""
    if not date_str or not date_str.strip():
        return ""
    s = date_str.strip()
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        try:
            return dt.datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return s

def normalize_numeric(num_str: str) -> float | str:
    """Attempts to convert a string to a float for consistent comparison."""
    if not num_str or not num_str.strip():
        return ""
    try:
        return float(num_str)
    except ValueError:
        return num_str

# --- EXCEL REPORTING FUNCTIONS ---

def get_pot_group(plant_type: str) -> str | None:
    """Determines which pot a project belongs to."""
    pt_lower = plant_type.lower()
    
    # Check Pot 3 (Offshore Wind)
    if any(t.lower() in pt_lower for t in POT_GROUPS["Pot 3 (Offshore Wind)"]):
        return "Pot 3 (Offshore Wind)"
    
    # Check Pot 1 (Onshore/Solar)
    if any(t.lower() in pt_lower for t in POT_GROUPS["Pot 1 (Onshore/Solar)"]):
        return "Pot 1 (Onshore/Solar)"
    
    return None

def write_excel_report(change_data: dict, current_csv: Path, out_dir: Path):
    """Creates a multi-sheet Excel report from the change data.
    Generates both a dated report and a 'latest' report that is overwritten."""
    
    # Dated report path
    report_path = current_csv.with_name(current_csv.stem + "_report.xlsx")
    
    # Latest report path (always the same name)
    latest_report_path = out_dir / "TEC_report_latest.xlsx"
    
    # Write the Excel report to both locations
    for target_path in [report_path, latest_report_path]:
        with pd.ExcelWriter(target_path, engine='openpyxl') as writer:
            for pot_name, data in change_data.items():
                sheet_name = pot_name.split('(')[0].strip() # e.g., "Pot 3" or "Pot 1"
                
                # 1. Create Summary DataFrame
                summary_data = {
                    "Change Type": ["New Entries", "Deleted Entries", "Modified Entries"],
                    "Count": [len(data['new']), len(data['deleted']), len(data['changed'])]
                }
                df_summary = pd.DataFrame(summary_data)
                
                # 2. Create Detailed Records DataFrame
                # First, process the 'changed' list to extract change details into columns
                detailed_records = []
                
                # New/Deleted rows contain the full CSV row dict
                for row in data['new']:
                    record = row.copy()
                    record['Change Type'] = 'NEW'
                    record['Change Details'] = 'New entry in TEC register.'
                    detailed_records.append(record)

                for row in data['deleted']:
                    record = row.copy()
                    record['Change Type'] = 'DELETED'
                    record.pop('_id', None) # Remove ID column if present
                    record['Change Details'] = 'Entry removed from TEC register.'
                    detailed_records.append(record)

                # Modified rows need specific change details attached
                for item in data['changed']:
                    record = item['new_row'].copy()
                    record['Change Type'] = 'MODIFIED'
                    record['Change Details'] = '; '.join(item['changes'])
                    detailed_records.append(record)

                # Convert to DataFrame
                df_detail = pd.DataFrame(detailed_records)
                
                # Reorder columns to put Change Type/Details first
                cols_to_move = ['Change Type', 'Change Details']
                df_detail = df_detail[[c for c in cols_to_move if c in df_detail] + [c for c in df_detail if c not in cols_to_move]]

                # 3. Write to Excel
                start_row = 0
                
                # Write Summary Table
                df_summary.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row)
                
                # Write a gap and a header for the detail table
                start_row += len(df_summary) + 3 
                
                writer.sheets[sheet_name].cell(row=start_row, column=1, value="Detailed Change Records:")
                
                start_row += 1
                
                # Write Detailed Table
                df_detail.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row)
                
                if target_path == report_path:
                    print(f"[OK] Wrote sheet: {sheet_name}")

    print(f"[OK] Dated Excel report saved: {report_path}")
    print(f"[OK] Latest Excel report saved: {latest_report_path}")

# --- MAIN COMPARISON LOGIC ---

def compare_registers(current_csv: Path, out_dir: Path):
    """
    Finds the most recent *previous* CSV, compares content, and structures changes 
    into pot groups for Excel output.
    """
    
    # 1. Find the previous CSV file
    current_name = current_csv.name
    all_csvs = sorted(out_dir.glob("tec-register-*.csv"), reverse=True)
    
    prev_csv = None
    for f in all_csvs:
        if f.name != current_name:
            prev_csv = f
            break
            
    if prev_csv is None:
        print("[INFO] No previous file to compare against. Skipping change report.")
        return

    print(f"[INFO] Comparing current download vs {prev_csv.name}...")

    curr_data = load_csv_as_dict(current_csv)
    prev_data = load_csv_as_dict(prev_csv)

    # Initialize data structure for Excel report
    change_data = {
        "Pot 3 (Offshore Wind)": {'new': [], 'deleted': [], 'changed': []},
        "Pot 1 (Onshore/Solar)": {'new': [], 'deleted': [], 'changed': []},
    }

    # 2. Check for New and Changed
    for uid, curr_row in curr_data.items():
        project_pot = get_pot_group(curr_row.get("Plant Type", ""))
        if not project_pot: continue # Should not happen with load_csv_as_dict logic, but safety check

        if uid not in prev_data:
            # NEW ENTRY
            change_data[project_pot]['new'].append(curr_row)
        else:
            # CHECK FOR CHANGES
            prev_row = prev_data[uid]
            changes = []
            
            for alias, col_name in COMPARE_COLS.items():
                val_c = curr_row.get(col_name, "").strip()
                val_p = prev_row.get(col_name, "").strip()
                
                # Apply normalization based on column name
                if col_name == "MW Effective From":
                    val_c_norm = normalize_date(val_c)
                    val_p_norm = normalize_date(val_p)
                elif col_name == "Cumulative Total Capacity (MW)":
                    val_c_norm = normalize_numeric(val_c)
                    val_p_norm = normalize_numeric(val_p)
                else:
                    val_c_norm = val_c
                    val_p_norm = val_p

                # Compare normalized values
                if val_c_norm != val_p_norm:
                    # Report the *original* values for clarity in the output
                    changes.append(f"{alias}: '{val_p}' -> '{val_c}'")
            
            if changes:
                change_data[project_pot]['changed'].append({
                    "id": uid,
                    "name": curr_row.get("Project Name", "Unknown"),
                    "changes": changes,
                    "new_row": curr_row # Store the full new row for Excel
                })

    # 3. Check for Deleted
    for uid, prev_row in prev_data.items():
        if uid not in curr_data:
            project_pot = get_pot_group(prev_row.get("Plant Type", ""))
            if project_pot:
                change_data[project_pot]['deleted'].append(prev_row)

    # 4. Write Excel Report (Only if any changes exist)
    if any(len(data['new']) + len(data['deleted']) + len(data['changed']) > 0 for data in change_data.values()):
        write_excel_report(change_data, current_csv, out_dir)
    else:
        print("[INFO] No relevant changes found in target categories.")

# --- MAIN EXECUTION ---

def main():
    # Determine the target filename based on today's date
    today_date_str = dt.datetime.now().strftime("%Y-%m-%d")
    target_csv_path = OUTPUT_DIR / f"tec-register-{today_date_str}.csv"
    
    # 1. Check if file already exists for today
    if target_csv_path.exists():
        print(f"[SKIP] File already exists for today: {target_csv_path.name}")
        print("Exiting without download or comparison.")
        return 0

    print("[INFO] Starting TEC download...")
    try:
        # 2. Download and save
        records, fields, meta = fetch_all_records(RESOURCE_ID, PAGE_LIMIT)
        csv_path, json_path = save_json_and_csv(records, fields, meta, OUTPUT_DIR)
        
        # 3. Run Comparison and generate Excel report
        compare_registers(csv_path, OUTPUT_DIR)
        
        print(f"[OK] Saved {len(records):,} rows")
        print(f"CSV : {csv_path}")
        return 0
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
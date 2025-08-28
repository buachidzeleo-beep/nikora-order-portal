# order_automation_local.py
# Usage:
#   python order_automation_local.py --orders "Nikora_Orders.xlsx" --barcode "barcode_map.xlsx" --schedule "shop_schedule.xlsx" --plants "tbilisi_plants.csv" --outdir "out"

import argparse
from datetime import datetime
from pathlib import Path
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
import numpy as np
import pandas as pd

# ---- Client column names (do not change incoming file) ----
ORD_DATE_COL  = "Дата документа"
ORD_PO_COL    = "Документ закупки"
ORD_SUPP_COL  = "Поставщик/завод-поставщик"
ORD_PLANT_COL = "Завод"
ORD_ADDR_COL  = "მაღაზიის მისამართი"
ORD_EAN_COL   = "Код EAN/UPC"
ORD_TEXT_COL  = "Краткий текст"
ORD_QTY_COL   = "Количество заказа"
ORD_PRICE_COL = "ღირებულება"

# ---------- Helpers ----------
def clean_ean(x):
    if pd.isna(x): return np.nan
    s = str(x).strip().replace(" ", "")
    if re.match(r"^\d+\.0$", s):  # Excel 123.0 → 123
        s = s[:-2]
    return s

def extract_shop_from_address(addr: str):
    if pd.isna(addr): return np.nan
    m = re.search(r"#\s*(\d{3,4})", str(addr))
    return f"#{m.group(1)}" if m else np.nan

def build_shop_from_plant(plant):
    if pd.isna(plant): return np.nan
    s = str(plant).strip()
    s = re.sub(r"\.0$", "", s)
    return s if s.startswith("#") else f"#{s}"

def normalize_weekday(v):
    if pd.isna(v): return np.nan
    s = str(v).strip()
    if s.isdigit():
        n = int(s); return n if 1 <= n <= 7 else np.nan
    en = {"monday":1,"tuesday":2,"wednesday":3,"thursday":4,"friday":5,"saturday":6,"sunday":7}
    ka = {"ორშაბათი":1,"სამშაბათი":2,"ოთხშაბათი":3,"ხუთშაბათი":4,"პარასკევი":5,"შაბათი":6,"კვირა":7}
    if s.lower() in en: return en[s.lower()]
    if s in ka: return ka[s]
    return np.nan

def load_table(path: Path):
    suf = path.suffix.lower()
    if suf in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        return pd.read_excel(path, dtype=str)
    if suf == ".csv":
        try:    return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        except: return pd.read_csv(path, dtype=str, encoding="cp1251")
    if suf == ".txt":
        lines = [ln.strip() for ln in path.read_text(encoding="utf-8-sig").splitlines() if ln.strip()]
        return pd.DataFrame({"value": lines})
    raise SystemExit(f"Unsupported file type: {path}")

def tidy_number_string(x):
    """
    Remove binary-float artifacts safely (e.g., 14.700000000000001 -> 14.7).
    Keeps as few decimals as needed, does not touch integers.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)) or (isinstance(x, str) and x.strip() == ""):
        return ""
    s = str(x).strip().replace(",", ".")  # tolerate commas if any
    try:
        d = Decimal(s)
    except InvalidOperation:
        return s  # leave non-numeric text as-is
    # Round to 4 dp for safety, then normalize & strip trailing zeros
    d = d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP).normalize()
    s2 = format(d, "f").rstrip("0").rstrip(".")
    return s2

def to_excel_file(path: Path, df_dict: dict, text_cols=None):
    """
    Write DataFrames to XLSX using TEXT format ('@') for selected columns.
    This prevents Excel float artifacts and keeps leading zeros.
    """
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        for sheet, df in df_dict.items():
            df_out = df.copy()
            if text_cols:
                for c in text_cols:
                    if c in df_out.columns:
                        df_out[c] = df_out[c].astype(str)
            df_out.to_excel(writer, index=False, sheet_name=sheet)

            if text_cols:
                ws = writer.sheets[sheet]
                book = writer.book
                text_fmt = book.add_format({"num_format": "@"})
                for c in text_cols:
                    if c in df_out.columns:
                        idx = df_out.columns.get_loc(c)
                        ws.set_column(idx, idx, None, text_fmt)

# ---------- Main pipeline ----------
def main():
    ap = argparse.ArgumentParser(description="Nikora Order → ERP (local, free). Incoming file stays unchanged.")
    ap.add_argument("--orders",   required=True, help="Daily order (.xlsx/.csv)")
    ap.add_argument("--barcode",  required=True, help="Barcode map (.xlsx/.csv): our EAN (target), client EAN (source)")
    ap.add_argument("--schedule", required=True, help="Schedule by PLANT (.xlsx/.csv): first col=plant, second=weekday 1..5")
    ap.add_argument("--plants",   required=True, help="Tbilisi-range PLANTS (.csv/.xlsx/.txt)")
    ap.add_argument("--outdir",   default=".",  help="Output folder")
    args = ap.parse_args()

    orders_path  = Path(args.orders)
    barcode_path = Path(args.barcode)
    schedule_path= Path(args.schedule)
    plants_path  = Path(args.plants)
    outdir       = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    # 0) Load (incoming order remains unchanged)
    orders = load_table(orders_path)
    orders.columns = [str(c).strip() for c in orders.columns]
    required = [ORD_DATE_COL, ORD_PO_COL, ORD_EAN_COL, ORD_ADDR_COL, ORD_PLANT_COL]
    miss = [c for c in required if c not in orders.columns]
    if miss: raise SystemExit(f"Orders missing required columns: {miss}")

    # 1) EAN map
    orders[ORD_EAN_COL] = orders[ORD_EAN_COL].apply(clean_ean)
    bm = load_table(barcode_path)
    bm.columns = [str(c).strip() for c in bm.columns]
    to_col   = "ძირითადი შტრიხკოდი" if "ძირითადი შტრიხკოდი" in bm.columns else bm.columns[0]
    from_col = "შტრიხკოდი"         if "შტრიხკოდი"         in bm.columns else bm.columns[1]
    bm[from_col] = bm[from_col].apply(clean_ean)
    bm[to_col]   = bm[to_col].apply(clean_ean)
    m = dict(zip(bm[from_col], bm[to_col]))
    orders["EAN_corrected"] = orders[ORD_EAN_COL].map(lambda x: m.get(x, x))

    # 2) shop_code + plant_str
    orders["shop_code_from_addr"]  = orders[ORD_ADDR_COL].apply(extract_shop_from_address)
    orders["shop_code_from_plant"] = orders[ORD_PLANT_COL].apply(build_shop_from_plant)
    orders["shop_code"] = orders["shop_code_from_addr"].combine_first(orders["shop_code_from_plant"])
    orders["plant_str"] = orders[ORD_PLANT_COL].astype(str).str.replace(".0", "", regex=False).str.strip()

    # 3) Tbilisi-range plants
    plants_df = load_table(plants_path)
    pcol = plants_df.columns[0]
    plant_set = set(plants_df[pcol].dropna().astype(str).str.replace(".0", "", regex=False).str.strip())
    orders["is_tbilisi_range"] = orders["plant_str"].isin(plant_set)

    # 4) Schedule by PLANT
    sch = load_table(schedule_path)
    sch.columns = [str(c).strip() for c in sch.columns]
    sch_plant_col = sch.columns[0]; sch_day_col = sch.columns[1]
    sch["plant_str"] = sch[sch_plant_col].astype(str).str.replace(".0", "", regex=False).str.strip()
    sch["allowed_weekday"] = sch[sch_day_col].apply(normalize_weekday)
    sch = sch[["plant_str", "allowed_weekday"]].dropna().drop_duplicates()

    orders = orders.merge(sch, on="plant_str", how="left")

    # 5) Wrong-day (only Tbilisi-range) using TODAY from this PC
    run_weekday = int(datetime.now().weekday()) + 1  # Mon=1..Sun=7
    wrong = (orders["is_tbilisi_range"]) & (~orders["allowed_weekday"].isna()) & (orders["allowed_weekday"] != run_weekday)
    wrong_day_orders = orders[wrong].copy()
    clean_orders    = orders[~wrong].copy()

    # 6) ERP view -> drop cols 1, 2, 5 (1-based); KEEP column 7
    base_cols = [
        ORD_DATE_COL, ORD_PO_COL, ORD_SUPP_COL, ORD_PLANT_COL, ORD_ADDR_COL,
        ORD_EAN_COL, ORD_TEXT_COL, ORD_QTY_COL, ORD_PRICE_COL, "shop_code"
    ]
    base_cols = [c for c in base_cols if c in clean_orders.columns or c == "shop_code"]
    erp = clean_orders.copy()

    # EAN visible = corrected
    if ORD_EAN_COL in erp.columns:
        erp[ORD_EAN_COL] = erp["EAN_corrected"]

    # Preserve price from original orders (string), then tidy artifacts
    if ORD_PRICE_COL in orders.columns:
        erp.loc[clean_orders.index, ORD_PRICE_COL] = orders.loc[clean_orders.index, ORD_PRICE_COL].astype(str)
        erp[ORD_PRICE_COL] = erp[ORD_PRICE_COL].apply(tidy_number_string)

    erp_view = erp[base_cols].copy()
    while erp_view.shape[1] < 10:
        erp_view[f"_extra_{erp_view.shape[1]+1}"] = ""

    drop_positions = [0, 1, 4]  # 1,2,5 (1-based)
    keep = [c for i, c in enumerate(erp_view.columns.tolist()) if i not in drop_positions]
    erp_final = erp_view[keep].copy()
    erp_final = erp_final[[c for c in erp_final.columns if not c.startswith("_extra_")]]

    # Force EAN to string (protect leading zeros) and tidy (no change to numeric EAN, just stringify)
    if ORD_EAN_COL in erp_final.columns:
        erp_final[ORD_EAN_COL] = erp_final[ORD_EAN_COL].astype(str)

    # 7) Outputs
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    out_erp   = outdir / f"orders_for_erp_{ts}.xlsx"
    out_wrong = outdir / f"wrong_day_orders_{ts}.xlsx"
    out_log   = outdir / f"_run_log_{ts}.txt"

    # Write with TEXT formatting for price + EAN
    to_excel_file(out_erp,   {"ERP_Upload": erp_final}, text_cols=[ORD_PRICE_COL, ORD_EAN_COL])
    to_excel_file(out_wrong, {"WrongDay":   wrong_day_orders}, text_cols=[ORD_PRICE_COL, ORD_EAN_COL])

    out_log.write_text(
        f"Run timestamp: {datetime.now().isoformat(timespec='seconds')}\n"
        f"Detected weekday (processing date): {run_weekday}\n"
        f"Rows total: {len(orders)}\n"
        f"Rows wrong-day (removed): {len(wrong_day_orders)}\n"
        f"Rows for ERP upload: {len(erp_final)}\n"
        "Note: Incoming order file unchanged; all adaptation performed on our side.\n",
        encoding="utf-8"
    )

    print("Done.")
    print(f"- ERP upload:      {out_erp}")
    print(f"- Wrong-day orders:{out_wrong}")
    print(f"- Log:             {out_log}")

if __name__ == "__main__":
    main()

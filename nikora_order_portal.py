
import io
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Order → ERP Automation (Free) — Option A", layout="wide")
st.title("Order → ERP Automation (Free) — Option A")
st.caption("Incoming order files stay **unchanged**. All adaptation is on our side for ERP upload and analytics.")

# ----------------------------- Defaults -----------------------------
BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"
DEFAULT_BARCODE = CONFIG_DIR / "barcode_map.xlsx"
DEFAULT_SCHEDULE = CONFIG_DIR / "shop_schedule.xlsx"
DEFAULT_TBILISI_PLANTS = CONFIG_DIR / "tbilisi_plants.csv"

# ----------------------------- Helpers -----------------------------

def clean_ean(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip().replace(" ", "")
    if re.match(r"^\d+\.0$", s):
        s = s[:-2]
    return s

def extract_shop_from_address(addr: str):
    if pd.isna(addr):
        return np.nan
    m = re.search(r"#\s*(\d{3,4})", str(addr))
    return f"#{m.group(1)}" if m else np.nan

def build_shop_from_plant(plant):
    if pd.isna(plant):
        return np.nan
    s = str(plant).strip()
    s = re.sub(r"\.0$", "", s)
    if s.startswith("#"):
        return s
    return f"#{s}"

def normalize_weekday(v):
    if pd.isna(v):
        return np.nan
    s = str(v).strip()
    if s.isdigit():
        n = int(s)
        return n if 1 <= n <= 7 else np.nan
    en = {"monday":1,"tuesday":2,"wednesday":3,"thursday":4,"friday":5,"saturday":6,"sunday":7}
    ka = {"ორშაბათი":1,"სამშაბათი":2,"ოთხშაბათი":3,"ხუთშაბათი":4,"პარასკევი":5,"შაბათი":6,"კვირა":7}
    if s.lower() in en: return en[s.lower()]
    if s in ka: return ka[s]
    return np.nan

def load_table(uploaded=None, default_path: Path=None, required=False, label="file"):
    """
    If an uploaded file is provided, load it.
    Else, try the default_path (for bundled configs).
    If required and neither exists, stop with an error.
    """
    def _read_bytes(data: bytes, name: str):
        bio = io.BytesIO(data)
        name = name.lower()
        if name.endswith((".xlsx",".xlsm",".xltx",".xltm")):
            return pd.read_excel(bio, dtype=str, keep_default_na=False)
        if name.endswith(".xls"):
            return pd.read_excel(bio, dtype=str, keep_default_na=False, engine="xlrd")
        if name.endswith(".csv"):
            try:
                return pd.read_csv(io.BytesIO(data), dtype=str, encoding="utf-8-sig", keep_default_na=False)
            except UnicodeDecodeError:
                return pd.read_csv(io.BytesIO(data), dtype=str, encoding="cp1251", keep_default_na=False)
        if name.endswith(".txt"):
            txt = data.decode("utf-8-sig").splitlines()
            txt = [ln.strip() for ln in txt if ln.strip()]
            return pd.DataFrame({"value": txt})
        return pd.read_excel(bio, dtype=str, keep_default_na=False)

    # 1) Uploaded override
    if uploaded is not None:
        data = uploaded.read()
        return _read_bytes(data, uploaded.name)

    # 2) Default path
    if default_path is not None and default_path.exists():
        with open(default_path, "rb") as f:
            data = f.read()
        return _read_bytes(data, default_path.name)

    if required:
        st.error(f"Missing {label}: upload it or place a default at {default_path}")
        st.stop()
    return None

def to_excel_bytes_price_qty(df_dict, price_col, qty_col, text_cols):
    """
    Write dfs to XLSX.
    - EAN and any text_cols are forced to TEXT via write_string.
    - PRICE column is written as NUMBER with exactly 2 decimals.
    - QTY column is written as NUMBER with 0 decimals.
    """
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        for sheet, df in df_dict.items():
            df_out = df.copy()
            # Write via pandas first
            df_out.to_excel(writer, index=False, sheet_name=sheet)
            ws = writer.sheets[sheet]
            wb = writer.book

            # Prepare formats
            text_fmt = wb.add_format({"num_format": "@"})
            price_fmt = wb.add_format({"num_format": "0.00"})
            qty_fmt = wb.add_format({"num_format": "0"})

            # Force TEXT columns
            for col in text_cols:
                if col in df_out.columns:
                    j = df_out.columns.get_loc(col)
                    ws.set_column(j, j, None, text_fmt)
                    vals = df_out[col].astype(str)
                    for i, val in enumerate(vals, start=1):
                        ws.write_string(i, j, val, text_fmt)

            # Write PRICE as NUMBER (2 decimals)
            if price_col in df_out.columns:
                j = df_out.columns.get_loc(price_col)
                vals = df_out[price_col].astype(str)
                for i, s in enumerate(vals, start=1):
                    s_clean = s.strip().replace(" ", "").replace(",", ".")
                    if re.match(r"^-?\d+(\.\d+)?$", s_clean):
                        try:
                            num = round(float(s_clean), 2)
                            ws.write_number(i, j, num, price_fmt)
                        except Exception:
                            ws.write_string(i, j, s, text_fmt)
                    else:
                        ws.write_string(i, j, s, text_fmt)

            # Write QTY as NUMBER (0 decimals)
            if qty_col in df_out.columns:
                j = df_out.columns.get_loc(qty_col)
                vals = df_out[qty_col].astype(str)
                for i, s in enumerate(vals, start=1):
                    s_clean = s.strip().replace(" ", "").replace(",", ".")
                    if re.match(r"^-?\d+(\.\d+)?$", s_clean):
                        try:
                            num = int(round(float(s_clean)))
                            ws.write_number(i, j, num, qty_fmt)
                        except Exception:
                            ws.write_string(i, j, s, text_fmt)
                    else:
                        ws.write_string(i, j, s, text_fmt)

    out.seek(0)
    return out.getvalue()

# ----------------------------- Constants -----------------------------

ORD_DATE_COL = "Дата документа"
ORD_PO_COL = "Документ закупки"
ORD_SUPP_COL = "Поставщик/завод-поставщик"
ORD_PLANT_COL = "Завод"
ORD_ADDR_COL = "მაღაზიის მისამართი"
ORD_EAN_COL = "Код EAN/UPC"
ORD_TEXT_COL = "Краткий текст"
ORD_QTY_COL = "Количество заказа"
ORD_PRICE_COL = "ღირებულება"

run_weekday = int(datetime.now().weekday()) + 1  # Mon=1..Sun=7, from processing date

# ----------------------------- Inputs -----------------------------
st.subheader("Upload daily order (client file — unchanged)")
orders_file = st.file_uploader("Daily order file (Excel/CSV)", type=["xlsx","xls","csv"])

st.subheader("Config files (optional overrides; default bundled files in ./config/ are auto-used)")
col1, col2, col3 = st.columns(3)
with col1:
    barcode_map_file = st.file_uploader("Override: Barcode map", type=["xlsx","xls","csv"])
    st.caption(f"Default: {DEFAULT_BARCODE.name}")
with col2:
    schedule_file = st.file_uploader("Override: Shop schedule", type=["xlsx","csv"])
    st.caption(f"Default: {DEFAULT_SCHEDULE.name}")
with col3:
    tbilisi_plants_file = st.file_uploader("Override: Tbilisi-range PLANTS", type=["csv","xlsx","txt"])
    st.caption(f"Default: {DEFAULT_TBILISI_PLANTS.name}")

if st.button("Process"):
    # Orders required
    orders = load_table(orders_file, required=True, label="Daily order")
    orders.columns = [str(c).strip() for c in orders.columns]
    req = [ORD_DATE_COL, ORD_PO_COL, ORD_EAN_COL, ORD_ADDR_COL, ORD_PLANT_COL]
    miss = [c for c in req if c not in orders.columns]
    if miss:
        st.error(f"Orders missing columns: {miss}")
        st.stop()

    # Load/bundle configs (upload overrides default)
    bm = load_table(barcode_map_file, default_path=DEFAULT_BARCODE, required=True, label="Barcode map")
    sch = load_table(schedule_file, default_path=DEFAULT_SCHEDULE, required=True, label="Shop schedule")
    tp = load_table(tbilisi_plants_file, default_path=DEFAULT_TBILISI_PLANTS, required=True, label="Tbilisi-range plants")

    # EAN clean + map
    orders[ORD_EAN_COL] = orders[ORD_EAN_COL].apply(clean_ean)
    bm.columns = [str(c).strip() for c in bm.columns]
    to_col = "ძირითადი შტრიხკოდი" if "ძირითადი შტრიხკოდი" in bm.columns else bm.columns[0]
    from_col = "შტრიხკოდი" if "შტრიხკოდი" in bm.columns else bm.columns[1]
    bm[from_col] = bm[from_col].apply(clean_ean)
    bm[to_col] = bm[to_col].apply(clean_ean)
    map_dict = dict(zip(bm[from_col], bm[to_col]))
    orders["EAN_corrected"] = orders[ORD_EAN_COL].map(lambda x: map_dict.get(x, x))

    # Shop code
    orders["shop_code_from_addr"] = orders[ORD_ADDR_COL].apply(extract_shop_from_address)
    orders["shop_code_from_plant"] = orders[ORD_PLANT_COL].apply(build_shop_from_plant)
    orders["shop_code"] = orders["shop_code_from_addr"].combine_first(orders["shop_code_from_plant"])

    # Plant normalization + Tbilisi-range
    tp_col = tp.columns[0]
    plant_set = set(tp[tp_col].dropna().astype(str).str.replace(".0","", regex=False).str.strip())
    orders["plant_str"] = orders[ORD_PLANT_COL].astype(str).str.replace(".0","", regex=False).str.strip()
    orders["is_tbilisi_range"] = orders["plant_str"].isin(plant_set)

    # Schedule (plant, weekday 1..5)
    sch.columns = [str(c).strip() for c in sch.columns]
    sch_plant_col = sch.columns[0]; sch_day_col = sch.columns[1]
    sch["plant_str"] = sch[sch_plant_col].astype(str).str.replace(".0","", regex=False).str.strip()
    sch["allowed_weekday"] = sch[sch_day_col].apply(normalize_weekday)
    sch = sch[["plant_str","allowed_weekday"]].dropna().drop_duplicates()

    # Merge and filter wrong-day (Tbilisi-range only)
    orders = orders.merge(sch, on="plant_str", how="left")
    wrong_mask = (orders["is_tbilisi_range"]) & (~orders["allowed_weekday"].isna()) & (orders["allowed_weekday"] != run_weekday)
    wrong_day_orders = orders[wrong_mask].copy()
    clean_orders = orders[~wrong_mask].copy()

    # Build ERP view in deterministic order
    base_cols = [
        ORD_DATE_COL, ORD_PO_COL, ORD_SUPP_COL, ORD_PLANT_COL, ORD_ADDR_COL,
        ORD_EAN_COL, ORD_TEXT_COL, ORD_QTY_COL, ORD_PRICE_COL, "shop_code"
    ]
    base_cols = [c for c in base_cols if c in clean_orders.columns or c=="shop_code"]
    erp = clean_orders.copy()

    if ORD_EAN_COL in erp.columns: erp[ORD_EAN_COL] = erp["EAN_corrected"]
    erp_view = erp[base_cols].copy()

    # Pad placeholders so we can drop by position safely
    while erp_view.shape[1] < 10:
        erp_view[f"_extra_{erp_view.shape[1]+1}"] = ""

    # Delete columns 1, 2, 5 (1-based) ONLY — keep column 7
    drop_positions = [0, 1, 4]  # zero-based
    keep = [c for i, c in enumerate(erp_view.columns.tolist()) if i not in drop_positions]
    erp_final = erp_view[keep].copy()

    # Metrics
    m1, m2, m3 = st.columns(3)
    with m1: st.metric("Detected weekday", run_weekday)
    with m2: st.metric("Wrong-day rows", len(wrong_day_orders))
    with m3: st.metric("Rows for ERP", len(erp_final))

    st.subheader("ERP Upload (first 20)")
    st.dataframe(erp_final.head(20))
    st.subheader("Wrong-day Orders (first 20)")
    st.dataframe(wrong_day_orders.head(20))

    # Downloads: PRICE number (0.00), QTY number (0), EAN text
    text_cols = [ORD_EAN_COL]  # keep EAN as TEXT
    erp_bytes = to_excel_bytes_price_qty({"ERP_Upload": erp_final}, price_col=ORD_PRICE_COL, qty_col=ORD_QTY_COL, text_cols=text_cols)
    wrong_bytes = to_excel_bytes_price_qty({"WrongDay": wrong_day_orders}, price_col=ORD_PRICE_COL, qty_col=ORD_QTY_COL, text_cols=text_cols)

    st.download_button("Download ERP Upload (XLSX)", erp_bytes, file_name="orders_for_erp.xlsx")
    st.download_button("Download Wrong-day Orders (XLSX)", wrong_bytes, file_name="wrong_day_orders.xlsx")

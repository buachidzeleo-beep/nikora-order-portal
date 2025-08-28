# nikora_order_portal.py
import io
import re
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Order → ERP Automation (Free) — Option A", layout="wide")
st.title("Order → ERP Automation (Free) — Option A")
st.caption("Incoming order file stays **unchanged**. All adaptation is on **our** side for ERP upload + analytics.")

# ----------------------------- Column constants (client file) -----------------------------
ORD_DATE_COL = "Дата документа"
ORD_PO_COL = "Документ закупки"
ORD_SUPP_COL = "Поставщик/завод-поставщик"
ORD_PLANT_COL = "Завод"
ORD_ADDR_COL = "მაღაზიის მისამართი"
ORD_EAN_COL = "Код EAN/UPC"
ORD_TEXT_COL = "Краткий текст"
ORD_QTY_COL = "Количество заказа"
ORD_PRICE_COL = "ღირებულება"

# ----------------------------- Helpers -----------------------------
def clean_ean(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip().replace(" ", "")
    # Excel sometimes renders barcodes as "123.0"
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
    """Accept 1..7 or EN/KA names. Monday=1 ... Sunday=7"""
    if pd.isna(v):
        return np.nan
    s = str(v).strip()
    if s.isdigit():
        n = int(s)
        return n if 1 <= n <= 7 else np.nan
    en = {"monday":1,"tuesday":2,"wednesday":3,"thursday":4,"friday":5,"saturday":6,"sunday":7}
    ka = {"ორშაბათი":1,"სამშაბათი":2,"ოთხშაბათი":3,"ხუთშაბათი":4,"პარასკევი":5,"შაბათი":6,"კვირა":7}
    if s.lower() in en:
        return en[s.lower()]
    if s in ka:
        return ka[s]
    return np.nan

def load_table(uploaded):
    """Reads xlsx/csv/txt. For .xls you would need xlrd installed."""
    if uploaded is None:
        return None
    name = uploaded.name.lower()
    data = uploaded.read()
    bio = io.BytesIO(data)

    if name.endswith((".xlsx", ".xlsm", ".xltx", ".xltm")):
        return pd.read_excel(bio, dtype=str)
    if name.endswith(".xls"):
        # If you really need .xls, add xlrd to requirements and uncomment engine="xlrd"
        # return pd.read_excel(bio, dtype=str, engine="xlrd")
        st.error("Legacy .xls not supported on this deployment. Please upload .xlsx or .csv.")
        st.stop()
    if name.endswith(".csv"):
        try:
            return pd.read_csv(io.BytesIO(data), dtype=str, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(data), dtype=str, encoding="cp1251")
    if name.endswith(".txt"):
        txt = data.decode("utf-8-sig").splitlines()
        txt = [ln.strip() for ln in txt if ln.strip()]
        return pd.DataFrame({"value": txt})
    # fallback
    return pd.read_excel(bio, dtype=str)

def to_excel_bytes(df_dict, text_cols=None):
    """
    Write one or more DataFrames to a single XLSX.
    text_cols: list of column names to force Excel TEXT format ('@')
               to avoid float artifacts and keep leading zeros.
    """
    import xlsxwriter  # ensured by requirements
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        for sheet, df in df_dict.items():
            df_out = df.copy()
            # ensure the requested text columns are strings before writing
            if text_cols:
                for c in text_cols:
                    if c in df_out.columns:
                        df_out[c] = df_out[c].astype(str)

            df_out.to_excel(writer, index=False, sheet_name=sheet)

            # apply TEXT ('@') format to requested columns
            if text_cols:
                ws = writer.sheets[sheet]
                book = writer.book
                text_fmt = book.add_format({"num_format": "@"})
                for c in text_cols:
                    if c in df_out.columns:
                        idx = df_out.columns.get_loc(c)
                        ws.set_column(idx, idx, None, text_fmt)
    out.seek(0)
    return out.getvalue()

# Use TODAY (processing date) automatically; Monday=1..Sunday=7
RUN_WEEKDAY = int(datetime.now().weekday()) + 1

# ----------------------------- Inputs -----------------------------
st.subheader("Upload inputs")
c1, c2 = st.columns(2)
with c1:
    orders_file = st.file_uploader("Daily order file (Excel/CSV)", type=["xlsx","xls","csv"], help="Client original — stays unchanged")
    barcode_map_file = st.file_uploader("Barcode map: Client EAN → Primary EAN (XLSX/CSV)", type=["xlsx","csv"])
with c2:
    schedule_file = st.file_uploader("Schedule (PLANT, allowed_weekday 1..5)", type=["xlsx","csv"])
    tbilisi_plants_file = st.file_uploader("Tbilisi-range PLANTS list (one column)", type=["csv","xlsx","txt"])

if st.button("Process"):
    # Required uploads
    if not all([orders_file, barcode_map_file, schedule_file, tbilisi_plants_file]):
        st.error("Please upload: Order, Barcode map, Schedule, and Tbilisi PLANTS list.")
        st.stop()

    # Read orders
    orders = load_table(orders_file)
    orders.columns = [str(c).strip() for c in orders.columns]

    required = [ORD_DATE_COL, ORD_PO_COL, ORD_EAN_COL, ORD_ADDR_COL, ORD_PLANT_COL]
    missing = [c for c in required if c not in orders.columns]
    if missing:
        st.error(f"Orders file is missing required columns: {missing}")
        st.stop()

    # EAN normalize + mapping
    orders[ORD_EAN_COL] = orders[ORD_EAN_COL].apply(clean_ean)

    bm = load_table(barcode_map_file)
    bm.columns = [str(c).strip() for c in bm.columns]
    # target (our) EAN col and source (client) EAN col
    to_col = "ძირითადი შტრიხკოდი" if "ძირითადი შტრიხკოდი" in bm.columns else bm.columns[0]
    from_col = "შტრიხკოდი" if "შტრიხკოდი" in bm.columns else bm.columns[1]
    bm[from_col] = bm[from_col].apply(clean_ean)
    bm[to_col] = bm[to_col].apply(clean_ean)
    map_dict = dict(zip(bm[from_col], bm[to_col]))
    orders["EAN_corrected"] = orders[ORD_EAN_COL].map(lambda x: map_dict.get(x, x))

    # Shop code: from address if present, else from plant
    orders["shop_code_from_addr"] = orders[ORD_ADDR_COL].apply(extract_shop_from_address)
    orders["shop_code_from_plant"] = orders[ORD_PLANT_COL].apply(build_shop_from_plant)
    orders["shop_code"] = orders["shop_code_from_addr"].combine_first(orders["shop_code_from_plant"])

    # Tbilisi-range plants
    tp = load_table(tbilisi_plants_file)
    tp_col = tp.columns[0]
    plant_set = set(tp[tp_col].dropna().astype(str).str.replace(".0", "", regex=False).str.strip())
    orders["plant_str"] = orders[ORD_PLANT_COL].astype(str).str.replace(".0", "", regex=False).str.strip()
    orders["is_tbilisi_range"] = orders["plant_str"].isin(plant_set)

    # Schedule by plant
    sch = load_table(schedule_file)
    sch.columns = [str(c).strip() for c in sch.columns]
    sch_plant_col = sch.columns[0]
    sch_day_col = sch.columns[1]
    sch["plant_str"] = sch[sch_plant_col].astype(str).str.replace(".0", "", regex=False).str.strip()
    sch["allowed_weekday"] = sch[sch_day_col].apply(normalize_weekday)
    sch = sch[["plant_str", "allowed_weekday"]].dropna().drop_duplicates()

    # Merge schedule onto orders
    orders = orders.merge(sch, on="plant_str", how="left")

    # Wrong-day filter (only for Tbilisi-range)
    wrong_mask = (orders["is_tbilisi_range"]) & (~orders["allowed_weekday"].isna()) & (orders["allowed_weekday"] != RUN_WEEKDAY)
    wrong_day_orders = orders[wrong_mask].copy()
    clean_orders = orders[~wrong_mask].copy()

    # ----------------------------- ERP view -----------------------------
    # Deterministic base order of columns
    base_cols = [
        ORD_DATE_COL, ORD_PO_COL, ORD_SUPP_COL, ORD_PLANT_COL, ORD_ADDR_COL,
        ORD_EAN_COL, ORD_TEXT_COL, ORD_QTY_COL, ORD_PRICE_COL, "shop_code"
    ]
    base_cols = [c for c in base_cols if c in clean_orders.columns or c == "shop_code"]

    erp = clean_orders.copy()

    # Overwrite visible EAN column with corrected values
    if ORD_EAN_COL in erp.columns:
        erp[ORD_EAN_COL] = erp["EAN_corrected"]

    # Preserve exact price strings from the original order file (no numeric coercion)
    if ORD_PRICE_COL in orders.columns:
        erp.loc[clean_orders.index, ORD_PRICE_COL] = orders.loc[clean_orders.index, ORD_PRICE_COL].astype(str)

    erp_view = erp[base_cols].copy()

    # Pad placeholders so positional delete is stable
    while erp_view.shape[1] < 10:
        erp_view[f"_extra_{erp_view.shape[1]+1}"] = ""

    # IMPORTANT: Delete columns 1, 2, and 5 (1-based) -> positions 0, 1, 4 (0-based).
    # Column 7 must be KEPT.
    drop_positions = [0, 1, 4]
    keep_cols = [c for i, c in enumerate(erp_view.columns.tolist()) if i not in drop_positions]
    erp_final = erp_view[keep_cols].copy()
    # Remove placeholders
    erp_final = erp_final[[c for c in erp_final.columns if not c.startswith("_extra_")]]

    # ----------------------------- UI + Downloads -----------------------------
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Detected weekday (processing date)", RUN_WEEKDAY)
    with m2:
        st.metric("Wrong-day rows removed", len(wrong_day_orders))
    with m3:
        st.metric("Rows for ERP file", len(erp_final))

    st.subheader("ERP Upload (first 20 rows)")
    st.dataframe(erp_final.head(20))

    st.subheader("Wrong-day Orders (first 20 rows)")
    st.dataframe(wrong_day_orders.head(20))

    # Force TEXT for price & EAN to avoid float artifacts and preserve leading zeros
    text_cols = [ORD_PRICE_COL, ORD_EAN_COL]

    erp_bytes = to_excel_bytes({"ERP_Upload": erp_final}, text_cols=text_cols)
    wrong_bytes = to_excel_bytes({"WrongDay": wrong_day_orders}, text_cols=text_cols)

    st.download_button("Download ERP Upload (XLSX)", erp_bytes, file_name="orders_for_erp.xlsx")
    st.download_button("Download Wrong-day Orders (XLSX)", wrong_bytes, file_name="wrong_day_orders.xlsx")

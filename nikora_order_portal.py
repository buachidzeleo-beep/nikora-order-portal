
import io, re, zipfile
from datetime import datetime
import numpy as np, pandas as pd, streamlit as st

st.set_page_config(page_title="Order → ERP Automation (Free) — Option A", layout="wide")
st.title("Order → ERP Automation (Free) — Option A")
st.caption("Incoming order files stay **unchanged**. All adaptation is on our side for ERP upload and analytics.")

# ---- Helpers ----
def clean_ean(x):
    if pd.isna(x): return np.nan
    s = str(x).strip().replace(" ", "")
    if re.match(r"^\d+\.0$", s): s = s[:-2]
    return s

def extract_shop_from_address(addr: str):
    if pd.isna(addr): return np.nan
    m = re.search(r"#\s*(\d{3,4})", str(addr))
    return f"#{m.group(1)}" if m else np.nan

def build_shop_from_plant(plant):
    if pd.isna(plant): return np.nan
    s = str(plant).strip()
    s = re.sub(r"\.0$", "", s)
    if s.startswith("#"): return s
    return f"#{s}"

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

def load_table(uploaded):
    if uploaded is None: return None
    name = uploaded.name.lower()
    data = uploaded.read()
    bio = io.BytesIO(data)
    if name.endswith((".xlsx",".xlsm",".xltx",".xltm")): return pd.read_excel(bio, dtype=str)
    if name.endswith(".xls"): return pd.read_excel(bio, dtype=str, engine="xlrd")
    if name.endswith(".csv"):
        try: return pd.read_csv(io.BytesIO(data), dtype=str, encoding="utf-8-sig")
        except UnicodeDecodeError: return pd.read_csv(io.BytesIO(data), dtype=str, encoding="cp1251")
    if name.endswith(".txt"):
        txt = data.decode("utf-8-sig").splitlines()
        txt = [ln.strip() for ln in txt if ln.strip()]
        return pd.DataFrame({"value": txt})
    return pd.read_excel(bio, dtype=str)

def to_excel_bytes(df_dict):
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        for sheet, df in df_dict.items():
            # Force string to preserve exact text (e.g., price)
            for c in df.columns: df[c] = df[c].astype(str)
            df.to_excel(w, index=False, sheet_name=sheet)
    out.seek(0); return out.getvalue()

run_weekday = int(datetime.now().weekday()) + 1  # use processing date (Mon=1..Sun=7)

# ---- Inputs ----
c1, c2 = st.columns(2)
with c1:
    orders_file = st.file_uploader("Daily order file (Excel/CSV)", type=["xlsx","xls","csv"])
    barcode_map_file = st.file_uploader("Barcode map (Client EAN → Primary EAN)", type=["xlsx","xls","csv"])
with c2:
    schedule_file = st.file_uploader("Shop schedule (PLANT, allowed_weekday)", type=["xlsx","csv"])
    tbilisi_plants_file = st.file_uploader("Tbilisi-range PLANTS list", type=["csv","xlsx","txt"])

if st.button("Process"):
    if not all([orders_file, barcode_map_file, schedule_file, tbilisi_plants_file]):
        st.error("Upload all four files."); st.stop()

    ORD_DATE_COL = "Дата документа"
    ORD_PO_COL = "Документ закупки"
    ORD_SUPP_COL = "Поставщик/завод-поставщик"
    ORD_PLANT_COL = "Завод"
    ORD_ADDR_COL = "მაღაზიის მისამართი"
    ORD_EAN_COL = "Код EAN/UPC"
    ORD_TEXT_COL = "Краткий текст"
    ORD_QTY_COL = "Количество заказа"
    ORD_PRICE_COL = "ღირებულება"

    orders = load_table(orders_file); orders.columns = [str(c).strip() for c in orders.columns]
    req = [ORD_DATE_COL, ORD_PO_COL, ORD_EAN_COL, ORD_ADDR_COL, ORD_PLANT_COL]
    miss = [c for c in req if c not in orders.columns]
    if miss: st.error(f"Orders missing columns: {miss}"); st.stop()

    orders[ORD_EAN_COL] = orders[ORD_EAN_COL].apply(clean_ean)

    bm = load_table(barcode_map_file); bm.columns = [str(c).strip() for c in bm.columns]
    to_col = "ძირითადი შტრიხკოდი" if "ძირითადი შტრიხკოდი" in bm.columns else bm.columns[0]
    from_col = "შტრიხკოდი" if "შტრიხკოდი" in bm.columns else bm.columns[1]
    bm[from_col] = bm[from_col].apply(clean_ean); bm[to_col] = bm[to_col].apply(clean_ean)
    map_dict = dict(zip(bm[from_col], bm[to_col]))
    orders["EAN_corrected"] = orders[ORD_EAN_COL].map(lambda x: map_dict.get(x, x))

    orders["shop_code_from_addr"] = orders[ORD_ADDR_COL].apply(extract_shop_from_address)
    orders["shop_code_from_plant"] = orders[ORD_PLANT_COL].apply(build_shop_from_plant)
    orders["shop_code"] = orders["shop_code_from_addr"].combine_first(orders["shop_code_from_plant"])

    tp = load_table(tbilisi_plants_file)
    tp_col = tp.columns[0]; plant_set = set(tp[tp_col].dropna().astype(str).str.replace(".0","", regex=False).str.strip())
    orders["plant_str"] = orders[ORD_PLANT_COL].astype(str).str.replace(".0","", regex=False).str.strip()
    orders["is_tbilisi_range"] = orders["plant_str"].isin(plant_set)

    sch = load_table(schedule_file); sch.columns = [str(c).strip() for c in sch.columns]
    sch_plant_col = sch.columns[0]; sch_day_col = sch.columns[1]
    sch["plant_str"] = sch[sch_plant_col].astype(str).str.replace(".0","", regex=False).str.strip()
    sch["allowed_weekday"] = sch[sch_day_col].apply(normalize_weekday)
    sch = sch[["plant_str","allowed_weekday"]].dropna().drop_duplicates()

    orders = orders.merge(sch, on="plant_str", how="left")

    wrong = (orders["is_tbilisi_range"]) & (~orders["allowed_weekday"].isna()) & (orders["allowed_weekday"] != run_weekday)
    wrong_day_orders = orders[wrong].copy()
    clean_orders = orders[~wrong].copy()

    # Build ERP view; drop columns 1,2,5 (1-based) ONLY
    base_cols = [ORD_DATE_COL, ORD_PO_COL, ORD_SUPP_COL, ORD_PLANT_COL, ORD_ADDR_COL,
                 ORD_EAN_COL, ORD_TEXT_COL, ORD_QTY_COL, ORD_PRICE_COL, "shop_code"]
    base_cols = [c for c in base_cols if c in clean_orders.columns or c=="shop_code"]
    erp = clean_orders.copy()
    if ORD_EAN_COL in erp.columns: erp[ORD_EAN_COL] = erp["EAN_corrected"]
    # preserve exact price text
    if ORD_PRICE_COL in orders.columns:
        erp.loc[clean_orders.index, ORD_PRICE_COL] = orders.loc[clean_orders.index, ORD_PRICE_COL].astype(str)
    erp_view = erp[base_cols].copy()
    while erp_view.shape[1] < 10:
        erp_view[f"_extra_{erp_view.shape[1]+1}"] = ""
    drop_positions = [0,1,4]  # <-- fixed
    keep = [c for i,c in enumerate(erp_view.columns.tolist()) if i not in drop_positions]
    erp_final = erp_view[keep].copy()
    for c in erp_final.columns: erp_final[c] = erp_final[c].astype(str)
    erp_final = erp_final[[c for c in erp_final.columns if not c.startswith("_extra_")]]

    # Output
    m1, m2, m3 = st.columns(3)
    with m1: st.metric("Detected weekday", run_weekday)
    with m2: st.metric("Wrong-day rows", len(wrong_day_orders))
    with m3: st.metric("Rows for ERP", len(erp_final))

    st.subheader("ERP Upload (first 20)")
    st.dataframe(erp_final.head(20))
    st.subheader("Wrong-day Orders (first 20)")
    st.dataframe(wrong_day_orders.head(20))

    erp_bytes = to_excel_bytes({"ERP_Upload": erp_final})
    wrong_bytes = to_excel_bytes({"WrongDay": wrong_day_orders})
    st.download_button("Download ERP Upload (XLSX)", erp_bytes, file_name="orders_for_erp.xlsx")
    st.download_button("Download Wrong-day Orders (XLSX)", wrong_bytes, file_name="wrong_day_orders.xlsx")

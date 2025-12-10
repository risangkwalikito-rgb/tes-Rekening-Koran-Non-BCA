# file: app.py
import io
import re
from typing import Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st


# ================== Helpers ==================
def _norm_cols(cols: List[str]) -> List[str]:
    return [str(c).strip() for c in cols]


def _detect_cols(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    Why: Nama kolom RK berbeda-beda antar bank; heuristik agar minim setting manual.
    """
    catalog = {
        "date": ["date", "transaction date", "tanggal", "tgl", "posting date", "value date"],
        "remark": ["remark", "description", "descriptions", "keterangan", "uraian", "transaksi", "details", "detail"],
        "credit": [
            "credit", "kredit", "cr", "cr.", "amount credit", "credit amount",
            "kredit (cr)", "jumlah kredit", "masuk", "in", "deposit", "setoran"
        ],
    }
    lower_map = {c.lower().strip(): c for c in df.columns}
    found = {"date": None, "remark": None, "credit": None}

    for key, names in catalog.items():
        for n in names:
            if n in lower_map:
                found[key] = lower_map[n]
                break

    # Regex fallback
    if not found["date"]:
        for c in df.columns:
            if re.search(r"\b(date|tanggal|tgl)\b", str(c), re.I):
                found["date"] = c
                break
    if not found["remark"]:
        for c in df.columns:
            if re.search(r"remark|ket|descrip|uraian|detail|transaksi", str(c), re.I):
                found["remark"] = c
                break
    if not found["credit"]:
        cred_like = [c for c in df.columns if re.search(r"cred|kred|(^cr\.?$)|masuk|deposit|setoran|in\b", str(c), re.I)]
        if cred_like:
            found["credit"] = cred_like[0]
        else:
            # Ambil numeric bukan saldo/debit
            for c in df.columns:
                if pd.api.types.is_numeric_dtype(df[c]) and not re.search(r"debit|dr|balance|saldo", str(c), re.I):
                    found["credit"] = c
                    break

    return found


def _parse_date_series(s: pd.Series) -> pd.Series:
    """
    Why: Hindari NameError dengan selalu mengisi `dt`; tidak pakai infer_datetime_format
    karena beda versi pandas bisa error.
    """
    s = s.astype(str).str.strip()
    dt1 = pd.to_datetime(s, errors="coerce", dayfirst=True)
    dt2 = pd.to_datetime(s, errors="coerce", dayfirst=False)
    # Pilih yang paling valid; jika seri campur, gabungkan
    if dt1.notna().sum() >= dt2.notna().sum():
        dt = dt1.fillna(dt2)
    else:
        dt = dt2.fillna(dt1)
    return dt


def _to_numeric_credit(series: pd.Series) -> pd.Series:
    """
    Why: Dukung format 1.234,56 (ID) dan 1,234.56 (EN) + buang simbol.
    """
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    s = series.astype(str).str.strip()

    both = s.str.contains(r"\.", na=False) & s.str.contains(r",", na=False)
    only_comma = ~both & s.str.contains(",", na=False)

    a = s[both].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    b = s[only_comma].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    c = s[~(both | only_comma)].str.replace(r"[^\d.\-]", "", regex=True)

    merged = pd.concat([a, b, c]).sort_index()
    return pd.to_numeric(merged, errors="coerce")


def _strip_cents(x: float) -> str:
    """
    Why: Keluarkan angka tanpa akhiran '.00' dan tanpa nol berlebih.
    """
    if pd.isna(x):
        return ""
    s = f"{float(x):.2f}"
    return s[:-3] if s.endswith(".00") else s.rstrip("0").rstrip(".")


@st.cache_data(show_spinner=False)
def _excel_sheet_names(uploaded) -> List[str]:
    xls = pd.ExcelFile(uploaded)
    return xls.sheet_names


def _read_df(uploaded, header_row: int, sheet_name: Optional[str]) -> pd.DataFrame:
    header_idx = max(header_row - 1, 0)
    name = getattr(uploaded, "name", "").lower()
    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(uploaded, sheet_name=sheet_name, header=header_idx, dtype=str)
    else:
        df = pd.read_csv(uploaded, header=header_idx, dtype=str, encoding="utf-8", engine="python")
    df.columns = _norm_cols(list(df.columns))
    return df


def _filter_remark(df: pd.DataFrame, remark_col: str, pattern: str) -> pd.DataFrame:
    rgx = re.compile(pattern, re.I)
    return df[df[remark_col].astype(str).str.contains(rgx, na=False)].copy()


def _build_outputs(df: pd.DataFrame, cols: Dict[str, Optional[str]], remark_pattern: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not cols["date"] or not cols["remark"] or not cols["credit"]:
        raise ValueError("Kolom Date/Tanggal, Remark, atau Credit tidak ditemukan. Coba atur baris header (13/14) atau pilih manual.")

    work = df.copy()
    work["__date"] = _parse_date_series(work[cols["date"]])
    work["__credit"] = _to_numeric_credit(work[cols["credit"]])
    work["__remark"] = work[cols["remark"]].astype(str)

    work = work[work["__date"].notna() & work["__credit"].notna()]
    work_f = _filter_remark(work, "__remark", remark_pattern)

    out_rows = (
        work_f[["__date", "__remark", "__credit"]]
        .rename(columns={"__date": "Date", "__remark": "Remark", "__credit": "Amount"})
        .copy()
    )
    out_rows["Amount"] = out_rows["Amount"].map(_strip_cents)

    grouped = (
        work_f.assign(Date=work_f["__date"].dt.date)
        .groupby("Date", as_index=False)["__credit"]
        .sum()
        .rename(columns={"__credit": "TotalAmount"})
    )
    grouped["TotalAmount"] = grouped["TotalAmount"].map(_strip_cents)

    return out_rows, grouped


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


# ================== App ==================
st.set_page_config(page_title="Rekening Koran Parser", page_icon="📄", layout="wide")
st.title("📄 Rekening Koran Uploader → Filter FINIF/FINON → Group per Tanggal")

with st.sidebar:
    st.markdown("**Pengaturan Pembacaan**")
    header_row = st.radio("Mulai baca dari baris (header):", options=[13, 14], index=0, horizontal=True)
    remark_pattern = st.text_input(
        "Filter Remark (regex):",
        value=r"(?:^|\s)(FINIF\w*|FINON\w*)",
        help="Case-insensitive; sesuaikan bila perlu.",
    )
    show_debug = st.checkbox("Tampilkan debug info", value=False)

uploaded = st.file_uploader("Unggah file Rekening Koran (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"])

sheet_name = None
if uploaded is not None and uploaded.name.lower().endswith((".xlsx", ".xls")):
    try:
        sheets = _excel_sheet_names(uploaded)
        sheet_name = st.selectbox("Pilih Sheet:", options=sheets, index=0)
    except Exception as e:
        st.error(f"Gagal membaca daftar sheet: {e}")

if uploaded is not None:
    try:
        df_raw = _read_df(uploaded, header_row=header_row, sheet_name=sheet_name)
        st.success(f"Data terbaca. Baris: {len(df_raw):,} | Kolom: {len(df_raw.columns)}")

        cols = _detect_cols(df_raw)
        c1, c2, c3 = st.columns(3)
        with c1:
            cols["date"] = st.selectbox("Kolom Date/Tanggal:", options=list(df_raw.columns), index=list(df_raw.columns).index(cols["date"]) if cols["date"] in df_raw.columns else 0)
        with c2:
            cols["remark"] = st.selectbox("Kolom Remark:", options=list(df_raw.columns), index=list(df_raw.columns).index(cols["remark"]) if cols["remark"] in df_raw.columns else 0)
        with c3:
            cols["credit"] = st.selectbox("Kolom Credit:", options=list(df_raw.columns), index=list(df_raw.columns).index(cols["credit"]) if cols["credit"] in df_raw.columns else 0)

        out_rows, grouped = _build_outputs(df_raw, cols, remark_pattern)

        st.subheader("🔎 Baris Terfilter (FINIF/FINON)")
        st.dataframe(out_rows, use_container_width=True, height=380)

        st.subheader("🗓️ Rekap Jumlah per Tanggal (Credit)")
        st.dataframe(grouped, use_container_width=True, height=300)

        d1, d2 = st.columns(2)
        with d1:
            st.download_button("💾 Unduh Baris Terfilter (CSV)", data=_to_csv_bytes(out_rows), file_name="filtered_rows.csv", mime="text/csv")
        with d2:
            st.download_button("💾 Unduh Rekap per Tanggal (CSV)", data=_to_csv_bytes(grouped), file_name="grouped_by_date.csv", mime="text/csv")

        if show_debug:
            st.divider()
            st.markdown("**Debug**")
            st.write("Detected columns:", cols)
            st.dataframe(df_raw.head(30), use_container_width=True, height=260)

    except Exception as e:
        st.error(f"Gagal memproses file: {e}")
else:
    st.info("Unggah file untuk mulai memproses.")

st.caption("Tips: Jika kolom tidak terdeteksi benar, ubah baris header (13/14) atau pilih manual di dropdown.")

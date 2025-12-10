# file: app.py
import io
import re
from typing import Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st


# ---------- Helpers ----------
def normalize_columns(cols: List[str]) -> List[str]:
    return [str(c).strip() for c in cols]


def detect_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    Why: Bank statements vary; we guess best-fit columns robustly.
    """
    candidates = {
        "date": [
            "date", "transaction date", "tanggal", "tgl", "posting date", "value date",
        ],
        "remark": [
            "remark", "description", "descriptions", "keterangan", "uraian", "transaksi", "transaction description",
            "details", "detail", "remarks"
        ],
        "credit": [
            "credit", "kredit", "cr", "cr.", "amount credit", "credit amount", "kredit (cr)", "jumlah kredit",
            "masuk", "in", "deposit", "setoran"
        ],
    }
    cols_lc = {c.lower().strip(): c for c in df.columns}
    found: Dict[str, Optional[str]] = {"date": None, "remark": None, "credit": None}

    for key, names in candidates.items():
        for n in names:
            if n in cols_lc:
                found[key] = cols_lc[n]
                break

    # Fallbacks by heuristic
    if found["date"] is None:
        for c in df.columns:
            if re.search(r"date|tanggal|tgl", str(c), re.I):
                found["date"] = c
                break

    if found["remark"] is None:
        for c in df.columns:
            if re.search(r"remark|ket|descrip|uraian|detail", str(c), re.I):
                found["remark"] = c
                break

    if found["credit"] is None:
        # Prefer columns named like credit or Cr and not debit-like
        credit_like = [c for c in df.columns if re.search(r"cred|kred|(^cr\.?$)", str(c), re.I)]
        debit_like = [c for c in df.columns if re.search(r"debit|deb|dr", str(c), re.I)]
        if credit_like:
            # If both exist, pick the one not also detected as debit
            for c in credit_like:
                if c not in debit_like:
                    found["credit"] = c
                    break
            if found["credit"] is None:
                found["credit"] = credit_like[0]
        else:
            # Fallback: pick a numeric column that isn't Balance nor Debit
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            for c in numeric_cols:
                if not re.search(r"debit|dr|balance|saldo", str(c), re.I):
                    found["credit"] = c
                    break

    return found


def parse_date_series(s: pd.Series) -> pd.Series:
    # Try common day-first formats typical in ID statements
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True, infer_datetime_format=True)
    # If too many NaT, retry without dayfirst
    if dt.isna().mean() > 0.5:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=False, infer_datetime_format=True)
    return dt


def to_number_stripping_cents(x: float) -> str:
    """
    Why: Requirement 'tanpa .00'; also strip redundant trailing zeros.
    """
    if pd.isna(x):
        return ""
    # Round to 2 decimal places to avoid float artifacts
    s = f"{float(x):.2f}"
    if s.endswith(".00"):
        return s[:-3]
    # Strip trailing zeros like ".50" -> "0.5" stays, ".10" -> ".1"
    s = s.rstrip("0").rstrip(".")
    return s


def to_numeric_credit(series: pd.Series) -> pd.Series:
    """
    Why: Handle mixed formats (1.234,56 vs 1,234.56).
    """
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    cleaned = series.astype(str).str.strip()

    # Case with both '.' and ',' -> assume '.' thousands, ',' decimal
    mask_both = cleaned.str.contains(r"\.", na=False) & cleaned.str.contains(r",", na=False)
    part_both = cleaned[mask_both].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

    # Only comma present -> often decimal in ID
    mask_comma = ~mask_both & cleaned.str.contains(",", na=False)
    part_comma = cleaned[mask_comma].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

    # Others -> remove non-digit, keep dot as decimal
    mask_other = ~(mask_both | mask_comma)
    part_other = cleaned[mask_other].str.replace(r"[^\d.\-]", "", regex=True)

    combined = pd.concat([part_both, part_comma, part_other]).sort_index()
    return pd.to_numeric(combined, errors="coerce")


@st.cache_data(show_spinner=False)
def read_excel_sheet_names(uploaded) -> List[str]:
    xls = pd.ExcelFile(uploaded)
    return xls.sheet_names


def read_dataframe(
    uploaded, header_row: int, sheet_name: Optional[str]
) -> pd.DataFrame:
    header_idx = max(header_row - 1, 0)
    name = getattr(uploaded, "name", "")
    if name.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(uploaded, sheet_name=sheet_name, header=header_idx, dtype=str)
    else:
        # CSV: header row line number is 1-based in UI; pandas expects 0-based
        df = pd.read_csv(uploaded, header=header_idx, dtype=str, encoding="utf-8", engine="python")
    df.columns = normalize_columns(list(df.columns))
    return df


def filter_by_remark(df: pd.DataFrame, remark_col: str, pattern: str) -> pd.DataFrame:
    regex = re.compile(pattern, re.I)
    mask = df[remark_col].astype(str).str.contains(regex, na=False)
    return df[mask].copy()


def build_output_tables(
    df: pd.DataFrame, cols: Dict[str, Optional[str]], remark_pattern: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Parse date & credit
    df = df.copy()
    if cols["date"] is None or cols["remark"] is None or cols["credit"] is None:
        raise ValueError(
            "Tidak menemukan kolom Date/Tanggal, Remark, atau Credit. Mohon periksa header baris (13/14) atau ubah nama kolom."
        )

    df["__date"] = parse_date_series(df[cols["date"]])
    df["__credit"] = to_numeric_credit(df[cols["credit"]])
    df["__remark"] = df[cols["remark"]].astype(str)

    df = df[df["__date"].notna() & df["__credit"].notna()]
    df_filt = filter_by_remark(df, "__remark", remark_pattern)

    # Table 1: filtered rows (Date, Remark, Amount)
    out_rows = df_filt[["__date", "__remark", "__credit"]].rename(
        columns={"__date": "Date", "__remark": "Remark", "__credit": "Amount"}
    ).copy()
    out_rows["Amount"] = out_rows["Amount"].map(to_number_stripping_cents)

    # Table 2: grouped by Date
    grouped = (
        df_filt.assign(Date=df_filt["__date"].dt.date)
        .groupby("Date", as_index=False)["__credit"]
        .sum()
        .rename(columns={"__credit": "TotalAmount"})
    )
    grouped["TotalAmount"] = grouped["TotalAmount"].map(to_number_stripping_cents)

    return out_rows, grouped


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


# ---------- App ----------
st.set_page_config(page_title="Rekening Koran Parser", page_icon="📄", layout="wide")
st.title("📄 Rekening Koran Uploader → Filter FINIF/FINON → Group per Tanggal")

with st.sidebar:
    st.markdown("**Pengaturan Pembacaan**")
    header_row = st.radio("Mulai baca dari baris (header):", options=[13, 14], index=0, horizontal=True)
    remark_pattern = st.text_input("Filter Remark (regex):", value=r"(?:^|\s)(FINIF\w*|FINON\w*)", help="Case-insensitive")
    st.caption("Catatan: Sesuaikan jika pola berbeda.")

uploaded = st.file_uploader("Unggah file Rekening Koran (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"])

sheet_name = None
if uploaded is not None and uploaded.name.lower().endswith((".xlsx", ".xls")):
    try:
        sheets = read_excel_sheet_names(uploaded)
        sheet_name = st.selectbox("Pilih Sheet:", options=sheets, index=0)
    except Exception as e:
        st.error(f"Gagal membaca daftar sheet: {e}")

if uploaded is not None:
    try:
        df_raw = read_dataframe(uploaded, header_row=header_row, sheet_name=sheet_name)
        st.success(f"Data terbaca. Jumlah baris: {len(df_raw):,} | Kolom: {len(df_raw.columns)}")

        cols = detect_columns(df_raw)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            date_col = st.selectbox("Kolom Date/Tanggal:", options=list(df_raw.columns), index=max(list(df_raw.columns).index(cols["date"]) if cols["date"] in df_raw.columns else 0, 0))
            cols["date"] = date_col
        with col2:
            remark_col = st.selectbox("Kolom Remark:", options=list(df_raw.columns), index=max(list(df_raw.columns).index(cols["remark"]) if cols["remark"] in df_raw.columns else 0, 0))
            cols["remark"] = remark_col
        with col3:
            credit_col = st.selectbox("Kolom Credit:", options=list(df_raw.columns), index=max(list(df_raw.columns).index(cols["credit"]) if cols["credit"] in df_raw.columns else 0, 0))
            cols["credit"] = credit_col
        with col4:
            st.write("")

        out_rows, grouped = build_output_tables(df_raw, cols, remark_pattern)

        st.subheader("🔎 Baris Terfilter (FINIF/FINON)")
        st.dataframe(out_rows, use_container_width=True, height=380)

        st.subheader("🗓️ Rekap Jumlah per Tanggal (Credit)")
        st.dataframe(grouped, use_container_width=True, height=300)

        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                label="💾 Unduh Baris Terfilter (CSV)",
                data=to_csv_bytes(out_rows),
                file_name="filtered_rows.csv",
                mime="text/csv",
            )
        with dl2:
            st.download_button(
                label="💾 Unduh Rekap per Tanggal (CSV)",
                data=to_csv_bytes(grouped),
                file_name="grouped_by_date.csv",
                mime="text/csv",
            )

        with st.expander("Lihat potongan data asli (untuk verifikasi)"):
            st.dataframe(df_raw.head(50), use_container_width=True, height=300)

    except Exception as e:
        st.error(f"Gagal memproses file: {e}")
        st.stop()
else:
    st.info("Unggah file untuk mulai memproses.")

st.caption(
    "Tips: Jika kolom tidak terdeteksi benar, ubah baris header (13/14) atau pilih manual di dropdown."
)

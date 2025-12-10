# file: app.py
import io
import re
from typing import Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st


# =============== Utils: Engines & IO ===============
def _ext(name: str) -> str:
    return (name or "").lower().rsplit(".", 1)[-1] if "." in (name or "") else ""

def _need_engine_msg(ext: str) -> str:
    if ext == "xls":
        return "Missing optional dependency 'xlrd'. Install: `pip install xlrd>=2.0.1`"
    if ext == "xlsx":
        return "Missing optional dependency 'openpyxl'. Install: `pip install openpyxl`"
    if ext == "xlsb":
        return "Missing optional dependency 'pyxlsb'. Install: `pip install pyxlsb`"
    return "Engine not found."

def _ensure_engine(ext: str) -> Optional[str]:
    """
    Why: Beri pesan ramah jika lib belum terpasang di environment.
    """
    try:
        if ext == "xlsx":
            import openpyxl  # noqa: F401
            return "openpyxl"
        if ext == "xls":
            import xlrd  # noqa: F401
            return "xlrd"
        if ext == "xlsb":
            import pyxlsb  # noqa: F401
            return "pyxlsb"
        return None
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def _excel_sheet_names(uploaded) -> List[str]:
    name = getattr(uploaded, "name", "")
    ext = _ext(name)
    engine = _ensure_engine(ext)
    if ext in {"xls", "xlsx", "xlsb"} and engine is None:
        raise ImportError(_need_engine_msg(ext))
    xls = pd.ExcelFile(uploaded, engine=engine)  # engine None=auto; kita supply jika ada
    return xls.sheet_names

def _read_df(uploaded, header_row: int, sheet_name: Optional[str]) -> pd.DataFrame:
    header_idx = max(header_row - 1, 0)
    name = getattr(uploaded, "name", "")
    ext = _ext(name)
    if ext in {"xls", "xlsx", "xlsb"}:
        engine = _ensure_engine(ext)
        if engine is None:
            raise ImportError(_need_engine_msg(ext))
        df = pd.read_excel(uploaded, sheet_name=sheet_name, header=header_idx, dtype=str, engine=engine)
    else:
        df = pd.read_csv(uploaded, header=header_idx, dtype=str, encoding="utf-8", engine="python")
    df.columns = [str(c).strip() for c in df.columns]
    return df


# =============== Detect & Parse ===============
def _detect_cols(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    catalog = {
        "date": ["date", "transaction date", "tanggal", "tgl", "posting date", "value date"],
        "remark": ["remark", "description", "descriptions", "keterangan", "uraian", "transaksi", "details", "detail"],
        "credit": [
            "credit", "kredit", "cr", "cr.", "amount credit", "credit amount",
            "kredit (cr)", "jumlah kredit", "masuk", "in", "deposit", "setoran"
        ],
    }
    low = {c.lower().strip(): c for c in df.columns}
    found = {"date": None, "remark": None, "credit": None}
    for k, names in catalog.items():
        for n in names:
            if n in low:
                found[k] = low[n]; break
    if not found["date"]:
        for c in df.columns:
            if re.search(r"\b(date|tanggal|tgl)\b", str(c), re.I): found["date"]=c; break
    if not found["remark"]:
        for c in df.columns:
            if re.search(r"remark|ket|descrip|uraian|detail|transaksi", str(c), re.I): found["remark"]=c; break
    if not found["credit"]:
        cand = [c for c in df.columns if re.search(r"cred|kred|(^cr\.?$)|masuk|deposit|setoran|\bin\b", str(c), re.I)]
        found["credit"] = cand[0] if cand else None
        if not found["credit"]:
            for c in df.columns:
                if pd.api.types.is_numeric_dtype(df[c]) and not re.search(r"debit|dr|balance|saldo", str(c), re.I):
                    found["credit"] = c; break
    return found

def _parse_date(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    d1 = pd.to_datetime(s, errors="coerce", dayfirst=True)
    d2 = pd.to_datetime(s, errors="coerce", dayfirst=False)
    return d1.fillna(d2) if d1.notna().sum() >= d2.notna().sum() else d2.fillna(d1)

def _to_numeric_credit(series: pd.Series) -> pd.Series:
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
    if pd.isna(x): return ""
    s = f"{float(x):.2f}"
    return s[:-3] if s.endswith(".00") else s.rstrip("0").rstrip(".")

def _filter_remark(df: pd.DataFrame, remark_col: str, pattern: str) -> pd.DataFrame:
    rgx = re.compile(pattern, re.I)
    return df[df[remark_col].astype(str).str.contains(rgx, na=False)].copy()

def _build_outputs(df: pd.DataFrame, cols: Dict[str, Optional[str]], remark_pattern: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not cols["date"] or not cols["remark"] or not cols["credit"]:
        raise ValueError("Kolom Date/Tanggal, Remark, atau Credit tidak ditemukan. Coba atur header (13/14) atau pilih manual.")
    w = df.copy()
    w["__date"] = _parse_date(w[cols["date"]])
    w["__credit"] = _to_numeric_credit(w[cols["credit"]])
    w["__remark"] = w[cols["remark"]].astype(str)
    w = w[w["__date"].notna() & w["__credit"].notna()]
    wf = _filter_remark(w, "__remark", remark_pattern)

    out_rows = wf[["__date", "__remark", "__credit"]].rename(columns={"__date":"Date","__remark":"Remark","__credit":"Amount"}).copy()
    out_rows["Amount"] = out_rows["Amount"].map(_strip_cents)

    grouped = (wf.assign(Date=wf["__date"].dt.date)
                 .groupby("Date", as_index=False)["__credit"].sum()
                 .rename(columns={"__credit":"TotalAmount"}))
    grouped["TotalAmount"] = grouped["TotalAmount"].map(_strip_cents)
    return out_rows, grouped

def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO(); df.to_csv(buf, index=False); return buf.getvalue().encode("utf-8")


# =============== App ===============
st.set_page_config(page_title="Rekening Koran Parser", page_icon="📄", layout="wide")
st.title("📄 Rekening Koran Uploader → Filter FINIF/FINON → Group per Tanggal")

with st.sidebar:
    st.markdown("**Pengaturan Pembacaan**")
    header_row = st.radio("Mulai baca dari baris (header):", options=[13, 14], index=0, horizontal=True)
    remark_pattern = st.text_input("Filter Remark (regex):", value=r"(?:^|\s)(FINIF\w*|FINON\w*)")
    show_debug = st.checkbox("Tampilkan debug info", value=False)

uploaded = st.file_uploader("Unggah Rekening Koran (CSV/XLSX/XLS/XLSB)", type=["csv","xlsx","xls","xlsb"])

sheet_name = None
if uploaded is not None:
    ext = _ext(uploaded.name)
    if ext in {"xls", "xlsx", "xlsb"}:
        try:
            sheets = _excel_sheet_names(uploaded)
            sheet_name = st.selectbox("Pilih Sheet:", options=sheets, index=0)
        except ImportError as e:
            st.error(f"Gagal membaca daftar sheet: {e}")
            st.info("Alternatif: simpan ulang sebagai .xlsx atau .csv, atau install dependency di bawah.")
            st.code("pip install pandas openpyxl xlrd pyxlsb", language="bash")
            st.stop()
        except Exception as e:
            st.error(f"Gagal membaca daftar sheet: {e}")
            st.stop()

if uploaded is not None:
    try:
        df_raw = _read_df(uploaded, header_row=header_row, sheet_name=sheet_name)
        st.success(f"Data terbaca. Baris: {len(df_raw):,} | Kolom: {len(df_raw.columns)}")

        cols = _detect_cols(df_raw)
        c1, c2, c3 = st.columns(3)
        with c1:
            cols["date"] = st.selectbox("Kolom Date/Tanggal:", options=list(df_raw.columns),
                                        index=list(df_raw.columns).index(cols["date"]) if cols["date"] in df_raw.columns else 0)
        with c2:
            cols["remark"] = st.selectbox("Kolom Remark:", options=list(df_raw.columns),
                                          index=list(df_raw.columns).index(cols["remark"]) if cols["remark"] in df_raw.columns else 0)
        with c3:
            cols["credit"] = st.selectbox("Kolom Credit:", options=list(df_raw.columns),
                                          index=list(df_raw.columns).index(cols["credit"]) if cols["credit"] in df_raw.columns else 0)

        out_rows, grouped = _build_outputs(df_raw, cols, remark_pattern)

        st.subheader("🔎 Baris Terfilter (FINIF/FINON)")
        st.dataframe(out_rows, use_container_width=True, height=380)

        st.subheader("🗓️ Rekap Jumlah per Tanggal (Credit)")
        st.dataframe(grouped, use_container_width=True, height=300)

        d1, d2 = st.columns(2)
        with d1:
            st.download_button("💾 Unduh Baris Terfilter (CSV)", data=_to_csv_bytes(out_rows),
                               file_name="filtered_rows.csv", mime="text/csv")
        with d2:
            st.download_button("💾 Unduh Rekap per Tanggal (CSV)", data=_to_csv_bytes(grouped),
                               file_name="grouped_by_date.csv", mime="text/csv")

        if show_debug:
            st.divider()
            st.markdown("**Debug**")
            st.write("Detected columns:", cols)
            st.dataframe(df_raw.head(30), use_container_width=True, height=260)

    except ImportError as e:
        st.error(f"Gagal memproses file: {e}")
        st.info("Solusi cepat: install dependency sesuai tipe file.")
        st.code("pip install pandas openpyxl xlrd pyxlsb", language="bash")
    except Exception as e:
        st.error(f"Gagal memproses file: {e}")
else:
    st.info("Unggah file untuk mulai memproses.")

st.caption("Tips: Jika kolom tidak terdeteksi benar, ubah baris header (13/14) atau pilih manual di dropdown.")

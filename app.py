# --- RK: Uang Masuk NON BCA (FINIF/FINON) — siap paste ---

import re
import pandas as pd

# Helper fallback (dipakai hanya jika belum ada di namespace)
if "_find_col" not in globals():
    def _find_col(df: pd.DataFrame, candidates):
        cand_norm = [c.lower() for c in candidates]
        cols = list(df.columns)
        cols_norm = [str(c).strip().lower() for c in cols]
        # exact
        for i, lc in enumerate(cols_norm):
            if lc in cand_norm:
                return cols[i]
        # startswith / contains
        for i, lc in enumerate(cols_norm):
            if any(lc.startswith(c) or c in lc for c in cand_norm):
                return cols[i]
        return None

if "_to_date" not in globals():
    def _to_date(v):
        return pd.to_datetime(v, errors="coerce", dayfirst=True)

if "_to_num" not in globals():
    def _to_num(s: pd.Series) -> pd.Series:
        x = s.astype(str).str.strip()
        x = x.str.replace(r"\s+", "", regex=True)
        x = x.str.replace(r"[^\d\-.,]", "", regex=True)
        # Asumsi rupiah tanpa pecahan; buang pemisah ribuan/desimal
        x = x.str.replace(",", "", regex=False).str.replace(".", "", regex=False)
        return pd.to_numeric(x, errors="coerce").fillna(0.0)

def _normalize_remark_series(ser: pd.Series) -> pd.Series:
    # Kenapa: tahan variasi spasi/tanda baca/case agar FINIF/FINON tetap ketemu
    return ser.astype(str).str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)

def build_uang_masuk_non_bca(
    rk_non_df: pd.DataFrame,
    month_start: "pd.Timestamp",
    month_end: "pd.Timestamp",
    remark_codes=("FINIF", "FINON"),
) -> pd.Series:
    uang_masuk_non = pd.Series(dtype=float)
    if rk_non_df is None or rk_non_df.empty:
        return uang_masuk_non

    rk_tgl_non  = _find_col(rk_non_df, ["Date","Tanggal","Transaction Date","Tgl"])
    rk_amt_non  = _find_col(rk_non_df, ["credit","kredit","cr","amount","nominal"])
    rk_rem_non  = _find_col(rk_non_df, ["Remark","Keterangan","Description","Deskripsi"])
    if not (rk_tgl_non and rk_amt_non and rk_rem_non):
        return uang_masuk_non

    nb = rk_non_df.copy()
    nb[rk_tgl_non] = nb[rk_tgl_non].apply(_to_date)
    nb = nb[nb[rk_tgl_non].notna()]
    if nb.empty:
        return uang_masuk_non

    nb = nb[(nb[rk_tgl_non] >= month_start) & (nb[rk_tgl_non] <= month_end)]
    if nb.empty:
        return uang_masuk_non

    rem_norm = _normalize_remark_series(nb[rk_rem_non])
    codes = [re.sub(r"[^A-Z0-9]", "", str(c).upper()) for c in remark_codes if str(c).strip()]
    if not codes:
        return uang_masuk_non
    pattern = r"(" + "|".join(map(re.escape, codes)) + r")"
    fin_mask = rem_norm.str.contains(pattern, regex=True, na=False)
    nb = nb[fin_mask]
    if nb.empty:
        return uang_masuk_non

    nb[rk_amt_non] = _to_num(nb[rk_amt_non])

    uang_masuk_non = nb.groupby(nb[rk_tgl_non].dt.date, dropna=True)[rk_amt_non].sum().astype(float)
    return uang_masuk_non

# ==== PANGGILAN (drop-in mengganti snippet lama) ====
# Pastikan variabel rk_non_df, month_start, month_end sudah ada di konteks Anda.
uang_masuk_non = build_uang_masuk_non_bca(
    rk_non_df=rk_non_df,
    month_start=month_start,
    month_end=month_end,
    remark_codes=("FINIF", "FINON"),
)

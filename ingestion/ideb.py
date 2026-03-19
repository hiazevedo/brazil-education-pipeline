"""Download IDEB municipal data from INEP and upload to Databricks."""

import os
import tempfile
import pandas as pd
from uploader import upload_file

# INEP changed the path for the 2023 edition — files are now direct XLSX (no ZIP)
# Previous editions used: download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/{year}/
IDEB_URLS = {
    "municipios_ef1": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_iniciais_municipios_2023.xlsx",
    "municipios_ef2": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_municipios_2023.xlsx",
}


def download_and_convert(key: str, url: str, tmp_dir: str) -> str:
    """Download IDEB XLSX, parse, and save as Parquet."""
    import requests

    xls_path = os.path.join(tmp_dir, f"ideb_{key}.xlsx")

    print(f"[{key}] Downloading from {url} ...")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(xls_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                f.write(chunk)

    print(f"[{key}] Parsing spreadsheet ...")
    # INEP IDEB sheets have metadata rows at the top.
    # Try header rows 9, 10, 8, 7 until we find one that yields non-empty columns.
    df = None
    for header_row in (9, 10, 8, 7):
        candidate = pd.read_excel(xls_path, sheet_name=0, header=header_row, dtype=str)
        candidate = candidate.dropna(how="all").dropna(axis=1, how="all")
        if len(candidate.columns) > 3:
            df = candidate
            print(f"[{key}] Header row: {header_row} | Columns ({len(df.columns)}): {list(df.columns[:5])}")
            break
    if df is None:
        raise ValueError(f"[{key}] Could not detect header row in spreadsheet.")

    # Tag which segment this is
    df["SEGMENTO"] = key

    parquet_path = os.path.join(tmp_dir, f"ideb_{key}.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"[{key}] Saved {len(df):,} rows → {parquet_path}")

    return parquet_path


def main():
    with tempfile.TemporaryDirectory() as tmp_dir:
        for key, url in IDEB_URLS.items():
            parquet_path = download_and_convert(key, url, tmp_dir)
            upload_file(parquet_path, f"ideb_{key}.parquet")


if __name__ == "__main__":
    main()

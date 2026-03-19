"""Download IDEB municipal data from INEP and upload to Databricks."""

import os
import zipfile
import tempfile
import pandas as pd
from uploader import upload_file

# IDEB is published biennially; one file covers all editions
# Municipal IDEB: Anos Iniciais (EF1) and Anos Finais (EF2)
IDEB_URLS = {
    "municipios_ef1": "https://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2023/divulgacao_municipios_ef_1_anos_2023.zip",
    "municipios_ef2": "https://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2023/divulgacao_municipios_ef_2_anos_2023.zip",
}


def download_and_convert(key: str, url: str, tmp_dir: str) -> str:
    """Download IDEB zip (XLS/XLSX), parse, and save as Parquet."""
    import requests

    zip_path = os.path.join(tmp_dir, f"ideb_{key}.zip")

    print(f"[{key}] Downloading from {url} ...")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                f.write(chunk)

    print(f"[{key}] Extracting ...")
    with zipfile.ZipFile(zip_path, "r") as z:
        xls_name = next(
            n for n in z.namelist()
            if n.lower().endswith((".xls", ".xlsx"))
        )
        z.extract(xls_name, tmp_dir)
        xls_path = os.path.join(tmp_dir, xls_name)

    print(f"[{key}] Parsing spreadsheet ...")
    # INEP IDEB sheets have metadata rows at the top — skip until header row
    df = pd.read_excel(xls_path, sheet_name=0, header=9, dtype=str)

    # Drop completely empty rows/columns
    df = df.dropna(how="all").dropna(axis=1, how="all")

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

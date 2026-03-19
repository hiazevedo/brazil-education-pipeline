"""Download ENEM microdata from INEP, filter relevant columns, and upload to Databricks."""

import os
import zipfile
import tempfile
import pandas as pd
from uploader import upload_file

# Columns to keep from the full ENEM microdata (~180 columns → 17 columns)
ENEM_COLUMNS = [
    "NU_ANO",
    "CO_MUNICIPIO_ESC",
    "SG_UF_ESC",
    "TP_FAIXA_ETARIA",
    "TP_SEXO",
    "TP_COR_RACA",
    "TP_ESCOLA",
    "TP_ENSINO",
    "IN_TREINEIRO",
    "Q001",   # Father's education level
    "Q002",   # Mother's education level
    "Q006",   # Household income bracket
    "NU_NOTA_CN",
    "NU_NOTA_CH",
    "NU_NOTA_LC",
    "NU_NOTA_MT",
    "NU_NOTA_REDACAO",
]

# INEP direct download URLs — update when new editions are released
# Format: https://download.inep.gov.br/microdados/microdados_enem_{year}.zip
ENEM_URLS = {
    2020: "https://download.inep.gov.br/microdados/microdados_enem_2020.zip",
    2021: "https://download.inep.gov.br/microdados/microdados_enem_2021.zip",
    2022: "https://download.inep.gov.br/microdados/microdados_enem_2022.zip",
    2023: "https://download.inep.gov.br/microdados/microdados_enem_2023.zip",
}


def download_and_filter(year: int, tmp_dir: str) -> str:
    """Download ENEM zip, extract CSV, filter columns, save as Parquet."""
    import requests

    url = ENEM_URLS[year]
    zip_path = os.path.join(tmp_dir, f"enem_{year}.zip")

    print(f"[{year}] Downloading from {url} ...")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

    print(f"[{year}] Extracting ...")
    with zipfile.ZipFile(zip_path, "r") as z:
        # INEP zips contain a CSV in DADOS/ subfolder
        csv_name = next(n for n in z.namelist() if n.endswith(".csv") and "DADOS" in n)
        z.extract(csv_name, tmp_dir)
        csv_path = os.path.join(tmp_dir, csv_name)

    print(f"[{year}] Detecting CSV format ...")
    with open(csv_path, encoding="latin1") as f:
        header_line = f.readline()
    sep = ";" if header_line.count(";") >= header_line.count(",") else ","
    print(f"[{year}] Detected separator: '{sep}'")

    # Cross-check expected columns against what actually exists in the file
    header_df = pd.read_csv(csv_path, sep=sep, encoding="latin1", nrows=0)
    print(f"[{year}] Columns in file ({len(header_df.columns)}): {list(header_df.columns[:10])} ...")
    available_cols = [c for c in ENEM_COLUMNS if c in header_df.columns]
    missing_cols = set(ENEM_COLUMNS) - set(available_cols)
    if missing_cols:
        print(f"[{year}] Warning: columns not found (skipping): {sorted(missing_cols)}")
    if not available_cols:
        raise ValueError(
            f"[{year}] None of the expected columns were found. "
            f"First 10 actual columns: {list(header_df.columns[:10])}"
        )
    print(f"[{year}] Reading {len(available_cols)}/{len(ENEM_COLUMNS)} columns ...")

    df = pd.read_csv(
        csv_path,
        sep=sep,
        encoding="latin1",
        usecols=available_cols,
        dtype=str,  # keep raw types; casting happens in Silver layer
    )

    parquet_path = os.path.join(tmp_dir, f"enem_{year}.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"[{year}] Saved {len(df):,} rows → {parquet_path}")

    return parquet_path


def main():
    years = [int(y) for y in os.environ.get("ENEM_YEARS", "2023").split(",")]

    with tempfile.TemporaryDirectory() as tmp_dir:
        for year in years:
            parquet_path = download_and_filter(year, tmp_dir)
            upload_file(parquet_path, f"enem_{year}.parquet")


if __name__ == "__main__":
    main()

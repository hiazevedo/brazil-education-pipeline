"""Download Censo Escolar from INEP, filter relevant columns, and upload to Databricks."""

import os
import zipfile
import tempfile
import pandas as pd
from uploader import upload_file

# Columns to keep — school infrastructure + location identifiers
CENSO_COLUMNS = [
    "NU_ANO_CENSO",
    "CO_ENTIDADE",
    "NO_ENTIDADE",
    "CO_MUNICIPIO",
    "NO_MUNICIPIO",
    "CO_UF",
    "SG_UF",
    "TP_DEPENDENCIA",       # 1=Federal 2=Estadual 3=Municipal 4=Privada
    "TP_LOCALIZACAO",       # 1=Urbana 2=Rural
    "TP_SITUACAO_FUNCIONAMENTO",
    "QT_MAT_BAS",           # Total basic education enrollments
    "QT_DOC_BAS",           # Total teachers
    "IN_ENERGIA_REDE_PUBLICA",
    "IN_AGUA_POTAVEL",
    "IN_ESGOTO_REDE_PUBLICA",
    "IN_BIBLIOTECA",
    "IN_LABORATORIO_INFORMATICA",
    "IN_LABORATORIO_CIENCIAS",
    "IN_INTERNET",
    "IN_EQUIP_MULTIMIDIA",
    "IN_QUADRA_ESPORTES",
]

# INEP direct download URLs — escola (school) file only
CENSO_URLS = {
    2020: "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2020.zip",
    2021: "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2021.zip",
    2022: "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2022.zip",
    2023: "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2023.zip",
}


def download_and_filter(year: int, tmp_dir: str) -> str:
    """Download Censo Escolar zip, extract school CSV, filter columns, save as Parquet."""
    import requests

    url = CENSO_URLS[year]
    zip_path = os.path.join(tmp_dir, f"censo_{year}.zip")

    print(f"[{year}] Downloading from {url} ...")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

    print(f"[{year}] Extracting escola CSV ...")
    with zipfile.ZipFile(zip_path, "r") as z:
        all_csvs = [n for n in z.namelist() if n.upper().endswith(".CSV")]
        print(f"[{year}] CSVs in ZIP: {all_csvs}")
        csv_name = next(
            n for n in all_csvs if "ESCOLA" in n.upper()
        )
        print(f"[{year}] Selected: {csv_name}")
        z.extract(csv_name, tmp_dir)
        csv_path = os.path.join(tmp_dir, csv_name)

    print(f"[{year}] Detecting CSV format ...")
    for encoding in ("utf-8-sig", "latin1"):
        try:
            with open(csv_path, encoding=encoding) as f:
                header_line = f.readline()
            break
        except UnicodeDecodeError:
            continue
    sep = "|" if header_line.count("|") >= header_line.count(";") else ";"
    print(f"[{year}] Encoding: {encoding} | Separator: '{sep}'")

    header_df = pd.read_csv(csv_path, sep=sep, encoding=encoding, nrows=0)
    header_df.columns = header_df.columns.str.strip()
    print(f"[{year}] First 5 columns: {list(header_df.columns[:5])}")

    available_cols = [c for c in CENSO_COLUMNS if c in header_df.columns]
    missing_cols = set(CENSO_COLUMNS) - set(available_cols)
    if missing_cols:
        print(f"[{year}] Warning: columns not found (skipping): {sorted(missing_cols)}")
    if not available_cols:
        raise ValueError(f"[{year}] None of the expected columns were found. First 10: {list(header_df.columns[:10])}")

    df = pd.read_csv(
        csv_path,
        sep=sep,
        encoding=encoding,
        usecols=available_cols,
        dtype=str,
    )
    df.columns = df.columns.str.strip()

    # Keep only active schools (if column exists)
    if "TP_SITUACAO_FUNCIONAMENTO" in df.columns:
        df = df[df["TP_SITUACAO_FUNCIONAMENTO"] == "1"].drop(columns=["TP_SITUACAO_FUNCIONAMENTO"])

    parquet_path = os.path.join(tmp_dir, f"censo_escolar_{year}.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"[{year}] Saved {len(df):,} rows → {parquet_path}")

    return parquet_path


def main():
    years = [int(y) for y in os.environ.get("CENSO_YEARS", "2023").split(",")]

    with tempfile.TemporaryDirectory() as tmp_dir:
        for year in years:
            parquet_path = download_and_filter(year, tmp_dir)
            upload_file(parquet_path, f"censo_escolar_{year}.parquet")


if __name__ == "__main__":
    main()

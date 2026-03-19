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
        # Censo zip contains multiple CSVs (escola, docente, matricula, turma)
        # We only need the escola file
        csv_name = next(
            n for n in z.namelist()
            if n.upper().endswith(".CSV") and "ESCOLA" in n.upper()
        )
        z.extract(csv_name, tmp_dir)
        csv_path = os.path.join(tmp_dir, csv_name)

    print(f"[{year}] Filtering columns ...")
    df = pd.read_csv(
        csv_path,
        sep="|",
        encoding="latin1",
        usecols=CENSO_COLUMNS,
        dtype=str,
    )

    # Keep only active schools
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

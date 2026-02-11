"""
ETL Polymarket -> Delta Lake (Local)
=====================================
Extrae todos los datos de la API Gamma de Polymarket (tags, events, markets, series)
y los guarda en formato Delta Lake en local.

Estructura de salida:
    raw/
    ├── tags/
    ├── events/
    ├── markets/
    └── series/

Requisitos:
    pip install deltalake pandas requests pyarrow
"""

import requests
import json
import time
import os
import pandas as pd
from deltalake import write_deltalake

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
BASE_URL = "https://gamma-api.polymarket.com"
OUTPUT_DIR = "raw"
PAGE_SIZE = 500
DELAY = 0.3

ENDPOINTS = ["tags", "events", "markets", "series"]

# =============================================================================
# EXTRACCIÓN DE DATOS DE LA API
# =============================================================================
def fetch_all_records(endpoint: str) -> list:
    all_records = []
    offset = 0

    print(f"\n{'='*60}")
    print(f"Extrayendo: {endpoint.upper()}")
    print(f"{'='*60}")

    while True:
        url = f"{BASE_URL}/{endpoint}"
        params = {"limit": PAGE_SIZE, "offset": offset}

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"  ERROR en petición (offset={offset}): {e}")
            break
        except json.JSONDecodeError:
            print(f"  ERROR: respuesta no es JSON válido (offset={offset})")
            break

        if not data or len(data) == 0:
            break

        all_records.extend(data)
        print(f"  Descargados: {len(all_records)} registros (offset={offset})")

        if len(data) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(DELAY)

    print(f"  TOTAL {endpoint}: {len(all_records)} registros")
    return all_records

# =============================================================================
# CARGA EN DELTA LAKE
# =============================================================================
def save_to_delta(data: list, endpoint: str):
    if not data:
        print(f"  Sin datos para {endpoint}, saltando...")
        return

    output_path = os.path.join(OUTPUT_DIR, endpoint)
    os.makedirs(output_path, exist_ok=True)

    # Crear DataFrame con json_normalize para aplanar campos anidados
    df = pd.json_normalize(data, sep="_")

    # Convertir columnas con objetos anidados (listas/dicts) a JSON string
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

    # Convertir a string para evitar conflictos de tipos mixtos entre registros
    for col in df.columns:
        df[col] = df[col].astype(str).replace({"nan": None, "None": None})

    print(f"  Columnas: {len(df.columns)}")
    print(f"  Registros: {len(df)}")

    # Guardar en Delta Lake
    write_deltalake(output_path, df, mode="overwrite")
    print(f"  Guardado en: {output_path}")

# =============================================================================
# REPORTE DE VOLUMETRÍA
# =============================================================================
def generate_report(data_dict: dict):
    print(f"\n{'='*60}")
    print("REPORTE DE VOLUMETRÍA")
    print(f"{'='*60}")

    for endpoint, data in data_dict.items():
        if not data:
            continue

        df = pd.json_normalize(data, sep="_")
        print(f"\n--- {endpoint.upper()} ---")
        print(f"  Total registros: {len(df)}")
        print(f"  Total columnas:  {len(df.columns)}")

        if endpoint == "markets":
            if "active" in df.columns:
                print(f"  Mercados activos:  {df['active'].sum()}")
            if "closed" in df.columns:
                print(f"  Mercados cerrados: {df['closed'].sum()}")
            if "volumeNum" in df.columns:
                df["volumeNum"] = pd.to_numeric(df["volumeNum"], errors="coerce")
                print(f"  Volumen total:     {df['volumeNum'].sum():.2f}")
                print(f"  Volumen medio:     {df['volumeNum'].mean():.2f}")

        elif endpoint == "events":
            if "active" in df.columns:
                print(f"  Eventos activos:   {df['active'].sum()}")
            if "closed" in df.columns:
                print(f"  Eventos cerrados:  {df['closed'].sum()}")

        elif endpoint == "series":
            if "active" in df.columns:
                print(f"  Series activas:    {df['active'].sum()}")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("POLYMARKET ETL - Extracción a Delta Lake")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    data_dict = {}

    for endpoint in ENDPOINTS:
        data = fetch_all_records(endpoint)
        data_dict[endpoint] = data
        save_to_delta(data, endpoint)

    generate_report(data_dict)

    print(f"\n{'='*60}")
    print("ETL COMPLETADO")
    print(f"Archivos Delta Lake en: ./{OUTPUT_DIR}/")
    print("Sube la carpeta 'raw/' al bucket S3 manualmente.")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
from typing import List, Optional
from pathlib import Path
import os
import pandas as pd
import numpy as np
from fastapi import HTTPException


EXCEL_FILE = os.getenv("EXCEL_FILE", "Base_Kaiserhaus.xlsx")
DATA_PATH = Path(__file__).resolve().parents[1] / "data" / EXCEL_FILE


class DataState:
    df: Optional[pd.DataFrame] = None
    cols: List[str] = []
    dtypes: dict = {}
    total_rows: int = 0
    source_path: Path = DATA_PATH


state = DataState()


def load_excel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado em: {path}")
    df = pd.read_excel(path, engine="openpyxl")
    df.columns = [str(c) for c in df.columns]
    return df


COLUMN_ALIASES = {
    "order_id": ["order_id", "id_pedido", "pedido_id", "id"],
    "order_datetime": ["order_datetime", "data_pedido", "created_at", "order_date"],
    "order_date": ["order_date", "data", "dt", "date"],
    "platform": ["platform", "plataforma"],
    "order_mode": ["order_mode", "modo_pedido", "channel"],
    "status": ["status", "order_status"],
    "macro_bairro": ["macro_bairro", "macro_bairros", "macro_bairro_nome"],
    "total_brl": ["total_brl", "valor_total", "total"],
    "num_itens": ["num_itens", "qtd_itens", "items_count"],
    "tempo_preparo_minutos": ["tempo_preparo_minutos", "prep_minutes", "preparo_min"],
    "actual_delivery_minutes": ["actual_delivery_minutes", "delivery_minutes", "tempo_entrega_min"],
    "eta_minutes_quote": ["eta_minutes_quote", "eta_minutos", "eta_min"],
    "distance_km": ["distance_km", "distancia_km", "km"],
    "platform_commision_pct": ["platform_commision_pct", "platform_commission_pct", "taxa_plataforma"],
    "satisfacao_nivel": ["satisfacao_nivel", "satisfacao", "satisfaction", "nota"],
    "cliente_id": ["cliente_id", "customer_id", "id_cliente"],
}


def resolve_column(df: pd.DataFrame, preferred: Optional[str], logical_name: str) -> Optional[str]:
    if preferred and preferred in df.columns:
        return preferred
    for alias in COLUMN_ALIASES.get(logical_name, []):
        if alias in df.columns:
            return alias
    return None


def ensure_datetime(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        raise HTTPException(status_code=400, detail=f"Coluna de data inválida: {col}")
    s = df[col]
    if not pd.api.types.is_datetime64_any_dtype(s):
        s = pd.to_datetime(s, errors="coerce")
    return s


def to_records(df: pd.DataFrame) -> List[dict]:
    return df.reset_index(drop=True).to_dict(orient="records")




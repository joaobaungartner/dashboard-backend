from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.shared import state, resolve_column, ensure_datetime


router = APIRouter()


@router.get("/platforms")
def platforms(platform_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    plc = resolve_column(df, platform_col, "platform")
    if not plc:
        raise HTTPException(status_code=400, detail="Coluna de plataforma não encontrada.")
    vals = sorted(df[plc].dropna().astype(str).unique())
    return {"data": list(vals)}


@router.get("/macros")
def macros(macro_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    mcc = resolve_column(df, macro_col, "macro_bairro")
    if not mcc:
        raise HTTPException(status_code=400, detail="Coluna de macro_bairro não encontrada.")
    vals = sorted(df[mcc].dropna().astype(str).unique())
    return {"data": list(vals)}


@router.get("/date_range")
def date_range(date_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    dtc = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    if not dtc:
        raise HTTPException(status_code=400, detail="Coluna de data não encontrada.")
    sdt = ensure_datetime(df, dtc)
    if len(sdt) == 0:
        return {"periodo": None}
    return {"periodo": {"min": str(sdt.min()), "max": str(sdt.max())}}




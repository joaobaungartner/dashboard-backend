from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.shared import state, resolve_column, ensure_datetime, to_records


router = APIRouter()


@router.get("/kpis")
def overview_kpis(
    date_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    items_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df

    total_brl_col = resolve_column(df, total_col, "total_brl")
    num_itens_col = resolve_column(df, items_col, "num_itens")

    total_pedidos = int(len(df))
    receita_total = float(df[total_brl_col].sum()) if total_brl_col else 0.0
    ticket_medio = float((df[total_brl_col] / df[num_itens_col]).mean()) if (total_brl_col and num_itens_col) else 0.0

    dt_col = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    periodo = None
    if dt_col:
        sdt = ensure_datetime(df, dt_col)
        periodo = {"min": sdt.min(), "max": sdt.max()}

    return {
        "total_pedidos": total_pedidos,
        "receita_total": receita_total,
        "ticket_medio": ticket_medio,
        "periodo": periodo,
    }


@router.get("/timeseries_orders")
def overview_timeseries_orders(
    date_col: Optional[str] = Query(None),
    freq: str = Query("D", description="D, W, M"),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    dt_col = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    if not dt_col:
        raise HTTPException(status_code=400, detail="Coluna de data não encontrada.")
    sdt = ensure_datetime(df, dt_col)
    ts = (
        sdt.to_frame(name="dt")
        .set_index("dt")
        .assign(v=1)
        .resample(freq)["v"].sum()
        .fillna(0)
        .reset_index()
        .rename(columns={"dt": "date", "v": "orders"})
    )
    return {"data": to_records(ts)}


@router.get("/by_platform")
def overview_by_platform(platform_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    platform = resolve_column(df, platform_col, "platform")
    if not platform:
        raise HTTPException(status_code=400, detail="Coluna de plataforma não encontrada.")
    g = df.groupby(platform).size().reset_index(name="orders").sort_values("orders", ascending=False)
    return {"data": to_records(g)}


@router.get("/status_distribution")
def overview_status_distribution(status_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    status = resolve_column(df, status_col, "status")
    if not status:
        raise HTTPException(status_code=400, detail="Coluna de status não encontrada.")
    dist = df[status].astype(str).value_counts(dropna=False).reset_index()
    dist.columns = ["status", "count"]
    return {"data": to_records(dist)}


@router.get("/macro_bairro_avg_receita")
def overview_macro_bairro_avg_receita(
    macro_col: Optional[str] = Query(None), total_col: Optional[str] = Query(None)
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    macro = resolve_column(df, macro_col, "macro_bairro")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not macro or not total_brl_col:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/total_brl não encontradas.")
    g = df.groupby(macro)[total_brl_col].mean().reset_index(name="avg_receita")
    return {"data": to_records(g.sort_values("avg_receita", ascending=False))}



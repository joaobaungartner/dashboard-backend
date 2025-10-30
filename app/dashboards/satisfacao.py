from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.main import state, resolve_column, ensure_datetime, to_records


router = APIRouter()


@router.get("/kpis")
def satisfaction_kpis(score_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    score = resolve_column(df, score_col, "satisfacao_nivel")
    if not score:
        raise HTTPException(status_code=400, detail="Coluna de satisfação não encontrada.")
    s = pd.to_numeric(df[score], errors="coerce")
    nivel_medio = float(s.mean()) if len(s) else 0.0
    pct_muito_satisfeitos = float((s >= 4.5).mean() * 100) if len(s) else 0.0
    return {"nivel_medio": nivel_medio, "%_muito_satisfeitos": pct_muito_satisfeitos}


@router.get("/by_macro_bairro")
def satisfaction_by_macro_bairro(macro_col: Optional[str] = Query(None), score_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    macro = resolve_column(df, macro_col, "macro_bairro")
    score = resolve_column(df, score_col, "satisfacao_nivel")
    if not macro or not score:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/satisfação não encontradas.")
    g = df.groupby(macro)[score].mean().reset_index(name="avg_satisfacao")
    return {"data": to_records(g.sort_values("avg_satisfacao", ascending=False))}


@router.get("/scatter_time_vs_score")
def satisfaction_scatter_time_vs_score(
    delivery_col: Optional[str] = Query(None), score_col: Optional[str] = Query(None)
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    delivery = resolve_column(df, delivery_col, "actual_delivery_minutes")
    score = resolve_column(df, score_col, "satisfacao_nivel")
    if not delivery or not score:
        raise HTTPException(status_code=400, detail="Colunas de entrega/satisfação não encontradas.")
    tmp = pd.DataFrame({
        "x": pd.to_numeric(df[delivery], errors="coerce"),
        "y": pd.to_numeric(df[score], errors="coerce"),
    }).dropna()
    return {"data": to_records(tmp.rename(columns={"x": "delivery_minutes", "y": "satisfacao"}))}


@router.get("/timeseries")
def satisfaction_timeseries(
    date_col: Optional[str] = Query(None), score_col: Optional[str] = Query(None), freq: str = "D"
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    dt_col = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    score = resolve_column(df, score_col, "satisfacao_nivel")
    if not dt_col or not score:
        raise HTTPException(status_code=400, detail="Colunas de data/satisfação não encontradas.")
    sdt = ensure_datetime(df, dt_col)
    ts = (
        pd.DataFrame({"dt": sdt, "v": pd.to_numeric(df[score], errors="coerce")})
        .dropna()
        .set_index("dt")
        .resample(freq)["v"].mean()
        .reset_index()
        .rename(columns={"dt": "date", "v": "avg_satisfacao"})
    )
    return {"data": to_records(ts)}


@router.get("/heatmap_platform")
def satisfaction_heatmap_platform(platform_col: Optional[str] = Query(None), score_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    platform = resolve_column(df, platform_col, "platform")
    score = resolve_column(df, score_col, "satisfacao_nivel")
    if not platform or not score:
        raise HTTPException(status_code=400, detail="Colunas de plataforma/satisfação não encontradas.")
    g = df.groupby(platform)[score].mean().reset_index(name="avg_satisfacao")
    return {"data": to_records(g)}



from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.main import state, resolve_column, ensure_datetime, to_records
from app.main import np as _np  # use numpy from main context to avoid extra import


router = APIRouter()


@router.get("/kpis")
def ops_kpis(
    prep_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    distance_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    prep = resolve_column(df, prep_col, "tempo_preparo_minutos")
    delivery = resolve_column(df, delivery_col, "actual_delivery_minutes")
    eta = resolve_column(df, eta_col, "eta_minutes_quote")
    distance = resolve_column(df, distance_col, "distance_km")

    tempo_medio_preparo = float(_np.nanmean(pd.to_numeric(df[prep], errors="coerce"))) if prep else 0.0
    tempo_medio_entrega = float(_np.nanmean(pd.to_numeric(df[delivery], errors="coerce"))) if delivery else 0.0
    atraso_medio = (
        float(_np.nanmean(pd.to_numeric(df[delivery], errors="coerce") - pd.to_numeric(df[eta], errors="coerce")))
        if (delivery and eta) else 0.0
    )
    distancia_media = float(_np.nanmean(pd.to_numeric(df[distance], errors="coerce"))) if distance else 0.0

    return {
        "tempo_medio_preparo": tempo_medio_preparo,
        "tempo_medio_entrega": tempo_medio_entrega,
        "atraso_medio": atraso_medio,
        "distancia_media": distancia_media,
    }


@router.get("/timeseries_delivery")
def ops_timeseries_delivery(
    date_col: Optional[str] = Query(None), delivery_col: Optional[str] = Query(None), freq: str = "D"
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    dt_col = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    delivery = resolve_column(df, delivery_col, "actual_delivery_minutes")
    if not dt_col or not delivery:
        raise HTTPException(status_code=400, detail="Colunas de data/tempo de entrega não encontradas.")
    sdt = ensure_datetime(df, dt_col)
    ts = (
        pd.DataFrame({"dt": sdt, "v": pd.to_numeric(df[delivery], errors="coerce")})
        .dropna()
        .set_index("dt")
        .resample(freq)["v"].mean()
        .reset_index()
        .rename(columns={"dt": "date", "v": "avg_delivery_minutes"})
    )
    return {"data": to_records(ts)}


@router.get("/boxplot_delivery_by_macro")
def ops_boxplot_delivery_by_macro(
    macro_col: Optional[str] = Query(None), delivery_col: Optional[str] = Query(None)
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    macro = resolve_column(df, macro_col, "macro_bairro")
    delivery = resolve_column(df, delivery_col, "actual_delivery_minutes")
    if not macro or not delivery:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/tempo de entrega não encontradas.")
    grouped = (
        df[[macro, delivery]]
        .dropna()
        .groupby(macro)[delivery]
        .apply(lambda s: list(pd.to_numeric(s, errors="coerce").dropna()))
        .reset_index(name="values")
    )
    return {"data": to_records(grouped)}


@router.get("/heatmap_delay_by_macro")
def ops_heatmap_delay_by_macro(
    macro_col: Optional[str] = Query(None), delivery_col: Optional[str] = Query(None), eta_col: Optional[str] = Query(None)
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    macro = resolve_column(df, macro_col, "macro_bairro")
    delivery = resolve_column(df, delivery_col, "actual_delivery_minutes")
    eta = resolve_column(df, eta_col, "eta_minutes_quote")
    if not macro or not delivery or not eta:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/entrega/eta não encontradas.")
    tmp = df[[macro, delivery, eta]].copy()
    tmp["delay"] = pd.to_numeric(tmp[delivery], errors="coerce") - pd.to_numeric(tmp[eta], errors="coerce")
    heat = tmp.groupby(macro)["delay"].mean().reset_index()
    return {"data": to_records(heat.rename(columns={"delay": "avg_delay"}))}


@router.get("/scatter_distance_vs_delivery")
def ops_scatter_distance_vs_delivery(
    distance_col: Optional[str] = Query(None), delivery_col: Optional[str] = Query(None)
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    distance = resolve_column(df, distance_col, "distance_km")
    delivery = resolve_column(df, delivery_col, "actual_delivery_minutes")
    if not distance or not delivery:
        raise HTTPException(status_code=400, detail="Colunas de distância/entrega não encontradas.")
    tmp = pd.DataFrame({
        "x": pd.to_numeric(df[distance], errors="coerce"),
        "y": pd.to_numeric(df[delivery], errors="coerce"),
    }).dropna()
    return {"data": to_records(tmp.rename(columns={"x": "distance_km", "y": "delivery_minutes"}))}



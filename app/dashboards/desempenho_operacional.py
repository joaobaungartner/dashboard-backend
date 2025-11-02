from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.shared import state, resolve_column, ensure_datetime, to_records
import numpy as _np


router = APIRouter()


def _apply_global_filters(
    df: pd.DataFrame,
    *,
    start_date: Optional[str],
    end_date: Optional[str],
    platform: Optional[List[str]],
    macro_bairro: Optional[List[str]],
    delivery_status: Optional[str],
    date_col: Optional[str],
    platform_col: Optional[str],
    macro_col: Optional[str],
    delivery_col: Optional[str],
    eta_col: Optional[str],
    threshold_min: Optional[float] = None,
):
    dtc = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    plc = resolve_column(df, platform_col, "platform")
    mcc = resolve_column(df, macro_col, "macro_bairro")
    dlc = resolve_column(df, delivery_col, "actual_delivery_minutes")
    etc = resolve_column(df, eta_col, "eta_minutes_quote")

    if start_date or end_date:
        if not dtc:
            raise HTTPException(status_code=400, detail="Coluna de data não encontrada para aplicar filtro.")
        sdt = ensure_datetime(df, dtc)
        if start_date:
            try:
                sd = pd.to_datetime(start_date)
            except Exception:
                raise HTTPException(status_code=422, detail="start_date inválido. Use yyyy-mm-dd")
            df = df[sdt >= sd]
            sdt = sdt[sdt >= sd]
        if end_date:
            try:
                ed = pd.to_datetime(end_date)
            except Exception:
                raise HTTPException(status_code=422, detail="end_date inválido. Use yyyy-mm-dd")
            df = df[sdt <= ed]

    if platform is not None and len(platform) > 0:
        if not plc:
            raise HTTPException(status_code=400, detail="Coluna de plataforma não encontrada para aplicar filtro.")
        df = df[df[plc].astype(str).isin(platform)]

    if macro_bairro is not None and len(macro_bairro) > 0:
        if not mcc:
            raise HTTPException(status_code=400, detail="Coluna de macro_bairro não encontrada para aplicar filtro.")
        df = df[df[mcc].astype(str).isin(macro_bairro)]

    if delivery_status:
        if delivery_status not in ("atrasado", "no_prazo"):
            raise HTTPException(status_code=422, detail="delivery_status inválido. Use atrasado|no_prazo")
        if not dlc or not etc:
            raise HTTPException(status_code=400, detail="delivery_status requer delivery_col e eta_col válidos")
        d = pd.to_numeric(df[dlc], errors="coerce")
        e = pd.to_numeric(df[etc], errors="coerce")
        thr = float(threshold_min) if threshold_min is not None else 0.0
        if delivery_status == "atrasado":
            df = df[(d - e) > thr]
        else:
            df = df[(d - e) <= thr]

    return df, dtc, plc, mcc, dlc, etc


@router.get("/kpis")
def ops_kpis(
    prep_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    distance_col: Optional[str] = Query(None),
    grace_min: float = Query(0.0, description="Tolerância em minutos além do ETA"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    prep = resolve_column(df, prep_col, "tempo_preparo_minutos")
    delivery = dlc or resolve_column(df, delivery_col, "actual_delivery_minutes")
    eta = etc or resolve_column(df, eta_col, "eta_minutes_quote")
    distance = resolve_column(df, distance_col, "distance_km")

    tempo_medio_preparo = float(_np.nanmean(pd.to_numeric(df[prep], errors="coerce"))) if prep else 0.0
    tempo_medio_entrega = float(_np.nanmean(pd.to_numeric(df[delivery], errors="coerce"))) if delivery else 0.0
    atraso_medio = (
        float(_np.nanmean(pd.to_numeric(df[delivery], errors="coerce") - pd.to_numeric(df[eta], errors="coerce")))
        if (delivery and eta) else 0.0
    )
    distancia_media = float(_np.nanmean(pd.to_numeric(df[distance], errors="coerce"))) if distance else 0.0

    on_time_rate_pct = 0.0
    if delivery and eta:
        d = pd.to_numeric(df[delivery], errors="coerce")
        e = pd.to_numeric(df[eta], errors="coerce")
        mask = (~d.isna()) & (~e.isna())
        d = d[mask]
        e = e[mask]
        if len(d) > 0:
            on_time_rate_pct = float(((d <= (e + grace_min)).mean()) * 100)

    return {
        "tempo_medio_preparo": tempo_medio_preparo,
        "tempo_medio_entrega": tempo_medio_entrega,
        "atraso_medio": atraso_medio,
        "distancia_media": distancia_media,
        "on_time_rate_pct": on_time_rate_pct,
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


@router.get("/orders_by_hour")
def orders_by_hour(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    if not dtc:
        raise HTTPException(status_code=400, detail="Coluna de data não encontrada.")
    sdt = ensure_datetime(df, dtc)
    hours = sdt.dt.hour
    g = hours.value_counts().sort_index()
    total = int(g.sum()) if len(g) else 0
    res = pd.DataFrame({"hour": g.index, "orders": g.values})
    res["share"] = (res["orders"] / total).fillna(0.0)
    return {"data": to_records(res)}


@router.get("/late_rate_by_macro")
def late_rate_by_macro(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=None,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    if not mcc or not dlc or not etc:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/entrega/eta não encontradas.")
    d = pd.to_numeric(df[dlc], errors="coerce")
    e = pd.to_numeric(df[etc], errors="coerce")
    thr = float(threshold_min) if threshold_min is not None else 0.0
    late_mask = (d - e) > thr
    tmp = pd.DataFrame({mcc: df[mcc].astype(str), "late": late_mask})
    g = tmp.groupby(mcc)["late"].agg(["sum", "count"]).reset_index()
    g = g.rename(columns={"sum": "late_count", "count": "total", mcc: "macro_bairro"})
    g["on_time_count"] = g["total"] - g["late_count"]
    g["late_rate"] = (g["late_count"] / g["total"]).replace({_np.inf: _np.nan}).fillna(0.0)
    g = g[["macro_bairro", "late_count", "on_time_count", "late_rate"]]
    return {"data": to_records(g.sort_values("late_rate", ascending=False))}


@router.get("/percentis_by_macro")
def percentis_by_macro(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    if not mcc or not dlc:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/tempo de entrega não encontradas.")
    x = pd.to_numeric(df[dlc], errors="coerce")
    grouped = (
        df.assign(v=x)
        .dropna(subset=["v"]) 
        .groupby(mcc)["v"]
        .agg(mean="mean", p50=lambda s: s.quantile(0.5), p75=lambda s: s.quantile(0.75), p90=lambda s: s.quantile(0.9), count="count")
        .reset_index()
        .rename(columns={mcc: "macro_bairro"})
    )
    return {"data": to_records(grouped.sort_values("p90", ascending=False))}


@router.get("/delivery_by_weekday")
def delivery_by_weekday(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    if not dtc or not dlc:
        raise HTTPException(status_code=400, detail="Colunas de data/tempo de entrega não encontradas.")
    sdt = ensure_datetime(df, dtc)
    weekdays = sdt.dt.dayofweek
    x = pd.to_numeric(df[dlc], errors="coerce")
    tmp = pd.DataFrame({"weekday": weekdays, "v": x}).dropna()
    grouped = tmp.groupby("weekday")["v"].apply(lambda s: list(s.dropna())).reset_index(name="values")
    grouped = grouped.sort_values("weekday")
    return {"data": to_records(grouped)}


@router.get("/avg_delivery_by_hour")
def avg_delivery_by_hour(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    if not dtc or not dlc:
        raise HTTPException(status_code=400, detail="Colunas de data/tempo de entrega não encontradas.")
    sdt = ensure_datetime(df, dtc)
    hours = sdt.dt.hour
    x = pd.to_numeric(df[dlc], errors="coerce")
    tmp = pd.DataFrame({"hour": hours, "v": x}).dropna()
    grouped = tmp.groupby("hour")["v"].mean().reset_index(name="avg_delivery_minutes")
    grouped = grouped.sort_values("hour")
    return {"data": to_records(grouped)}


@router.get("/heatmap_hour_weekday")
def heatmap_hour_weekday(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
    metric: str = Query("avg_delivery_minutes", description="avg_delivery_minutes ou avg_delay"),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    if not dtc or not dlc:
        raise HTTPException(status_code=400, detail="Colunas de data/tempo de entrega não encontradas.")
    sdt = ensure_datetime(df, dtc)
    hours = sdt.dt.hour
    weekdays = sdt.dt.dayofweek
    if metric == "avg_delay":
        if not etc:
            raise HTTPException(status_code=400, detail="metric=avg_delay requer eta_col válido.")
        d = pd.to_numeric(df[dlc], errors="coerce")
        e = pd.to_numeric(df[etc], errors="coerce")
        v = (d - e)
    else:
        v = pd.to_numeric(df[dlc], errors="coerce")
    tmp = pd.DataFrame({"hour": hours, "weekday": weekdays, "v": v}).dropna()
    grouped = tmp.groupby(["hour", "weekday"])["v"].mean().reset_index(name="value")
    grouped = grouped.sort_values(["weekday", "hour"])
    return {"data": to_records(grouped)}


@router.get("/late_rate_by_platform")
def late_rate_by_platform(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    threshold_min: Optional[float] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        delivery_status=None,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        eta_col=eta_col,
        threshold_min=threshold_min,
    )
    if not plc or not dlc or not etc:
        raise HTTPException(status_code=400, detail="Colunas de plataforma/entrega/eta não encontradas.")
    d = pd.to_numeric(df[dlc], errors="coerce")
    e = pd.to_numeric(df[etc], errors="coerce")
    thr = float(threshold_min) if threshold_min is not None else 0.0
    late_mask = (d - e) > thr
    tmp = pd.DataFrame({plc: df[plc].astype(str), "late": late_mask})
    g = tmp.groupby(plc)["late"].agg(["sum", "count"]).reset_index()
    g = g.rename(columns={"sum": "late_count", "count": "total", plc: "platform"})
    g["on_time_count"] = g["total"] - g["late_count"]
    g["late_rate"] = (g["late_count"] / g["total"]).replace({_np.inf: _np.nan}).fillna(0.0)
    g = g[["platform", "late_count", "on_time_count", "late_rate"]]
    return {"data": to_records(g.sort_values("late_rate", ascending=False))}

from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.shared import state, resolve_column, ensure_datetime, to_records


router = APIRouter()


def _apply_global_filters(
    df: pd.DataFrame,
    *,
    start_date: Optional[str],
    end_date: Optional[str],
    platform: Optional[List[str]],
    macro_bairro: Optional[List[str]],
    score_min: Optional[float],
    score_max: Optional[float],
    delivery_status: Optional[str],
    date_col: Optional[str],
    platform_col: Optional[str],
    macro_col: Optional[str],
    delivery_col: Optional[str],
    score_col: Optional[str],
    eta_col: Optional[str],
):
    dtc = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    plc = resolve_column(df, platform_col, "platform")
    mcc = resolve_column(df, macro_col, "macro_bairro")
    dlc = resolve_column(df, delivery_col, "actual_delivery_minutes")
    scc = resolve_column(df, score_col, "satisfacao_nivel")
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

    smin = 1.0 if score_min is None else float(score_min)
    smax = 5.0 if score_max is None else float(score_max)
    if smin > smax:
        raise HTTPException(status_code=422, detail="score_min não pode ser maior que score_max")
    if scc:
        s = pd.to_numeric(df[scc], errors="coerce")
        df = df[(s >= smin) & (s <= smax)]

    if delivery_status:
        if delivery_status not in ("atrasado", "no_prazo"):
            raise HTTPException(status_code=422, detail="delivery_status inválido. Use atrasado|no_prazo")
        if not dlc or not etc:
            raise HTTPException(status_code=400, detail="delivery_status requer delivery_col e eta_col válidos")
        d = pd.to_numeric(df[dlc], errors="coerce")
        e = pd.to_numeric(df[etc], errors="coerce")
        if delivery_status == "atrasado":
            df = df[(d > e)]
        else:
            df = df[(d <= e)]

    return df, dtc, plc, mcc, dlc, scc, etc


@router.get("/kpis")
def satisfaction_kpis(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, scc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        score_min=score_min,
        score_max=score_max,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        score_col=score_col,
        eta_col=eta_col,
    )
    if not scc:
        raise HTTPException(status_code=400, detail="Coluna de satisfação não encontrada.")
    s = pd.to_numeric(df[scc], errors="coerce")
    nivel_medio = float(s.mean()) if len(s) else 0.0
    pct_muito_satisfeitos = float((s >= 4.5).mean() * 100) if len(s) else 0.0
    periodo = None
    if dtc:
        sdt = ensure_datetime(df, dtc)
        if len(sdt):
            periodo = {"min": str(sdt.min()), "max": str(sdt.max())}
    return {
        "nivel_medio": nivel_medio,
        "%muito_satisfeitos": pct_muito_satisfeitos,
        "total_avaliacoes": int(s.count()),
        "periodo": periodo,
    }


@router.get("/by_macro_bairro")
def satisfaction_by_macro_bairro(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, scc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        score_min=score_min,
        score_max=score_max,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        score_col=score_col,
        eta_col=eta_col,
    )
    macro = mcc
    score = scc
    if not macro or not score:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/satisfação não encontradas.")
    grp = df.groupby(macro)[score]
    g_mean = grp.mean()
    g_cnt = grp.count()
    g = (
        pd.concat([g_mean, g_cnt], axis=1)
        .reset_index()
        .rename(columns={score: "avg_satisfacao", 0: "count"})
    )
    g.columns = [macro, "avg_satisfacao", "count"]
    return {"data": to_records(g.sort_values("avg_satisfacao", ascending=False))}


@router.get("/scatter_time_vs_score")
def satisfaction_scatter_time_vs_score(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, scc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        score_min=score_min,
        score_max=score_max,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        score_col=score_col,
        eta_col=eta_col,
    )
    delivery = dlc
    score = scc
    if not delivery or not score:
        raise HTTPException(status_code=400, detail="Colunas de entrega/satisfação não encontradas.")
    order_id_col = resolve_column(df, None, "order_id")
    tmp = pd.DataFrame({
        "order_id": df[order_id_col] if order_id_col and order_id_col in df.columns else None,
        "date": ensure_datetime(df, dtc) if dtc else None,
        "platform": df[plc].astype(str) if plc else None,
        "macro_bairro": df[mcc].astype(str) if mcc else None,
        "delivery_minutes": pd.to_numeric(df[delivery], errors="coerce"),
        "satisfacao": pd.to_numeric(df[score], errors="coerce"),
        "eta_minutes": pd.to_numeric(df[etc], errors="coerce") if etc else None,
    })
    tmp = tmp.dropna(subset=["delivery_minutes", "satisfacao"])
    if "date" in tmp.columns and tmp["date"].notna().any():
        tmp["date"] = tmp["date"].astype("datetime64[ns]").astype(str)
    for c in ["order_id", "platform", "macro_bairro", "eta_minutes"]:
        if c in tmp.columns and tmp[c].isna().all():
            tmp = tmp.drop(columns=[c])
    return {"data": to_records(tmp)}


@router.get("/timeseries")
def satisfaction_timeseries(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
    freq: str = "M",
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, scc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        score_min=score_min,
        score_max=score_max,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        score_col=score_col,
        eta_col=eta_col,
    )
    if not dtc or not scc:
        raise HTTPException(status_code=400, detail="Colunas de data/satisfação não encontradas.")
    sdt = ensure_datetime(df, dtc)
    vals = pd.to_numeric(df[scc], errors="coerce")
    ts_mean = (
        pd.DataFrame({"dt": sdt, "v": vals})
        .dropna()
        .set_index("dt")
        .resample(freq)
        .agg({"v": "mean"})
        .rename(columns={"v": "avg_satisfacao"})
    )
    ts_count = (
        pd.DataFrame({"dt": sdt, "v": vals})
        .dropna()
        .set_index("dt")
        .resample(freq)
        .agg({"v": "count"})
        .rename(columns={"v": "count"})
    )
    ts = pd.concat([ts_mean, ts_count], axis=1).reset_index().rename(columns={"dt": "date"}).fillna(0)
    return {"data": to_records(ts)}


@router.get("/heatmap_platform")
def satisfaction_heatmap_platform(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    delivery_status: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    delivery_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    eta_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, dlc, scc, etc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        score_min=score_min,
        score_max=score_max,
        delivery_status=delivery_status,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        delivery_col=delivery_col,
        score_col=score_col,
        eta_col=eta_col,
    )
    if not plc or not scc:
        raise HTTPException(status_code=400, detail="Colunas de plataforma/satisfação não encontradas.")
    grp = df.groupby(plc)[scc]
    g_mean = grp.mean()
    g_cnt = grp.count()
    g = (
        pd.concat([g_mean, g_cnt], axis=1)
        .reset_index()
        .rename(columns={plc: "platform", scc: "avg_satisfacao", 0: "count"})
    )
    if "count" not in g.columns:
        g.columns = ["platform", "avg_satisfacao", "count"]
    return {"data": to_records(g)}



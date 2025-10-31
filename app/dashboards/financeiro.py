from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.main import state, resolve_column, ensure_datetime, to_records
from app.main import np as _np


router = APIRouter()


@router.get("/kpis")
def finance_kpis(
    total_col: Optional[str] = Query(None),
    pct_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    total_brl_col = resolve_column(df, total_col, "total_brl")
    pct = resolve_column(df, pct_col, "platform_commision_pct")
    receita_total = float(df[total_brl_col].sum()) if total_brl_col else 0.0
    receita_liquida = float((df[total_brl_col] * (1 - pd.to_numeric(df[pct], errors="coerce"))).sum()) if (total_brl_col and pct) else 0.0
    return {
        "receita_total": receita_total,
        "receita_liquida": receita_liquida,
    }


@router.get("/timeseries_revenue")
def finance_timeseries_revenue(
    date_col: Optional[str] = Query(None), total_col: Optional[str] = Query(None), pct_col: Optional[str] = Query(None), freq: str = "D"
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    dt_col = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    pct = resolve_column(df, pct_col, "platform_commision_pct")
    if not dt_col or not total_brl_col:
        raise HTTPException(status_code=400, detail="Colunas de data/total_brl não encontradas.")
    sdt = ensure_datetime(df, dt_col)
    gross = pd.DataFrame({"dt": sdt, "gross": pd.to_numeric(df[total_brl_col], errors="coerce")}).dropna()
    ts_gross = gross.set_index("dt").resample(freq)["gross"].sum()
    if pct and pct in df.columns:
        net_val = pd.to_numeric(df[total_brl_col], errors="coerce") * (1 - pd.to_numeric(df[pct], errors="coerce"))
        net = pd.DataFrame({"dt": sdt, "net": net_val}).dropna()
        ts_net = net.set_index("dt").resample(freq)["net"].sum()
        ts = pd.concat([ts_gross, ts_net], axis=1).reset_index().fillna(0)
    else:
        ts = ts_gross.reset_index()
    ts = ts.rename(columns={"dt": "date"})
    return {"data": to_records(ts)}


@router.get("/margin_by_platform")
def finance_margin_by_platform(
    platform_col: Optional[str] = Query(None), total_col: Optional[str] = Query(None), pct_col: Optional[str] = Query(None)
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    platform = resolve_column(df, platform_col, "platform")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    pct = resolve_column(df, pct_col, "platform_commision_pct")
    if not platform or not total_brl_col or not pct:
        raise HTTPException(status_code=400, detail="Colunas de plataforma/total_brl/comissão não encontradas.")
    tmp = df[[platform, total_brl_col, pct]].copy()
    tmp["net"] = pd.to_numeric(tmp[total_brl_col], errors="coerce") * (1 - pd.to_numeric(tmp[pct], errors="coerce"))
    g = tmp.groupby(platform).agg(gross=(total_brl_col, "mean"), net=("net", "mean")).reset_index()
    g["margin_pct"] = (g["net"] / g["gross"]).replace({_np.inf: _np.nan}).fillna(0)
    return {"data": to_records(g.sort_values("margin_pct", ascending=False))}


@router.get("/revenue_by_class")
def finance_revenue_by_class(class_col: Optional[str] = Query(None), total_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not total_brl_col:
        raise HTTPException(status_code=400, detail="Coluna total_brl não encontrada.")

    # fallback automático quando class_col não é informado
    group_col = None
    if class_col and class_col in df.columns:
        group_col = class_col
    else:
        # tenta order_mode, depois platform
        for candidate in ["order_mode", "platform"]:
            cand = resolve_column(df, None, candidate)
            if cand:
                group_col = cand
                break
    if not group_col:
        raise HTTPException(status_code=400, detail="Nenhuma coluna de classe encontrada (tente class_col, order_mode ou platform).")

    g = df.groupby(group_col)[total_brl_col].sum().reset_index(name="revenue")
    return {"data": to_records(g.sort_values("revenue", ascending=False))}


@router.get("/top_clients")
def finance_top_clients(
    client_col: Optional[str] = Query(None), total_col: Optional[str] = Query(None), top_n: int = 10
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    client = resolve_column(df, client_col, "cliente_id")
    if not client:
        # tenta variações comuns de nome de cliente
        for cand in ["cliente_nome", "customer_name", "nome_cliente", "cliente", "user_name"]:
            if cand in df.columns:
                client = cand
                break
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not total_brl_col:
        raise HTTPException(status_code=400, detail="Coluna total_brl não encontrada.")
    if not client:
        # sem coluna de cliente, retorna lista vazia para não quebrar o frontend
        return {"data": []}
    g = df.groupby(client)[total_brl_col].sum().reset_index(name="spent")
    g = g.sort_values("spent", ascending=False).head(top_n)
    return {"data": to_records(g)}



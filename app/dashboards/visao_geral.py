from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import pandas as pd
from app.main import state, resolve_column, ensure_datetime, to_records

router = APIRouter()


@router.get("/kpis")
def overview_kpis(
    date_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    items_col: Optional[str] = Query(None),
    status_col: Optional[str] = Query(None),
    cancel_match: str = Query("cancel", description="Texto para identificar status cancelado (case-insensitive)"),
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
    pedidos_por_dia = None
    if dt_col:
        sdt = ensure_datetime(df, dt_col)
        dmin, dmax = sdt.min(), sdt.max()
        periodo = {"min": dmin, "max": dmax}
        if pd.notna(dmin) and pd.notna(dmax) and total_pedidos > 0:
            num_days = max((dmax - dmin).days + 1, 1)
            pedidos_por_dia = float(total_pedidos / num_days)

    cancelados_pct = None
    st_col = resolve_column(df, status_col, "status")
    if st_col:
        s = df[st_col].astype(str)
        cancelados = s.str.contains(cancel_match, case=False, na=False).sum()
        cancelados_pct = float((cancelados / total_pedidos) * 100) if total_pedidos > 0 else 0.0

    payload = {
        "total_pedidos": total_pedidos,
        "receita_total": receita_total,
        "ticket_medio": ticket_medio,
        "cancelados_pct": cancelados_pct,
        "pedidos_por_dia": pedidos_por_dia,
        "periodo": periodo,
    }

    return {"data": payload}


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


@router.get("/timeseries_revenue_with_orders")
def overview_timeseries_revenue_with_orders(
    date_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    freq: str = Query("M", description="D, W, M"),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    dt_col = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not dt_col or not total_brl_col:
        raise HTTPException(status_code=400, detail="Colunas de data/total_brl não encontradas.")
    sdt = ensure_datetime(df, dt_col)
    tmp = pd.DataFrame({
        "dt": sdt,
        "receita_total": pd.to_numeric(df[total_brl_col], errors="coerce")
    })
    tmp["orders"] = 1
    agg = (
        tmp.set_index("dt")
        .resample(freq)
        .agg({"receita_total": "sum", "orders": "sum"})
        .reset_index()
        .rename(columns={"dt": "date"})
        .fillna(0)
    )
    return {"data": to_records(agg)}


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


@router.get("/top_macro_bairros_by_orders")
def overview_top_macro_bairros_by_orders(
    macro_col: Optional[str] = Query(None), top_n: int = Query(5, ge=1, le=50)
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    macro = resolve_column(df, macro_col, "macro_bairro")
    if not macro:
        raise HTTPException(status_code=400, detail="Coluna de macro_bairro não encontrada.")
    g = (
        df.groupby(macro)
        .size()
        .reset_index(name="orders")
        .sort_values("orders", ascending=False)
        .head(top_n)
    )
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


@router.get("/ticket_histogram")
def overview_ticket_histogram(
    total_col: Optional[str] = Query(None),
    items_col: Optional[str] = Query(None),
    bins: int = Query(15, ge=3, le=100),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    total_brl_col = resolve_column(df, total_col, "total_brl")
    num_itens_col = resolve_column(df, items_col, "num_itens")
    if not total_brl_col or not num_itens_col:
        raise HTTPException(status_code=400, detail="Colunas total_brl/num_itens não encontradas.")
    ticket = pd.to_numeric(df[total_brl_col], errors="coerce") / pd.to_numeric(df[num_itens_col], errors="coerce")
    ticket = ticket.replace([pd.NA, pd.NaT], None).dropna()
    if len(ticket) == 0:
        return {"data": []}
    counts, edges = pd.np.histogram(ticket, bins=bins)  # type: ignore[attr-defined]
    payload = [
        {"bin_start": float(edges[i]), "bin_end": float(edges[i+1]), "count": int(counts[i])}
        for i in range(len(counts))
    ]
    return {"data": payload}

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



from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.shared import state, resolve_column, ensure_datetime, to_records
from app.main import np as _np

router = APIRouter()


def _apply_global_filters(
    df: pd.DataFrame,
    *,
    start_date: Optional[str],
    end_date: Optional[str],
    platform: Optional[List[str]],
    macro_bairro: Optional[List[str]],
    classe_pedido: Optional[List[str]],
    score_min: Optional[float],
    score_max: Optional[float],
    date_col: Optional[str],
    platform_col: Optional[str],
    macro_col: Optional[str],
    classe_col: Optional[str],
    score_col: Optional[str],
):
    dtc = resolve_column(df, date_col, "order_datetime") or resolve_column(df, date_col, "order_date")
    plc = resolve_column(df, platform_col, "platform")
    mcc = resolve_column(df, macro_col, "macro_bairro")
    cpc = resolve_column(df, classe_col, "order_mode")
    scc = resolve_column(df, score_col, "satisfacao_nivel")

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

    if classe_pedido is not None and len(classe_pedido) > 0:
        if not cpc:
            raise HTTPException(status_code=400, detail="Coluna de classe_pedido não encontrada para aplicar filtro.")
        df = df[df[cpc].astype(str).isin(classe_pedido)]

    if score_min is not None or score_max is not None:
        smin = 1.0 if score_min is None else float(score_min)
        smax = 5.0 if score_max is None else float(score_max)
        if smin > smax:
            raise HTTPException(status_code=422, detail="score_min não pode ser maior que score_max")
        if scc:
            s = pd.to_numeric(df[scc], errors="coerce")
            df = df[(s >= smin) & (s <= smax)]

    return df, dtc, plc, mcc, cpc


@router.get("/kpis")
def finance_kpis(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    total_col: Optional[str] = Query(None),
    pct_col: Optional[str] = Query(None),
    qty_col: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )

    total_brl_col = resolve_column(df, total_col, "total_brl")

    pct = resolve_column(df, pct_col, "platform_commision_pct")
    if not pct:
        pct = resolve_column(df, pct_col, "platform_commission_pct")

    num_itens_col = resolve_column(df, qty_col, "num_itens")

    receita_total = float(df[total_brl_col].sum()) if total_brl_col else 0.0

    if total_brl_col and pct:
        receita_liquida = float(
            (df[total_brl_col] * (1 - pd.to_numeric(df[pct], errors="coerce"))).sum()
        )
    else:
        receita_liquida = 0.0

    if total_brl_col and num_itens_col:
        total_itens = pd.to_numeric(df[num_itens_col], errors="coerce").sum()
        if total_itens and total_itens > 0:
            ticket_medio = float(receita_total / total_itens)
        else:
            ticket_medio = 0.0
    else:
        ticket_medio = 0.0

    total_pedidos = int(len(df))

    return {
        "receita_total": receita_total,
        "receita_liquida": receita_liquida,
        "ticket_medio": ticket_medio,
        "total_pedidos": total_pedidos,
    }


@router.get("/orders_count")
def finance_orders_count(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
):
    """
    Retorna apenas o número total de pedidos após aplicar os filtros.
    """
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    
    total_pedidos = int(len(df))
    
    return {"total_pedidos": total_pedidos}


@router.get("/timeseries_revenue")
def finance_timeseries_revenue(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    date_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    pct_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    freq: str = "D",
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dt_col, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    total_brl_col = resolve_column(df, total_col, "total_brl")
    pct = resolve_column(df, pct_col, "platform_commision_pct")
    if not pct:
        pct = resolve_column(df, pct_col, "platform_commission_pct")

    if not dt_col or not total_brl_col:
        raise HTTPException(status_code=400, detail="Colunas de data/total_brl não encontradas.")

    sdt = ensure_datetime(df, dt_col)

    gross = pd.DataFrame(
        {"dt": sdt, "gross": pd.to_numeric(df[total_brl_col], errors="coerce")}
    ).dropna()
    ts_gross = gross.set_index("dt").resample(freq)["gross"].sum()

    if pct and pct in df.columns:
        net_val = pd.to_numeric(df[total_brl_col], errors="coerce") * (
            1 - pd.to_numeric(df[pct], errors="coerce")
        )
        net = pd.DataFrame({"dt": sdt, "net": net_val}).dropna()
        ts_net = net.set_index("dt").resample(freq)["net"].sum()
        ts = pd.concat([ts_gross, ts_net], axis=1).reset_index().fillna(0)
    else:
        ts = ts_gross.reset_index()

    ts = ts.rename(columns={"dt": "date"})
    return {"data": to_records(ts)}


@router.get("/margin_by_platform")
def finance_margin_by_platform(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    platform_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    pct_col: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    platform_col_resolved = resolve_column(df, platform_col, "platform")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    pct = resolve_column(df, pct_col, "platform_commision_pct")
    if not pct:
        pct = resolve_column(df, pct_col, "platform_commission_pct")

    if not platform_col_resolved or not total_brl_col or not pct:
        raise HTTPException(status_code=400, detail="Colunas de plataforma/total_brl/comissão não encontradas.")

    tmp = df[[platform_col_resolved, total_brl_col, pct]].copy()
    tmp["net"] = pd.to_numeric(tmp[total_brl_col], errors="coerce") * (
        1 - pd.to_numeric(tmp[pct], errors="coerce")
    )

    g = (
        tmp.groupby(platform_col_resolved)
        .agg(gross=(total_brl_col, "mean"), net=("net", "mean"))
        .reset_index()
    )
    g["margin_pct"] = (g["net"] / g["gross"]).replace({_np.inf: _np.nan}).fillna(0)
    return {"data": to_records(g.sort_values("margin_pct", ascending=False))}


@router.get("/revenue_by_class")
def finance_revenue_by_class(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    class_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not total_brl_col:
        raise HTTPException(status_code=400, detail="Coluna total_brl não encontrada.")

    group_col = None
    if class_col and class_col in df.columns:
        group_col = class_col
    else:
        for candidate in ["order_mode", "platform"]:
            cand = resolve_column(df, None, candidate)
            if cand:
                group_col = cand
                break
    if not group_col:
        raise HTTPException(
            status_code=400,
            detail="Nenhuma coluna de classe encontrada (tente class_col, order_mode ou platform).",
        )

    g = df.groupby(group_col)[total_brl_col].sum().reset_index(name="revenue")
    return {"data": to_records(g.sort_values("revenue", ascending=False))}


@router.get("/top_clients")
def finance_top_clients(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    client_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    top_n: int = 10,
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    client = resolve_column(df, client_col, "cliente_id")
    if not client:
        for cand in ["cliente_nome", "customer_name", "nome_cliente", "cliente", "user_name"]:
            if cand in df.columns:
                client = cand
                break
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not total_brl_col:
        raise HTTPException(status_code=400, detail="Coluna total_brl não encontrada.")
    if not client:
        return {"data": []}

    g = df.groupby(client)[total_brl_col].sum().reset_index(name="spent")
    g = g.sort_values("spent", ascending=False).head(top_n)
    return {"data": to_records(g)}


@router.get("/revenue_by_platform")
def finance_revenue_by_platform(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    platform_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    pct_col: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
):
    """
    Retorna receita total (bruta e líquida) agrupada por plataforma.
    """
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    platform_col_resolved = resolve_column(df, platform_col, "platform")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    pct = resolve_column(df, pct_col, "platform_commision_pct")
    if not pct:
        pct = resolve_column(df, pct_col, "platform_commission_pct")

    if not platform_col_resolved or not total_brl_col:
        raise HTTPException(status_code=400, detail="Colunas de plataforma/total_brl não encontradas.")

    g = df.groupby(platform_col_resolved)[total_brl_col].sum().reset_index(name="receita_bruta")
    
    if pct and pct in df.columns:
        df_with_net = df.copy()
        df_with_net["receita_liq"] = pd.to_numeric(df_with_net[total_brl_col], errors="coerce") * (
            1 - pd.to_numeric(df_with_net[pct], errors="coerce")
        )
        g_net = df_with_net.groupby(platform_col_resolved)["receita_liq"].sum().reset_index(name="receita_liquida")
        g = g.merge(g_net, on=platform_col_resolved, how="left")
        g["receita_liquida"] = g["receita_liquida"].fillna(0.0)
    else:
        g["receita_liquida"] = g["receita_bruta"]

    g = g.rename(columns={platform_col_resolved: "platform"})
    return {"data": to_records(g.sort_values("receita_bruta", ascending=False))}


@router.get("/revenue_by_macro_bairro")
def finance_revenue_by_macro_bairro(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    macro_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    pct_col: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
    top_n: Optional[int] = Query(None, ge=1, description="Limitar a top N bairros (opcional)"),
):
    """
    Retorna receita total (bruta e líquida) agrupada por macro_bairro.
    """
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    macro = resolve_column(df, macro_col, "macro_bairro")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    pct = resolve_column(df, pct_col, "platform_commision_pct")
    if not pct:
        pct = resolve_column(df, pct_col, "platform_commission_pct")

    if not macro or not total_brl_col:
        raise HTTPException(status_code=400, detail="Colunas de macro_bairro/total_brl não encontradas.")

    g = df.groupby(macro)[total_brl_col].sum().reset_index(name="receita_bruta")
    
    if pct and pct in df.columns:
        df_with_net = df.copy()
        df_with_net["receita_liq"] = pd.to_numeric(df_with_net[total_brl_col], errors="coerce") * (
            1 - pd.to_numeric(df_with_net[pct], errors="coerce")
        )
        g_net = df_with_net.groupby(macro)["receita_liq"].sum().reset_index(name="receita_liquida")
        g = g.merge(g_net, on=macro, how="left")
        g["receita_liquida"] = g["receita_liquida"].fillna(0.0)
    else:
        g["receita_liquida"] = g["receita_bruta"]

    g = g.rename(columns={macro: "macro_bairro"})
    g = g.sort_values("receita_bruta", ascending=False)
    
    if top_n:
        g = g.head(top_n)
    
    return {"data": to_records(g)}


@router.get("/revenue_by_item_class_barplot")
def finance_revenue_by_item_class_barplot(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platform: Optional[List[str]] = Query(None),
    macro_bairro: Optional[List[str]] = Query(None),
    classe_pedido: Optional[List[str]] = Query(None),
    score_min: Optional[float] = Query(None, ge=1, le=5),
    score_max: Optional[float] = Query(None, ge=1, le=5),
    class_col: Optional[str] = Query(None),
    total_col: Optional[str] = Query(None),
    items_col: Optional[str] = Query(None),
    date_col: Optional[str] = Query(None),
    platform_col: Optional[str] = Query(None),
    macro_col: Optional[str] = Query(None),
    classe_col: Optional[str] = Query(None),
    score_col: Optional[str] = Query(None),
):
    """
    Retorna receita agrupada por classe de item (individual, combo, família) para barplot.
    Se não encontrar coluna explícita, categoriza automaticamente por num_itens:
    - individual: 1 item
    - combo: 2-3 itens
    - família: 4+ itens
    """
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df.copy()
    df, dtc, plc, mcc, cpc = _apply_global_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        platform=platform,
        macro_bairro=macro_bairro,
        classe_pedido=classe_pedido,
        score_min=score_min,
        score_max=score_max,
        date_col=date_col,
        platform_col=platform_col,
        macro_col=macro_col,
        classe_col=classe_col,
        score_col=score_col,
    )
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not total_brl_col:
        raise HTTPException(status_code=400, detail="Coluna total_brl não encontrada.")

    group_col = None
    class_mapping = {}
    
    if class_col and class_col in df.columns:
        group_col = class_col
        s = df[class_col].astype(str).str.lower()
        for val in ["individual", "1", "single", "simples"]:
            class_mapping[val] = "individual"
        for val in ["combo", "2", "duo", "par"]:
            class_mapping[val] = "combo"
        for val in ["família", "familia", "family", "4+", "4"]:
            class_mapping[val] = "família"
    else:
        items_col_resolved = resolve_column(df, items_col, "num_itens")
        if not items_col_resolved:
            raise HTTPException(
                status_code=400,
                detail="Coluna num_itens não encontrada para categorização automática.",
            )
        items = pd.to_numeric(df[items_col_resolved], errors="coerce").fillna(0)
        
        def categorize(num):
            if num <= 1:
                return "individual"
            elif num <= 3:
                return "combo"
            else:
                return "família"
        
        df = df.copy()
        group_col = "_item_class"
        df[group_col] = items.apply(categorize)

    g = df.groupby(group_col)[total_brl_col].sum().reset_index(name="revenue")
    
    if class_mapping:
        g[group_col] = g[group_col].astype(str).str.lower()
        for k, v in class_mapping.items():
            g[group_col] = g[group_col].replace(k, v)
    
    order_map = {"individual": 1, "combo": 2, "família": 3, "familia": 3}
    g["_sort"] = g[group_col].map(order_map).fillna(99)
    g = g.sort_values("_sort").drop("_sort", axis=1)
    
    g = g.rename(columns={group_col: "classe"})
    return {"data": to_records(g)}

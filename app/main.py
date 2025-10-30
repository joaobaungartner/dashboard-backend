from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from typing import List, Optional
from pathlib import Path
import os
import pandas as pd
import numpy as np

# Core/shared state and helpers (previously in core.py)
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

from app.dashboards.desempenho_operacional import router as ops_router
from app.dashboards.visao_geral import router as overview_router

app = FastAPI(
    title="Kaiserhaus Data API",
    version="1.0.0",
    default_response_class=ORJSONResponse
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8001",
        "http://127.0.0.1:8001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def require_columns(df: pd.DataFrame, columns: List[str]):
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Colunas ausentes: {missing}")

def filter_df(df: pd.DataFrame, q: Optional[str]) -> pd.DataFrame:
    if not q:
        return df
    mask = pd.Series([False] * len(df))
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in str_cols:
        mask = mask | df[c].astype(str).str.contains(q, case=False, na=False)
    return df[mask]

def select_columns(df: pd.DataFrame, columns: Optional[List[str]]) -> pd.DataFrame:
    if not columns:
        return df
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Colunas inválidas: {missing}")
    return df[columns]

# Routers
app.include_router(ops_router, prefix="/api/dashboard/ops", tags=["ops"])
app.include_router(overview_router, prefix="/api/dashboard/overview", tags=["overview"])

@app.on_event("startup")
def on_startup():
    df = load_excel(state.source_path)
    state.df = df
    state.cols = list(df.columns)
    state.dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
    state.total_rows = len(df)
    print(f"[startup] Carregado: {state.source_path} com {state.total_rows} linhas.")

@app.get("/api/health")
def health():
    return {"status": "ok", "rows": state.total_rows, "file": str(state.source_path.name)}

@app.get("/api/columns")
def columns():
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    return {"columns": state.cols, "dtypes": state.dtypes}

@app.get("/api/count")
def count(q: Optional[str] = Query(None, description="Busca simples em colunas textuais")):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df_f = filter_df(state.df, q)
    return {"count": len(df_f)}

@app.get("/api/data")
def data(
    q: Optional[str] = Query(None, description="Busca simples em colunas textuais"),
    columns: Optional[List[str]] = Query(None, description="Ex.: columns=colA&columns=colB"),
    sort: Optional[str] = Query(None, description="Nome da coluna para ordenar"),
    order: int = Query(-1, description="1 crescente, -1 decrescente"),
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=5000),
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")

    df = filter_df(state.df, q)
    total = len(df)

    df = select_columns(df, columns)

    if sort:
        if sort not in df.columns:
            raise HTTPException(status_code=400, detail=f"Coluna de ordenação inválida: {sort}")
        ascending = True if order == 1 else False
        df = df.sort_values(by=sort, ascending=ascending, kind="mergesort")

    end = offset + limit
    df_page = df.iloc[offset:end]

    data_json = df_page.to_dict(orient="records")

    return {
        "meta": {
            "total": total,
            "returned": len(data_json),
            "offset": offset,
            "limit": limit,
            "columns": list(df_page.columns),
            "sorted_by": sort,
            "order": "asc" if order == 1 else "desc" if sort else None
        },
        "data": data_json
    }

@app.get("/api/feature/{column}/summary")
def feature_summary(column: str):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    if column not in state.df.columns:
        raise HTTPException(status_code=400, detail=f"Coluna inválida: {column}")

    s = state.df[column]
    if pd.api.types.is_numeric_dtype(s):
        desc = s.describe().to_dict()
        return {"column": column, "type": "numeric", "summary": desc}
    else:
        counts = s.astype(str).value_counts(dropna=False).head(20).to_dict()
        return {"column": column, "type": "categorical", "top_counts": counts}



@app.get("/api/dashboard/finance/kpis")
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

@app.get("/api/dashboard/finance/timeseries_revenue")
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

@app.get("/api/dashboard/finance/margin_by_platform")
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
    g["margin_pct"] = (g["net"] / g["gross"]).replace({np.inf: np.nan}).fillna(0)
    return {"data": to_records(g.sort_values("margin_pct", ascending=False))}

@app.get("/api/dashboard/finance/revenue_by_class")
def finance_revenue_by_class(class_col: str = Query(...), total_col: Optional[str] = Query(None)):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not total_brl_col or class_col not in df.columns:
        raise HTTPException(status_code=400, detail="Colunas inválidas para receita por classe.")
    g = df.groupby(class_col)[total_brl_col].sum().reset_index(name="revenue")
    return {"data": to_records(g.sort_values("revenue", ascending=False))}

@app.get("/api/dashboard/finance/top_clients")
def finance_top_clients(
    client_col: Optional[str] = Query(None), total_col: Optional[str] = Query(None), top_n: int = 10
):
    if state.df is None:
        raise HTTPException(status_code=500, detail="DataFrame não carregado.")
    df = state.df
    client = resolve_column(df, client_col, "cliente_id")
    total_brl_col = resolve_column(df, total_col, "total_brl")
    if not client or not total_brl_col:
        raise HTTPException(status_code=400, detail="Colunas de cliente/total_brl não encontradas.")
    g = df.groupby(client)[total_brl_col].sum().reset_index(name="spent")
    g = g.sort_values("spent", ascending=False).head(top_n)
    return {"data": to_records(g)}

@app.get("/api/dashboard/satisfaction/kpis")
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

@app.get("/api/dashboard/satisfaction/by_macro_bairro")
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

@app.get("/api/dashboard/satisfaction/scatter_time_vs_score")
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

@app.get("/api/dashboard/satisfaction/timeseries")
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

@app.get("/api/dashboard/satisfaction/heatmap_platform")
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8001, reload=True)

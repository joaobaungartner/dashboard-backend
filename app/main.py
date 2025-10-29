from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from typing import List, Optional
from pathlib import Path
import pandas as pd
import os

EXCEL_FILE = os.getenv("EXCEL_FILE", "Base_Kaiserhaus.xlsx")
DATA_PATH = Path(__file__).resolve().parents[1] / "data" / EXCEL_FILE

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8001, reload=True)

# Dashboard Backend - Kaiserhaus Data API

Backend FastAPI para análise e visualização de dados do Kaiserhaus. Fornece endpoints RESTful para dashboards de análise de pedidos, desempenho operacional, análise financeira e satisfação do cliente.

## 📋 Requisitos

- Python 3.8 ou superior
- Arquivo Excel com os dados: `Base_Kaiserhaus.xlsx` na pasta `data/`

## 🚀 Instalação

1. Clone o repositório:
```bash
git clone <url-do-repositorio>
cd dashboard-backend
```

2. Instale as dependências:
```bash
pip install fastapi uvicorn pandas numpy openpyxl orjson
```

Ou crie um arquivo `requirements.txt` com:
```
fastapi
uvicorn
pandas
numpy
openpyxl
orjson
```

E instale com:
```bash
pip install -r requirements.txt
```

## 📁 Estrutura do Projeto

```
dashboard-backend/
├── app/
│   ├── main.py                 # Aplicação FastAPI principal
│   ├── shared.py               # Utilitários compartilhados
│   └── dashboards/
│       ├── visao_geral.py      # Dashboard: Visão Geral
│       ├── desempenho_operacional.py  # Dashboard: Desempenho Operacional
│       ├── financeiro.py       # Dashboard: Análise Financeira
│       ├── satisfacao.py       # Dashboard: Satisfação do Cliente
│       └── meta.py             # Endpoints de metadados
├── data/
│   └── Base_Kaiserhaus.xlsx   # Arquivo de dados (deve estar aqui)
└── README.md
```

## 🏃 Como Executar

### Opção 1: Executar diretamente com Python
```bash
python -m app.main
```

### Opção 2: Executar com uvicorn
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

### Opção 3: Executar via arquivo main.py
```bash
python app/main.py
```

O servidor estará disponível em: `http://localhost:8001`

## 📚 Documentação da API

Após iniciar o servidor, acesse:

- **Swagger UI (interativo)**: http://localhost:8001/docs
- **ReDoc (documentação)**: http://localhost:8001/redoc
- **OpenAPI JSON**: http://localhost:8001/openapi.json

## 🔧 Variáveis de Ambiente

Você pode configurar o arquivo Excel através da variável de ambiente:

```bash
export EXCEL_FILE=Base_Kaiserhaus.xlsx
```

Por padrão, o sistema procura por `Base_Kaiserhaus.xlsx` na pasta `data/`.

## 📡 Principais Endpoints

### Health Check
- `GET /api/health` - Status da aplicação

### Metadados
- `GET /api/columns` - Lista todas as colunas do dataset
- `GET /api/count` - Contagem total de registros
- `GET /api/data` - Dados paginados com filtros

### Dashboards

#### Visão Geral (`/api/dashboard/overview`)
- `GET /api/dashboard/overview/kpis` - KPIs principais
- `GET /api/dashboard/overview/timeseries_orders` - Série temporal de pedidos
- `GET /api/dashboard/overview/timeseries_revenue_with_orders` - Receita e pedidos ao longo do tempo
- `GET /api/dashboard/overview/by_platform` - Distribuição por plataforma
- `GET /api/dashboard/overview/top_macro_bairros_by_orders` - Top bairros por pedidos
- `GET /api/dashboard/overview/status_distribution` - Distribuição de status
- `GET /api/dashboard/overview/ticket_histogram` - Histograma de ticket médio
- `GET /api/dashboard/overview/macro_bairro_avg_receita` - Receita média por bairro
- `GET /api/dashboard/overview/macro_bairro_choropleth` - Dados para mapa choropleth

#### Desempenho Operacional (`/api/dashboard/ops`)
- `GET /api/dashboard/ops/kpis` - KPIs operacionais
- `GET /api/dashboard/ops/timeseries_delivery` - Tempo de entrega ao longo do tempo
- `GET /api/dashboard/ops/boxplot_delivery_by_macro` - Boxplot de entrega por bairro
- `GET /api/dashboard/ops/heatmap_delay_by_macro` - Heatmap de atrasos por bairro
- `GET /api/dashboard/ops/scatter_distance_vs_delivery` - Scatter plot distância vs entrega
- `GET /api/dashboard/ops/orders_by_hour` - Pedidos por hora
- `GET /api/dashboard/ops/late_rate_by_macro` - Taxa de atraso por bairro
- `GET /api/dashboard/ops/percentis_by_macro` - Percentis de entrega por bairro
- `GET /api/dashboard/ops/delivery_by_weekday` - Entrega por dia da semana
- `GET /api/dashboard/ops/avg_delivery_by_hour` - Entrega média por hora
- `GET /api/dashboard/ops/heatmap_hour_weekday` - Heatmap hora x dia da semana
- `GET /api/dashboard/ops/late_rate_by_platform` - Taxa de atraso por plataforma

#### Análise Financeira (`/api/dashboard/finance`)
- `GET /api/dashboard/finance/kpis` - KPIs financeiros
- `GET /api/dashboard/finance/orders_count` - Contagem de pedidos
- `GET /api/dashboard/finance/timeseries_revenue` - Série temporal de receita
- `GET /api/dashboard/finance/margin_by_platform` - Margem por plataforma
- `GET /api/dashboard/finance/revenue_by_class` - Receita por classe
- `GET /api/dashboard/finance/top_clients` - Top clientes
- `GET /api/dashboard/finance/revenue_by_platform` - Receita por plataforma
- `GET /api/dashboard/finance/revenue_by_macro_bairro` - Receita por bairro
- `GET /api/dashboard/finance/revenue_by_item_class_barplot` - Receita por classe de item

#### Satisfação do Cliente (`/api/dashboard/satisfaction`)
- `GET /api/dashboard/satisfaction/kpis` - KPIs de satisfação
- `GET /api/dashboard/satisfaction/by_macro_bairro` - Satisfação por bairro
- `GET /api/dashboard/satisfaction/scatter_time_vs_score` - Scatter tempo vs score
- `GET /api/dashboard/satisfaction/timeseries` - Série temporal de satisfação
- `GET /api/dashboard/satisfaction/heatmap_platform` - Heatmap por plataforma

#### Metadados (`/api/dashboard/meta`)
- `GET /api/dashboard/meta/platforms` - Lista de plataformas disponíveis
- `GET /api/dashboard/meta/macros` - Lista de macro-bairros disponíveis
- `GET /api/dashboard/meta/date_range` - Período de dados disponível

## 🔍 Filtros Globais

A maioria dos endpoints aceita os seguintes filtros opcionais como query parameters:

- `start_date` - Data inicial (formato: yyyy-mm-dd)
- `end_date` - Data final (formato: yyyy-mm-dd)
- `platform` - Lista de plataformas (pode passar múltiplos valores)
- `macro_bairro` - Lista de macro-bairros (pode passar múltiplos valores)
- `classe_pedido` - Lista de classes de pedido (pode passar múltiplos valores)
- `score_min` - Score mínimo de satisfação (1-5)
- `score_max` - Score máximo de satisfação (1-5)
- `delivery_status` - Status de entrega ("atrasado" ou "no_prazo") - apenas em endpoints operacionais

### Exemplo de uso com filtros:
```
GET /api/dashboard/overview/kpis?start_date=2024-01-01&end_date=2024-12-31&platform=iFood&macro_bairro=Brooklin
```

## 🛠️ Resolução Automática de Colunas

O sistema resolve automaticamente nomes de colunas através de aliases, permitindo flexibilidade nos nomes das colunas do Excel. Os principais aliases suportados:

- **order_id**: `order_id`, `id_pedido`, `pedido_id`, `id`
- **order_datetime**: `order_datetime`, `data_pedido`, `created_at`, `order_date`
- **platform**: `platform`, `plataforma`
- **macro_bairro**: `macro_bairro`, `macro_bairros`, `macro_bairro_nome`
- **total_brl**: `total_brl`, `valor_total`, `total`
- **num_itens**: `num_itens`, `qtd_itens`, `items_count`
- E muitos outros...

## 🔒 CORS

A API está configurada para aceitar requisições dos seguintes origens:
- `http://localhost:5173`
- `http://127.0.0.1:5173`
- `http://localhost:8001`
- `http://127.0.0.1:8001`

## 📝 Notas

- O arquivo Excel é carregado automaticamente na inicialização da aplicação
- Todos os endpoints retornam dados em formato JSON
- A API usa `ORJSONResponse` para melhor performance
- Os dados são carregados em memória para acesso rápido

## 🐛 Troubleshooting

### Erro: "DataFrame não carregado"
- Verifique se o arquivo `Base_Kaiserhaus.xlsx` existe na pasta `data/`
- Verifique se o arquivo Excel está acessível e não está corrompido

### Erro: "Coluna não encontrada"
- O sistema tenta resolver colunas automaticamente, mas se falhar, você pode especificar o nome exato da coluna através dos parâmetros `*_col` (ex: `date_col`, `platform_col`)

### Erro de porta já em uso
- Altere a porta no comando uvicorn: `--port 8002`
- Ou altere no arquivo `app/main.py` linha 150

## 📄 Licença

Este projeto é privado e de uso interno.


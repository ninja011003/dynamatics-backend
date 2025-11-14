# Dynamatics Backend

Dynamatics is **FlowGraph Analytics Builder**. This repo provides APIs to handles
**flow execution** using a powerful **graph executor** capable of processing arbitrary
flows with multiple inputs and outputs. In addition to that, it also has utility
APIs for **dashboard and flow related operations**

> The executor uses **topological sorting** to ensure nodes are executed in the
> correct order and is generic enough to handle **any node type**.

![alt text](dynamatics.png)


## Features

- **Flow Execution API**: Execute arbitrary flows with multiple nodes, inputs, and outputs.
- **Flow Metadata API**: Get column metadata for each node without executing data operations (using PseudoRunner).
- **Dashboard API**: Fetch aggregated data for frontend visualizations.
- **Generic Node Executor**:
  - Handles any type of data transformation or visualization node.
  - Uses topological sorting to respect dependencies between nodes.
  - Supports multiple inputs and outputs per node.
- **PseudoRunner**: Lightweight metadata extraction that determines output columns for each node without processing actual data.

## Tech Stack

- **Backend Framework**: Python (3.12.7) + FastAPI
- **Data Processing / Execution**: Pandas
- **Database**: MongoDB

## Installation

1. Clone the repository

```bash
git clone https://github.com/your-org/dynamatics-backend.git
cd dynamatics-backend
```

2. Setup virtualenv and install dependencies (this project uses [https://docs.astral.sh/uv/](uv))

```bash
uv venv .
source .venv/bin/activate

uv sync
```

3. Run locally

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Server should now be running at http://localhost:8000

## API Endpoints

### Flow Execution
- `POST /api/flows/execute` - Execute a flow and return data/metadata
- `POST /api/flows/metadata/execute` - Get column metadata for each node without processing data (fast)

### Flow Management
- `POST /api/flows` - Create a new flow
- `GET /api/flows` - Get all flows
- `GET /api/flows/{flow_uid}` - Get a specific flow
- `PUT /api/flows/{flow_uid}` - Update a flow
- `DELETE /api/flows/{flow_uid}` - Delete a flow

### Dataset Metadata
- `GET /api/flows/metadata/{dataset_name}` - Get column metadata for a dataset

## PseudoRunner

The PseudoRunner is a lightweight tool that analyzes flow graphs to determine what columns will be available at each node **without executing any data operations**. This is significantly faster than running the full flow when you only need metadata.

### Use Cases
- Preview available columns for UI dropdowns and field selectors
- Validate flow configurations before execution
- Quick schema discovery for flow design
- Debugging column transformations in complex flows

### Example Usage

```python
from flow_graph.pseudorunner import PseudoRunner

flow = {
    "nodes": [
        {
            "id": "node1",
            "type": "datasource",
            "config": {"input": [{"date": "2024-01-01", "sales": 100}]}
        },
        {
            "id": "node2",
            "type": "group",
            "config": {
                "group_by": ["date"],
                "aggregations": ["sum", "mean"],
                "fields": ["sales"]
            }
        }
    ],
    "edges": [{"source": "node1", "target": "node2"}]
}

runner = PseudoRunner(flow)
metadata = runner.execute()

# Returns:
# {
#   "node1": ["date", "sales"],
#   "node2": ["date", "sales_sum", "sales_mean"]
# }
```

### API Example

```bash
curl -X POST http://localhost:8000/api/flows/metadata/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_graph": {
      "nodes": [...],
      "edges": [...]
    }
  }'
```

## Available Nodes

### Data Transformation Nodes

| Node      | Description                           | Column Behavior                                   |
| --------- | ------------------------------------- | ------------------------------------------------- |
| DataSource| Load data from mock files or inline   | Columns from input data (nested keys flattened)   |
| Filter    | Filter rows based on conditions       | Preserves input columns                           |
| Sort      | Sort rows ascending/descending        | Preserves input columns                           |
| Merge     | Combine data from multiple nodes      | Combines columns with suffixes for duplicates     |
| Group     | Group data and calculate aggregates   | Group keys + aggregated columns (e.g., sales_sum) |
| Forecast  | Time series forecasting               | Creates new columns (date, forecast, source)      |

### Visualization/Export Nodes

| Node       | Description                                                         |
| ---------- | ------------------------------------------------------------------- |
| Export     | Export data (preserves columns)                                     |
| Bar Chart  | Render bar charts                                                   |
| Line Chart | Render trends over time                                             |
| Pie Chart  | Show proportion of categories                                       |
| Area Chart | Show trends with filled areas                                       |

import json
import os
import pandas as pd
from typing import Dict, List, Any

from flow_graph.parser import Parser


class PseudoRunner:
    
    def __init__(self, flow_graph_dict: dict):
        self.raw_data = flow_graph_dict
        self.parser = Parser()
        self.parser.parse(flow_graph_dict)
        self.nodes = self.parser.nodes
        self.req_nodes = self.parser.req_nodes
        self.exec_order = self.parser.topo_sort()
        self.node_metadata = {}
        
    def _infer_type(self, value: Any) -> str:
        if value is None:
            return "str"
        elif isinstance(value, bool):
            return "bool"
        elif isinstance(value, int):
            return "int"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            try:
                pd.to_datetime(value)
                return "timestamp"
            except:
                return "str"
        elif isinstance(value, (list, dict)):
            return "object"
        else:
            return "str"
    
    def get_datasource_columns(self, config: dict) -> Dict[str, str]:
        input_data = config.get("input")
        
        if isinstance(input_data, dict):
            return self._flatten_keys_with_types(input_data)
        elif isinstance(input_data, list) and len(input_data) > 0:
            if isinstance(input_data[0], dict):
                return self._flatten_keys_with_types(input_data[0])
            return {}
        elif isinstance(input_data, str):
            return self._load_mock_data_columns(input_data)
        
        return {}
    
    def _load_mock_data_columns(self, filename: str) -> Dict[str, str]:
        try:
            file_path = os.path.join(
                os.path.dirname(__file__), "mock_data", f"{filename}.ndjson"
            )
            if not os.path.exists(file_path):
                return {}
            
            # Load all data to capture columns from all rows
            df = pd.read_json(file_path, lines=True)
            
            # Collect all columns from all rows
            all_columns = {}
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                row_columns = self._flatten_keys_with_types(row_dict)
                all_columns.update(row_columns)
            
            return all_columns
        except Exception:
            return {}
    
    def _flatten_keys_with_types(self, data: dict, parent_key: str = "", sep: str = ".") -> Dict[str, str]:
        columns = {}
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                columns.update(self._flatten_keys_with_types(v, new_key, sep=sep))
            else:
                columns[new_key] = self._infer_type(v)
        return columns
    
    def get_filter_columns(self, input_columns: Dict[str, str], config: dict) -> Dict[str, str]:
        return input_columns.copy()
    
    def get_sort_columns(self, input_columns: Dict[str, str], config: dict) -> Dict[str, str]:
        return input_columns.copy()
    
    def get_group_columns(self, input_columns: Dict[str, str], config: dict) -> Dict[str, str]:
        group_by = config.get("group_by", [])
        aggregations = config.get("aggregations")
        fields = config.get("fields")
        
        result_columns = {}
        
        for col in group_by:
            if col in input_columns:
                result_columns[col] = input_columns[col]
            else:
                result_columns[col] = "str"
        
        if aggregations and fields:
            for field in fields:
                for agg in aggregations:
                    col_name = f"{field}_{agg}"
                    if agg in ["count"]:
                        result_columns[col_name] = "int"
                    elif agg in ["sum"]:
                        original_type = input_columns.get(field, "float")
                        result_columns[col_name] = original_type if original_type in ["int", "float"] else "float"
                    else:
                        result_columns[col_name] = "float"
        elif aggregations:
            result_columns["<aggregated_columns>"] = "float"
        else:
            result_columns["count"] = "int"
        
        return result_columns
    
    
    def get_merge_columns(
        self, 
        columns1: Dict[str, str], 
        columns2: Dict[str, str], 
        config: dict
    ) -> Dict[str, str]:
        suffixes = config.get("suffixes", ("_x", "_y"))
        on = config.get("on")
        left_on = config.get("left_on")
        right_on = config.get("right_on")
        
        merge_keys = set()
        if on:
            merge_keys.add(on)
        if left_on:
            merge_keys.add(left_on)
        if right_on:
            merge_keys.add(right_on)
        
        result_columns = {}
        cols1_set = set(columns1.keys())
        cols2_set = set(columns2.keys())
        
        for col, typ in columns1.items():
            if col in cols2_set and col not in merge_keys:
                result_columns[f"{col}{suffixes[0]}"] = typ
            else:
                result_columns[col] = typ
        
        for col, typ in columns2.items():
            if col in merge_keys and col in cols1_set:
                continue
            elif col in cols1_set:
                result_columns[f"{col}{suffixes[1]}"] = typ
            else:
                result_columns[col] = typ
        
        return result_columns
    
    def get_forecast_columns(self, input_columns: Dict[str, str], config: dict) -> Dict[str, str]:
        ts_col = config.get("ts_col")
        combine = config.get("combine", False)
        
        result = {}
        if ts_col:
            result[ts_col] = input_columns.get(ts_col, "timestamp")
        
        result["forecast"] = "float"
        
        if combine:
            result["source"] = "str"
        
        return result
    
    def get_anomaly_columns(self, input_columns: Dict[str, str], config: dict) -> Dict[str, str]:

        result = {}
        result["anomaly_score"] = "float"
        result["is_anomaly"] = "bool"
        
        return result
    
    def get_export_columns(self, input_columns: Dict[str, str], config: dict) -> Dict[str, str]:
        return input_columns.copy()
    
    def execute(self):
        # First pass: compute all node outputs
        for node_id in self.exec_order:
            try:
                node = self.nodes[node_id]
                node_type = node.get("type", "export").lower().strip()
                config = node.get("config", {})
                
                columns = {}
                
                if node_type in ["exampledata", "datasource"]:
                    columns = self.get_datasource_columns(config)
                    
                elif node_type == "filter":
                    prev_nodes = self.req_nodes.get(node_id, [])
                    if prev_nodes and prev_nodes[0] in self.node_metadata:
                        prev_columns = self.node_metadata[prev_nodes[0]]
                        columns = self.get_filter_columns(prev_columns, config)
                    
                elif node_type == "sort":
                    prev_nodes = self.req_nodes.get(node_id, [])
                    if prev_nodes and prev_nodes[0] in self.node_metadata:
                        prev_columns = self.node_metadata[prev_nodes[0]]
                        columns = self.get_sort_columns(prev_columns, config)
                    
                elif node_type == "group":
                    prev_nodes = self.req_nodes.get(node_id, [])
                    if prev_nodes and prev_nodes[0] in self.node_metadata:
                        prev_columns = self.node_metadata[prev_nodes[0]]
                        columns = self.get_group_columns(prev_columns, config)
                    
                elif node_type == "merge":
                    prev_nodes = self.req_nodes.get(node_id, [])
                    if len(prev_nodes) >= 2 and prev_nodes[0] in self.node_metadata and prev_nodes[1] in self.node_metadata:
                        columns1 = self.node_metadata[prev_nodes[0]]
                        columns2 = self.node_metadata[prev_nodes[1]]
                        columns = self.get_merge_columns(columns1, columns2, config)
                    
                elif node_type == "forecast":
                    prev_nodes = self.req_nodes.get(node_id, [])
                    if prev_nodes and prev_nodes[0] in self.node_metadata:
                        prev_columns = self.node_metadata[prev_nodes[0]]
                        columns = self.get_forecast_columns(prev_columns, config)

                elif node_type == "anomaly":
                    prev_nodes = self.req_nodes.get(node_id, [])
                    if prev_nodes and prev_nodes[0] in self.node_metadata:
                        prev_columns = self.node_metadata[prev_nodes[0]]
                        columns = self.get_anomaly_columns(prev_columns, config)
                    
                elif node_type in ["export", "linechart", "barchart", "areachart", "piechart"]:
                    prev_nodes = self.req_nodes.get(node_id, [])
                    if prev_nodes and prev_nodes[0] in self.node_metadata:
                        prev_columns = self.node_metadata[prev_nodes[0]]
                        columns = self.get_export_columns(prev_columns, config)
                
                self.node_metadata[node_id] = columns
                
            except Exception:
                continue
        
        # Second pass: yield what each node receives from previous nodes
        for node_id in self.exec_order:
            try:
                prev_nodes = self.req_nodes.get(node_id, [])
                
                # Get input columns from previous node(s)
                input_columns = {}
                if prev_nodes:
                    if len(prev_nodes) == 1 and prev_nodes[0] in self.node_metadata:
                        input_columns = self.node_metadata[prev_nodes[0]]
                    elif len(prev_nodes) >= 2:
                        # For merge nodes, show both inputs
                        # For simplicity, we'll show the first input's columns
                        # (The actual merge logic combines them, but this shows what it receives)
                        if prev_nodes[0] in self.node_metadata:
                            input_columns = self.node_metadata[prev_nodes[0]]
                
                yield json.dumps({"node_id": node_id, "allowed_fields": input_columns}) + "\n"
                
            except Exception:
                continue


if __name__ == "__main__":
    sample_flow = {
        "nodes": [
            {
                "id": "node1",
                "type": "datasource",
                "config": {
                    "input": [
                        {"date": "2024-01-01", "product": "A", "sales": 100, "region": "North"},
                        {"date": "2024-01-02", "product": "B", "sales": 150, "region": "South"}
                    ]
                }
            },
            {
                "id": "node2",
                "type": "filter",
                "config": {
                    "rules": [{"field": "sales", "condition": "gt", "value": 50}]
                }
            },
            {
                "id": "node3",
                "type": "group",
                "config": {
                    "group_by": ["product", "region"],
                    "aggregations": ["sum", "mean", "max"],
                    "fields": ["sales"]
                }
            },
            {
                "id": "node4",
                "type": "export",
                "config": {}
            }
        ],
        "edges": [
            {"source": "node1", "target": "node2"},
            {"source": "node2", "target": "node3"},
            {"source": "node3", "target": "node4"}
        ]
    }
    
    runner = PseudoRunner(sample_flow)
    for output in runner.execute():
        print(output, end='')

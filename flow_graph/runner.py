import math
import json
import pandas as pd

from typing import Any, Dict

from flow_graph.parser import Parser
from flow_graph.merge import Merge
from flow_graph.filter import Filter
from flow_graph.group import Group
from flow_graph.sort import Sort
from flow_graph.data_source import DataSource
from flow_graph.export import Export
from flow_graph.forecast import Forecast

func_map = {
    # always lowercase the key
    "exampledata": DataSource,
    "datasource": DataSource,
    "merge": Merge,
    "filter": Filter,
    "group": Group,
    "sort": Sort,
    "forecast": Forecast,
    "export": Export,
    "linechart": Export,
    "barchart": Export,
    "areachart": Export,
    "piechart": Export,
}

def flatten_json(y: Dict[str, Any], parent_key="", sep=".") -> Dict[str, Any]:
    items = []
    for k, v in y.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(isinstance(i, dict) for i in v):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        else:
            items.append((new_key, v))
    return dict(items)

def serialize_and_flatten(output_data):
    flat_data = []
    for row in output_data:
        flat_row = flatten_json(row)
        # Replace NaN/inf and convert timestamps
        for k, v in flat_row.items():
            if isinstance(v, pd.Timestamp):
                flat_row[k] = v.isoformat()
            elif isinstance(v, float) and (pd.isna(v) or pd.isnull(v) or v == float("inf") or v == float("-inf")):
                flat_row[k] = None
        flat_data.append(flat_row)
    return flat_data


class Runner:
    def __init__(self, flow_graph_dict: dict):
        self.raw_data = flow_graph_dict
        self.parser = Parser()
        self.parser.parse(flow_graph_dict)
        self.nodes = self.parser.nodes
        self.req_nodes = self.parser.req_nodes
        self.exec_order = self.parser.topo_sort()
        self.executed_processes = {}

    def execute(self):
        prev_output = None
        try:
            for node_id in self.exec_order:
                node = self.nodes[node_id]
                type = node.get("type", "export").lower().strip()
                _func = func_map[type]

                cur_process = None

                if _func is DataSource:
                    config = node.get("config", {})
                    cur_process = _func(**config)
                    prev_output = cur_process.output

                elif _func is Merge:
                    df1 = self.executed_processes[self.req_nodes[node_id][0]].output
                    df2 = self.executed_processes[self.req_nodes[node_id][1]].output
                    cur_process = _func(df1, df2)
                    cur_process.run(**node.get("config", {}))

                elif _func is Export:
                    prev_node = self.executed_processes[
                        self.req_nodes.get(node_id, [])[0]
                    ]
                    cur_process = _func(prev_node.output)
                    cur_process.run(**node.get("config", {}))

                    if hasattr(cur_process.output, "to_dict"):
                        df_copy = cur_process.output.copy()
                        if isinstance(df_copy.columns, pd.MultiIndex):
                            df_copy.columns = [
                                "_".join(map(str, col)).strip()
                                for col in df_copy.columns.values
                            ]
                        output_data = df_copy.to_dict(orient="records")
                    else:
                        output_data = cur_process.output

                    output_serializable = serialize_and_flatten(output_data)
                    yield (
                        json.dumps({"node_id": node_id, "output": output_serializable})
                        + "\n"
                    )

                else:
                    prev_node = self.executed_processes[
                        self.req_nodes.get(node_id, [])[0]
                    ]
                    cur_process = _func(prev_node.output)
                    cur_process.run(**node.get("config", {}))

                self.executed_processes[node_id] = cur_process
                prev_output = cur_process.output

                if type not in ["export"]:
                    if hasattr(prev_output, "to_dict"):
                        df_copy = prev_output.copy()
                        if isinstance(df_copy.columns, pd.MultiIndex):
                            df_copy.columns = [
                                "_".join(map(str, col)).strip()
                                for col in df_copy.columns.values
                            ]
                        output_data = df_copy.to_dict(orient="records")
                    else:
                        output_data = prev_output

                    output_serializable = serialize_and_flatten(output_data)
                    yield (
                        json.dumps({"node_id": node_id, "output": output_serializable})
                        + "\n"
                    )

            return prev_output

        except Exception as e:
            print(e)
            return prev_output

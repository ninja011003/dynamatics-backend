from func.parser import Parser
from func.merge import Merge
from func.filter import Filter
from func.group import Group
from func.sort import Sort
from func.data_source import DataSource
from func.export import Export
import json

func_map = {
    # always lowercase the key
    "dataSource": DataSource,
    "merge": Merge,
    "filter": Filter,
    "group": Group,
    "sort": Sort,
    "export": Export,
    "linechart": Export,
    "barchart": Export,
}


import json


class Runner:
    def __init__(self, payload):
        self.nodes = payload.get("nodes", {})
        self.exec_order = payload.get("exec_order", [])
        self.req_nodes = payload.get("req_nodes", {})
        self.executed_processes = {}

    def execute(self):
        prev_output = None

        for node_id in self.exec_order:
            node = self.nodes[node_id]
            type = node_id.split("-")[0].lower()
            _func = func_map[type]

            cur_process = None

            if isinstance(DataSource, _func):
                cur_process = _func(node.get("input"))
                prev_output = cur_process.output

            elif isinstance(Merge, _func):
                df1 = self.executed_processes[self.req_nodes[node_id][0]].output
                df2 = self.executed_processes[self.req_nodes[node_id][1]].output
                cur_process = _func(df1, df2)
                cur_process.run(**node.get("config", {}))

            elif isinstance(Export, _func):
                cur_process = _func(prev_output)
                cur_process.run(**node.get("config", {}))

                yield (
                    json.dumps({"node_id": node_id, "output": cur_process.output})
                    + "\n"
                )

            else:
                cur_process = _func(prev_output)
                cur_process.run(**node.get("config", {}))

            self.executed_processes[node_id] = cur_process
            prev_output = cur_process.output

            if type not in ["export"]:
                yield json.dumps({"node_id": node_id, "output": prev_output}) + "\n"

        return prev_output

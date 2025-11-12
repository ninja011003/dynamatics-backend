import json

from flow_graph.parser import Parser
from flow_graph.merge import Merge
from flow_graph.filter import Filter
from flow_graph.group import Group
from flow_graph.sort import Sort
from flow_graph.data_source import DataSource
from flow_graph.export import Export

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


class Runner:
    def __init__(self, flow_graph_str):
        self.raw_data = json.load(flow_graph_str)
        self.parser = Parser()
        self.parser.parse(flow_graph_str)
        self.nodes = self.parser.nodes
        self.req_nodes = self.parser.req_nodes
        self.exec_order = self.parser.topo_sort()
        self.executed_processes = {}

    def execute(self):
        prev_output = None

        for node_id in self.exec_order:
            node = self.nodes[node_id]
            type = node_id.split("-")[0].lower()
            _func = func_map[type]

            cur_process = None

            if _func is DataSource:
                cur_process = _func(node.get("input"))
                prev_output = cur_process.output

            elif _func is Merge:
                df1 = self.executed_processes[self.req_nodes[node_id][0]].output
                df2 = self.executed_processes[self.req_nodes[node_id][1]].output
                cur_process = _func(df1, df2)
                cur_process.run(**node.get("config", {}))

            elif _func is Export:
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

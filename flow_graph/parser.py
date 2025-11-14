from typing import Dict,List,Any
import json
from collections import defaultdict, deque


class Parser:
    def __init__(self):
        self.nodes ={}
        self.graph = defaultdict(list) # node_id -> [node_id]
        self.req_nodes = defaultdict(list) # node_id -> [node_id]
        self.exe_order = []
    

    def parse(self,flow:Dict):

        # print("\n\nflow : \n\n", flow)

        for node in flow["nodes"]:
            self.nodes[node["id"]] = node
        edges = flow["edges"]
        for edge in edges:
            self.graph[edge["source"]].append(edge["target"])
            self.req_nodes[edge["target"]].append(edge["source"])

    def topo_sort(self):
        in_degree = defaultdict(int)
        
        for source in self.graph.keys():
            for target in self.graph[source]:
                in_degree[target] += 1
        
        queue = deque([node["id"] for node in self.nodes.values() if in_degree[node["id"]] == 0])
    
        self.exe_order = []
        while queue:
            current = queue.popleft()
            self.exe_order.append(current)
            
            if current in self.graph:
                for neighbor in self.graph[current]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
        
        if len(self.exe_order) != len(self.nodes):
            raise Exception("Graph contains a cycle! Cannot determine execution order.")
        
        return self.exe_order
        
        
if __name__ == "__main__":
    parser = Parser()
    parser.parse(open("test_data/test_flow.json","r"))
    print(parser.topo_sort())
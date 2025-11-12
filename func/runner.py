from func.parser import Parser
from func.merge import Merge
from func.filter import Filter
from func.group import Group
from func.sort import Sort
from func.data_source import DataSource
import json

func_map ={
    "dataSource": DataSource,
    "merge": Merge,
    "filter": Filter,
    "group": Group,
    "sort": Sort,
}



class Runner:
    def __init__(self,flow):
        self.raw_data = json.load(flow)
        self.parser = Parser()
        self.parser.parse(flow)
        self.nodes = self.parser.nodes
        self.exec_order = self.parser.topo_sort()
        
    def execute(self):
        pass
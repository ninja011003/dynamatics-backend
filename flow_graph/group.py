import pandas as pd
from typing import List,Dict,Any,Optional


class Group:
    def __init__(self,input : pd.DataFrame):
        self.input = input
        self.output = None
        self.group_by = []
        self.aggregations = []
        
    def run(self, group_by: List[str], aggregations: Optional[List] = None, fields: Optional[List[str]] = None):
        if not group_by:
            raise ValueError("Group by is required")
        self.group_by = group_by
        
        if aggregations and fields:
            agg_dict = {}
            for field in fields:
                agg_dict[field] = aggregations
            self.output = self.input.groupby(group_by).agg(agg_dict).reset_index()
        elif aggregations:
            self.output = self.input.groupby(group_by).agg(aggregations).reset_index()
        else:
            self.output = self.input.groupby(group_by).size().reset_index(name='count')
        return self.output
    
if __name__ == "__main__":
    group = Group(pd.DataFrame({"A": [1,2,2,4,5,5,5,8,9,9], "B": [1,2,3,4,5,6,7,8,9,10], "C": ["a","b","b","a","b","c","a","b","c","a"]}))
    group.run(["A","C"],["sum","max"],["B"])
    # group.run(["A","C"])
    print(group.output)
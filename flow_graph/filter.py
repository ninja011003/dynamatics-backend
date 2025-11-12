import pandas as pd

Filters={
    "gt": lambda x,y : x>y,
    "gte": lambda x,y : x>=y,
    "lt": lambda x,y : x<y,
    "lte": lambda x,y : x<=y,
    "eq": lambda x,y : x==y,
    "neq": lambda x,y : x!=y,
    "in": lambda x,y : x in y,
    "nin": lambda x,y : x not in y,
    "contains": lambda x,y : x in y,
    "ncontains": lambda x,y : x not in y,
    "startswith": lambda x,y : x.startswith(y),
    "nstartswith": lambda x,y : not x.startswith(y),
} 

class Filter:
    def __init__(self,input : pd.DataFrame):
        self.is_completed = False
        self.input = input
        self.output= None
        self.condition = None
        self.value = None
        self.field = None
    

    #TODO add multiple conditions
    def run(self,condition,value,field):
        
        self.condition = condition
        self.value = value
        self.field = field
        
        filter_func = Filters.get(self.condition,None)
        if not filter_func:
            raise ValueError(f"Invalid filter operator: {self.condition}")
        
        mask = filter_func(self.input[self.field],self.value)
        self.output = self.input[mask].reset_index(drop=True)
        return self.output
    
if __name__ == "__main__":
    filter = Filter(pd.DataFrame({"A": [1,2,3,5,3,24,7,8,], "B": [1,2,3,4,5,6,7,8,]}))
    filter.run("gte",8,"A")
    print(filter.output)
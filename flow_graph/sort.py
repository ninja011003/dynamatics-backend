from typing import List,Dict 
import pandas as pd

class Sort:
    def __init__(self,input : pd.DataFrame):
        self.input = input
        self.output = None
        
    def run(self,field,asc:bool = True):
        self.output = self.input.sort_values(by=field, ascending=asc)
        return self.output
    
if __name__ == "__main__":
    sort = Sort(pd.DataFrame({"A": [1,2,3,4,5]}))
    sort.run()
    print(sort.output)
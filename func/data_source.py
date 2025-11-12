from typing import List,Dict
import pandas as pd


class DataSource:
    def __init__(self,input :[Dict,List,pd.DataFrame]):
        if isinstance(input,Dict):
            #TODO conventional column names
            self.input = pd.DataFrame(input)
        elif isinstance(input,List):
            self.input = pd.DataFrame(input)
        elif isinstance(input,pd.DataFrame):
            self.input = input
        else:
            raise ValueError("Invalid input type")
        
    def get_info(self):
        return self.input.info()
    
    def get_shape(self):
        return self.input.shape
    
    def get_columns(self):
        return self.input.columns
    
    def get_head(self,n:int=5):
        return self.input.head(n)
    
    
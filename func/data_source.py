from typing import List,Dict
import pandas as pd
import os


class DataSource:
    def __init__(self,input :[Dict,List,pd.DataFrame,str]):
        if isinstance(input,Dict):
            #TODO conventional column names
            self.output = pd.DataFrame(input)
        elif isinstance(input,List):
            self.output = pd.DataFrame(input)
        elif isinstance(input,pd.DataFrame):
            self.output = input
        elif isinstance(input,str):
            file_path = os.path.join(os.path.dirname(__file__), "mock_data", input + ".ndjson")
            self.output = pd.read_json(file_path, lines=True)
        else:
            raise ValueError("Invalid input type")
        
    def get_info(self):
        return self.output.info()
    
    def get_shape(self):
        return self.output.shape
    
    def get_columns(self):
        return self.output.columns
    
    def get_head(self,n:int=5):
        return self.output.head(n)
    
    
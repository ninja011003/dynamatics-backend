import pandas as pd
class Export:
    def __init__(self,input : pd.DataFrame):
        self.input = input
        self.output = None
        
    def run(self,config : dict = None):
        self.output = self.input
        return self.output
    
if __name__ == "__main__":
    export = Export(pd.DataFrame({"A": [1,2,3,4,5]}))
    export.run()
    print(export.output)
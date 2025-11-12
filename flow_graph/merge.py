import pandas as pd

class Merge:
    def __init__(self,df1: pd.DataFrame, df2: pd.DataFrame, ):
        self.df1 = df1
        self.df2 = df2
        self.output = None
    
    
    def run(self, how='outer', axis=1 , ignore_index : bool = True):
        self.inputs = [self.df1, self.df2]
        self.output = pd.merge(self.df1, self.df2, how=how, axis=axis, ignore_index=ignore_index)
        return self.output
    
if __name__ == "__main__":
    merge = Merge()
    merge.merge(pd.DataFrame({"A": [1,2,3]}), pd.DataFrame({"B": [4,5,6]}))
    print(merge.output) 
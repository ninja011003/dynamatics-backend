import pandas as pd

class Merge:
    def __init__(self):
        self.inputs=[]
        self.output = None
    
    def append(self, dataframe: pd.DataFrame):
        self.inputs.append(dataframe)
    
    def merge(self, *dataframes: pd.DataFrame, how='outer', axis=1 , ignore_index : bool = True):
       
        if not dataframes:
            raise ValueError("At least one DataFrame must be provided")
        for i, df in enumerate(dataframes):
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Argument {i+1} is not a pandas DataFrame")
        self.inputs = dataframes
        if len(self.inputs) == 1:
            self.output = dataframes[0]
            return self.output
        self.output = pd.concat(self.inputs, axis=axis, join=how, ignore_index=ignore_index)
        return self.output
    
if __name__ == "__main__":
    merge = Merge()
    merge.merge(pd.DataFrame({"A": [1,2,3]}), pd.DataFrame({"B": [4,5,6]}))
    print(merge.output) 
import pandas as pd

class Merge:
    def __init__(self, df1: pd.DataFrame, df2: pd.DataFrame):
        self.df1 = df1
        self.df2 = df2
        self.output = None
    
    def run(self, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, suffixes=('_x', '_y')):
        self.inputs = [self.df1, self.df2]
        
        merge_params = {
            'how': how,
            'suffixes': suffixes
        }
        
        if on is not None:
            merge_params['on'] = on
        if left_on is not None:
            merge_params['left_on'] = left_on
        if right_on is not None:
            merge_params['right_on'] = right_on
        if left_index:
            merge_params['left_index'] = left_index
        if right_index:
            merge_params['right_index'] = right_index
            
        self.output = pd.merge(self.df1, self.df2, **merge_params)
        return self.output
    
if __name__ == "__main__":
    merge = Merge(pd.DataFrame({"A": [1,2,3]}), pd.DataFrame({"B": [4,5,6]}))
    merge.run()
    print(merge.output) 
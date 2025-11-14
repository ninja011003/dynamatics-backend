import pandas as pd

Filters = {
    "gt": lambda x, y: x > y,
    "gte": lambda x, y: x >= y,
    "lt": lambda x, y: x < y,
    "lte": lambda x, y: x <= y,
    "eq": lambda x, y: x == y,
    "neq": lambda x, y: x != y,
    "in": lambda x, y: x in y,
    "nin": lambda x, y: x not in y,
    "range": lambda x, low_lim,high_lim: x >= low_lim and x <= high_lim,
    "contains": lambda x, y: y in x if isinstance(x, str) else False,
    "ncontains": lambda x, y: y not in x if isinstance(x, str) else True,
    "startswith": lambda x, y: str(x).startswith(y),
    "nstartswith": lambda x, y: not str(x).startswith(y),
}


class Filter:
    def __init__(self, input: pd.DataFrame):
        self.input = input
        self.output = None

    def run(self, field: str, condition: str, value1, value2=None):
        if not field or not condition:
            return self.input
        
        filter_func = Filters.get(condition)
        if not filter_func:
            raise ValueError(f"Invalid filter condition: {condition}")
        if value2:
            mask = self.input[field].apply(lambda x: filter_func(x, value1, value2))
        else:
            mask = self.input[field].apply(lambda x: filter_func(x, value1))
        self.output = self.input[mask].reset_index(drop=True)
        return self.output


if __name__ == "__main__":
    df = pd.DataFrame({"A": [1, 2, 3, 5, 3, 24, 7, 8], "B": [1, 2, 3, 4, 5, 6, 7, 8]})
    f = Filter(df)
    result = f.run(field="A", condition="range", value1=3, value2=20)
    print(result)

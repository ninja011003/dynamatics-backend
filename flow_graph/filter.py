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
    "contains": lambda x, y: y in x if isinstance(x, str) else False,
    "ncontains": lambda x, y: y not in x if isinstance(x, str) else True,
    "startswith": lambda x, y: str(x).startswith(y),
    "nstartswith": lambda x, y: not str(x).startswith(y),
}


class Filter:
    def __init__(self, input_df: pd.DataFrame):
        self.is_completed = False
        self.input = input_df
        self.output = None
        self.condition = None
        self.value = None
        self.field = None

    def run(self, condition: str, value, field: str):
        self.condition = condition
        self.value = value
        self.field = field

        filter_func = Filters.get(self.condition, None)
        if not filter_func:
            raise ValueError(f"Invalid filter operator: {self.condition}")

        mask = self.input[field].apply(lambda x: filter_func(x, value))

        self.output = self.input[mask].reset_index(drop=True)
        return self.output


if __name__ == "__main__":
    df = pd.DataFrame({"A": [1, 2, 3, 5, 3, 24, 7, 8], "B": [1, 2, 3, 4, 5, 6, 7, 8]})
    filter_obj = Filter(df)
    result = filter_obj.run("gte", 8, "A")
    print(result)

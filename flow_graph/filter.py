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
        self.input = input_df
        self.output = input_df.copy()

    def run(self, rules: list):
        """
        rules: list of dicts
        [
            {"field": "A", "condition": "gte", "value": 5, "operator": "AND"},
            {"field": "B", "condition": "lt", "value": 10, "operator": "OR"},
            ...
        ]
        The first rule's "operator" is ignored (assume AND)
        """
        if not rules:
            return self.input

        mask = pd.Series(True, index=self.input.index)

        for i, rule in enumerate(rules):
            field = rule["field"]
            condition = rule["condition"]
            value = rule["value"]
            operator = rule.get("operator", "AND").upper()

            filter_func = Filters.get(condition)
            if not filter_func:
                raise ValueError(f"Invalid filter operator: {condition}")

            current_mask = self.input[field].apply(lambda x: filter_func(x, value))

            if i == 0:
                mask = current_mask
            else:
                if operator == "AND":
                    mask &= current_mask
                elif operator == "OR":
                    mask |= current_mask
                else:
                    raise ValueError(f"Invalid logical operator: {operator}")

        self.output = self.input[mask].reset_index(drop=True)
        return self.output


if __name__ == "__main__":
    df = pd.DataFrame({"A": [1, 2, 3, 5, 3, 24, 7, 8], "B": [1, 2, 3, 4, 5, 6, 7, 8]})
    f = Filter(df)
    rules = [
        {"field": "A", "condition": "gte", "value": 3, "operator": "AND"},
        {"field": "B", "condition": "lt", "value": 7, "operator": "OR"},
        {"field": "A", "condition": "eq", "value": 24, "operator": "AND"},
    ]
    result = f.run(rules)
    print(result)

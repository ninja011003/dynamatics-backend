import pandas as pd


class Merge:
    def __init__(self, df1: pd.DataFrame, df2: pd.DataFrame):
        self.df1 = df1
        self.df2 = df2
        self.output = None

    def run(
        self,
        how="inner",
        left_on: str = None,
        right_on: str = None,
        left_index: bool = True,
        right_index: bool = True,
        suffixes=("_x", "_y"),
    ):
        self.inputs = [self.df1, self.df2]

        merge_params = {
            "how": how,
            "suffixes": suffixes,
            "left_index": left_index,
            "right_index": right_index,
        }

        if left_on is not None:
            merge_params["left_on"] = left_on
            merge_params["left_index"] = False
        if right_on is not None:
            merge_params["right_on"] = right_on
            merge_params["right_index"] = False

        self.output = pd.merge(self.df1, self.df2, **merge_params)
        return self.output


if __name__ == "__main__":
    merge = Merge(
        pd.DataFrame({"A": [1, 2, 3]}),
        pd.DataFrame(
            {
                "B": [
                    4,
                    5,
                    6,
                    7
                ]
            }
        ),
    )
    # merge.run(how="outer")
    # print(merge.output)
    merge.run(how="outer", left_on="B", right_on="A")
    print(merge.output)
    # merge.run(how="outer", left_on="A", right_index=True)
    # print(merge.output)

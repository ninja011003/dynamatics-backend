import os
import pandas as pd
from typing import Union, List, Dict
from pandas import json_normalize
import json


class DataSource:
    def __init__(self, input: Union[Dict, List, pd.DataFrame, str]):
        if isinstance(input, (Dict, List)):
            self.output = self._load_from_dict_or_list(input)
        elif isinstance(input, pd.DataFrame):
            self.output = input
        elif isinstance(input, str):
            file_path = os.path.join(
                os.path.dirname(__file__), "mock_data", f"{input}.ndjson"
            )
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            data = pd.read_json(file_path, lines=True).head(1)
            self.output = self._load_from_dict_or_list(data.to_dict(orient="records"))
        else:
            raise ValueError("Invalid input type")

    def _load_from_dict_or_list(self, data: Union[Dict, List[Dict]]):
        if isinstance(data, dict):
            data = [data]
        df = json_normalize(data, sep=".", max_level=None)
        return df

    def get_info(self):
        return self.output.info()

    def get_shape(self):
        return self.output.shape

    def get_columns(self):
        return self.output.columns.tolist()

    def get_head(self, n: int = 5):
        return self.output.head(n)

    def to_json(self, orient: str = "records"):
        return self.output.applymap(self._convert_value).to_dict(orient=orient)

    @staticmethod
    def _convert_value(val):
        if isinstance(val, pd.Timestamp):
            return val.isoformat()
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return val

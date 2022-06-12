import csv

from typing import TextIO, IO, List, Tuple, Dict

import pandas as pd


class MetaCSVToPandas:
    def __init__(self, fo: TextIO, sep: str = ","):
        self.fo = fo
        self.sep = sep

    @staticmethod
    def cols_to_meta(header: List[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
        return {col: col.split(":")[0] for col in header}, {
            col.split(":")[0]: col.split(":")[1]
            if col.split(":")[1].lower() != "none"
            else None
            for col in header
            if len(col.split(":")) > 1
        }

    def get_df(self) -> pd.DataFrame:
        col_map, meta = self.cols_to_meta(next(csv.reader(self.fo, delimiter=self.sep)))
        self.fo.seek(0)
        df = pd.read_csv(self.fo, sep=self.sep, dtype=meta)
        df = df.rename(columns=col_map)
        return df


class PandasToMetaCSV:
    def __init__(self, df: pd.DataFrame, sep: str = ","):
        self.df = df
        self.sep = sep

    @staticmethod
    def meta_to_cols(cols: List[str], meta: Dict[str, str]) -> Dict[str, str]:
        return {col: f"{col}:{meta[col]}" if col in meta else col for col in cols}

    def write_file(self, output: IO) -> None:
        self.df.rename(
            columns=self.meta_to_cols(
                list(self.df.columns),
                {key: str(val) for key, val in self.df.dtypes.to_dict().items()},
            )
        ).to_csv(output, index=False, sep=self.sep)

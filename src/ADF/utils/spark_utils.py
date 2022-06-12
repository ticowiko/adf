from typing import Callable, Dict, Any, List, Type

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, array


def outer(
    inner: Callable[[Dict], Any], cols: List[str], cast: Type
) -> Callable[[List], Any]:
    return lambda vals: cast(inner({col: vals[i] for i, col in enumerate(cols)}))


def sdf_apply(
    sdf: DataFrame, target_col: str, inner: Callable[[Dict], Any], cast: Type
):
    cols = sdf.columns
    return sdf.withColumn(
        target_col, udf(outer(inner, cols, cast))(array(*[sdf[col] for col in cols]))
    )

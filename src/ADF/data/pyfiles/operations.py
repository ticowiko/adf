import datetime

from typing import Optional, List, Dict

import pandas as pd

from pyspark import sql as ps
from pyspark.sql.functions import lit

from ADF.utils import concretize
from ADF.config import ADFGlobalConfig
from ADF.components.data_structures import AbstractDataStructure, RawSQLConcretization


def col_compute(ads: AbstractDataStructure) -> AbstractDataStructure:
    ads["col_4"] = ads["col_0"] + "/" + ads["col_2"].as_type(str)
    return ads


def group_op(ads: AbstractDataStructure) -> AbstractDataStructure:
    return ads.group_by(
        ["col_0"],
        outputs={
            "min_col_1": (lambda x: x["col_1"].min(), int),
            "max_col_1": (lambda x: x["col_1"].max(), int),
            "count": (lambda x: x.count(), int),
            "n_col_1": (lambda x: x.distinct(["col_1"]).count(), int),
            "col_1_sum": (lambda x: x["col_1"].as_type(int).sum(), int),
        },
    )


def join_op(ads: AbstractDataStructure) -> AbstractDataStructure:
    return ads.join(
        ads,
        left_on=["col_0"],
        right_on=["col_0"],
        how="inner",
        l_modifier=lambda x: f"l_{x}",
        r_modifier=lambda x: f"r_{x}",
        modify_on=False,
    )


@concretize(pd.DataFrame)
def concrete_op(df: pd.DataFrame, fill_val=999) -> pd.DataFrame:
    return df.fillna(fill_val)


def union_op(ads: AbstractDataStructure) -> AbstractDataStructure:
    return ads.union(ads, ads, all=False).union(ads, all=True)


def distinct_op(ads: AbstractDataStructure) -> AbstractDataStructure:
    return ads.distinct()


def args_combination_op(
    *args: AbstractDataStructure, time_col: str = "TIME"
) -> AbstractDataStructure:
    common_cols = list(set.intersection(*[set(ads.list_columns()) for ads in args]))
    to_concat = [ads[common_cols] for ads in args]
    ret = to_concat[0].union(*to_concat[1:], all=True)
    ret[time_col] = str(datetime.datetime.utcnow())
    return ret


def join_combination_op(
    *args: AbstractDataStructure, join_keys: Optional[List] = None
) -> AbstractDataStructure:
    join_keys = join_keys or ["col_0"]
    ret = args[0]
    for ads in args[1:]:
        ret = ret.join(
            ads,
            join_keys,
            join_keys,
            "inner",
            l_modifier=lambda x: "l_" + x,
            r_modifier=lambda x: "r_" + x,
            modify_on=False,
        )
    return ret


def kwarg_op(
    ads: AbstractDataStructure, null_cols=None, fill_cols=None, fill_val=None
) -> AbstractDataStructure:
    null_cols = null_cols or []
    fill_cols = fill_cols or []
    for col in null_cols:
        ads[col] = None
    for col in fill_cols:
        ads[col] = fill_val
    return ads


def apply_op(
    ads: AbstractDataStructure, input_col=None, output_col=None
) -> AbstractDataStructure:
    if not input_col or not output_col:
        raise ValueError("Need input and output col")
    return ads.apply(output_col, lambda x: str(x[input_col]).upper(), str)


@concretize(ps.DataFrame)
def concrete_args_combination_op(
    *dfs: ps.DataFrame, time_col: str = "TIME"
) -> ps.DataFrame:
    common_cols = list(set.intersection(*[set(df.columns) for df in dfs]))
    to_concat = [df[common_cols] for df in dfs]
    ret = to_concat[0]
    for df in to_concat[1:]:
        ret = ret.unionByName(df)
    ret.withColumn(time_col, lit(str(datetime.datetime.utcnow())))
    return ret


def combination_join_op(
    *ads_list: AbstractDataStructure, join_col: str
) -> AbstractDataStructure:
    ads = ads_list[0]
    for ads_right in ads_list[1:]:
        ads = ads.join(
            ads_right.prune_tech_cols(),
            left_on=[join_col],
            right_on=[join_col],
            how="inner",
        )
    return ads


def custom_load_op(
    full_ads: AbstractDataStructure,
    incoming_ads: AbstractDataStructure,
    pk: str,
) -> AbstractDataStructure:
    distinct = incoming_ads[[pk]].distinct().count()
    total = incoming_ads.count()
    if distinct != total:
        raise ValueError(
            f"Failed unicity constraint on PK '{pk}' (distinct : {distinct}, total : {total})."
        )
    return full_ads.prune_tech_cols()[
        ~full_ads[pk].isin(
            [entry[pk] for entry in incoming_ads[[pk]].distinct().to_list_of_dicts()]
        )
    ].union(incoming_ads.prune_tech_cols())


def generic_join_op(
    left: AbstractDataStructure, right: AbstractDataStructure, join_cols: List[str]
) -> AbstractDataStructure:
    return left.join(
        right,
        left_on=join_cols,
        right_on=join_cols,
        how="inner",
        l_modifier=lambda x: f"l_{x}",
        r_modifier=lambda x: f"r_{x}",
        modify_on=False,
    )


def unicity_check_op(
    ads: AbstractDataStructure, pk: List[str]
) -> AbstractDataStructure:
    if ads.distinct(pk).count() != ads.count():
        raise ValueError(f"Failed unicity check using pk columns {pk}")
    return ads


def multiplex_op(
    main_input: AbstractDataStructure,
    secondary_input: AbstractDataStructure,
) -> Dict[str, AbstractDataStructure]:
    common_cols = list(
        set(main_input.prune_tech_cols().list_columns())
        & set(secondary_input.prune_tech_cols().list_columns())
    )
    return {
        "union": main_input[common_cols].union(secondary_input[common_cols]),
        "join": main_input.prune_tech_cols().join(
            secondary_input.prune_tech_cols(),
            ["col_3"],
            ["col_3"],
            l_modifier=lambda x: f"l_{x}",
            r_modifier=lambda x: f"r_{x}",
            modify_on=False,
        ),
    }


@concretize(pd.DataFrame)
def concrete_multiplex_op(
    main_input: pd.DataFrame,
    secondary_input: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    main_input = main_input[
        [
            column
            for column in main_input.columns
            if column not in ADFGlobalConfig.get_tech_cols()
        ]
    ]
    secondary_input = secondary_input[
        [
            column
            for column in secondary_input.columns
            if column not in ADFGlobalConfig.get_tech_cols()
        ]
    ]
    common_cols = list(
        {column for column in main_input.columns}
        & {column for column in secondary_input.columns}
    )
    union = pd.concat([main_input[common_cols], secondary_input[common_cols]])
    join = main_input.merge(
        secondary_input,
        how="inner",
        on="col_3",
    )
    return {
        "union": union,
        "join": join,
    }


@concretize(RawSQLConcretization)
def raw_sql_rename_op(raw: RawSQLConcretization) -> RawSQLConcretization:
    sql = f"SELECT "
    sql += ",\n".join([f"{col} AS renamed_{col}" for col in raw.cols])
    sql += f"\nFROM ({raw.sql})"
    return RawSQLConcretization(
        sql=sql, cols={f"renamed_{col}": raw.cols[col] for col in raw.cols}
    )

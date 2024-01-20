from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F


@transform_df(
    Output("/Faurecia/Ontology/Purchasing/data/clean/taxo_hierarchy/taxo_hierarchy/"),
    purchasing_segment=Input("ri.foundry.main.dataset.a58f2577-d41d-4da9-b766-24ec501dd464"),
    purchasing_sub_commodity=Input("ri.foundry.main.dataset.14b9e6cf-8545-41d5-a518-258e2a051988"),
    purchasing_commodity=Input("ri.foundry.main.dataset.ded3ba14-e011-4116-9bb4-44ad2cf9fb83"),
    purchasing_portfolio=Input("ri.foundry.main.dataset.07815c9e-2532-44bb-a1b1-a729e4be8fb2"),



)
def my_compute_function(purchasing_segment, purchasing_sub_commodity, purchasing_commodity, purchasing_portfolio):
    """
    Parameter
    ---------------------------------------
    purchasing_segment : catalog dataset
    purchasing_sub_commodity : catalog dataset
    purchasing_commodity: catalog dataset
    purchasing_portfolio: catalog dataset

    Returns
    -----------------------------------------
    datafarme
            contains all the required columns from the required joined datasets
    """

    # Table.ExpandTableColumn(Source, "Sub-Commodity", {"Name", "Validity status", "Commodity - Code"}
    # Select required columns from purchasing_sub_commodity
    df_select_sub_commodity_cols = purchasing_sub_commodity.select(F.col("code").alias("subcommodity_code"),
                                                                   F.col("name").alias("subcommodity_name"),
                                                                   F.col("commodity"),
                                                                   F.col("validityStatus").
                                                                   alias("subcommodity_validity_status"))

    # Table.NestedJoin(Segment, {"Sub-commodity-Code"}, #"Sub-Commodity", {"Code"}, "Sub-Commodity", JoinKind.LeftOuter)
    # Joining segment and sub_commodity table
    df_join_sub_comodity = purchasing_segment.join(
        df_select_sub_commodity_cols, purchasing_segment.subCommodity == df_select_sub_commodity_cols.subcommodity_code,
        "left")

    # Table.ExpandTableColumn(#"Merged Queries", "Commodity", {"Name"}, {"Commodity"})
    # select required columns from commodity
    df_select_commodity_cols = purchasing_commodity.select(F.col("code").alias("commodity_code"),
                                                           F.col("name").alias("commodity_name"))

    # Table.NestedJoin(#"Renamed Columns", {"Commodity - Code"}, Commodity, {"Code"}, "Commodity", JoinKind.LeftOuter)
    # Joining commodity with segment and sub_commodity table
    df_join_commodity = df_join_sub_comodity.join(
        df_select_commodity_cols, df_join_sub_comodity.commodity == df_select_commodity_cols.commodity_code, "left")

    # Table.ExpandTableColumn(#"Merged Queries1", "Portfolio", {"Name"}, {"Portfolio"})
    # Select required columns from purchasing table
    df_select_portfolio = purchasing_portfolio.select(F.col("code").alias("portfolio_code"),
                                                      F.col("name").alias("portfolio_name"))
    # Table.NestedJoin(#"Expanded Commodity",{"Purchasing Portfolio-Code"},Portfolio,{"Code"},"Portfolio",JoinKind.LeftOuter)
    # Joining portfolio with segment, sub_commodity and commodity table
    df_join_portfolio = df_join_commodity.join(
        df_select_portfolio, df_join_commodity.purchasingPortfolio == df_select_portfolio.portfolio_code, "left")

    # Rename purchasing_segement columns "name" and "validityStatus"
    df_rename = df_join_portfolio.withColumnRenamed(
        'name', 'segment_name').withColumnRenamed('validityStatus', 'segment_validity_status').withColumnRenamed(
            'code', 'segment_code')
    # Across the purchasing stream , "segement_code" is of length 6, however , in MDM source this is not the case.
    # adding required trailing hypens to make the length uniform. This is needed for linking objects.
    df_rename = df_rename.withColumn("system_segment_code", F.rpad(F.col("segment_code"), 6, "-"))
    return df_rename

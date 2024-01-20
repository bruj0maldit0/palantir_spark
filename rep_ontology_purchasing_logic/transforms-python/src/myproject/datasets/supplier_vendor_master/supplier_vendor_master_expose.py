from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F


@transform_df(
    Output("ri.foundry.main.dataset.c4707eb7-b93f-47ee-90ab-e83ed41a4957"),
    vendor_master=Input("ri.foundry.main.dataset.bbed6606-e689-404c-bb28-4afd38cf5c57"),
)
def my_compute_function(vendor_master):
    """
    Parameter
    ---------------------------------------
    vendor_master : catalog dataset

    Returns
    -----------------------------------------
    datafarme
            contains all the required columns from the required joined datasets
    """
    df_supplier_vendor = (
        vendor_master
        # Table.SelectColumns(#"Promoted Headers",{"LIFNR","LAND1","NAME1","NAME2","NAME3","NAME4","STRAS","ORT01",...}
        .select(F.col("supplier_|_lifnr").alias('lifnr'),
                F.col("country_|_land1"),
                F.col("name_|_name1"),
                F.col("name_2_|_name2"),
                F.col("name_3_|_name3"),
                F.col("name_4_|_name4"),
                F.col("street_|_stras"),
                F.col("city_|_ort01"),
                F.col("district_|_ort02"),
                F.col("region_|_regio"),
                F.col("postal_code_|_pstlz"),
                F.col("address_|_adrnr"),
                F.col("vat_registration_no_|_stceg"),
                F.col("tax_number_1_|_stcd1"),
                F.col("tax_number_2_|_stcd2"),
                F.col("tax_number_3_|_stcd3"),
                F.col("tax_number_4_|_stcd4"),
                F.col("account_group_|_ktokk"),
                F.col("confirmation_date_|_updat"),
                F.col("created_on_|_erdat"),
                F.col("customer_|_kunnr"),
                F.col("alternative_payee_|_lnrza"),
                F.col("plant_|_werks"),
                F.col("profit_center_|_zzprctr"),
                F.col("teletex_number_|_teltx"),
                F.col("central_posting_block_|_sperr"),
                F.col("central_purchasing_block_|_sperm"),
                F.col("block_function_|_sperq"))
    )

    # Table.Sort(#"Changed Type",{{"UPDAT", Order.Descending}, {"ERDAT", Order.Descending}})
    df_sort_dates = df_supplier_vendor.sort(F.col("confirmation_date_|_updat").desc(),
                                            F.col("created_on_|_erdat").desc()
                                            )

    #  Table.Distinct(#"Buffered", {"LIFNR"})
    df_filter_distinct_vendor = df_sort_dates.dropDuplicates(["lifnr"])

    # Table.AddColumn(#"Removed Duplicates", "Blocked", each if [SPERR]="X" or [SPERM] ="X" or [SPERQ] <> "" then "Block"
    df_blocked_column = df_filter_distinct_vendor.withColumn("Blocked", F.when(F.col("central_posting_block_|_sperr") == "X",
                                                                               "block").when(F.col("central_purchasing_block_|_sperm") == "X", "block")
                                                             .when(F.col("block_function_|_sperq").cast("Int").isNotNull(), "block")
                                                             )

    # Table.SelectRows(#"Add Blocked Flag", each [KTOKK] = "Z001" or [KTOKK] = "Z002" or ....
    df_filter_account_group = df_blocked_column.filter((F.col("account_group_|_ktokk") == "Z001") |
                                                       (F.col("account_group_|_ktokk") == "Z002") |
                                                       (F.col("account_group_|_ktokk") == "Z003") |
                                                       (F.col("account_group_|_ktokk") == "Z004") |
                                                       (F.col("account_group_|_ktokk") == "Z005") |
                                                       (F.col("account_group_|_ktokk") == "Z006") |
                                                       (F.col("account_group_|_ktokk") == "Z007") |
                                                       # Added new account_group requested on task 25979
                                                       (F.col("account_group_|_ktokk") == "Z008") |
                                                       (F.col("account_group_|_ktokk") == "Z010") |
                                                       (F.col("account_group_|_ktokk") == "Z014") |
                                                       # End change requested on task 25979
                                                       (F.col("account_group_|_ktokk") == "Z201") |
                                                       (F.col("account_group_|_ktokk") == "Z101") |
                                                       (F.col("account_group_|_ktokk") == "Z104") |
                                                       (F.col("account_group_|_ktokk") == "Z106"))

    # Table.ReplaceValue(#"Filtered Rows remove DLP trigger","‼","",Replacer.ReplaceText,{"NAME1"})
    df_remove_exclamation_prefix = df_filter_account_group.withColumn(
        "name_|_name1", F.regexp_replace(F.col("name_|_name1"), r"‼", ""))
    return df_remove_exclamation_prefix

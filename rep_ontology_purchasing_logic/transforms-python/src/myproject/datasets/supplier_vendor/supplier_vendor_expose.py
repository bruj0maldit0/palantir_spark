from transforms.api import transform_df, Input, Output
# from myproject.datasets import utils
from pyspark.sql import functions as F
# from pyspark.sql.functions import *


@transform_df(
    Output("/Faurecia/Ontology/Purchasing/data/clean/supplier_vendor/supplier_vendor"),
    vendor_master=Input("ri.foundry.main.dataset.ce28be19-2110-4068-b342-9035302fe877"),
    vendor_master_company_code=Input("ri.foundry.main.dataset.c16b3c80-ef8a-4183-ab33-b174731e91c9"),
    vendor_master_vat=Input("ri.foundry.main.dataset.e2a7af69-2327-4ca4-9e1f-4198c9171b7d"),
    vendor_master_general_section=Input("ri.foundry.main.dataset.bbed6606-e689-404c-bb28-4afd38cf5c57")
)
def my_compute_function(vendor_master, vendor_master_company_code, vendor_master_vat, vendor_master_general_section):
    """
    Parameter
    ---------------------------------------
    vendor_master : dataset
    vendor_master_company_code : dataset
    vendor_master_vat: dataset
    vendor_master_general_section: catalog dataset

    Returns
    -----------------------------------------
    datafarme
            contains all the required columns from the required joined datasets
    """

    # select required columns
    df_select_col = vendor_master.select(F.col("supplier_|_lifnr").alias("lifnr"),
                                         F.col("created_on_|_erdat"),
                                         F.col("purch_organization_|_ekorg"),
                                         F.col("confirmation_control_|_bstae"),
                                         F.col("terms_of_payment_|_zterm"),
                                         F.col("incoterms_|_inco1"),
                                         F.col("delete_flag_for_purchasing_organization_|_loevm"),
                                         F.col("purch_block_for_purchasing_organization_|_sperm"),
                                         F.col("eval_receipt_sett_|_xersy"),
                                         F.col("aut_ev_grsetmtret_|_xersr"),
                                         F.col("source"))

    # filter "lifnr" on given conditions
    df_filter_lifnr_purchase_flag = df_select_col.filter((F.col('lifnr') > 100000) & (
        F.col('lifnr') < 300000) | ((F.col("lifnr").startswith('9')) == True))

    # filter purch_organization_|_ekorg  on given conditions
    df_filter_purch_organization = df_filter_lifnr_purchase_flag.filter(~((F.col("purch_organization_|_ekorg").startswith('AP')) == True) &
                                                                        ~((F.col("purch_organization_|_ekorg").startswith('PO')) == True) &
                                                                        ~((F.col("purch_organization_|_ekorg").startswith('XS')) == True) &
                                                                        ~((F.col("purch_organization_|_ekorg").startswith('AE')) == True))
    # select required columns
    df_select_filtered_cols = df_filter_purch_organization.select(F.col("lifnr"),
                                                                  F.col("source"),
                                                                  F.col("purch_organization_|_ekorg").alias("purchase_org"))
    # select required columns
    df_select_central_purchasing_block = vendor_master_general_section.select(F.col("supplier_|_lifnr").alias('lifnr'),
                                                                              F.col("central_purchasing_block_|_sperm").alias("central_purchasing_block"))

    # join operation
    df_join_df_select_central_purchasing_block = df_select_filtered_cols.join(
                    df_select_central_purchasing_block, ["lifnr"], "left")

    # replacing with "(X)" string if value is matched to "X" for column "central_purchasing_block"
    df_update_purchasing_flag = df_join_df_select_central_purchasing_block.withColumn("central_purchasing_block",
                                                                                      F.when(F.col("central_purchasing_block") == "X", "(X)"))

    # concat "purchase_org" and "central_purchasing_block" if corresponding data value is equal to "(X)"
    df_select_concat = df_update_purchasing_flag.withColumn("updated_purchasing_flag", F.when(F.col("central_purchasing_block") == "(X)",
                                                                                              F.concat(df_update_purchasing_flag.purchase_org, df_update_purchasing_flag.central_purchasing_block))
                                                            .otherwise(df_update_purchasing_flag.purchase_org))

    # aggregate and sort "updated_purchasing_flag" on "lifnr"
    df_aggregate_purch_organization = df_select_concat.groupby(
        F.col("lifnr")).agg(F.array_sort(F.collect_set("updated_purchasing_flag")).alias("purchasing_org"))

    # aggregate and sort "company_code_|_bukrs" on "lifnr"
    df_aggregate_company_code = vendor_master_company_code.groupby(
        F.col("supplier_|_lifnr")).agg(F.array_sort(F.collect_set("company_code_|_bukrs")).alias("company_code"))

    # rename "supplier_|_lifnr" to "lifnr"
    df_rename = df_aggregate_company_code.withColumnRenamed("supplier_|_lifnr", "lifnr")

    # join opeartion
    df_join_purchasing_and_company_code = df_aggregate_purch_organization.join(df_rename, ["lifnr"], "left")

    # select required columns
    df_select_col_vendor_master_vat = vendor_master_vat.select(F.col("vat_registration_no_|_stceg").alias("fiscal_representation_vat_number"),
                                                               F.col("supplier_|_lifnr").alias("lifnr"))

    # dropping duplicates
    df_join_fiscal_rep_vat = df_join_purchasing_and_company_code.join(
        df_select_col_vendor_master_vat, ["lifnr"], "left").dropDuplicates()

    return df_join_fiscal_rep_vat

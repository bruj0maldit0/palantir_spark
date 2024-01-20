from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F


@transform_df(
    Output("ri.foundry.main.dataset.a09596ea-a532-49fe-9fb1-124068e262cf"),
    vendor_master_purchasing_org=Input("ri.foundry.main.dataset.ce28be19-2110-4068-b342-9035302fe877"),
    vendor_master_company_code=Input("ri.foundry.main.dataset.c16b3c80-ef8a-4183-ab33-b174731e91c9"),
    vendor_master_general_section=Input("ri.foundry.main.dataset.bbed6606-e689-404c-bb28-4afd38cf5c57"),
    purchasing_org_data=Input("ri.foundry.main.dataset.94822829-b4b4-4d98-8b4e-9d8afdfd5574"),
)
def my_compute_function(vendor_master_purchasing_org, vendor_master_company_code, vendor_master_general_section, purchasing_org_data):
    """
    Parameter
    ---------------------------------------
    vendor_master_purchasing_org : dataset
    vendor_master_company_code : dataset
    vendor_master_general_section  :catalog dataset

    Returns
    -----------------------------------------
    datafarme
            contains all the required columns from the required joined datasets
    """
    # select required columns
    df_select_col_purchasing_org_data = (
        purchasing_org_data
        .filter(F.col("validityStatus").isin(["Valid"]))
        .select(F.col("code").alias("purchasingOrganization"))
    )

    # select required columns
    df_select_col_vendor_master_general_section = (
        vendor_master_general_section
        # Keep only data from FCW source
        .filter(F.col("source") == "FCW")
        .select(
                F.col("supplier_|_lifnr").alias("sap_vendor_code"),
                F.col("teletex_number_|_teltx").alias("faurecia_id"),
                F.col("country_|_land1"),
                F.col("block_function_|_sperq"),
                F.col("central_purchasing_block_|_sperm"),
                F.col("account_group_|_ktokk"),
                )
    )

    # select required columns
    df_select_col_vendor_master_company_code = (
        vendor_master_company_code
        # Keep only data from FCW source
        .filter(F.col("source") == "FCW")
        .select(
                F.col("supplier_|_lifnr").alias("sap_vendor_code"),
                F.col("company_code_|_bukrs").alias("legalEntity"),
                F.col("payment_methods_|_zwels"),
                F.col("reconciliation_acct_|_akont"),
                F.col("sort_key_|_zuawa"),
                F.col("withholding_tax_code_|_qsskz"),
                F.col("clrks_internet_add_|_intad"),
                F.col("terms_of_payment_|_zterm").alias("le_terms_of_payment"))
    )

    # join operation
    df_join_general_selection_company_code = df_select_col_vendor_master_general_section.join(
                df_select_col_vendor_master_company_code, ["sap_vendor_code"], how="inner")

    # select required columns
    df_select_col_vendor_master_purchasing_org = (
        vendor_master_purchasing_org
        # Keep only data from FCW source
        .filter(F.col("source") == "FCW")
        .select(
                F.col("supplier_|_lifnr").alias("sap_vendor_code"),
                F.col("purch_organization_|_ekorg").alias("purchasingOrganization"),
                F.col("terms_of_payment_|_zterm").alias("purchasing_org_terms_of_payment"),
                F.col("order_currency_|_waers"),
                F.col("incoterms_|_inco1"),
                F.col("incoterms_part_2_|_inco2"),
                F.col("confirmation_control_|_bstae"),
                F.col("purch_block_for_purchasing_organization_|_sperm"),
                )
    )

    # join operation
    df_join_df_select_col_vendor_master_purchasing_org = df_join_general_selection_company_code.join(
                df_select_col_vendor_master_purchasing_org, ["sap_vendor_code"], how="inner")

    # join to keep only valid Purchasing Organization
    df_join_df_select_col_vendor_master_purchasing_org = (
        df_join_df_select_col_vendor_master_purchasing_org
        .join(df_select_col_purchasing_org_data, ["purchasingOrganization"], how="inner")
    )

    return df_join_df_select_col_vendor_master_purchasing_org

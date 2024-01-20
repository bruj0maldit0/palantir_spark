from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F


@transform_df(
    Output("ri.foundry.main.dataset.6e4ff805-d610-482d-a7e3-37377aaf4a42"),
    purchasing_org_specs=Input("ri.foundry.main.dataset.9a1dfd7d-c2e0-4882-a81e-437abac46a50"),
    faurecia_legal_entity=Input("ri.foundry.main.dataset.ce9ebd65-bc7f-431a-8f77-42aab1188197"),
    purchasing_org_data=Input("ri.foundry.main.dataset.94822829-b4b4-4d98-8b4e-9d8afdfd5574"),
)
def my_compute_function(purchasing_org_specs, faurecia_legal_entity, purchasing_org_data):
    """
    Parameter
    ---------------------------------------
    purchasing_org_specs : catalog dataset
    faurecia_legal_entity : catalog dataset
    purchasing_org_data : dataset

    Returns
    -----------------------------------------
    dataframe
            contains all the required columns from the required joined datasets
    """

    # select required columns
    df_select_col_purchasing_org_data = (
        purchasing_org_data
        .filter(F.col("validityStatus").isin(["Valid"]))
        .select(F.col("code").alias("purchasingOrganization"))
    )

    # select required columns
    df_select_col_purchasing_org_specs = purchasing_org_specs.select(
                    F.col("faureciaCountry").alias("mdm_faureciaCountry"),
                    F.col("purchasingOrganization").alias("purchasingOrganization"),
                    F.col("purchasingBlock").alias("mdm_purchasingBlock"),
                    F.col("oldPaymentTerm").alias("mdm_oldPaymentTerm"),
                    F.col("paymentTerms").alias("mdm_paymentTerms"),
                    F.col("orderCurrency").alias("mdm_orderCurrency"),
                    F.col("incoterms").alias("mdm_incoterms"),
                    F.col("incotermLocation").alias("mdm_incotermLocation"),
                    F.col("confirmationControlKey").alias("mdm_confirmationControlKey"),
                    F.col("orderReceiverAgent").alias("mdm_orderReceiverAgent"),
                    F.col("goodsSupplierAgent").alias("mdm_goodsSupplierAgent"),
                    F.col("forwardingAgent").alias("mdm_forwardingAgent"),
                    F.col("invoicePresenterAgent").alias("mdm_invoicePresenterAgent"))

    # select required columns
    df_select_col_faurecia_legal_entity = faurecia_legal_entity.select(
                    F.col("faureciaCountry").alias("mdm_faureciaCountry"),
                    F.col("legalEntity").alias("legalEntity"),
                    F.col("paymentMethod").alias("mdm_paymentMethod"),
                    F.col("reconciliationAccount").alias("mdm_reconciliationAccount"),
                    F.col("sortKey").alias("mdm_sortKey"),
                    F.col("withholdingTaxCode").alias("mdm_withholdingTaxCode"),
                    F.col("previousAccountNumber").alias("mdm_previousAccountNumber"),
                    F.col("checkDoubleInvoice").alias("mdm_checkDoubleInvoice"),
                    F.col("clerksInternet").alias("mdm_clerksInternet"),
                    F.col("acctgClerk").alias("mdm_acctgClerk"),
                    F.col("accountMemo").alias("mdm_accountMemo"),
                    F.col("houseBank").alias("mdm_houseBank"),
    )

    # join to keep just valid Purchasing Organization
    df_select_col_purchasing_org_specs = (
        df_select_col_purchasing_org_specs
        .join(df_select_col_purchasing_org_data, ["purchasingOrganization"], how="inner")
    )

    # join operation
    df_join_faurecia_legal_entity = df_select_col_purchasing_org_specs.join(
                df_select_col_faurecia_legal_entity, ["mdm_faureciaCountry"], how="left"
    )

    # splitting "faureciaCountry" to get "faurecia ID" and "faurecia country"
    df_split_faurecia_country = df_join_faurecia_legal_entity.withColumn(
                "faurecia_id", F.split(df_join_faurecia_legal_entity.mdm_faureciaCountry, r'\|').getItem(0))

    return df_split_faurecia_country

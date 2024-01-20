from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F


@transform_df(
    Output("ri.foundry.main.dataset.1679e005-4c17-486a-ad5c-bc40da3b7b61"),
    purchasing_organization=Input("ri.foundry.main.dataset.6e4ff805-d610-482d-a7e3-37377aaf4a42"),
    p_org=Input("ri.foundry.main.dataset.a09596ea-a532-49fe-9fb1-124068e262cf"),
    legal_entity=Input("ri.foundry.main.dataset.b3de89fd-7dbb-4725-a8a7-d7c780ce5b62"),

)
def my_compute_function(purchasing_organization, p_org, legal_entity):
    """
    Parameter
    ---------------------------------------
    purchasing_organization : dataset
    p_org : dataset

    Returns
    -----------------------------------------
    datafarme
            contains all the required columns from the required joined datasets
    """
    # select data from legal_entity
    df_legal_entity = (
        legal_entity
        .filter(F.col("legal_entity_is_valid"))
        .select(F.col("legal_entity_code").alias("legalEntity"))
    )

    # join purchasing_organization and p_org input datasets
    join_condition = ["faurecia_id", "purchasingOrganization", "legalEntity"]
    df_join = (
        p_org
        .join(purchasing_organization,
              join_condition,
              how="left")
    ).dropDuplicates()

    # join to keep just valid Legal Entities
    df_join = (
        df_join
        .join(df_legal_entity, ["legalEntity"], how="inner")
    )

    # Add a primary key column
    df_join = (
        df_join
        .withColumn("pk",
                    F.concat_ws("|",
                                F.col("sap_vendor_code"),
                                F.col("mdm_faureciaCountry"),
                                F.col("purchasingOrganization"),
                                F.col("legalEntity")))
    )
    # Last select to order the field's output.
    df_join = (
        df_join
        .select("faurecia_id",
                "sap_vendor_code",
                "account_group_|_ktokk",
                "country_|_land1",
                "purchasingOrganization",
                "purch_block_for_purchasing_organization_|_sperm",
                "legalEntity",
                "block_function_|_sperq",
                "central_purchasing_block_|_sperm",
                "payment_methods_|_zwels",
                "reconciliation_acct_|_akont",
                "sort_key_|_zuawa",
                "withholding_tax_code_|_qsskz",
                "clrks_internet_add_|_intad",
                "le_terms_of_payment",
                "purchasing_org_terms_of_payment",
                "order_currency_|_waers",
                "incoterms_|_inco1",
                "incoterms_part_2_|_inco2",
                "confirmation_control_|_bstae",
                "mdm_faureciaCountry",
                "mdm_purchasingBlock",
                "mdm_oldPaymentTerm",
                "mdm_paymentTerms",
                "mdm_orderCurrency",
                "mdm_incoterms",
                "mdm_incotermLocation",
                "mdm_confirmationControlKey",
                "mdm_orderReceiverAgent",
                "mdm_goodsSupplierAgent",
                "mdm_invoicePresenterAgent",
                "mdm_paymentMethod",
                "mdm_reconciliationAccount",
                "mdm_sortKey",
                "mdm_withholdingTaxCode",
                "mdm_previousAccountNumber",
                "mdm_checkDoubleInvoice",
                "mdm_clerksInternet",
                "mdm_acctgClerk",
                "mdm_accountMemo",
                "mdm_houseBank",
                "pk")
    )

    return df_join

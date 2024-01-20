from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F


@transform_df(
    Output("ri.foundry.main.dataset.7494e11e-e893-4811-939c-7c3608216b44"),
    supplier_xreference=Input("ri.foundry.main.dataset.205471a9-d400-4218-ae44-dc95db50620b"),
    supplier_vendor_master=Input("ri.foundry.main.dataset.c4707eb7-b93f-47ee-90ab-e83ed41a4957"),
    supplier_site=Input("ri.foundry.main.dataset.f5d5d9b8-6c6d-4bbb-b49b-02d1a599796e"),
    link_supplier_generic_site=Input("ri.foundry.main.dataset.de08f369-0fd6-4dc6-86f1-d7bd0d4ce5db"),
    generic_supplier=Input("ri.foundry.main.dataset.0866a368-c1b6-495b-99d9-6a6061b5a0c3"),
    taxo_hierarchy=Input("ri.foundry.main.dataset.9a638bbb-bb8f-4939-bb50-b49baed25fe2"),
    link_supplier_generic_ultimate=Input("ri.foundry.main.dataset.4addb17e-87ce-44b1-a7b3-97d78583a752"),
    ultimate_supplier=Input("ri.foundry.main.dataset.cfebe57d-4802-4360-93c6-b4d2cf421dd2"),
    supplier_vendor=Input("ri.foundry.main.dataset.53477a92-0c68-4110-a538-c3c26f712b28"),
    vendor_general_selection=Input("ri.foundry.main.dataset.bbed6606-e689-404c-bb28-4afd38cf5c57"),
    permitted_alternative_payee=Input("ri.foundry.main.dataset.8f3488f1-c3b1-46da-88ea-7d961f02f4f0"),
    vendor_bank_details=Input("ri.foundry.main.dataset.caf8b95c-4adc-419f-8f06-19beaa519e59"),
    vendor_master_record_purchasing_org=Input("ri.foundry.main.dataset.ce28be19-2110-4068-b342-9035302fe877"),
    gpm_supplier=Input("ri.foundry.main.dataset.f93d5e3d-dd37-4daa-92a6-6612e2abb445")
)
def my_compute_function(supplier_vendor_master,
                        supplier_xreference,
                        supplier_site,
                        link_supplier_generic_site,
                        generic_supplier,
                        taxo_hierarchy,
                        link_supplier_generic_ultimate,
                        ultimate_supplier,
                        supplier_vendor,
                        vendor_general_selection,
                        permitted_alternative_payee,
                        vendor_bank_details,
                        vendor_master_record_purchasing_org,
                        gpm_supplier):
    """
    Parameter
    ---------------------------------------
    supplier_vendor_master : dataset
    SuppplierXreference : dataset
    supplier_site: catalog dataset
    link_supplier_generic_site : dataset
    generic_suppliern : catalog dataset
    taxo_hierarchy : dataset
    link_supplier_generic_ultimate : dataset
    ultimate_supplier : catalog dataset
    supplier_vendor : dataset
    vendor_general_selection : catalog dataset
    permitted_alternative_payee : dataset
    vendor_bank_details : dataset
    vendor_master_record_purchasing_org : dataset
    gpm_supplier : dataset

    Returns
    -----------------------------------------
    datafarme
            contains all the required columns from the required joined datasets
    """

    # Table.ExpandTableColumn(#"Renamed Columns","X-Reference",{"Is valid FC","Is BusinessActiveinSAP","Supplier ID"}
    # Selecting required columns from "SupplierXreference"
    # Filter out by "systemCode" == "SAP01", only FCS suppliers
    df_supplier_xreference = (
        supplier_xreference
        .filter(F.col("systemCode") == "SAP01")
        .select(F.col("supplierLocalId").alias("XRef"),
                F.col("isValid"),
                F.col("isBusinessActive"),
                F.col("supplierSiteId").alias("supplierID"))
    )
    # Table.NestedJoin(LFA1, {"LIFNR"}, #"X-Reference", {"Supplier local ID"}, "X-Reference", JoinKind.LeftOuter)
    # join operation
    df_join_supplier_site_xref = supplier_vendor_master.join(
                                      df_supplier_xreference,
                                      supplier_vendor_master.lifnr == df_supplier_xreference.XRef, "left")

    # Table.ExpandTableColumn(#"Merged Supplier Site", "Supplier Site", {"Global Block - Code", "Faurecia ID",...
    # selecting required columns
    df_select_col_supplier_site = supplier_site.select(F.col("supplierID").alias("supplier_site_supplierID"),
                                                       F.col("globalBlock"),
                                                       F.col("supplierName"),
                                                       F.col("supplierName2"),
                                                       F.col("tradeName"),
                                                       F.col("nameInLocalLanguage"),
                                                       F.col("nameInLocalLanguagePart2"),
                                                       F.col("addressLine1"),
                                                       F.col("country"),
                                                       F.col("email"),
                                                       F.col("dunsNumber"),
                                                       F.col("nameDB"),
                                                       F.col("dunsGlobalUltimate"),
                                                       F.col("primaryNameGlobalUltimate"),
                                                       F.col("dunsAlternateGlobalUltimate"),
                                                       F.col("primaryNameAlternateGlobalUltimate"),
                                                       F.col("aribaFlagCreation"),
                                                       F.col("anID"),
                                                       F.col("supplierManagementID"),
                                                       F.col("aribaCommonSupplierID"),
                                                       F.col("registrationStatus"),
                                                       F.col("GPSLocalID"))

    # Table.NestedJoin(#"Expanded X-Reference", {"MDM Site ID xref"}, #"Supplier Site", {"Faurecia ID"}
    # join operation
    df_join_supplier_xref_and_supplier_site = df_join_supplier_site_xref.join(
        df_select_col_supplier_site,
        df_join_supplier_site_xref.supplierID == df_select_col_supplier_site.supplier_site_supplierID,
        "left")

    # select required columns
    df_select_col_link_supplier_generic_site = link_supplier_generic_site.select(F.col("siteSupplierID"),
                                                                                 F.col("genericSupplierID"))

    # join operation
    df_join_link_supplier_generic_site = df_join_supplier_xref_and_supplier_site.join(
        df_select_col_link_supplier_generic_site,
        df_join_supplier_xref_and_supplier_site.supplier_site_supplierID == df_select_col_link_supplier_generic_site.siteSupplierID,
        "left")

    # select required columns
    df_select_col_generic_supplier = generic_supplier.select(F.col("faureciaID").alias("generic_supplier_faureciaID"),
                                                             F.col("name"),
                                                             F.col("mainSegment"),
                                                             F.col("supplierAccountmanager"))

    # join operation
    df_join_generic_supplier = df_join_link_supplier_generic_site.join(df_select_col_generic_supplier,
                                                                       df_join_link_supplier_generic_site.genericSupplierID == df_select_col_generic_supplier.generic_supplier_faureciaID,
                                                                       "left")

    # select required columns
    df_select_col_taxo_hierarchy = taxo_hierarchy.select(F.col("segment_code"))

    # join operation
    df_join_taxo_hierarchy = df_join_generic_supplier.join(df_select_col_taxo_hierarchy,
                                                           df_join_generic_supplier.mainSegment == df_select_col_taxo_hierarchy.segment_code,
                                                           "left")

    # select required columns
    df_select_col_link_supplier_generic_ultimate = link_supplier_generic_ultimate.select(F.col("genericSupplierID").alias("link_ult_genericSupplierID"),
                                                                                         F.col("ultimateSupplierID"))

    # join operation
    df_join_link_supplier_generic_ultimate = df_join_taxo_hierarchy.join(df_select_col_link_supplier_generic_ultimate,
                                                                         df_join_taxo_hierarchy.genericSupplierID == df_select_col_link_supplier_generic_ultimate.link_ult_genericSupplierID,
                                                                         "left")

    # select required columns
    df_select_col_ultimate_supplier = ultimate_supplier.select(F.col("faureciaID").alias("ultimate_supplier_faureciaID"),
                                                               F.col("name").alias("ultimate_supplier_name"),
                                                               F.col("isInternal").alias("ultimate_isInternal"),
                                                               F.col("validityStatus").alias("ultimate_supplie_validityStatus"))

    # join operation
    df_join_ultimate_supplier = df_join_link_supplier_generic_ultimate.join(df_select_col_ultimate_supplier,
                                                                            df_join_link_supplier_generic_ultimate.ultimateSupplierID == df_select_col_ultimate_supplier.ultimate_supplier_faureciaID,
                                                                            "left")

    drop_columns = ["teletex_number_|_teltx", "supplier_site_supplierID", "siteSupplierID", "XRef",
                    "generic_supplier_faureciaID", "link_ult_genericSupplierID"]

    # drop columns
    df_droped_col = df_join_ultimate_supplier.drop(*drop_columns)

    # select required columns
    df_select_col_supplier_vendor = supplier_vendor.select(F.col("lifnr").alias("supplier_vendor_lifnr"),
                                                           F.col("purchasing_org"),
                                                           F.col("company_code"),
                                                           F.col("fiscal_representation_vat_number"))

    # Aggregates fiscal_representation_vat_number for a supplier_vendor_lifnr
    df_select_col_supplier_vendor = (
        df_select_col_supplier_vendor
        .groupby("supplier_vendor_lifnr",
                 "purchasing_org",
                 "company_code")
        .agg(F.collect_list("fiscal_representation_vat_number").alias("fiscal_representation_vat_number"))
    )

    # join operation
    df_join_supplier_vendor = df_droped_col.join(
        df_select_col_supplier_vendor, df_droped_col.lifnr == df_select_col_supplier_vendor.supplier_vendor_lifnr,
        "left").drop("supplier_vendor_lifnr")

    data = ["GPS_1722381", "GPS_1723029", "GPS_1721829", "GPS_1733444", "GPS_1739584", "GPS_1746153", "GPS_1746309",
            "GPS_1722483", "GPS_1722159", "GPS_1739581", "GPS_248258", "GPS_250137", "GPS_1748740", "GPS_1650514",
            "GPS_1684955", "GPS_1732054", "GPS_231174", "GPS_1717030", "GPS_226407", "GPS_233408", "GPS_250312", "GPS_1671373", "GPS_26284",
            "GPS_1600214", "GPS_231413", "GPS_1669100", "GPS_26275", "GPS_248212", "GPS_1675210", "GPS_1631492", "GPS_59236", "GPS_1671601",
            "GPS_248496", "GPS_26548", "GPS_26846", "GPS_1628870", "GPS_57128", "GPS_50379", "GPS_48969", "GPS_48970", "GPS_48968", "GPS_48967",
            "GPS_48971", "GPS_48960", "GPS_1749306", "GPS_1715835", "GPS_1721789", "GPS_51082"
            ]

    # replacing matched value from "data" variable with "DUMMY" string
    df_update_col_supplierAccountmanager = df_join_supplier_vendor.withColumn(
        "supplierAccountmanager", F.when(F.col("ultimateSupplierID").isin(*data), "DUMMY").otherwise(df_join_supplier_vendor.supplierAccountmanager)).dropDuplicates()

    # select required columns
    df_col_select_vendor_general_selection = (
        vendor_general_selection
        .filter(F.col("source") == "FCW")
        .select(F.col("supplier_|_lifnr").alias("lifnr"), F.col("accts_for_alt_payee_|_xlfza"))
    )

    # join operation
    df_join_vendor_general_selection = df_update_col_supplierAccountmanager.join(df_col_select_vendor_general_selection,
                                                                                  ["lifnr"], "left")

    # select required columns
    df_select_permitted_alternative_payee = (
        permitted_alternative_payee
        .filter(F.col("source") == "FCW")
        .select(F.col("supplier_|_lifnr").alias("lifnr"), F.col("payee_|_empfk"))
    )

    # Aggregates alternative payee by lifnr
    df_select_permitted_alternative_payee = (
        df_select_permitted_alternative_payee
        .groupBy("lifnr")
        .agg(F.collect_list("payee_|_empfk").alias("payee_|_empfk"))
    )

    # join operation
    df_join_permitted_alternative_payee = df_join_vendor_general_selection.join(df_select_permitted_alternative_payee,
                                                                                ["lifnr"], "left")

    # select required columns
    # TO DO: Recover "purch_organization_|_ekorg as "purchasing_org" to use to join with later tables.
    df_select_vendor_master_record_purchasing_org = (
        vendor_master_record_purchasing_org
        .filter(F.col("source") == "FCW")
        .select(F.col("supplier_|_lifnr").alias("lifnr"),
                F.col("purch_organization_|_ekorg"),
                F.col("eval_receipt_sett_|_xersy"),
                F.col("aut_ev_grsetmtret_|_xersr"),
                )
    )
    # concatenate xersy & purch_organization and xersr & purchasing_organization
    df_select_vendor_master_record_purchasing_org = (
        df_select_vendor_master_record_purchasing_org
        .withColumn("xersy",
                    F.concat_ws(" - ", F.col("eval_receipt_sett_|_xersy"), F.col("purch_organization_|_ekorg")))
        .withColumn("xersr",
                    F.concat_ws(" - ", F.col("aut_ev_grsetmtret_|_xersr"), F.col("purch_organization_|_ekorg")))
    )

    # Drop unnecesary columns
    df_select_vendor_master_record_purchasing_org = (
        df_select_vendor_master_record_purchasing_org
        .drop("eval_receipt_sett_|_xersy", "aut_ev_grsetmtret_|_xersr", "purch_organization_|_ekorg")
    )
    # Agregates values by lifnr
    df_select_vendor_master_record_purchasing_org = (
        df_select_vendor_master_record_purchasing_org
        .groupBy("lifnr")
        .agg(F.collect_list("xersy").alias("xersy"),
             F.collect_list("xersr").alias("xersr"))
    )

    # join operation
    df_join_vendor_master_record_purchasing_org = (
        df_join_permitted_alternative_payee
        .join(df_select_vendor_master_record_purchasing_org,
              ["lifnr"], "left"))

    # select required columns
    df_select_vendor_bank_details = (
        vendor_bank_details
        .filter(F.col("source") == "FCW")
        .select(F.col("supplier_|_lifnr").alias("lifnr"),
                F.col("bank_account_|_bankn").alias("bank_account"),
                F.col("bank_country_|_banks").alias("bank_country"))
    )

    # select required columns
    df_concat_country_bank_account = df_select_vendor_bank_details.withColumn("bank_country_and_account",
                                                                              F.concat(df_select_vendor_bank_details.bank_country,
                                                                                       df_select_vendor_bank_details.bank_account))

    # aggregating and sorting "bank_country_and_account" on "lifnr"
    df_aggregate_bank_account = df_concat_country_bank_account.groupby(
        F.col("lifnr")).agg(F.array_sort(F.collect_set("bank_country_and_account")).alias("bank_country_and_account"))

    # join operation
    df_join_df_aggregate_bank_account = df_join_vendor_master_record_purchasing_org.join(
        df_aggregate_bank_account, ["lifnr"], "left")

    # select required columns
    df_select_gpm_supplier = (
        gpm_supplier
        # Filter out only valid records
        .filter(~F.col("isDeleted"))
        .select(F.col("gpsCode"),
                F.col("status"),
                F.col("segments"),
                )
    )
    # Getting the BG using RegEx
    df_select_gpm_supplier = (
        df_select_gpm_supplier
        .withColumn("segments_bg",
                    F.regexp_extract(F.col("segments"), 
                                     r'(FIS|FIC|FAS|FCE|FCM|FAE|SAS)', 1))
    )

    # concatinating "GPS_" string to "gps_code"
    df_concat_prefix_gpscode = df_select_gpm_supplier.withColumn("gps_code", F.concat(F.lit("GPS_"),
                                                                                      df_select_gpm_supplier.gpsCode))

    # join operation
    df_join_df_select_gpm_supplier = df_join_df_aggregate_bank_account.join(df_concat_prefix_gpscode,
                                                                            df_join_df_aggregate_bank_account.ultimate_supplier_faureciaID ==
                                                                            df_concat_prefix_gpscode.gps_code,
                                                                            "left").dropDuplicates()
    drop_col = ["ultimate_supplier_faureciaID", "gpsCode"]

    # drop columns specified in "drop_col" list
    df_drop_columns = df_join_df_select_gpm_supplier.drop(*drop_col)

    # Replace "Null" by "Empty string" on columns supplier_duns_site_no, supplier_duns_ultimate_no &
    # supplier_duns_alternate_ultimate_no
    df_drop_columns = (
        df_drop_columns
        .na.fill("",
                 subset=["dunsNumber",
                         "dunsGlobalUltimate",
                         "dunsAlternateGlobalUltimate",
                         "nameDB",
                         "primaryNameGlobalUltimate",
                         "primaryNameAlternateGlobalUltimate"])
    )

    # reorder dataset as per the requirement and removing duplicates

    # REMOVING COLUMNS GENERATING DUPLICATES and DROPDUPLICATE => TO REMOVE
    df_reorder_and_rename = df_drop_columns.select(
        F.col("lifnr").alias("vendor_id"),
        F.col("country_|_land1").alias("vendor_country"),
        F.col("name_|_name1").alias("vendor_name_1"),
        F.col("name_2_|_name2").alias("vendor_name_2"),
        F.col("name_3_|_name3").alias("vendor_name_3"),
        F.col("name_4_|_name4").alias("vendor_name_4"),
        F.col("street_|_stras").alias("vendor_street"),
        F.col("city_|_ort01").alias("vendor_city"),
        F.col("district_|_ort02").alias("vendor_district"),
        F.col("region_|_regio").alias("vendor_region"),
        F.col("postal_code_|_pstlz").alias("vendor_postal_code"),
        F.col("address_|_adrnr").alias("vendor_address_id"),
        F.col("vat_registration_no_|_stceg").alias("vendor_vat_registration_no"),
        F.col("tax_number_1_|_stcd1").alias("vendor_tax_number_1"),
        F.col("tax_number_2_|_stcd2").alias("vendor_tax_number_2"),
        F.col("tax_number_3_|_stcd3").alias("vendor_tax_number_3"),
        F.col("tax_number_4_|_stcd4").alias("vendor_tax_number_4"),
        F.col("fiscal_representation_vat_number").alias("vendor_fiscal_representations_vat_no"),
        F.col("account_group_|_ktokk").alias("vendor_account_group"),
        F.col("confirmation_date_|_updat").alias("vendor_last_update_date"),
        F.col("created_on_|_erdat").alias("vendor_creation_date"),
        F.col("customer_|_kunnr").alias("vendor_customer_id_link"),
        F.col("alternative_payee_|_lnrza").alias("vendor_alternative_payee"),
        F.col("plant_|_werks").alias("vendor_plant_id"),
        F.col("profit_center_|_zzprctr").alias("vendor_profit_center"),
        F.col("central_posting_block_|_sperr").alias("vendor_central_posting_block"),
        F.col("central_purchasing_block_|_sperm").alias("vendor_purchasing_block"),
        F.col("block_function_|_sperq").alias("vendor_block_function"),
        F.col("Blocked").alias("vendor_blocked"),
        F.col("isValid").alias("supplier_is_valid_for_FCS"),
        F.col("isBusinessActive").alias("supplier_is_business_active_in_SAP"),
        F.col("supplierID").alias("supplier_id"),
        F.col("globalBlock").alias("supplier_global_block"),
        F.col("supplierName").alias("supplier_name"),
        F.col("supplierName2").alias("supplier_name_2"),
        F.col("tradeName").alias("supplier_trade_name"),
        F.col("nameInLocalLanguage").alias("supplier_local_name"),
        F.col("nameInLocalLanguagePart2").alias("supplier_local_name_2"),
        F.col("addressLine1").alias("supplier_address_1"),
        F.col("country").alias("supplier_country"),
        F.col("email").alias("supplier_order_email"),
        F.col("dunsNumber").alias("supplier_duns_site_no"),
        F.col("nameDB").alias("supplier_duns_site_name"),
        F.col("dunsGlobalUltimate").alias("supplier_duns_ultimate_no"),
        F.col("primaryNameGlobalUltimate").alias("supplier_duns_ultimate_name"),
        F.col("dunsAlternateGlobalUltimate").alias("supplier_duns_alternate_ultimate_no"),
        F.col("primaryNameAlternateGlobalUltimate").alias("supplier_duns_alternate_ultimate_name"),
        F.col("aribaFlagCreation").alias("supplier_ariba_creation_type"),
        F.col("anID").alias("supplier_an_id"),
        F.col("supplierManagementID").alias("supplier_sm_id"),
        F.col("aribaCommonSupplierID").alias("supplier_common_supplier_id"),
        F.col("registrationStatus").alias("supplier_SLP_registration_status"),
        F.col("GPSLocalID").alias("gps_site_id"),
        F.col("genericSupplierID").alias("generic_id"),
        F.col("name").alias("generic_name"),
        F.col("mainSegment"),
        F.col("supplierAccountmanager").alias("generic_sam"),
        F.col("segment_code"),
        F.col("ultimateSupplierID").alias("ultimate_id"),
        F.col("ultimate_supplier_name").alias("ultimate_name"),
        F.col("ultimate_isInternal"),
        F.col("ultimate_supplie_validityStatus").alias("ultimate_status"),
        F.col("status").alias("gpm_status"),
        F.col("segments_bg").alias("gpm_business_group"),
        F.col("gps_code").alias("gpm_gps_code"),
        F.col("purchasing_org").alias("vendor_purchasing_org"),
        F.col("company_code").alias("vendor_company_code"),
        F.col("accts_for_alt_payee_|_xlfza"),
        F.col("payee_|_empfk"),
        F.col("xersy").alias("AutoEvalGRSetmt_Del_|_xersy"),
        F.col("xersr").alias("AutoEvalGRSetmt_Ret_|_xersr"),
        F.col("bank_country_and_account"),
        ).dropDuplicates()
    return df_reorder_and_rename

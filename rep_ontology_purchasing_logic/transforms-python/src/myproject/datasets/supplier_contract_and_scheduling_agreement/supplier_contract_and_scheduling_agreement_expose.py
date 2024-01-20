from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
import datetime


@transform_df(
    Output("ri.foundry.main.dataset.96799bbc-6dfd-4845-939f-5c8a5fbe549c"),
    ekko=Input("ri.foundry.main.dataset.4388de43-4e32-4fd8-93e1-daed3e7e033c"),
    ekpo=Input("ri.foundry.main.dataset.9a9d6d6b-c031-4e32-abc1-95968ad02c87"),
)
def transform(ekko, ekpo):
    # selecting fields from ekko table as the requirement
    ekko = (
            ekko . select('source', 'supplier_|_lifnr', 'purchasing_doc_type_|_bsart', 'purchasing_document_|_ebeln',
                          'purch_organization_|_ekorg', 'purchasing_group_|_ekgrp', 'terms_of_payment_|_zterm',
                          'incoterms_|_inco1', 'incoterms_part_2_|_inco2', 'validity_per_start_|_kdatb',
                          'validity_period_end_|_kdate',
                          )
           )
    # Merge two columns into one
    ekko = (
           ekko
           .withColumn("incoterms", F.concat_ws("_",  F.col("incoterms_|_inco1"), F.col("incoterms_part_2_|_inco2")))
           )
    # Applied the filter on ZRA and LPA
    ekko = (
           ekko
           .filter((F.col('purchasing_doc_type_|_bsart') == 'ZRA') | (F.col('purchasing_doc_type_|_bsart') == 'LPA'))
            )
    # selecting fields from ekpo table as the requirement
    ekpo = (
           ekpo
           .select('source', 'purchasing_document_|_ebeln', 'item_|_ebelp', 'material_|_matnr',
                   'material_group_|_matkl', 'plant_|_werks', 'shipping_instr_|_evers', 'outline_agreement_|_konnr',
                   'princagreement_item_|_ktpnr', 'eval_receipt_sett_|_xersy', 'tax_code_|_mwskz',
                   'confirmation_control_|_bstae', 'deletion_indicator_|_loekz', 'item_category_|_pstyp',
                   "net_weight_|_ntgew",
                   "gross_weight_|_brgew",
                   "unit_of_weight_|_gewei",
                   "volume_|_volum",
                   "planned_deliv_time_|_plifz",
                   "gr_processing_time_|_webaz",
                   "firm_zone_|_etfz1",
                   "tradeoff_zone_|_etfz2",
                   "binding_on_mrp_|_kzstu",
                   "release_creation_profile_|_abueb",
                   )
           )

    # Joined both tables on purchasing document no and source
    pur_document = (
        ekko
        .join(ekpo, ['purchasing_document_|_ebeln', 'source'], 'inner')
    )
    # Renaming as per naming Convention
    pur_document = pur_document.withColumnRenamed('plant_|_werks', 'plant_code')
    pur_document = pur_document.withColumnRenamed('material_|_matnr', 'material_number')

    # calculate is_schedulling_document_active property
    TODAY = datetime.datetime.today()
    pur_document = (
            pur_document
            .withColumn("material_number_trim", 
                        F.when(F.col("material_number").rlike("^[0-9]+$"),
                               F.regexp_replace(F.col("material_number"), r"^[0]+", ""))
                        .otherwise(F.col("material_number")))
            .withColumn("material_id",
                        F.concat_ws("_", "plant_code", "material_number_trim"))
            .withColumn("is_schedulling_document_active",
                        F.when((F.col("validity_per_start_|_kdatb") <= TODAY)
                               & (F.col("validity_period_end_|_kdate") >= TODAY)
                               & (F.col("deletion_indicator_|_loekz").isNull()), F.lit("Active"))
                         .otherwise(F.lit("Pasive")))
    )

    # creating primary key
    pur_document = pur_document.withColumn("pk", F.concat_ws('|', "source", "purchasing_document_|_ebeln",
                                                                  "item_|_ebelp")
                                           )
    pur_document = (pur_document.select("source",
                                        "plant_code",
                                        "purchasing_document_|_ebeln",
                                        "item_|_ebelp",
                                        "material_number",
                                        "supplier_|_lifnr",
                                        "purchasing_doc_type_|_bsart",
                                        "incoterms",
                                        "purch_organization_|_ekorg",
                                        "purchasing_group_|_ekgrp",
                                        "terms_of_payment_|_zterm",
                                        "validity_per_start_|_kdatb",
                                        "validity_period_end_|_kdate",
                                        "material_group_|_matkl",
                                        "shipping_instr_|_evers",
                                        "outline_agreement_|_konnr",
                                        "princagreement_item_|_ktpnr",
                                        "eval_receipt_sett_|_xersy",
                                        "tax_code_|_mwskz",
                                        "confirmation_control_|_bstae",
                                        "deletion_indicator_|_loekz",
                                        "item_category_|_pstyp",
                                        "is_schedulling_document_active",
                                        "net_weight_|_ntgew",
                                        "gross_weight_|_brgew",
                                        "unit_of_weight_|_gewei",
                                        "volume_|_volum",
                                        "incoterms_|_inco1",
                                        "incoterms_part_2_|_inco2",
                                        "planned_deliv_time_|_plifz",
                                        "gr_processing_time_|_webaz",
                                        "firm_zone_|_etfz1",
                                        "tradeoff_zone_|_etfz2",
                                        "binding_on_mrp_|_kzstu",
                                        "release_creation_profile_|_abueb",
                                        "material_id",
                                        "pk"
                                        ))

    return pur_document

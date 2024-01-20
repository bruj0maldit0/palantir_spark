from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from pyspark.sql.window import Window


@transform_df(
    Output("ri.foundry.main.dataset.c09063d8-1114-4250-ae5e-7b615542a20a"),
    ekpo=Input("ri.foundry.main.dataset.9a9d6d6b-c031-4e32-abc1-95968ad02c87"),
    ekko=Input("ri.foundry.main.dataset.4388de43-4e32-4fd8-93e1-daed3e7e033c"),
    ekkn=Input("ri.foundry.main.dataset.82658bff-c42b-484c-992d-5b02614199ee"),
    mara=Input('ri.foundry.main.dataset.9d3c8c78-ffab-4bd8-bbe0-580569e36b12'),
    supplier_lineage=Input('ri.foundry.main.dataset.7494e11e-e893-4811-939c-7c3608216b44'),
    currency_exchange=Input('ri.foundry.main.dataset.d811e144-8010-475c-b43c-0929046fa76a'),
    skat=Input('ri.foundry.main.dataset.433d4c2f-d563-439e-8795-fa4800a0c98c'),
    aufk=Input('ri.foundry.main.dataset.82eeec02-b2a4-4463-a8d1-b56e537a652a'),
    prps=Input('ri.foundry.main.dataset.99dc710a-2c05-4400-bee5-50da530784be'),
    cskt=Input("ri.foundry.main.dataset.eb11bfe4-9dfe-403c-8c54-a9b41999cc20"),
    plant=Input("ri.foundry.main.dataset.22460c68-a524-4226-8dc2-fb89962d88f3"),
    taxo_hierarchy=Input("ri.foundry.main.dataset.9a638bbb-bb8f-4939-bb50-b49baed25fe2"),
    )
def compute(ekpo, ekko, ekkn, mara, supplier_lineage, currency_exchange, skat, aufk, prps, cskt, plant, taxo_hierarchy):
    # ekpo
    ekpo = (
           ekpo
           .select(
                   'source', 'purchasing_document_|_ebeln', 'item_|_ebelp', 'order_quantity_|_menge',
                   'base_unit_of_measure_|_lmein', 'order_unit_|_meins', 'price_unit_|_peinh',
                   'gross_order_value_|_brtwr', 'deletion_indicator_|_loekz', 'net_order_price_|_netpr',
                   'quantity_conversion_|_bpumz', 'quantity_conversion_|_bpumn', 'last_changed_on_|_aedat',
                   'short_text_|_txz01', 'material_|_matnr', 'material_|_ematn', 'company_code_|_bukrs',
                   'plant_|_werks', 'storage_location_|_lgort', 'material_group_|_matkl', 'req_tracking_number_|_bednr',
                   'vendor_material_no_|_idnlf', 'purchasing_info_rec_|_infnr', 'item_category_|_pstyp',
                   'acct_assignment_cat_|_knttp', 'net_order_value_|_netwr', 'package_number_|_packno',
                   'requisitioner_|_afnam', 'purchase_requisition_|_banfn', 'item_of_requisition_|_bnfpo'
                   )
            )

    # renaming columns as per the contou ISS Report by Dominique
    ekpo = ekpo.withColumnRenamed('order_quantity_|_menge', 'order_quantity')
    ekpo = ekpo.withColumnRenamed('base_unit_of_measure_|_lmein', 'base_unit_key')
    ekpo = ekpo.withColumnRenamed('order_unit_|_meins', 'order_unit_key')
    ekpo = ekpo.withColumnRenamed('price_unit_|_peinh', 'price_unit')

    # removing "_" from material_group_|_matkl
    ekpo = ekpo.withColumn('purchase_category', F.regexp_replace("material_group_|_matkl", "-", ""))

    # ekko
    ekko = (
            ekko
            . select(
                     'purchasing_document_|_ebeln', 'terms_of_payment_|_zterm', 'supplier_|_lifnr',
                     'purch_organization_|_ekorg', 'purchasing_group_|_ekgrp', 'purchasing_doc_type_|_bsart',
                     'purch_doc_category_|_bstyp', 'document_date_|_bedat', 'currency_|_waers',
                     'created_on_|_aedat', 'document_category_|_pohf_type', 'your_reference_|_ihrez'
                     )
            )

    # Joined tables ekpo and ekko on purchasing document
    ekpo_ekko = (ekpo.join(ekko, ['purchasing_document_|_ebeln'], "left"))

    # ekkn
    ekkn = (
            ekkn
            .select(
                    "source", "purchasing_document_|_ebeln", "item_|_ebelp", "gl_account_|_sakto",
                    "cost_center_|_kostl", "order_|_aufnr", "wbs_element_|_ps_psp_pnr", "quantity_|_menge"
                    )
            )

    # joining on output of ekpo and ekko with ekkn
    ekpo_ekko_ekkn = (ekpo_ekko.join(ekkn, ['source', 'purchasing_document_|_ebeln', 'item_|_ebelp'], "left"))
    ekpo_ekko_ekkn = ekpo_ekko_ekkn.withColumn('price_key', F.concat("purchasing_document_|_ebeln", "item_|_ebelp"))
    ekpo_ekko_ekkn = ekpo_ekko_ekkn.dropDuplicates()

    # Taking max value of quantity_|_menge (percentage value in decimal per cost_center_|_kostl) for unique price key()
    glfilter_window = (
                       Window
                       .partitionBy("price_key")
                       .orderBy(F.col("quantity_|_menge").desc())
                       )
    ekpo_ekko_ekkn = (
                      ekpo_ekko_ekkn
                      .withColumn("glfilter", F.row_number().over(glfilter_window))
                      .filter(F.col("glfilter") == 1)
                      .drop("glfilter")
                      )

    # mara
    mara = (
            mara.select(
                        "source", "material_|_matnr", "manufacturer_part_no_|_mfrpn", "manufacturer_|_mfrnr",
                        "created_on_|_ersda", "last_change_|_laeda", "old_material_number_|_bismt",
                        "base_unit_of_measure_|_meins"
                        )
           )

    # joining on mara with output of ekpo, ekko, ekkn
    ekpo_ekko_ekkn_mara = (ekpo_ekko_ekkn.join(mara, ['material_|_matnr', 'source'], 'left'))
    ekpo_ekko_ekkn_mara = ekpo_ekko_ekkn_mara.dropDuplicates()

    # supplier lineage
    supplier_lineage = (
                        supplier_lineage
                        .select(
                                'vendor_id', 'vendor_name_1', 'vendor_name_2', 'generic_name', 'ultimate_name',
                                'ultimate_id', 'generic_id', 'supplier_id'
                                 )
                        )
    supplier_lineage = supplier_lineage.dropDuplicates()
    supplier_lineage = supplier_lineage.withColumnRenamed('vendor_id', 'supplier_|_lifnr')

    # joining supplier lineage with output of ekpo_ekko_ekkn_mara dataset
    supplier_lineage = (ekpo_ekko_ekkn_mara.join(supplier_lineage, ['supplier_|_lifnr'], 'left'))

    # currency exchange
    # filter data on single source and EUR Currency and date for valid source_dataset_name
    currency_exchange = (currency_exchange.filter(
                                                  (F.col('type') == 'FGER') &
                                                  (F.col('to_currency') == 'EUR') &
                                                  (F.col('date') > '2020-01-01')
                                                  )
                         )

    currency_exchange = currency_exchange.dropDuplicates()

    currency_exchange = (currency_exchange.select(
                                                  F.col('source'),
                                                  F.col('date').alias("document_date_|_bedat"),
                                                  F.col('from_currency').alias("currency_|_waers"),
                                                  F.col('exchange_rate')
                                                  )
                         )
    currency_exchange = currency_exchange.dropDuplicates()

    # joining currency_exchange with output of ekpo_ekko_ekkn_mara_supplier_lineage dataset
    currency_exchange = (
                        supplier_lineage
                        .join(currency_exchange, ['source', 'document_date_|_bedat', "currency_|_waers"], 'left')
                         )
    # creating new column for order EUR value
    currency_exchange = (
                         currency_exchange
                         .withColumn('Order_Value_EUR', F.when(F.col("currency_|_waers") == 'EUR',
                                                               F.col("net_order_value_|_netwr"))
                                                         .otherwise(F.round(F.col("exchange_rate") *
                                                                    F.col("net_order_value_|_netwr"), 2)
                                                                    )
                                     )
                        )
    # SKAT
    # Filtering Dataset on Account and language

    skat = (skat.filter((F.col('chart_of_accounts_|_ktopl') == 'FCOA') & (F.col('language_key_|_spras') == 'E')))
    skat = skat. select(
                        F.col('source'),
                        F.col('gl_account_|_saknr').alias('gl_account_|_sakto'),
                        F.col('short_text_|_txt20'), F.col('gl_acct_long_text_|_txt50'),
                        F.col('language_key_|_spras'), F.col('gl_long_text_|_mcod1')
                        )
    # windows function for unique description by gl_account_|_sakto
    windowSpec = (
                  Window.
                  partitionBy(["gl_account_|_sakto", "short_text_|_txt20", "gl_acct_long_text_|_txt50"])
                  .orderBy("gl_account_|_sakto", "short_text_|_txt20", "gl_acct_long_text_|_txt50")
                 )

    skat = (
                 skat
                 .withColumn("row_number1", F.row_number().over(windowSpec))
                 .filter(F.col('row_number1') == 1)
                 .drop("row_number1")
                 .dropDuplicates()
                 )

    #  joining skat with output of ekpo_ekko_ekkn_mara_supplier_lineage_currency_exchange dataset
    skat = currency_exchange.join(skat, ["source", 'gl_account_|_sakto'], 'left')

    # AUFK
    # Filtering data on language English
    aufk = aufk.filter((F.col('sddi_language_key_|_spras') == 'E'))

    aufk = aufk.select(
                       F.col('order_|_aufnr'),
                       F.col('sddi_language_key_|_spras').alias('language_key_|_spras'),
                       F.col('source'),
                       F.col('description_|_ktext')
                       )

    # windows function for Unique description in english language by order_|_aufnr
    windowSpec = (
                  Window
                  .partitionBy(["order_|_aufnr", "description_|_ktext"])
                  .orderBy("order_|_aufnr", "description_|_ktext")
                 )
    aufk = (
             aufk
             .withColumn("row_number", F.row_number().over(windowSpec))
             .filter(F.col('row_number') == 1)
             .drop("row_number")
           )

    # joining aufk with output of ekpo_ekko_ekkn_mara_supplier_lineage_currency_exchange_skat dataset
    aufk = skat.join(aufk, ['source', 'order_|_aufnr', 'language_key_|_spras'], 'left')

    # PRPS
    # Filtering data on language English
    prps = prps.filter((F.col('sddi_language_key_|_spras') == 'E'))

    prps = prps.select(
                       F.col('wbs_element_|_pspnr').alias('wbs_element_|_ps_psp_pnr'),
                       F.col('wbs_element_|_posid'),
                       F.col('description_|_post1'),
                       F.col('source'),
                       F.col('last_changed_on_|_aedat').alias('last_changed_on_|_aedat_prps')
                       )
    # Unique description per wbs_element_|_ps_psp_pnr
    windowSpec = (
                  Window.
                  partitionBy(["wbs_element_|_ps_psp_pnr", "description_|_post1"])
                  .orderBy("wbs_element_|_ps_psp_pnr", "description_|_post1")
                 )

    prps = (
            prps
            .withColumn("row_number", F.row_number().over(windowSpec))
            .filter(F.col('row_number') == 1)
            .drop("row_number")
            )
    prps = prps.dropDuplicates()

    # joining prps with output of ekpo_ekko_ekkn_mara_supplier_lineage_currency_exchange_skat_aufk datase
    prps = aufk.join(prps, ['source', 'wbs_element_|_ps_psp_pnr'], 'left')

    # cskt
    # Filtering data on language English
    cskt = cskt.filter(F.col('language_key_|_spras') == 'E')

    cskt = cskt.select(
                       F.col('language_key_|_spras'),
                       F.col('cost_center_|_kostl'),
                       F.col('description_|_ltext'),
                       F.col('valid_to_|_datbi')
                       )
    #  filtering the data on valid cost center
    test_window = (
                    Window
                    .partitionBy("cost_center_|_kostl")
                    .orderBy(F.col("valid_to_|_datbi").desc())
                    )
    cskt = (
            cskt
            .withColumn("row_number", F.row_number().over(test_window))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
            )
    # unique description per cost center
    windowSpec = (
                  Window.
                  partitionBy(["cost_center_|_kostl", "description_|_ltext"])
                  .orderBy("cost_center_|_kostl", "description_|_ltext")
                 )

    cskt = (
                 cskt
                 .withColumn("rank", F.row_number().over(windowSpec))
                 .filter(F.col("rank") == 1)
                 .drop("rank")
                )
    cskt = cskt.dropDuplicates()

    # joining cskt with output of ekpo_ekko_ekkn_mara_supplier_lineage_currency_exchange_skat_aufk_prps datase
    cskt = prps.join(cskt, ['language_key_|_spras', 'cost_center_|_kostl'], 'left')

    # plant
    # filtering data on valid plant for avoid duplicates
    plant = plant.filter(F.col("bau_is_valid") == True)

    plant = (
             plant.
             select(F.col('plant_code').alias('plant_|_werks'),
                    F.col('legal_site_name').alias('site_name'),
                    F.col('country_code_iso_alpha2').alias('site_country'),
                    F.col('bg_name'),
                    F.col('division_name'),
                    F.col('bau_activity'),
                    F.col('bau_short_name'),
                    F.col('bau_code'),
                    F.col('steering_unit'),
                    F.col('legal_site_status').alias("site_status")
                    )
                )
    # joining plant with output of ekpo_ekko_ekkn_mara_supplier_lineage_currency_exchange_skat_aufk_prps_cskt datase
    plant = cskt.join(plant, ['plant_|_werks'], 'left')

    # taxo_hierarchy
    taxo_hierarchy = (
                      taxo_hierarchy.
                      select(F.col('segment_code').alias('purchase_category'),
                             F.col('segment_name'),
                             F.col('subcommodity_name'),
                             F.col('commodity_code'),
                             F.col('commodity_name'),
                             F.col('segment_validity_status')
                             )
                       )

    # joining taxo_hierarchy with ekpo_ekko_ekkn_mara_supplier_lineage_currency_exchange_skat_aufk_prps_cskt_plant
    closed_po = plant.join(taxo_hierarchy, ['purchase_category'], 'left')

    # Renaming column name as per naming convention
    closed_po = (
                 closed_po
                 .withColumnRenamed('plant_|_werks', 'plant_code')
                 .withColumnRenamed('material_|_matnr', 'material_number')
                )
    # creating primary key
    closed_po = closed_po.withColumn("pk", F.concat_ws('|', "source", "plant_code", "purchasing_document_|_ebeln",
                                                       "item_|_ebelp", "supplier_id"))

    closed_po = plant.join(taxo_hierarchy, ['purchase_category'], 'left')

    # giving names to the fields as per naming convention
    closed_po = closed_po.withColumnRenamed('plant_|_werks', 'plant_code')\
                         .withColumnRenamed('material_|_matnr', 'material_number')

    # Filtering data on PO type purchasing_doc_type_|_.bsart
    closed_po = closed_po.filter(~F.col('purchasing_doc_type_|_bsart').isin(["ZRA", "LPA", "ZMK"]))

    # Pk
    closed_po = closed_po.withColumn("pk", F.concat_ws('|', "source", "plant_code", "purchasing_document_|_ebeln",
                                                       "item_|_ebelp", "supplier_id", "generic_id"))
    closed_po = (closed_po.select("source",
                                  "plant_code",
                                  "purchasing_document_|_ebeln",
                                  "item_|_ebelp",
                                  "material_number",
                                  "supplier_|_lifnr",
                                  "purchase_category",
                                  "language_key_|_spras",
                                  "cost_center_|_kostl",
                                  "wbs_element_|_ps_psp_pnr",
                                  "order_|_aufnr",
                                  "gl_account_|_sakto",
                                  "document_date_|_bedat",
                                  "currency_|_waers",
                                  "order_quantity",
                                  "base_unit_key",
                                  "order_unit_key",
                                  "price_unit",
                                  "gross_order_value_|_brtwr",
                                  "deletion_indicator_|_loekz",
                                  "net_order_price_|_netpr",
                                  "quantity_conversion_|_bpumz",
                                  "quantity_conversion_|_bpumn",
                                  "last_changed_on_|_aedat",
                                  "short_text_|_txz01",
                                  "material_|_ematn",
                                  "company_code_|_bukrs",
                                  "storage_location_|_lgort",
                                  "material_group_|_matkl",
                                  "req_tracking_number_|_bednr",
                                  "vendor_material_no_|_idnlf",
                                  "purchasing_info_rec_|_infnr",
                                  "item_category_|_pstyp",
                                  "acct_assignment_cat_|_knttp",
                                  "net_order_value_|_netwr",
                                  "package_number_|_packno",
                                  "terms_of_payment_|_zterm",
                                  "purch_organization_|_ekorg",
                                  "purchasing_group_|_ekgrp",
                                  "purchasing_doc_type_|_bsart",
                                  "purch_doc_category_|_bstyp",
                                  "created_on_|_aedat",
                                  "document_category_|_pohf_type",
                                  "quantity_|_menge",
                                  "price_key",
                                  "manufacturer_part_no_|_mfrpn",
                                  "manufacturer_|_mfrnr",
                                  "created_on_|_ersda",
                                  "last_change_|_laeda",
                                  "old_material_number_|_bismt",
                                  "base_unit_of_measure_|_meins",
                                  "vendor_name_1",
                                  "vendor_name_2",
                                  "generic_name",
                                  "ultimate_name",
                                  "ultimate_id",
                                  "generic_id",
                                  "supplier_id",
                                  "exchange_rate",
                                  "Order_Value_EUR",
                                  "short_text_|_txt20",
                                  "gl_acct_long_text_|_txt50",
                                  "gl_long_text_|_mcod1",
                                  "description_|_ktext",
                                  "wbs_element_|_posid",
                                  "description_|_post1",
                                  "last_changed_on_|_aedat_prps",
                                  "description_|_ltext",
                                  "valid_to_|_datbi",
                                  "site_name",
                                  "site_country",
                                  "bg_name",
                                  "division_name",
                                  "bau_activity",
                                  "bau_short_name",
                                  "bau_code",
                                  "steering_unit",
                                  "site_status",
                                  "segment_name",
                                  "subcommodity_name",
                                  "commodity_code",
                                  "commodity_name",
                                  "segment_validity_status",
                                  "requisitioner_|_afnam",
                                  "purchase_requisition_|_banfn",
                                  "item_of_requisition_|_bnfpo",
                                  "your_reference_|_ihrez",
                                  "pk"
                                  )
                 )
    return closed_po

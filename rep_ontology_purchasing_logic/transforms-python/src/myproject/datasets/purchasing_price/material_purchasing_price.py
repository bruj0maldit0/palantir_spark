from pyspark.sql import functions as F, Window as W
from transforms.api import Input, Output, transform
from myproject.datasets.purchasing_price.utils import *


@transform(
    purchasing_price_df=Output("ri.foundry.main.dataset.b0872cf6-bb82-4f9c-a09c-3d556b7622d4"),
    info_record_df=Output("ri.foundry.main.dataset.e1af6922-d942-4dee-9e1e-4462594389d3"),
    ekko=Input("ri.foundry.main.dataset.4388de43-4e32-4fd8-93e1-daed3e7e033c"),
    ekpo=Input("ri.foundry.main.dataset.9a9d6d6b-c031-4e32-abc1-95968ad02c87"),
    lfa1=Input("ri.foundry.main.dataset.bbed6606-e689-404c-bb28-4afd38cf5c57"),
    konh=Input("ri.foundry.main.dataset.e9aac48d-df18-47bf-a6d8-de638d892cbc"),
    konp=Input("ri.foundry.main.dataset.bba21968-a1a7-4478-bef6-d86ddc91a7e8"),
    t685t=Input("ri.foundry.main.dataset.d2d948f4-293b-4d17-8875-de678bade617"),
    a017=Input("ri.foundry.main.dataset.ab5d6de4-abe4-4f32-8fc2-b23f709d7a87"),
    a016=Input("ri.foundry.main.dataset.6fa34469-dfbc-474a-80b5-9b8038e3d15b"),
    tcurx=Input("ri.foundry.main.dataset.b1e93127-c7c0-4f5a-b31c-b6472da9cbd1")

)
def compute(ekko, ekpo, lfa1, konh, konp, t685t,  a017, a016, purchasing_price_df, info_record_df, tcurx):
    ekko = ekko.dataframe()
    ekpo = ekpo.dataframe()
    konp = konp.dataframe()
    konh = konh.dataframe()
    lfa1 = lfa1.dataframe()
    t685t = t685t.dataframe()
    a017 = a017.dataframe()
    a016 = a016.dataframe()
    tcurx = tcurx.dataframe()
    konh = (
        konh
        .select(*COL_TO_SELECT_KONH, F.concat(F.col('usage_|_kvewe'), F.col('table_|_kotabnr')).alias('original_dataset'))
        .drop(F.col('usage_|_kvewe')).drop(F.col('table_|_kotabnr'))
    )
    konp = (
        konp.select(*COL_TO_SELECT_KONP)
    )

    # joining KONP & KONH on source & knumh

    price_condition = (
        konp.join(konh, ['condition_record_no_|_knumh', 'source'], how='inner')
        .dropDuplicates()
    )

    # selecting columns in business order

    price_condition = (
        price_condition.select(*COL_ORDER_PRICE_CONDITION)
    )

    # FILTERING t685t on language_key_|_spras,usage_|_kvewe & application_|_kappl

    t685t = (
        t685t
        .filter((t685t['language_key_|_spras'] == 'E') & (t685t['usage_|_kvewe'] == 'A') &
                (t685t['application_|_kappl'] == 'M'))
        .select(*COL_TO_SELECT_T685T)
    )

    # joining price_condition with t685t on source & kschl to get all records from price_condititon

    price_condition_with_description = (
        price_condition
        .join(t685t, ['source', 'condition_type_|_kschl', 'application_|_kappl'], how='left')
        .dropDuplicates()
    )

    # adding pkey

    price_condition_with_description = (
        price_condition_with_description
        .withColumn('pkey', F.concat_ws(
            '|',
            F.col('condition_record_no_|_knumh'),
            F.col('sequentno_of_cond_|_kopos'),
            F.col('source')
        )))

    # selecting columns in business order.

    price_condition_with_description = (
        price_condition_with_description.select(*COL_ORDER_PRICE_CONDITION_With_DESCRIPTION)
    )
    price_condition_with_description = (
        price_condition_with_description
        .filter((F.col('original_dataset') == 'A016') | ((F.col('original_dataset') == 'A017')))
        .filter(F.col('application_|_kappl') == 'M')
    )

    # selecting a016 columns

    a016 = (
        a016.select(*COL_TO_SELECT_A016)
    )

    # inner joining price_condition_with_description with a016 on client_|_mandt, application_|_kappl,...
    # ...condition_record_no_|_knumh, and source to get purchasing_contract_price + dropping duplicates

    purchasing_contract_price = (
        price_condition_with_description
        .join(a016, ['condition_record_no_|_knumh', 'source', 'client_|_mandt', 'application_|_kappl'], how='inner')
        .dropDuplicates()
    )

    # filtering price_condition_with_description on original_dataset = A016 and application = M
    # renaming column application

    purchasing_contract_price = (
        purchasing_contract_price.select(*COL_ORDER_PURCHASING_CONTRACT_PRICE)
    )

    # Purchasing consignment price logic

    a017 = (
        a017.select(*COL_TO_SELECT_A017)
    )
    purchasing_consignment_price = (
        price_condition_with_description
        .join(a017, ['condition_record_no_|_knumh', 'source'], how="inner").dropDuplicates()
    )
    purchasing_consignment_price = (
        purchasing_consignment_price.select(*COL_ORDER_PURCHASING_CONSIGNMENT_PRICE)
    )

    # Purchasing contract logic

    ekko = (
        ekko
        .select(*COL_TO_SELECT_EKKO)
    )

    # selecting ekpo columns

    ekpo = (
        ekpo
        .select(*COL_TO_SELECT_EKPO).withColumnRenamed("item_category_|_pstyp", "info_record_category_|_esokz")
    )

    # selecting lfa1 columns adding new column intercompany which should be true/false with "true" if ktokk = Z007

    lfa1 = (
        lfa1
        .select(*COL_TO_SELECT_LFA1)
        .withColumn('intercompany', F.when(F.col('account_group_|_ktokk') == 'Z007', 'True').otherwise('False'))
    )

    # inner joining ekko with ekpo on purchasing_document_|_ebeln, and source to get pur_document

    pur_document = (
        ekko
        .join(ekpo, ['purchasing_document_|_ebeln', 'source'], how='inner')
    )

    # left joining pur_document with lfa1 on vendor_|_lifnr, and source to get purchasing_scheduling & dropping duplicates

    purchasing_contract = (
        pur_document
        .join(lfa1, ['supplier_|_lifnr', 'source'], how='left')
        .filter(F.col('purchasing_doc_type_|_bsart') == 'ZRA')
        .dropDuplicates()
    )
    purchasing_contract = (
        purchasing_contract.select(
            *COL_ORDER_PURCHASING_CONTRACT).withColumnRenamed("short_text_|_txz01", "material_description_|_txz01")
    )
    condition = (
        (F.col('purchasing_document_|_evrtn') == F.col('purchasing_document_|_ebeln')) &
        (F.col('item_|_evrtp') == F.col('item_|_ebelp')) &
        (purchasing_contract['source'] == purchasing_contract_price['source'])
    )

    # inner joining purchasing_contract with purchasing_contract_price on condition to get purchasing_price
    # dropping column found in key from table purchasing_contract_price + dropping duplicates

    purchasing_price = (
        purchasing_contract
        .join(purchasing_contract_price, condition, how='inner')
        .drop(purchasing_contract_price['source'])
        .dropDuplicates()
    )

    # joining purchasing_consignment_price with purchasing_contract on lifnr,werks & material_|_matnr

    purchasing_consignment_contract = (
        purchasing_contract.join(purchasing_consignment_price, [
                                 "material_|_matnr", "plant_|_werks", "supplier_|_lifnr", "source", "info_record_category_|_esokz"], how="inner")
        .dropDuplicates()
    )

    #  creating missing columns with None values in order union purchasing_price & purchasing_consignment_contract

    diff1 = [c for c in purchasing_price.columns if c not in purchasing_consignment_contract.columns]
    diff2 = [c for c in purchasing_consignment_contract.columns if c not in purchasing_price.columns]
    df = (
        purchasing_consignment_contract.select('*', *[F.lit(None).alias(c) for c in diff1])
        .unionByName(purchasing_price.select('*', *[F.lit(None).alias(c) for c in diff2]))
    )

    # adding pkey column by concatenating concatenate knumh|kopos|source

    df = (
        df.withColumn("material_|_matnr_trimmed", F.regexp_replace("material_|_matnr", r'^[0]*', "")).
        withColumn("supplier_|_lifnr_trimmed", F.regexp_replace("supplier_|_lifnr", r'^[0]*', ""))

    )

    # separating out duplicate values from A016 and A017 in two different datasets

    w = W.partitionBy("purchasing_document_|_ebeln", "item_|_ebelp", "material_|_matnr", "supplier_|_lifnr").orderBy("original_dataset")
    df = df.withColumn("rank", F.dense_rank().over(w))

    # divide data between MPP & Info records on following conditions :
    # keep all A016 records , keep only info = 2 for A017 in MPP and move rest in info
    df = df.withColumn("record_type", F.when(((F.col("original_dataset") == "A016") | (
        (F.col("original_dataset") == "A017") & (F.col("info_record_category_|_esokz") == 2))), "MPP").otherwise("Info"))

    # decimal correction for different currencies.
    tcurx = tcurx.select(
        "source", "decimals_|_currdec", F.col("_|_currkey").alias("condition_currency_|_konwa")
    )
    df = df.join(tcurx, ["source", "condition_currency_|_konwa"], how="left")
    df = df.withColumn(
        "rate_|_kbetr", F.when(
            F.col("decimals_|_currdec").isNotNull(), F.col("rate_|_kbetr") / (F.pow(F.lit(10), (F.col("decimals_|_currdec") - F.lit(2))))  # KONP has 2 decimals
        ).otherwise(F.col("rate_|_kbetr"))
    )

    # filtering data based on record type
    df_a016 = df.filter(F.col("record_type") == "MPP")
    df_a017 = df.filter(F.col("record_type") == "Info")

    # selecting required columns in bussiness order and sorting them
    df_a016 = (
        df_a016.withColumnRenamed("plant_|_werks", "plant_code")
        .withColumnRenamed("material_|_matnr", "material_number")
        .withColumnRenamed("material_|_matnr_trimmed", "material_number_trim")
        .withColumnRenamed("supplier_|_lifnr_trimmed", "supplier_|_lifnr_trim")
        .withColumn("foreign_key_csa", F.concat_ws('|', "source", "purchasing_document_|_ebeln", "item_|_ebelp"))
        .withColumn('pk', F.concat_ws(
            '|',
            F.col("pkey"),
            F.col("purchasing_document_|_ebeln"),
            F.col("item_|_ebelp"),
            F.col("valid_to_|_datbi")
        ))
    )
    df_a017 = (
        df_a017.withColumnRenamed("plant_|_werks", "plant_code")
        .withColumnRenamed("material_|_matnr", "material_number")
        .withColumnRenamed("material_|_matnr_trimmed", "material_number_trim")
        .withColumnRenamed("supplier_|_lifnr_trimmed", "supplier_|_lifnr_trim")
        .withColumn('pk', F.concat_ws(
            '|',
            F.col("pkey"),
            F.col("purchasing_document_|_ebeln"),
            F.col("item_|_ebelp"),
            F.col("valid_to_|_datbi")
        ))
    )
    df_a016 = (
        df_a016.select(*COL_ORDER_PURCHASING_PRICE)
        .orderBy(["purchasing_document_|_ebeln", "item_|_ebelp", "valid_from_|_datab"]).dropDuplicates()
    )
    df_a017 = (
        df_a017.select(*COL_ORDER_INFO_RECORDS)
        .orderBy(["purchasing_document_|_ebeln", "item_|_ebelp", "valid_from_|_datab"]).dropDuplicates()
    )
    # writing final dataframes to datasets.

    purchasing_price_df.write_dataframe(df_a016)
    info_record_df.write_dataframe(df_a017)

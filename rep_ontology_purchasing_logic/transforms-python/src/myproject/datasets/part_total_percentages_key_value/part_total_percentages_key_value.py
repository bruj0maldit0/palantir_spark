from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, configure

from myproject.datasets.part_total_percentages_key_value.constants import PURCHASE_REFERENCE_KEY, PART_SUBSTANCE
from myproject.datasets.part_total_percentages_key_value.utils import clean_column_names

my_profile = ['DRIVER_MEMORY_OVERHEAD_LARGE',
              'EXECUTOR_MEMORY_OVERHEAD_MEDIUM',
              'AUTO_BROADCAST_JOIN_DISABLED',
              'DYNAMIC_ALLOCATION_MAX_64']


@configure(profile=my_profile)
@transform_df(
    Output('ri.foundry.main.dataset.69ad619e-8855-456f-b6ed-1be777671989'),
    df_parts=Input('ri.foundry.main.dataset.f638c4f9-a609-4fe3-9074-1b35c049fe07'),
    df_sap_imds=Input('ri.foundry.main.dataset.41a9ef3b-5a5a-447a-bfd0-1d642cd307cf'),
    df_substances=Input('ri.foundry.main.dataset.3f472360-16cd-49ff-a46d-9854db9cbb81'),
    df_parts_name_descr=Input('ri.foundry.main.dataset.d02f4a72-7b49-48ac-ab5d-d1180efff1c5')
)
def get_part_total_percentages(df_parts, df_sap_imds, df_substances, df_parts_name_descr):
    df_parts = (
        df_parts
        .select(
            F.col('id'),
            F.col('measured_weight'),
            F.col('path'),
            F.col('last_node'),
            F.col('substance_node_id'),
            F.col('substance_percentage')
        )
    )

    df_sap_imds = (
        df_sap_imds
        .select(
            F.col('part_or_item_number'),
            F.col('node_id'),
            F.col('module_id'),
            F.col('bg'),
            F.col('purchase_reference_key')
        )
        .withColumn('id', F.concat_ws('-', F.col('module_id'), F.col('node_id')))
        .distinct()
    )

    df_substances = (
        df_substances
        .withColumn('synonym', F.regexp_replace('synonym', '[^A-Za-z0-9_\\s]+', ''))
        .withColumn('synonym', F.regexp_replace('synonym', '_+', ' '))
        .withColumn('synonym', F.lower(F.col('synonym')))
        .withColumn('synonym', F.regexp_replace('synonym', '_', ' '))
        .withColumn('synonym', F.trim(F.col('synonym')))
        .withColumn('synonym', F.regexp_replace('synonym', ' ', '_'))
        .filter(F.col('synonym').isNotNull())
        .select(
            F.col('node_id_of_the_basic_substance'),
            F.col('synonym')
        )
    )

    df_parts_name_descr = (
        df_parts_name_descr
        .select(
            F.col('module_id').alias("module_id_pnd"),
            F.col('node_id').alias("node_id_pnd"),
            F.col('part_or_item_number').alias("part_or_item_number_pnd"),
            F.col('article_name'),
            F.col('description')
        )
        .withColumn('article_name_descr', F.when(F.col("article_name").isNull(), F.col("description")))
        .distinct()
    )

    df_forbidden_substance = df_parts.join(df_sap_imds, 'id', how='left')
    df_forbidden_substance = (
        df_forbidden_substance
        .join(
            df_substances, on=F.col('substance_node_id') == F.col('node_id_of_the_basic_substance'),
            how='left')
        .join(
            df_parts_name_descr,
            on=(df_sap_imds.module_id == df_parts_name_descr.module_id_pnd)
            & (df_sap_imds.node_id == df_parts_name_descr.node_id_pnd)
            & (df_sap_imds.part_or_item_number == df_parts_name_descr.part_or_item_number_pnd),
            how='left')
        .drop('last_node')
        .withColumn('synonym', F.concat_ws('_', F.lit('part_substance'), F.col('synonym')))
    )

    df_forbidden_substance = (
        df_forbidden_substance.select(
            F.col('purchase_reference_key'),
            F.col('synonym').alias('substance'),
            F.col('measured_weight'),
            F.col('substance_percentage').alias('percentage'),
            F.col('part_or_item_number'),
            F.col('node_id'),
            F.col('module_id'),
            F.col('bg'),
            F.col('substance_node_id'),
            F.col('article_name_descr')
        )
        .groupby('purchase_reference_key', 'substance', 'bg', 'node_id', 'module_id', 'part_or_item_number', 'substance_node_id', 'article_name_descr')
        .agg(F.sum('percentage').alias('substance_percentage'),
             F.first('measured_weight').alias('measured_weight')
             )
        .withColumn('substance_weight', F.col('substance_percentage') * F.col('measured_weight'))
        .fillna(0)
    )

    return df_forbidden_substance

from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from pyspark.sql.window import Window


@transform_df(
    Output("ri.foundry.main.dataset.08e66588-7b6b-4391-ae7d-bae85cbd1b45",
           checks=[Check(E.primary_key("pk"), 'Primary Key Check', on_error="FAIL")]),
    ippro_fcm_var_df=Input("ri.foundry.main.dataset.674bddfb-134e-457a-ad9f-0f79fff78c47"),
    ippro_fcm_nppv_df=Input("ri.foundry.main.dataset.131b771f-032c-47d0-9e68-943dd0d33185"),
    ippro_fcm_prog_df=Input("ri.foundry.main.dataset.80f82a56-412b-4559-883a-f36b6c4defa6"),
    ippro_fcm_bopgen_df=Input("ri.foundry.main.dataset.97fe0eab-03c2-450a-92d0-6f6aa2c56a3a"),
    plant_df=Input("ri.foundry.main.dataset.d9b3161d-2a44-4eba-a8fa-3542bf8c7213"),
)
def compute(ippro_fcm_var_df, ippro_fcm_nppv_df, ippro_fcm_prog_df, ippro_fcm_bopgen_df, plant_df):
    # Select properties from plant_df
    plant_df = (
        plant_df
        .select("plant_code", "bg_name", "source")
    )

    # Build part_df by joining data from ippro_fcm_bopgen_df & ippro_fcm_prog_df
    part_df = (
        ippro_fcm_bopgen_df
        .join(ippro_fcm_prog_df, ["programid"], how="left")
    )

    # Joining ippro_fcm_nppv_df & ippro_fcm_var_df
    ippro_fcm_nppv_df = (
        ippro_fcm_nppv_df
        .join(ippro_fcm_var_df, ["programid", "variantid"], how="left")
    )
    # Group by partid, programid to calculate part_yearly_volume
    ippro_fcm_nppv_df = (
        ippro_fcm_nppv_df
        .groupBy("partid", "programid")
        .agg(F.sum(F.col("number_parts_per_variant") * F.col("yearly_volume_cars_average")).alias("part_yearly_volume"))
    )

    # Join part_df & ippro_fcm_nppv_df to add up the information to part_df
    part_df = (
        part_df
        .join(ippro_fcm_nppv_df, ["programid", "partid"], how="left")
    )

    # Calculate part_lifetime_volume & part_lifetime_spend in Local & Euro currency
    part_df = (
        part_df
        .withColumn("part_lifetime_volume", F.col("part_yearly_volume") * F.col("lifetime_years"))
        .withColumn("part_lifetime_spend", F.col("part_lifetime_volume") * F.col("ibp_bp_dap_incl_ea"))
        .withColumn("part_lifetime_spend_euro", F.col("part_lifetime_spend") * F.col("exchange_rate"))
    )

    # Partition by "part_number" ascending order
    part_window = Window.partitionBy("part_number").orderBy("part_number")
    # Summing up by "part_number"
    part_df = (
            part_df
            .withColumn("ibp_bp_dap_incl_ea", F.sum('ibp_bp_dap_incl_ea').over(part_window))
            .withColumn("supplier_net_weight_kg_or_lb", F.sum('supplier_net_weight_kg_or_lb').over(part_window))
            .withColumn("gross_weight_kg_or_lb", F.sum('gross_weight_kg_or_lb').over(part_window))
            .withColumn("lifetime_years", F.sum('lifetime_years').over(part_window))
            .withColumn("part_yearly_volume", F.sum('part_yearly_volume').over(part_window))
            .withColumn("part_lifetime_volume", F.sum('part_lifetime_volume').over(part_window))
            .withColumn("part_lifetime_spend", F.sum('part_lifetime_spend').over(part_window))
            .withColumn("part_lifetime_spend_euro", F.sum('part_lifetime_spend_euro').over(part_window))
            .withColumn("row_num", F.row_number().over(part_window))
            .filter(F.col("row_num") == 1)
    )

    # Extracting plant_code from "part_plant"
    part_df = (
        part_df
        .withColumn("plant_code", F.substring("part_plant", -4, 4))
        .withColumn("plant_code", F.when(F.col("plant_code").isin(["INED"]), F.lit(""))
                                   .otherwise(F.col("plant_code")))
    )

    # Adding properties from plant_df
    part_df = (
        part_df
        .join(plant_df, ["plant_code"], how="left")
    )

    # Create pk column
    part_df = (
        part_df.withColumn("pk", F.col("part_number"))
    )

    return part_df

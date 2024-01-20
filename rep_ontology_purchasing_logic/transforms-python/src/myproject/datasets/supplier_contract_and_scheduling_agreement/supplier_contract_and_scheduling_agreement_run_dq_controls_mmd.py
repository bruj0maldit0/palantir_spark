"Script to generate the dataset with all controls "

from transforms.api import transform_df, Input, Output, Check, configure
from transforms import expectations as E
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from control import dq_control_constants
from myproject.datasets.supplier_contract_and_scheduling_agreement import supplier_contract_and_scheduling_agreement_registry as control_registry


DATASET_NAME = "contract_&_purchasing_scheduling_agreement"
PRIMARY_KEY = "pk"
RID_DICT = {
    "dataset_with_all_controls": "ri.foundry.main.dataset.e3031a5b-be93-4247-83da-43fb6b1b88e1",
    "dataset_with_control_dict": dq_control_constants.control_dict_rid,
    "ontology_dataset": "ri.foundry.main.dataset.96799bbc-6dfd-4845-939f-5c8a5fbe549c",
    "plant_dataset": "ri.foundry.main.dataset.d9b3161d-2a44-4eba-a8fa-3542bf8c7213"
}


@configure(profile=["DRIVER_MEMORY_MEDIUM", "DRIVER_CORES_MEDIUM", "EXECUTOR_MEMORY_SMALL",
                    "EXECUTOR_CORES_SMALL"])

@transform_df(
    Output(RID_DICT["dataset_with_all_controls"],
           checks=[
                      Check(E.primary_key(PRIMARY_KEY), 'Primary Key Check', on_error='FAIL'),
                      Check(E.col('plant_code').non_null(), 'Plant Code Not Null Check', on_error='FAIL'),
                      Check(E.col('plant_name').non_null(), 'Plant Name Not Null Check', on_error='FAIL'),
                      Check(E.col('bg_name').non_null(), 'BG Not Null Check', on_error='FAIL'),
                      Check(E.col('division_name').non_null(), 'Division Name Not Null Check', on_error='FAIL'),
                      Check(E.col('subdivision_name').non_null(), 'Sub-Division Name Not Null Check', on_error='FAIL'),
                      ]

           ),
    input_dataset=Input(RID_DICT["ontology_dataset"]),
    control_dict=Input(RID_DICT["dataset_with_control_dict"]),
    plant_details=Input(RID_DICT["plant_dataset"]),
    supplier_lineage= Input("ri.foundry.main.dataset.7494e11e-e893-4811-939c-7c3608216b44") ,# input for few columns, 
    plant_production_material = Input("ri.foundry.main.dataset.7ba0a89e-d647-4c90-ab65-4a037597b205") # input for few columns, 
)
def my_compute_function(ctx, input_dataset, control_dict, plant_details,supplier_lineage,plant_production_material):

    plant_details = plant_details.select(F.col("plant_code"),
                                         F.col("plant_name"),
                                         F.col("bg_name"),
                                         F.col("division_name"),
                                         F.col("subdivision_name"),
                                         F.col("bau_short_name"),
                                         F.col("bau_activity"))
    input_dataset = input_dataset.join(plant_details, ['plant_code'], 'inner')

    input_dataset = input_dataset.withColumnRenamed("source", "source_system")
    supplier_lineage = supplier_lineage.select("vendor_id",
                                               "vendor_blocked",
                                               "vendor_account_group")
    supplier_lineage = supplier_lineage.drop_duplicates(["vendor_id"])
    input_dataset = input_dataset.join(supplier_lineage, input_dataset['supplier_|_lifnr'] == supplier_lineage['vendor_id'], 'left')
    plant_production_material = plant_production_material.select("material_type",
                                                                 "material_number",
                                                                 "plant_code",
                                                                 "is_obsolete_material",)
    input_dataset = input_dataset.join(plant_production_material, ['plant_code', 'material_number'], 'left')
    # Using self join for multiple controls
    input_dataset = input_dataset.withColumn("concat_purchasing", F.concat(F.col("purchasing_document_|_ebeln"), F.col("item_|_ebelp")))
    input_dataset = input_dataset.withColumn("concat_outline", F.concat(F.col("outline_agreement_|_konnr"), F.col("princagreement_item_|_ktpnr")))

    df2 = input_dataset.select(F.col("concat_purchasing").alias("new_concat_purchasing"),
                               F.col("deletion_indicator_|_loekz").alias("new_deletion_indicator_|_loekz"),
                               F.col("shipping_instr_|_evers").alias("new_shipping_instr_|_evers"),
                               F.col("eval_receipt_sett_|_xersy").alias("new_eval_receipt_sett_|_xersy"),
                               F.col("tax_code_|_mwskz").alias("new_tax_code_|_mwskz"),
                               F.col("incoterms").alias("new_incoterms"),
                               F.col("terms_of_payment_|_zterm").alias("new_terms_of_payment_|_zterm")
                               ).dropDuplicates()
    condition = (input_dataset["concat_outline"] == df2["new_concat_purchasing"])
    input_dataset = input_dataset.join(df2, condition, how="left").dropDuplicates()

    control_list = control_dict.collect()
    combine_result = ctx.spark_session.createDataFrame([], StructType([]))
    bg_list = [column.split("_")[1] for column in control_dict.columns if column.startswith("BG_")]

    for bg in bg_list:
        input_dataset_copy = input_dataset
        input_dataset_copy = input_dataset_copy.filter(F.col("bg_name") == bg)

        for control in control_list:
            # check for each control production values
            if int(control.implementation_status.split("-")[0].strip()) > 3 and control.dataset == DATASET_NAME:
                control_name = control.control_name
                control_dict1 = control.asDict()
                bg_columns_list = [key.split("_")[1] for key,value in control_dict1.items() if key.startswith("BG_") if value]
                if bg in bg_columns_list:
                    H = "control_registry.control_" + control_name + "(input_dataset_copy, control_name)"
                    input_dataset_copy = eval(H)  # call to execute H # noqa
        combine_result = combine_result.unionByName(input_dataset_copy, allowMissingColumns=True)

    # add column control date
    combine_result = combine_result.withColumn(
      dq_control_constants.CONTROL_RUN_DATE, F.lit(F.current_timestamp())  # add control date
    )
    list_col = ["new_concat_purchasing", "concat_outline", "concat_purchasing", "new_eval_receipt_sett_|_xersy", "new_deletion_indicator_|_loekz", "new_shipping_instr_|_evers", "new_eval_receipt_sett_|_xersy", "new_tax_code_|_mwskz", "new_incoterms", "new_terms_of_payment_|_zterm"]
    combine_result = combine_result.drop(*list_col)
    return combine_result

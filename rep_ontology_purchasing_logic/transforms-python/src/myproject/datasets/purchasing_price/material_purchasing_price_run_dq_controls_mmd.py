"Script to generate the dataset with all controls"

from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from control import dq_control_constants
from myproject.datasets.purchasing_price import material_purchasing_price_registry as control_registry


DATASET_NAME = "material_purchasing_price"
PRIMARY_KEY = "pk"
RID_DICT = {
    "dataset_with_all_controls": "ri.foundry.main.dataset.ec115580-c7ab-483f-82c4-50b379d9f281",
    "dataset_with_control_dict": dq_control_constants.control_dict_rid,
    "ontology_dataset": "ri.foundry.main.dataset.b0872cf6-bb82-4f9c-a09c-3d556b7622d4",
    "plant_dataset": "ri.foundry.main.dataset.d9b3161d-2a44-4eba-a8fa-3542bf8c7213"
}


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
)
def my_compute_function(ctx, input_dataset, control_dict, plant_details):

    plant_details = plant_details.select(F.col("plant_code"),
                                         F.col("plant_name"),
                                         F.col("bg_name"),
                                         F.col("division_name"),
                                         F.col("subdivision_name"),
                                         F.col("bau_short_name"))
    input_dataset = input_dataset.join(plant_details, ['plant_code'], 'inner')
    input_dataset = input_dataset.withColumnRenamed("source", "source_system")

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

    return combine_result

"Script to generate the dataset with all controls"

from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from control import dq_control_constants
from myproject.datasets.taxo_hierarchy import taxo_hierarchy_registry as control_registry


DATASET_NAME = "taxo_hierarchy"
PRIMARY_KEY = "segment_code"
RID_DICT = {
    "dataset_with_all_controls": "ri.foundry.main.dataset.7ba2786c-82e9-431a-8625-da13183c0c11",
    "dataset_with_control_dict": dq_control_constants.control_dict_rid,
    "ontology_dataset": "ri.foundry.main.dataset.9a638bbb-bb8f-4939-bb50-b49baed25fe2",
    "plant_dataset": "ri.foundry.main.dataset.d9b3161d-2a44-4eba-a8fa-3542bf8c7213"
}


@transform_df(
    Output(RID_DICT["dataset_with_all_controls"],
           checks=[
                      Check(E.primary_key(PRIMARY_KEY), 'Primary Key Check', on_error='FAIL'),
                    #   Check(E.col('plant_code').non_null(), 'Plant Code Not Null Check', on_error='FAIL'),
                    #   Check(E.col('plant_name').non_null(), 'Plant Name Not Null Check', on_error='FAIL'),
                    #   Check(E.col('bg_name').non_null(), 'BG Not Null Check', on_error='FAIL'),
                    #   Check(E.col('division_name').non_null(), 'Division Name Not Null Check', on_error='FAIL'),
                    #   Check(E.col('subdivision_name').non_null(), 'Sub-Division Name Not Null Check', on_error='FAIL'),
                      ]

           ),
    input_dataset=Input(RID_DICT["ontology_dataset"]),
    control_dict=Input(RID_DICT["dataset_with_control_dict"]),
    plant_details=Input(RID_DICT["plant_dataset"]),
)
def my_compute_function(ctx, input_dataset, control_dict, plant_details):
    control_list = control_dict.collect()  # collect list of controls from fusion registry # noqa

    # Coding the granular controls:
    for control in control_list:
        # check for each control production values
        if int(control.implementation_status.split("-")[0].strip()) > 3 and control.dataset == DATASET_NAME:
            control_name = control.control_name
            # run function control: plant_registry.control_ + control name + dataset
            H = "control_registry.control_" + control_name + "(input_dataset, control_name)"
            # call function H
            input_dataset = eval(H)  # call to execute H # noqa

 #   add column control date
    input_dataset = input_dataset.withColumn(
     dq_control_constants.CONTROL_RUN_DATE, F.lit(F.current_timestamp())  # add control date
    )
    
    input_dataset = (
        input_dataset
        .withColumn("source_system", F.lit(None).cast("string"))
        .withColumn("plant_code", F.lit(None).cast("string"))
        .withColumn("plant_name", F.lit(None).cast("string"))
        .withColumn("bg_name", F.lit(None).cast("string"))
        .withColumn("division_name", F.lit(None).cast("string"))
        .withColumn("subdivision_name", F.lit(None).cast("string"))
        .withColumn("bau_short_name", F.lit(None).cast("string"))
                        )

    return input_dataset



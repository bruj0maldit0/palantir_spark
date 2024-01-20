from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F

from control.dq_control_utils import gen_nb_ctrl_lines


@transform_df(
    Output("ri.foundry.main.dataset.ba846674-c6fd-4b04-8f81-56125734e12b"),
    all_controls=Input("ri.foundry.main.dataset.f5ce195a-3575-4159-8920-f8fc27b4e1a8"),
    dq_remediation=Input("ri.foundry.main.dataset.24ae5a70-2973-49be-b564-4def6cf2fc33"),
)
def compute(ctx, all_controls, dq_remediation):
    all_controls = all_controls.withColumn("plant_code", F.lit("customer_plant_code"))
    dq_remediation = dq_remediation.withColumn("plant_code", F.lit("customer_plant_code"))
    nb_ctrl_lines = gen_nb_ctrl_lines(ctx, all_controls, dq_remediation)
    nb_ctrl_lines = nb_ctrl_lines.withColumn("plant_code", F.lit(None).cast("string"))
    return nb_ctrl_lines
from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F

from control.dq_control_utils import gen_nb_ctrl_lines


@transform_df(
    Output("ri.foundry.main.dataset.612aef18-e1e1-435a-9780-9302cd7eceb9"),
    all_controls=Input("ri.foundry.main.dataset.7ba2786c-82e9-431a-8625-da13183c0c11"),
    dq_remediation=Input("ri.foundry.main.dataset.e73f3869-ad75-4112-9825-f826f28354c8"),
)
def compute(ctx, all_controls, dq_remediation):
    all_controls = all_controls.withColumn("plant_code", F.lit("customer_plant_code"))
    dq_remediation = dq_remediation.withColumn("plant_code", F.lit("customer_plant_code"))
    nb_ctrl_lines = gen_nb_ctrl_lines(ctx, all_controls, dq_remediation)
    nb_ctrl_lines = nb_ctrl_lines.withColumn("plant_code", F.lit(None).cast("string"))
    return nb_ctrl_lines
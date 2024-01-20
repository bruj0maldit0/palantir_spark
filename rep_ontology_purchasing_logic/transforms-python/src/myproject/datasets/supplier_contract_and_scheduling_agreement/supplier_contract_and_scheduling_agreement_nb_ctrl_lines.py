from transforms.api import transform_df, Input, Output

from control.dq_control_utils import gen_nb_ctrl_lines


@transform_df(
    Output("ri.foundry.main.dataset.6642ba81-deef-4c20-8bb8-f37e18c144c2"),
    all_controls=Input("ri.foundry.main.dataset.e3031a5b-be93-4247-83da-43fb6b1b88e1"),
    dq_remediation=Input("ri.foundry.main.dataset.303dedb7-b8a2-4167-b1d0-e17caf7ad199"),
)
def compute(ctx, all_controls, dq_remediation):
    return gen_nb_ctrl_lines(ctx, all_controls, dq_remediation)

from transforms.api import transform_df, Input, Output

from control.dq_control_utils import gen_nb_ctrl_lines


@transform_df(
    Output("ri.foundry.main.dataset.1bab0d87-a0a2-40d6-9c89-c90386205a8d"),
    all_controls=Input("ri.foundry.main.dataset.ec115580-c7ab-483f-82c4-50b379d9f281"),
    dq_remediation=Input("ri.foundry.main.dataset.64d73f39-b0d2-480e-87bd-bf08d58c6090")
)
def compute(ctx, all_controls, dq_remediation):
    return gen_nb_ctrl_lines(ctx, all_controls, dq_remediation)

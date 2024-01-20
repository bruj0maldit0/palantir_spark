"Script to generate individual remediation table"
from transforms.api import transform_df, Input, Output, Check
from functools import reduce
from pyspark.sql import DataFrame
from transforms import expectations as E

from control.dq_control_utils import remediation_lines
from control import dq_control_constants
from control.dq_control_schemas import REMEDIATION_SCHEMA

DATASET_NAME = "supplier_lineage"
PRIMARY_KEY = "vendor_id"
RID_DICT = {
    "dataset_with_all_controls": "ri.foundry.main.dataset.f5ce195a-3575-4159-8920-f8fc27b4e1a8",
    "dataset_with_control_dict": dq_control_constants.control_dict_rid,
    "dataset_dq_remediation": "ri.foundry.main.dataset.24ae5a70-2973-49be-b564-4def6cf2fc33"
}


@transform_df(
    Output(RID_DICT["dataset_dq_remediation"],
           checks=[
                Check(E.primary_key('dq_pk'), 'Primary Key Check', on_error='FAIL'),
                Check(E.col('dataset_name').equals(DATASET_NAME), 'Dataset Name Check', on_error='FAIL'),
                Check(E.col('issue').non_null(), 'Issue non null', on_error='FAIL'),
                Check(E.col('data_domain').is_in('PCL', 'INDUSTRIAL', 'FINANCE', 'SALES', 'PURCHASING'),
                      'Data Domain Not Standard', on_error='FAIL'),
                Check(E.col('dimension').is_in('CONSISTENCY', 'COMPLETENESS', 'ACCURACY', 'CONFORMITY',
                                               'UNIQUENESS'), 'Dimension Not Standard', on_error='FAIL'),
                ]
           ),
    dataset_with_controls=Input(RID_DICT["dataset_with_all_controls"]),
    dataset_with_control_dict=Input(RID_DICT["dataset_with_control_dict"]),
)
# enter a name for datatransform
def generate_remediation_transforms(ctx, dataset_with_controls, dataset_with_control_dict):
    transforms = []
    dataset_with_controls_rid = RID_DICT["dataset_with_all_controls"]
    control_list = dataset_with_control_dict.collect()  # noqa
    for control in control_list:  # noqa
        if int(control.implementation_status.split("-")[0].strip()) > 3 and control.dataset == DATASET_NAME:
            transforms.append(remediation_lines(ctx, dataset_with_controls, control, PRIMARY_KEY, dataset_with_controls_rid))
    if transforms:
        return reduce(DataFrame.unionByName, transforms)
    return ctx.spark_session.createDataFrame([], schema=REMEDIATION_SCHEMA)

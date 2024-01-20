import re

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, BooleanType

from myproject.datasets.part_total_percentages_key_value.constants import DIVISION, PLANT_ID, SEGMENT, COMPONENT_TYPE

# To clean strings like column names.
def normalize_string(s):
    s = re.sub('[^A-Za-z0-9_\s]+', '', str(s))
    s = re.sub('_+', ' ', s)
    if s == '':
        return 'none'
    else:
        return s.lower().replace('_', ' ').strip().replace(' ', '_')

normalize_string_udf = F.udf(lambda a: normalize_string(a))

# Designed to handle new filename format from workshop media uploader widget.
# New format is <Timestamp in milliseconds of upload date>-<Actual filename>
def handle_filename_split(filename):
    split_string = filename.split("-", 1)

    if len(split_string) > 1:
        return "".join(split_string[1:])
    else:
        return split_string[0]

def clean_column_names(df):
    clean_col_names_df = df
    for c in clean_col_names_df.columns:
        clean_col_names_df = clean_col_names_df.withColumnRenamed(c, normalize_string(c))
    return clean_col_names_df


def drop_unused_columns(df, drop_col_name='untitled_column'):
    for c in df.columns:
        if c == drop_col_name:
            df = df.drop(c)
    return df


def cast_columns_to_type(df, columns, new_type):
    for c in columns:
        df = df.withColumn(c, F.col(c).cast(new_type))
    return df


def uniformize_object_df_filter_columns(df):
    for c in [DIVISION, PLANT_ID]:
        if c in df.columns:
            df = df.withColumn(c, F.upper(F.col(c)))
    for c in [SEGMENT, COMPONENT_TYPE]:
        if c in df.columns:
            df = df.withColumn(c, F.initcap(F.col(c)))
    return df


# TODO: remove me when we can handle bools in pipeline.
def convert_all_booleans_to_strings(df):
    schema = df.schema
    for col in schema.fields:
        if col.dataType == BooleanType():
            df = df.withColumn(
                col.name,
                F.col(col.name).cast(StringType())
            )
    return df


def write_deduplicated_output(output, df, join_keys):
    # Deduplicate the input given the previous incremental
    previous_output = output.dataframe('previous', schema=df.schema)

    df = df.join(previous_output, on=join_keys, how='leftanti')

    output.write_dataframe(df)

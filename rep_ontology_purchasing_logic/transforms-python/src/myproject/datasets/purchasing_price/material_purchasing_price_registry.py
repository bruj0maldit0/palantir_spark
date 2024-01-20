from pyspark.sql import functions as F

from control import dq_control_constants
from datetime import date

NB_CONTROLLED_LINES = dq_control_constants.NB_CONTROLLED_LINES
OK = dq_control_constants.OK

# --------------------------------
# For each control, add a function named control_"control_name", where "control name" is the name of the control in the Fusion control registry
# Please remove template comments after use to increase readibility and add control explanation
# --------------------------------


def control_purchasing_material_purchasing_price_kmein_completeness(input_dataset, control_id):
    """
    If unit_of_measure_|_kmein is filled, then OK, else KO

    Parameters
    ----------
    input_data : dataframe
        This is the input dataset where we need to control the columns
    control_id : string
        This is the control name attributed by the data manager

    Returns
    -------
    dataframe
        a dataframe with the column controls added
    """
    unit_of_measure_filled = ((F.col("unit_of_measure_|_kmein").isNotNull()) & (F.col("unit_of_measure_|_kmein") != ""))

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(unit_of_measure_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_material_purchasing_price_meins_completeness(input_dataset, control_id):
    """
    If base_unit_of_measure_|_meins is filled, then OK, else KO

    Parameters
    ----------
    input_data : dataframe
        This is the input dataset where we need to control the columns
    control_id : string
        This is the control name attributed by the data manager

    Returns
    -------
    dataframe
        a dataframe with the column controls added
    """
    base_unit_of_measure_filled = ((F.col("base_unit_of_measure_|_meins").isNotNull()) & (F.col("base_unit_of_measure_|_meins") != ""))

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(base_unit_of_measure_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_material_purchasing_price_datab_accuracy(input_dataset, control_id):
    """
    If valid_from_|_datab between 1990 and 2100, then OK, else KO

    Parameters
    ----------
    input_data : dataframe
        This is the input dataset where we need to control the columns
    control_id : string
        This is the control name attributed by the data manager

    Returns
    -------
    dataframe
        a dataframe with the column controls added
    """
    min_date = date(1990, 1, 1)
    max_date = date(2100, 12, 31)
    input_dataset = (
         input_dataset
         .withColumn(control_id+"_"+OK,
                     F.when(((F.col("valid_from_|_datab") > min_date) & (F.col("valid_from_|_datab") < max_date)), 1)
                     .otherwise(0).cast('int'))
         .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                     F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

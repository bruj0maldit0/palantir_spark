from pyspark.sql import functions as F

from control import dq_control_constants
from datetime import date
from pyspark.sql.functions import length 
NB_CONTROLLED_LINES = dq_control_constants.NB_CONTROLLED_LINES
OK = dq_control_constants.OK

# --------------------------------
# For each control, add a function named control_"control_name", where "control name" is the name of the control in the Fusion control registry
# Please remove template comments after use to increase readibility and add control explanation
# --------------------------------


def control_taxo_hierarchy_segname_completeness(input_dataset, control_id):
    """
    If segment_name is filled, then OK, else KO

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
    segment_name_filled = F.col("segment_name").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(segment_name_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_taxo_hierarchy_subcommo_completeness(input_dataset, control_id):
    """
    If subCommodity is filled, then OK, else KO

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
    subcommodity_filled = F.col("subCommodity").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(subcommodity_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_portfolio_completeness(input_dataset, control_id):
    """
    If purchasingPortfolio is filled, then OK, else KO

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
    purchasingportfolio_filled = F.col("purchasingPortfolio").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(purchasingportfolio_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_rawmat_completeness(input_dataset, control_id):
    """
    If isRawMaterial is filled, then OK, else KO

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
    israwmaterial_filled = F.col("isRawMaterial").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(israwmaterial_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_validitystart_completeness(input_dataset, control_id):
    """
    If startValidityDate is filled, then OK, else KO

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
    startvaliditydate_filled = F.col("startValidityDate").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(startvaliditydate_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_validityend_completeness(input_dataset, control_id):
    """
    If endValidityDate is filled, then OK, else KO

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
    endvaliditydate_filled = F.col("endValidityDate").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(endvaliditydate_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_validitystatus_completeness(input_dataset, control_id):
    """
    If segment_validity_status is filled, then OK, else KO

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
    segment_validity_status_filled = F.col("segment_validity_status").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(segment_validity_status_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_subcommo_code_completeness(input_dataset, control_id):
    """
    If subcommodity_code is filled, then OK, else KO

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
    subcommodity_filled = F.col("subcommodity_code").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(subcommodity_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_subcommo_name_completeness(input_dataset, control_id):
    """
    If subcommodity_name is filled, then OK, else KO

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
    subcommodity_name_filled = F.col("subcommodity_name").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(subcommodity_name_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_commodity_completeness(input_dataset, control_id):
    """
    If commodity is filled, then OK, else KO

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
    commodity_filled = F.col("commodity").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(commodity_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_subcommo_validitystat_completeness(input_dataset, control_id):
    """
    If subcommodity_validity_status is filled, then OK, else KO

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
    subcommodity_validity_status_filled = F.col("subcommodity_validity_status").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(subcommodity_validity_status_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_commodity_code_completeness(input_dataset, control_id):
    """
    If commodity_code is filled, then OK, else KO

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
    commodity_code_filled = F.col("commodity_code").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(commodity_code_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_commodity_name_completeness(input_dataset, control_id):
    """
    If commodity_name is filled, then OK, else KO

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
    commodity_name_filled = F.col("commodity_name").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(commodity_name_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_portfolio_code_completeness(input_dataset, control_id):
    """
    If portfolio_code is filled, then OK, else KO

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
    portfolio_code_filled = F.col("portfolio_code").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(portfolio_code_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_portfolio_name_completeness(input_dataset, control_id):
    """
    If portfolio_name is filled, then OK, else KO

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
    portfolio_name_filled = F.col("portfolio_name").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(portfolio_name_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_taxo_hierarchy_system_segment_conformity(input_dataset, control_id):
    """
    If length of system_segment_code is 6, then OK, else KO

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
    system_segment_check = (length(F.col("system_segment_code")) == 6)

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(system_segment_check, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


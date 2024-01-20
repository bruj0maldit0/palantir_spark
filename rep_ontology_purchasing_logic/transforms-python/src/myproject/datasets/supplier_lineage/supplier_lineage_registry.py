from pyspark.sql import functions as F

from control import dq_control_constants
from datetime import date

NB_CONTROLLED_LINES = dq_control_constants.NB_CONTROLLED_LINES
OK = dq_control_constants.OK

# --------------------------------
# For each control, add a function named control_"control_name", where "control name" is the name of the control in the Fusion control registry
# Please remove template comments after use to increase readibility and add control explanation
# --------------------------------


def control_supplier_lineage_country_completeness(input_dataset, control_id):
    """
    each vendor id must have a country

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
    vendor_country_filled = F.col("vendor_country").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(vendor_country_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_supplier_lineage_vendorname_completeness(input_dataset, control_id):
    """
   each vendor id must have a name

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
    vendor_name_filled = F.col("vendor_name_1").isNotNull()

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(vendor_name_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_supplier_lineage_vendorcity_completeness(input_dataset, control_id):
    """
    each vendor id must have a city, if not OTV or blocked

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
    vendor_city_filled = F.col("vendor_city").isNotNull()
    input_dataset = input_dataset.withColumn("vendor_name",F.lower(input_dataset.vendor_name_1))
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(vendor_city_filled, 1)
                    .when((F.col('vendor_name').contains('one'))&(F.col('vendor_name').contains('time'))&(F.col('vendor_name').contains('vendor')),1)
                    .when((F.col('vendor_blocked')=='block'),1)
                    .otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_supplier_lineage_vendorzip_completeness(input_dataset, control_id):
    """
    each vendor id must have a postal code, if not OTV or blocked

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
    vendor_postal_filled = F.col("vendor_postal_code").isNotNull()
    input_dataset = input_dataset.withColumn("vendor_name",F.lower(input_dataset.vendor_name_1))
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(vendor_postal_filled, 1)
                    .when((F.col('vendor_name').contains('one'))&(F.col('vendor_name').contains('time'))&(F.col('vendor_name').contains('vendor')),1)
                    .when((F.col('vendor_blocked')=='block'),1)
                    .otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_supplier_lineage_vendorstreet_completeness(input_dataset, control_id):
    """
    each vendor id must have a street, if not OTV or blocked

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
    vendor_postal_filled = F.col("vendor_street").isNotNull()
    input_dataset = input_dataset.withColumn("vendor_name",F.lower(input_dataset.vendor_name_1))
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(vendor_postal_filled, 1)
                    .when((F.col('vendor_name').contains('one'))&(F.col('vendor_name').contains('time'))&(F.col('vendor_name').contains('vendor')),1)
                    .when((F.col('vendor_blocked')=='block'),1)
                    .otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset




def control_supplier_lineage_vendorstreet_consistency(input_dataset, control_id):
    """
    street information in SAP and MDM should be aligned

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
    vendor_street = (F.col("vendor_street") == F.col("supplier_address_1"))

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(vendor_street, 1)
                    .otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset



def control_supplier_lineage_taxo_consistency(input_dataset, control_id):
    """
    MainSegment has to be a valid one, if supplier is active

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
    con_1 = (F.col('ultimate_status')!='Valid')|(F.col('ultimate_status').isNull())
    con_2 = (F.col('mainSegment')!=F.col("segment_code"))|(F.col('mainSegment').isNull())|(F.col('segment_code').isNull())
    con_3 = (F.col('segment_validity_status')!='Valid')|(F.col('segment_validity_status').isNull())
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when((F.col('ultimate_status')=='Valid')&(F.col('mainSegment')==F.col("segment_code"))&(con_3), 0)
                    .when((F.col('ultimate_status')=='Valid')&(con_2)&(F.col('segment_validity_status')=='Valid'),1)
                    .when((con_1)&(F.col('mainSegment')==F.col("segment_code"))&(F.col('segment_validity_status')=='Valid'),1)
                    .when((F.col('ultimate_status')=='Valid')&(F.col('mainSegment')==F.col("segment_code"))&(F.col('segment_validity_status')=='Valid'),1)
                    .when((con_1)&(F.col('mainSegment')==F.col("segment_code"))&(con_3),1)
                    .otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset

def control_supplier_lineage_vat_completeness(input_dataset, control_id):
    """
    each vendor id must have a VAT no, if not OTV or blocked

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
    vendor_postal_filled = F.col("vendor_vat_registration_no").isNotNull()
    input_dataset = input_dataset.withColumn("vendor_name",F.lower(input_dataset.vendor_name_1))
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(vendor_postal_filled, 1)
                    .when((F.col('vendor_name').contains('one'))&(F.col('vendor_name').contains('time'))&(F.col('vendor_name').contains('vendor')),1)
                    .when((F.col('vendor_blocked')=='block'),1)
                    .otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset
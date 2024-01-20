from pyspark.sql import functions as F
from pyspark.sql.window import Window
from control import dq_control_constants
from datetime import date

NB_CONTROLLED_LINES = dq_control_constants.NB_CONTROLLED_LINES
OK = dq_control_constants.OK

# --------------------------------
# For each control, add a function named control_"control_name", where "control name" is the name of the control in the Fusion control registry
# Please remove template comments after use to increase readibility and add control explanation
# --------------------------------


def control_purchasing_contract_and_purchasing_scheduling_agreement_matkl_completeness(input_dataset, control_id):
    """
    If material_group_|_matkl is filled, then OK, else KO

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
    material_group_filled = ((F.col("material_group_|_matkl").isNotNull()) & (F.col("material_group_|_matkl") != ""))

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(material_group_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_incoterms_completeness(input_dataset, control_id):
    """
    If incoterms is filled, then OK, else KO

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
    incoterms_filled = ((F.col("incoterms").isNotNull()) & (F.col("incoterms") != ""))

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(incoterms_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_terms_of_payment_completeness(input_dataset, control_id):
    """
    If terms_of_payment_|_zterm filled, then OK, else KO

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
    term_of_payment_filled = ((F.col("terms_of_payment_|_zterm").isNotNull()) & (F.col("terms_of_payment_|_zterm") != ""))

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(term_of_payment_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_kdate_completeness(input_dataset, control_id):
    """
    If validity_period_end_|_kdate filled, then OK, else KO

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
    validity_end_filled = (F.col("validity_period_end_|_kdate").isNotNull())

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(validity_end_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_kdatb_completeness(input_dataset, control_id):
    """
    If validity_per_start_|_kdatb filled, then OK, else KO

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
    validity_start_filled = (F.col("validity_per_start_|_kdatb").isNotNull())

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(validity_start_filled, 1).otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_kdatb_accuracy(input_dataset, control_id):
    """
    If validity_per_start_|_kdatb between 1990 and 2100, then OK, else KO

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
                     F.when(((F.col("validity_per_start_|_kdatb") > min_date) & (F.col("validity_per_start_|_kdatb") < max_date)), 1)
                     .otherwise(0).cast('int'))
         .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                     F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_kdate_accuracy(input_dataset, control_id):
    """
    If validity_period_end_|_kdate less than 2100, then OK, else KO

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
    max_date = date(2100, 12, 31)
    input_dataset = (
         input_dataset
         .withColumn(control_id+"_"+OK,
                     F.when((F.col("validity_period_end_|_kdate") < max_date), 1)
                     .otherwise(0).cast('int'))
         .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                     F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_mwskz_consistency_001(input_dataset, control_id):
    """
    If eval_receipt_sett_|_xersy == X, and tax_code_|_mwskz == null, then NOK, else OK

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
    selfbilling_taxcode_filled = ((F.col("eval_receipt_sett_|_xersy") == "X") & ((F.col("tax_code_|_mwskz").isNull()) |
                                  (F.col("tax_code_|_mwskz") == "")))

    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when(selfbilling_taxcode_filled, 0).otherwise(1).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_matkl_accuracy(input_dataset, control_id):
    """
    If material_group_|_matkl is "150101", then NOK, else OK

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
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when((F.col("material_group_|_matkl") == "150101"), 0)
                    .otherwise(1).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )

    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_mwskz_consistency_002(input_dataset, control_id):
    """
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
    input_dataset = (input_dataset
                     .withColumn(control_id+"_"+OK, F.when(F.col('purchasing_doc_type_|_bsart') == "ZRA", None)
                                                     .when(~(F.col("tax_code_|_mwskz") == F.col("new_tax_code_|_mwskz")), 0)
                                                     .when(((F.col("tax_code_|_mwskz").isNull() & F.col("new_tax_code_|_mwskz").isNotNull()) | (F.col("tax_code_|_mwskz").isNotNull() & F.col("new_tax_code_|_mwskz").isNull())), 0)
                                                     .otherwise(1).cast('int')
                                 ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int'))
                     )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_inco1_consistency(input_dataset, control_id):
    """
    Test if the column is filled

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
    input_dataset = (input_dataset
                     .withColumn(control_id+"_"+OK, F.when(F.col('purchasing_doc_type_|_bsart') == "ZRA", None)
                                                     .when(~(F.col("incoterms") == F.col("new_incoterms")), 0)
                                                     .when(((F.col("incoterms").isNull() & F.col("new_incoterms").isNotNull()) | (F.col("incoterms").isNotNull() & F.col("new_incoterms").isNull())), 0)
                                                     .otherwise(1).cast('int')
                                 ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int'))
                     )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_zterm_consistency(input_dataset, control_id):
    """
    Test if the column is filled

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
    input_dataset = (input_dataset
                     .withColumn(control_id+"_"+OK, F.when(F.col('purchasing_doc_type_|_bsart') == "ZRA", None)
                                                     .when(~(F.col("terms_of_payment_|_zterm") == F.col("new_terms_of_payment_|_zterm")), 0)
                                                     .when(((F.col("terms_of_payment_|_zterm").isNull() & F.col("new_terms_of_payment_|_zterm").isNotNull()) | (F.col("terms_of_payment_|_zterm").isNotNull() & F.col("new_terms_of_payment_|_zterm").isNull())), 0)
                                                     .otherwise(1).cast('int')
                                 ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int'))
                     )
    list_col = ["new_concat_purchasing", "concat_outline", "concat_purchasing" "new_eval_receipt_sett_|_xersy"]
    input_dataset = input_dataset.drop(*list_col)
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_open_po_uniqueness(input_dataset, control_id):
    """
    if OR(unique_count(concat(material_number;supplier_|_lifnr;plant_code;purchasing_doc_type_|_bsart))=1;
    AND((count(concat(material_number;supplier_|_lifnr;plant_code;purchasing_doc_type_|_bsart))>1);
    count(deletion_indicator_|_loekz=NULL)=1)), then OK

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
    input_dataset = input_dataset.withColumn("concat", F.concat_ws('|', F.col("material_number"), F.col("supplier_|_lifnr"), F.col("plant_code"), F.col("purchasing_doc_type_|_bsart")))
    cnt = (Window.partitionBy("concat"))
    input_dataset = input_dataset.withColumn("count", F.count("concat").over(cnt))
    input_dataset = input_dataset.withColumn("key", F.when(F.col("deletion_indicator_|_loekz").isNull(), 1)
                                                     .otherwise(0))
    cntn = (Window.partitionBy(F.col("material_number"), F.col("supplier_|_lifnr"), F.col("plant_code"),F.col("plant_code"), F.col("purchasing_doc_type_|_bsart")))
    input_dataset = input_dataset.withColumn("cnt_delindi", F.sum("key").over(cntn))
    input_dataset = (
            input_dataset
            .withColumn(control_id+"_"+OK,
                        F.when((F.col("count") >= 1) & (F.col("cnt_delindi") <= 1), 1)
                         .otherwise(0).cast('int')
                        ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                     F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
        )
    list_col = ["concat", "count", "key", "cnt_delindi"]
    input_dataset = input_dataset.drop(*list_col)
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_matkl_consistency(input_dataset, control_id):
    """
    "
    if AND(material_number=material_number(in different row); 
    material_group_|_matkl=material_group_|_matkl(in same different row)) then OK"

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
    test_window = (
                    Window
                    .partitionBy("material_number")
                    .orderBy(F.col("material_group_|_matkl").desc())
                    )
    input_dataset = (
            input_dataset
            .withColumn("dense_rank", F.dense_rank().over(test_window))
            )
    count = (Window.partitionBy("material_number"))
    input_dataset = input_dataset.withColumn("count_dense_rank", F.sum("dense_rank").over(count))
    input_dataset = input_dataset.withColumn("count_material_number", F.count("material_number").over(count))
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK, F.when((F.col("count_material_number") == F.col("count_dense_rank")), 1)
                                        .otherwise(0).cast('int')
                    ).withColumn(control_id+"_"+NB_CONTROLLED_LINES, F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                                                      .otherwise(None).cast('int'))
                      )
    list_col = ["dense_rank", "count_dense_rank", "count_material_number"]
    input_dataset = input_dataset.drop(*list_col)
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_vendorblock_consistency(input_dataset, control_id):
    """
    "if AND(supplier_|_lifnr = !supplier_lineage!""vendor_id"; !supplier_lineage!"vendor_blocked"!=null;
     deletion_indicator_|_loekz = null) then NOK else OK"

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
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when((F.col("vendor_blocked").isNull() | (F.col("vendor_blocked").isNotNull() & F.col("deletion_indicator_|_loekz").isNotNull())),1)
                    .otherwise(0).cast('int'))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                    F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1).otherwise(None).cast('int'))
    )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_bom_consistency(input_dataset, control_id):
    """
    "if AND(!Plant Production Material!""material_type" = "COMP"; material_number = !Plant Production Material!"
    material_number"; !Plant Production Material!"plant_code" = plant_code; 
    !Plant Production Material!"is_obsolete_material" = true; deletion_indicator_|_loekz = null) then NOK else OK"

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
    input_dataset = (
        input_dataset
        .withColumn(control_id+"_"+OK,
                    F.when((F.col("is_obsolete_material") == True) & (F.col("deletion_indicator_|_loekz").isNull()), 0)
                    .when((F.col("is_obsolete_material") == True) & (F.col("deletion_indicator_|_loekz").isNotNull()), 1)
                    .otherwise(1).cast("int"))
        .withColumn(control_id+"_"+NB_CONTROLLED_LINES, F.when(F.col(control_id+"_"+OK).isin([0,1]), 1).otherwise(None).cast('int'))
    )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_loekz_consistency(input_dataset, control_id):
    """
    "if AND(purchasing_doc_type_|_bsart=""LPA"";deletion_indicator_|_loekz!=NULL) and
     in different row: AND(concat(outline_agreement_|_konnr;princagreement_item_|_ktpnr)=concat(purchasing_document_|_ebeln;item_|_ebelp);deletion_indicator_|_loekz!=NULL) then OK"

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
    input_dataset = (input_dataset
                     .withColumn(control_id+"_"+OK, F.when(F.col('purchasing_doc_type_|_bsart') == "ZRA", None)
                                                     .when((F.col("deletion_indicator_|_loekz").isNull()) & (~(F.col("deletion_indicator_|_loekz") == F.col("new_deletion_indicator_|_loekz"))), 0)
                                                     .when(((F.col("deletion_indicator_|_loekz").isNull() & F.col("new_deletion_indicator_|_loekz").isNotNull()) | (F.col("deletion_indicator_|_loekz").isNotNull() & F.col("new_deletion_indicator_|_loekz").isNull())), 0)
                                                     .otherwise(1).cast('int')
                                 ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int'))
                     )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_evers_consistency(input_dataset, control_id):
    """
    "if AND(purchasing_doc_type_|_bsart=""LPA"";shipping_instr_|_evers=""M1"") and
    in different row: AND(concat(outline_agreement_|_konnr;princagreement_item_|_ktpnr)=concat(purchasing_document_|_ebeln;item_|_ebelp);shipping_instr_|_evers=""M1"") then OK "
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
    input_dataset = (input_dataset
                     .withColumn(control_id+"_"+OK, F.when(F.col("purchasing_doc_type_|_bsart") == "ZRA", None)
                                                     .when(~(F.col("shipping_instr_|_evers") == F.col("new_shipping_instr_|_evers")), 0)
                                                     .when(((F.col("shipping_instr_|_evers").isNull() & F.col("new_shipping_instr_|_evers").isNotNull()) | (F.col("shipping_instr_|_evers").isNotNull() & F.col("new_shipping_instr_|_evers").isNull())), 0)
                                                     .otherwise(1).cast('int')
                                 ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int'))
                     )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_xersy_consistency(input_dataset, control_id):
    """
   "if AND(purchasing_doc_type_|_bsart=""LPA"";eval_receipt_sett_|_xersy=""X"") and
    in different row: AND(concat(outline_agreement_|_konnr;princagreement_item_|_ktpnr)=concat(purchasing_document_|_ebeln;item_|_ebelp);eval_receipt_sett_|_xersy=""X"") then OK"
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
    input_dataset = (input_dataset
                     .withColumn(control_id+"_"+OK, F.when(F.col("purchasing_doc_type_|_bsart") == "ZRA", None)
                                                     .when(~(F.col("eval_receipt_sett_|_xersy") == F.col("new_eval_receipt_sett_|_xersy")), 0)
                                                     .when(((F.col("eval_receipt_sett_|_xersy").isNull() & F.col("new_eval_receipt_sett_|_xersy").isNotNull()) | (F.col("eval_receipt_sett_|_xersy").isNotNull() & F.col("new_eval_receipt_sett_|_xersy").isNull())), 0)
                                                     .otherwise(1).cast('int')
                                 ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int'))
                     )
    return input_dataset


def control_purchasing_contract_and_purchasing_scheduling_agreement_interco_consistency(input_dataset, control_id):
    """
   "if AND(purchasing_doc_type_|_bsart=""LPA"";eval_receipt_sett_|_xersy=""X"") and
    in different row: AND(concat(outline_agreement_|_konnr;princagreement_item_|_ktpnr)=concat(purchasing_document_|_ebeln;item_|_ebelp);eval_receipt_sett_|_xersy=""X"") then OK"
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
    input_dataset = (input_dataset
                     .withColumn(control_id+"_"+OK, F.when(F.col("deletion_indicator_|_loekz").isNotNull(), None)
                                                     .when((F.col("vendor_account_group") == "Z007") & (F.col("purchasing_group_|_ekgrp")=="10A"), 1)
                                                     .when((F.col("vendor_account_group") != "Z007") & (F.col("purchasing_group_|_ekgrp")!="10A"), 1)
                                                     .otherwise(0).cast('int')
                                 ).withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int'))
                     )
    return input_dataset

def control_purchasing_contract_and_purchasing_scheduling_agreement_prodplant_consistency(input_dataset,control_id):
    """
    "Filter for deletion_indicator_|_loekz = NULL

    if AND(plant_code = !Plant!"plant_code; !Plant!"; bau_activity" = "Production non JIT") then OK

    if AND(plant_code = !Plant!"plant_code"; !Plant!"bau_activity" = "Production JIT") then OK

    else NOK"

    Parameters
    ----
    input_data : dataframe
        This is the input dataset where we need to control the columns
    control_id : string
        This is the control name attributed by the data manager

    Returns
    -------
    dataframe
        a dataframe with the column controls added    

    """

    condition= ( (F.col("bau_activity")=="Production non JIT") | (F.col("bau_activity")=="Production JIT") )
    input_dataset= (input_dataset.withColumn(control_id+"_"+OK, F.when(F.col("deletion_indicator_|_loekz").isNotNull(),None)
                    .when(F.col("deletion_indicator_|_loekz").isNull() & condition , 1).otherwise(0).cast('int'))
                    .withColumn(control_id+"_"+NB_CONTROLLED_LINES,
                                              F.when(F.col(control_id+"_"+OK).isin([0, 1]), 1)
                                               .otherwise(None).cast('int')))
    
    return  input_dataset

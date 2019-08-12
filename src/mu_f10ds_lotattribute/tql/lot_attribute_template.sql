SELECT DISTINCT
step,
LEFT(lot_id, 7) as lot_id,
lot_attribute

FROM

(SELECT distinct
    ci.corr_item_OID AS corr_item_OID1,
    rtrim(s.step_name) AS step,
    s.sys_tt_upd_ts AS datetime

from FAB_{fab}_FREC_DM.exception e
    inner join FAB_{fab}_FREC_DM.exception_member em
        on e.exception_OID = em.exception_OID
    inner join FAB_{fab}_FREC_DM.attr a
        on em.member_OID = a.attr_OID
    inner join FAB_{fab}_REF_DM.corr_item ci
        on a.generic_attr_OID = ci.corr_item_OID
    inner join FAB_{fab}_FREC_DM.process_exception pe
        on pe.exception_OID = e.exception_OID
    inner join FAB_{fab}_FREC_DM.process p
        on p.process_OID = pe.process_OID
    inner join FAB_{fab}_REF_DM.step s
        on s.step_OID = p.step_OID
    where
    a.attr_name = 'LOT_ATTRIBUTE'
) table_with_step

INNER JOIN

(SELECT
LAV.LOT_ID AS lot_id,
LAV.corr_item_OID AS corr_item_OID2,
RCI.corr_item_desc AS lot_attribute

FROM WW_MFG_IDS_DM.FT_FAB_LOT_ATTR_VALUE LAV
INNER JOIN WW_MFG_IDS_DM.REF_CORR_ITEM RCI
ON LAV.corr_item_OID = RCI.corr_item_OID
WHERE LAV.dwh_SrcID = 'FAB {fab}' and
REGEXP_SIMILAR(SUBSTR(lot_id,1,7),'[0-9]{num}') = 1 and
lot_attribute like 'Z%'
) table_with_lt

on table_with_step.corr_item_OID1 = table_with_lt.corr_item_OID2
where
lot_id like any ({lot_id});
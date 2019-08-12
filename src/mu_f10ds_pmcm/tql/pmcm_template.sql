SELECT equip_id,
       context,
       datetime,
       TRIM(StepName) as StepName,
       ROW_NUMBER() OVER (PARTITION BY equip_id
       ORDER BY datetime ASC ) AS ranking
       FROM(
        SELECT DISTINCT equip_id,
        context,
        datetime,
        StepName
        --ROW_NUMBER() OVER (PARTITION BY equip_id
        --ORDER BY datetime ASC ) AS ranking
        FROM
        (
        SELECT *
            FROM
            (
            SELECT t1.*, cast('null' as CHAR(50)) as StepName
                FROM
                (
                SELECT
                    DISTINCT RTRIM(LTRIM(E.equip_id)) AS equip_id,
                    RTRIM(LTRIM(ESFA.equip_state_id)) AS context,
                    EH.equip_state_out_datetime AS datetime
                    FROM WW_MFG_IDS_DM.ET_EVENT_HISTORY EH
                    INNER JOIN WW_MFG_IDS_DM.ET_EQUIPMENT E ON EH.equip_OID=E.equip_OID
                    LEFT OUTER JOIN (
                    SELECT *
                        FROM WW_MFG_IDS_DM.ET_EQUIPMENT_state_for_area
                        WHERE dwh_srcID='FAB {fab}') ESFA ON EH.equip_state_OID=ESFA.equip_state_OID
                    LEFT OUTER JOIN (
                    SELECT *
                        FROM WW_MFG_IDS_DM.REF_MFG_AREA
                        WHERE dwh_srcID='FAB {fab}') MFG_AREA ON ESFA.mfg_area_OID=MFG_AREA.mfg_area_OID
                    LEFT OUTER JOIN (
                    SELECT *
                        FROM WW_MFG_IDS_DM.ET_EVENT_CODE_HISTORY
                        WHERE dwh_srcID='FAB {fab}') ECH ON EH.production_state_change_OID=ECH.production_state_change_OID
                    LEFT OUTER JOIN (
                    SELECT *
                        FROM WW_MFG_IDS_DM.ET_EVENT_NOTE
                        WHERE dwh_srcID='FAB {fab}') EN ON EH.production_state_change_OID=EN.production_state_change_OID
                    WHERE CAST( EH.mod_dt AS DATE) > '{start_date}'
                        AND  CAST(EH.mod_dt AS DATE) <= '{end_date}'
                        AND  EH.dwh_srcID='FAB {fab}'
                        AND  E.dwh_srcID='FAB {fab}') AS t1

                WHERE (context LIKE '%PM%' OR context LIKE '%REPAIR%')
                    AND  equip_id LIKE '%0'


UNION ALL

SELECT equip_id, context, datetime, StepName
FROM((SELECT FabLotHistOid, StepName
    FROM WW_MFG_DM.d_LotStepMES
    WHERE dwh_SrcId='FAB {fab}') AS LOT_OID_T1

INNER JOIN

(SELECT equip_id,
    context,
    datetime,
    t2.FAB_LOT_HIST_OID AS FabLotHistOid
    FROM(
    SELECT lot_id AS context,
        tracked_in_datetime AS datetime,
        FAB_LOT_HIST_OID
        FROM "FAB_{fab}_FT_DM"."FAB_LOT_HIST" lh
WHERE CAST(tracked_in_datetime AS DATE) >='{start_date}' and REGEXP_SIMILAR(SUBSTR(lot_id,1,7),'[0-9]{num}') = 1) AS t2

INNER JOIN

(SELECT equip_id,
    FAB_LOT_HIST_OID
    FROM "FAB_{fab}_FT_DM"."FAB_LOT_EQUIP_HIST" eh) t3
ON t2.FAB_LOT_HIST_OID = t3.FAB_LOT_HIST_OID
WHERE CAST( datetime AS DATE) > '{start_date}') LOT_OID_T2
ON LOT_OID_T1.FabLotHistOid = LOT_OID_T2.FabLotHistOid)
) StepLotEquipDtTable) t4) dist_table
WHERE CAST(datetime AS DATE) >='{start_date}' and CAST(datetime AS DATE) <='{end_date}' and equip_id in any ({equip_id})
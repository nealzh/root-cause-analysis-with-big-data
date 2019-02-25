
SELECT DISTINCT
    WG.wafer_scribe AS wafer_scribe,
    WG.from_lot_id AS from_lot_id,
    WG.to_lot_id AS lot_id,
    WS.wafer_vendor AS wafer_vendor,
    STEP.STEP_NAME AS to_step,
    SW.WaferId AS wafer_id,
    SW2.MfgProcessStep AS from_step,
    SW2.WaferId AS from_wafer_id
    FROM FAB_{fab}_FT_DM.WAFER_GENEALOGY WG
         
    INNER JOIN FAB_{fab}_FT_DM.WAFER_HIST WH
    ON WG.WAFER_SCRIBE = WH.WAFER_SCRIBE
         
    INNER JOIN FAB_{fab}_FT_DM.WAFER_STATUS WS
    ON WG.WAFER_SCRIBE = WS.WAFER_SCRIBE
    AND WH.WAFER_OID = WS.WAFER_OID
         
    INNER JOIN FAB_{fab}_FT_DM.FAB_LOT_HIST FLH
    ON WH.FAB_LOT_HIST_OID = FLH.FAB_LOT_HIST_OID
         
    INNER JOIN FAB_{fab}_TRV_DM.TRAV_STEP TS
    ON TS.TRAV_STEP_OID = FLH.TRAV_STEP_OID
         
    INNER JOIN FAB_{fab}_REF_DM.STEP STEP
    ON STEP.STEP_OID = TS.STEP_OID
         
    INNER JOIN (
    SELECT *
        FROM "WW_MFG_DM"."SigmaWafer"
        WHERE dwh_SrcId = 'FAB {fab}'
            AND  RunCompleteDatetime >= '{start_datetime}'
            AND  RunCompleteDatetime <= '{end_datetime}') SW
    ON WG.to_lot_id = SW.lotId
    AND WG.WAFER_SCRIBE = SW.WaferScribe
    AND STEP.STEP_NAME = SW.MfgProcessStep
         
    INNER JOIN (
    SELECT *
        FROM "WW_MFG_DM"."SigmaWafer"
        WHERE dwh_SrcId = 'FAB {fab}'
            AND  RunCompleteDatetime >= '{start_datetime}'
            AND  RunCompleteDatetime <= '{end_datetime}') SW2
    ON WG.from_lot_id = SW2.lotId
    AND WG.WAFER_SCRIBE = SW2.WaferScribe
    WHERE WG.to_lot_id IN ({lot_list_csv_quoted})
        AND  STEP.STEP_NAME = '{current_step}'
    ORDER BY TS.TRAV_STEP_SEQ_NO ASC;
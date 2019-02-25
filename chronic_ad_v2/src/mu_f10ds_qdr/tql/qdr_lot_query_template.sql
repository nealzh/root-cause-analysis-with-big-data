SELECT
	qw.QDR_NO,
	qw.LOT_NO,
	CAST(qw.SCRIBE AS char(4)) ||'-'|| CAST(CAST(qw.WAFER_NO AS FORMAT'9(2)') AS CHAR(2)) AS WAFER_ID,
	qw.EQUIP_ID,
	qt.QDR_TEXT ,
	ds.StepName
FROM "FAB_{fab}_QDR_DM"."QDR_WAFER" qw

INNER JOIN "FAB_{fab}_QDR_DM"."QDR_TEXT" qt

ON qt.QDR_NO = qw.QDR_NO

INNER JOIN "FAB_{fab}_QDR_DM"."QDR_HEAD" qh

ON qh.QDR_NO = qw.QDR_NO

INNER JOIN "WW_MFG_DM"."d_Step" ds

ON ds.StepOid = qh.OCCURRED_STEP_OID

WHERE qw.LOT_NO LIKE ANY ({lot_id}) AND qt.TEXT_TYPE = 'DEVIATION';
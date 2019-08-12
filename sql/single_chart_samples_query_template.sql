SELECT spl.[fab]
      ,spl.[module]
      ,spl.[sample_date]
      ,spl.[design_id]
      ,spl.[ch_id]
      ,spl.[current_step]
      ,spl.[process_step]
      ,spl.[process_tool] as current_process_tool
      ,spl.[process_position] as current_process_position
      ,spl.[parameter_name]
      ,spl.[lot_id]
      ,spl.[wafer_id]
      ,spl.[sample_id]
      ,spl.[chart_type]
      ,spl.[value]
      ,spl.[ucl]
      ,spl.[lcl]
      ,spl.[label]
      ,spl.[channel_type]
      ,spl.[violation_type]
      ,spl.[query_session]
      ,spl.[analysis_type]
      ,cf.[ch_name]
      ,cf.[folder]
FROM {samples_table_name} spl

INNER JOIN

{tracking_table_name} cht

ON
    spl.fab = cht.fab
and spl.query_session = cht.query_session
and spl.ch_id = cht.ch_id
and spl.design_id = cht.design_id
and spl.chart_type = cht.chart_type
and spl.[analysis_type] = cht.[analysis_type]
and spl.[process_step] = cht.[process_step]

LEFT JOIN {ch_folder_table_name} cf
ON
CAST(substring(spl.ch_id, 0, charindex('_',spl.ch_id)) as INT) = cf.ch_id
and spl.fab = cf.fab

WHERE
      cht.[fab] = {fab}
  and cht.[ch_id] = '{ch_id}'
  and cht.[query_session] = '{query_session}'
  and cht.[analysis_type] = '{analysis_type}'

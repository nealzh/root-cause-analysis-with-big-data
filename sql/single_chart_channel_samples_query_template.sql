-- This query is used for main channel query
SELECT spl.[analysis_type]
      ,spl.[fab]
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

      ,cf.[ch_name]
      ,cf.[folder]
FROM {samples_table_name} spl
LEFT JOIN {ch_folder_table_name} cf
ON
CAST(substring(spl.ch_id, 0, charindex('_',spl.ch_id)) as INT) = cf.ch_id
and spl.fab = cf.fab


WHERE
      spl.[fab] = {fab}
  and spl.[ch_id] = '{ch_id}'
  and spl.[chart_type] = '{chart_type}'
  and spl.[query_session] = '{query_session}'
  and spl.[analysis_type] = '{analysis_type}'

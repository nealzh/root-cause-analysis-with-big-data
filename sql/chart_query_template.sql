SELECT [analysis_type]
      ,[fab]
      ,[module]
      ,[ch_id]
      ,[chart_type]
      ,[channel_type]
      ,[design_id]
      ,[query_session]
      ,[current_step]
      ,[parameter_name]
      ,[process_step]
      ,[normal]
      ,[ooc]
FROM {tracking_table_name}
WHERE
      (final_status IS NULL
      OR final_status like '%Not enough OOC points%'
      OR final_status like '%No last OOC sigma data%'
      OR final_status like '%channel missing data%')
  and [fab] = {fab}
  and [analysis_type] = '{analysis_type}'
  and [query_session] >= '{min_query_date}'
ORDER BY [query_session] desc
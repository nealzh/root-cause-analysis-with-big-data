SELECT
      [ch_id]
      ,[session_id]
      ,[actual_root_cause_step]
      ,[actual_root_cause_type]
      ,[actual_root_cause]

FROM {feedback_table_name}
WHERE
  [fab] = {fab} and
  [ch_id] = '{ch_id}' and
  [session_id] >= '{fb_min_query_date}' and
  [session_id] < '{query_session}' and
  [feedback_category] < 2

ORDER BY [session_id] desc

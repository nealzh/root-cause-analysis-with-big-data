SELECT
  DISTINCT ch_id
FROM {ch_folder_table_name}
WHERE fab = {fab}
AND folder in ({folder_list});
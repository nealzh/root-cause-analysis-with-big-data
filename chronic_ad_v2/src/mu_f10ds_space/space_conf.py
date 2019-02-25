time_format_config = {
    'datetime_standard_format': "%Y-%m-%d %H:%M:%S"
}

region_mapping = {
    10: 'singapore',
    7: 'singapore',
    4: 'boise',
    6: 'manassas',
    11: 'taoyuan',
    15: 'hiroshima',
    16: 'taichung'
}

tz_mapping = {
    10: 'Asia/Singapore',
    7: 'Asia/Singapore',
    4: 'America/Boise',
    6: 'EST',
    11: 'Asia/Taipei',
    15: 'Japan',
    16: 'Asia/Taipei'
}

default_spark_config = {
  'spark.executor.instances': '10',
  'spark.yarn.queue': 'eng_f10n-01',
  'spark.yarn.principal': 'hdfsf10n@NA.MICRON.COM',
  'spark.yarn.keytab': '/home/hdfsf10n/.keytab/hdfsf10n.keytab',
  'spark.app.name': 'SPACE_QUERY_mu_f10ds_space',
  'spark.master': 'yarn'
}
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


table_name = {
    'prod': {
        'lot': 'prod_mti_' + '{region}' + '_fab_' + '{fab}' + "_sigma:sigma_lot_v2",
        'wafer': 'prod_mti_' + '{region}' + '_fab_' + '{fab}' + "_sigma:sigma_wafer_v2",
        'measurement': 'prod_mti_' + '{region}' + '_fab_' + '{fab}' + "_sigma:sigma_measurement_v2"},
    'stream': {
        'lot': 'prod_mti_' + '{region}' + '_fab_' + '{fab}' + "_sigma:sigma_lot",
        'wafer': 'prod_mti_' + '{region}' + '_fab_' + '{fab}' + "_sigma:sigma_wafer",
        'measurement': 'prod_mti_' + '{region}' + '_fab_' + '{fab}' + "_sigma:sigma_measurement"}
}




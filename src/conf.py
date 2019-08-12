time_format_config = {
    'datetime_standard_format': "%Y-%m-%d %H:%M:%S"
}

fab_mapping = {
    'F4': 4,
    'F6': 6,
    'F11': 11,
    'F10W': 7,
    'F10N': 10,
    'F15': 15,
    'F16': 16
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

catrgory_mapping = {
    '(M1) 1st Strike': 1,
    '(M2) 2nd Strike': 2,
    '(M3) 3rd Strike': 3
}

chart_mapping = {
    'Mean': 3,
    'Range': 6,
    'Sigma': 5,
    'EWMA-Mean': 11
}

type_mapping = {
    'Mean': 'Mean',
    'Range': 'Range',
    'Sigma': 'Sigma',
    'EWMA': 'EWMA-Mean'
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

domain_knowledge = {
    'RDA': {
        "search_key": "parameter",
        "regex": "no",

        "CB_13_SURFACE": {
            "step_key_words": "PHOTO$",
            "mode": "ge",
            "loop_range": "previous_loop,at_loop"
        },

        "CB_16_EMBEDDED": {
            "step_key_words": "DEP$",
            "mode": "lt",
            "loop_range": "all"
        },

        "CB_18_BUBBLE": {
            "step_key_words": "^4620",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_19_RESIDUE": {
            "step_key_words": "PHOTO$",
            "mode": "ge",
            "loop_range": "at_loop"
        },

        "CB_20_DIVOT": {
            "step_key_words": "CMP$",
            "mode": "eq",
            "loop_range": "all"
        },

        "CB_21_BALLISTIC_SCRATCH": {
            "step_key_words": "PHOTO$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_22_FM_SHORT": {
            "step_key_words": "DRY ETCH$",
            "mode": "lt",
            "loop_range": "at_loop"
        },

        "CB_26_CONE": {
            "step_key_words": "DRY ETCH$,PHOTO$",
            "mode": "lt",
            "loop_range": "at_loop"
        },

        "CB_28_SURFACE FLAKE": {
            "step_key_words": "PHOTO$",
            "mode": "ge",
            "loop_range": "at_loop"
        },

        "CB_30_OPEN": {
            "step_key_words": "DRY ETCH$",
            "mode": "le",
            "loop_range": "all"
        },

        "CB_32_RESIDUAL_FILM": {
            "step_key_words": "CMP$,WET PROCESS$,WET ETCH$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_33_CMP_SCRATCH": {
            "step_key_words": "CMP$",
            "mode": "eq",
            "loop_range": "all"

        },

        "CB_34_CMP_SCRATCH_FILLED": {
            "step_key_words": "CMP$",
            "mode": "eq",
            "loop_range": "previous_loop"
        },

        "CB_36_HOLES": {
            "step_key_words": "DRY ETCH$",
            "mode": "le",
            "loop_range": "all"
        },

        "CB_38_PITTING": {
            "step_key_words": "DRY ETCH$,CMP$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_39_PL_BLOCKED_ETCH": {
            "step_key_words": "DRY ETCH$,CVD$",
            "mode": "eq",
            "loop_range": "previous_loop"
        },

        "CB_40_PRIOR_LEVEL": {
            "step_key_words": "",
            "mode": "eq",
            "loop_range": "incoming"
        },

        "CB_43_OVER_CMP": {
            "step_key_words": "DRY ETCH$,CMP$",
            "mode": "eq",
            "loop_range": "all"
        },

        "CB_44_UNDER_CMP": {
            "step_key_words": "CMP$",
            "mode": "eq",
            "loop_range": "all"
        },

        "CB_45_BLOW_OUT": {
            "step_key_words": "DRY ETCH$",
            "mode": "le",
            "loop_range": "all"
        },

        "CB_50_UNDER_ETCH": {
            "step_key_words": "DRY ETCH$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_51_BLOCKED_ETCH": {
            "step_key_words": "DRY ETCH$,CVD$,PHOTO$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_52_SMALL_BLOCKED_ETCH": {
            "step_key_words": "DRY ETCH$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_54_BLOCKED_ETCH_PARTIAL": {
            "step_key_words": "DRY ETCH$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_56_DEFORMED": {
            "step_key_words": "DRY ETCH$,PHOTO$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_59_BLOCKED_ETCH_ROUND": {
            "step_key_words": "DRY ETCH$,CVD$,PHOTO$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_60_CORROSION": {
            "step_key_words": "CMP$,5.*WET",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_61_GALVANIC_CORROSION": {
            "step_key_words": "CMP$,5.*WET",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_63_MISSING": {
            "step_key_words": "DRY ETCH$,PHOTO$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_67_RESIST_FAIL": {
            "step_key_words": "CMP$,PHOTO$, DRY ETCH$",
            "mode": "eq",
            "loop_range": "all"
        },

        "CB_68_TOPPLING": {
            "step_key_words": "CMP$,PHOTO$",
            "mode": "eq",
            "loop_range": "all"
        },

        "CB_71_PUDDLE": {
            "step_key_words": "DRY ETCH$",
            "mode": "le",
            "loop_range": "all"
        },

        "CB_72_ARCING": {
            "step_key_words": "DRY ETCH$,PVD$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_90_UNDERETCH": {
            "step_key_words": "DRY ETCH$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_93_LARGE_VOID": {
            "step_key_words": "DRY ETCH$,CVD$,PVD$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_94_PARTIAL_VOID": {
            "step_key_words": "PVD$,5.*WET,CMP$",
            "mode": "eq",
            "loop_range": "at_loop"
        },

        "CB_97_ROUGHNESS": {
            "step_key_words": "DRY ETCH$,CMP$",
            "mode": "eq",
            "loop_range": "all"
        }
    },

    'DRY ETCH': {
        "search_key": "NA",
        "regex": "no",
        "all":
        {
            "step_key_words": "PHOTO$",
            "mode": "eq",
            "loop_range": "at_loop"
        }
    },

    'PHOTO': {
        "search_key": "mfg_process_step",
        "regex": "yes",

        "all": {
            "regex": "1200-.*PHOTO REG",
            "step_key_words": "PHOTO$",
            "mode": "eq",
            "loop_range": "align_loop"
        }
    }

}



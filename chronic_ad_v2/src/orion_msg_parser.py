import pandas as pd
import logging
from conf import fab_mapping
logger = logging.getLogger(__name__)


def parser_space_xml(input_str):
    try:
        import re
        parameter_sequence = {
            "fab": "fab",
            "module": "module",
            "did": "design_id",
            "chId": "channel_id",
            "ckcId": "ckc_id",
            "violid": "violation_id",
            "para": "parameter_name",
            "p": "process_step",
            "m": "current_step",
            "ucl": "ucl",
            "lcl": "lcl",
            "s": "sample_id",
            "l": "lot_id",
            "w": "wafer_id",
            "mv": "value",
            "v": "label",
            "t": "tool",
            "d": "sample_date"
        }
        title = re.findall('\[.*?\]', input_str)
        processed_title = [y[y.find("[") + 1:y.find("]")] for y in title if y[1] != '/']
        processed_title = [parameter_sequence[i] for i in processed_title]

        return_var = re.split('\[.*?\]', input_str)
        processed_return_var = [x for x in return_var if x != '']

        combined = list(zip(processed_title, processed_return_var))
        combined = [list(z) for z in combined]

        r = {}
        for list_element in combined:
            if r.get(list_element[0]):
                r[list_element[0]].append(list_element[1])
            else:
                r[list_element[0]] = [list_element[1]]
        return r
    except Exception as e:
        logger.error("Error to parse the msg.")
        logger.error(str(e), exc_info=True)
        return {}


def get_final_df(d):
    try:
        col = ['sample_id', 'lot_id', 'wafer_id', 'value', 'label', 'tool', 'sample_date']
        required_col = ['fab', 'module', 'design_id', 'channel_id', 'ckc_id', 'current_step', 'process_step', 'tool',
                        'parameter_name', 'lot_id', 'wafer_id', 'value', 'lcl', 'ucl', 'label', 'sample_date']
        col_single_entry = set(d.keys()) - set(col)

        df = pd.DataFrame.from_dict({k: d[k] for k in col if k in d})
        for x in col_single_entry:
            df[x] = d[x][0]
        df['label'] = [int(x == 'true') for x in df['label']]
        df = df[required_col]
        df['fab'] = df['fab'].map(fab_mapping)
        df['lot_id'] = df['lot_id'].astype(str)
        df['lot_id'] = df['lot_id'].str[0:7]
        df['value'] = df['value'].astype(float)
        df['lcl'] = df['lcl'].astype(float)
        df['ucl'] = df['ucl'].astype(float)
        df['process_position'] = df['tool'].astype(str)
        return df
    except Exception as e:
        logger.error("Error to generate the dataframe.")
        logger.error(str(e), exc_info=True)
        return pd.DataFrame()


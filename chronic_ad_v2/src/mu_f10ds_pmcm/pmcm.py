from mu_f10ds_common import teradata
from mu_f10ds_common import util
import pandas as pd
import pmcm_config as config
import os


class PmCmCheck:
    def __init__(self, fab, lot_list, equip_list, step_list, start_date, end_date, depth, all_lot, teradata_config):
        """
        :param example
        fab = 7
        lot_list = ['9595311%']
        equip_list = ['L2KY7A1340']
        step_list = ['5030-23 PERIPH INSITU DRY ETCH']
        start_date = '2018-10-01'
        end_date = '2018-10-30'
        """

        self.fab = fab
        self.lot_list = lot_list
        self.equip_list = equip_list
        self.step_list = step_list
        self.start_date = start_date
        self.end_date = end_date
        self.depth = depth
        self.all_lot = all_lot
        self.config = teradata_config

    def pm_cm_query(self):
        teradata_config = self.config
        # initialize Teradata config
        server = teradata_config.get('server')
        user = teradata_config.get('user')
        password = teradata_config.get('password')

        td = teradata.TeradataUtil(server=server, user=user, password=password)
        teradata_connection = td.initial_connection()

        file_dir = os.path.dirname(os.path.abspath(__file__))
        pmcm_template = config.PM_CM_CONFIG['pmcm_template']
        pmcm_sql_template = util.read_file(file_dir + "/tql/" + pmcm_template)
        pmcm_exe = pmcm_sql_template.format(fab=self.fab, start_date=self.start_date, end_date=self.end_date, num='{7}',
                                            equip_id=str(self.equip_list)[1:-1])
        #print(pmcm_exe)
        pmcm_df = pd.read_sql(pmcm_exe, teradata_connection)

        if pmcm_df.shape[0] > 0:
            '''
            equip_id  context(lot_id and pm/cm status)   datetime    StepName   ranking
            '''
            pmcm_df.columns = ['context_value', 'lot_id', 'datetime', 'mfg_process_step', 'ranking']
            pmcm_df['ranking'] = pmcm_df['ranking'].astype(int)
            pmcm_result = pmcm_df.drop_duplicates()
            return pmcm_result
        else:
            return pd.DataFrame()

    def filter_non_measured(self, df):
        def check_lot_id(x):
            if '.' in x:
                return x[0:7]
            else:
                return x

        df['lot_id'] = df['lot_id'].apply(check_lot_id)
        #convert unicode
        all_lot_list = []
        for x in self.all_lot:
            all_lot_list.append(str(x)[0:7])

        #filter non measures rows
        df_staging = df[(df['lot_id'].isin(all_lot_list)) | (df['lot_id'].str.contains('REPAIR', na=False)) | \
                (df['lot_id'].str.contains('PM', na=False))]
        df_staging = df_staging.reset_index(drop=True).reset_index().drop(['ranking'], axis=1).rename(columns={"index": "ranking"})
        df_staging['ranking'] += 1

        return df_staging

    def recursive_pm_cm(self, df):
        '''
        :param df: the result from def pm_cm_query
        :return: df with pm/cm checking result
        '''
        if len(self.step_list) == len(self.equip_list) and len(self.lot_list) == len(self.equip_list):
            def check_lot_id(x):
                if '.' in x:
                    return x[0:7]
                else:
                    return x

            tmp_df = [pd.DataFrame()] * len(self.step_list)
            for index, equip in enumerate(self.equip_list):
                df['lot_id'] = df['lot_id'].apply(check_lot_id)
                df['filter'] = df['lot_id'] + '_' + df['mfg_process_step'] + '_' + df['context_value']
                # find tail_rank
                pd_filter = df[df['filter'] == str(self.lot_list[index])[0:7] + '_' + \
                               str(self.step_list[index]) + '_' + str(equip)]

                if pd_filter.shape[0] > 0:
                    '''
                    size of pd_filter is possible to be larger than 1 since it got different ranking
                    therefore,
                    select the smallest ranking number as tail_rank  
                    '''
                    tail_rank = int(pd_filter[pd_filter['context_value'] == equip]['ranking'].iloc[0])

                    pd_filter = pd_filter.drop(['filter'], axis=1)

                    # find the most recent PM/CM
                    df = df.drop(['filter'], axis=1)
                    pd_rank = df[(df['ranking'] <= tail_rank) & (df['context_value'] == str(equip))]

                    pd_contains_pmcm = pd_rank[pd_rank['lot_id'].str.contains('PM') \
                                               | pd_rank['lot_id'].str.contains('REPAIR')].sort_values(by='ranking',
                                                                                                       ascending=True)
                    pd_the_most_recent_pmcm = pd_contains_pmcm.tail(1)

                    if pd_the_most_recent_pmcm.shape[0] > 0:
                        '''
                        why checking df.shape[0]
                        when finished for loop, number of tail_rank >= number of head_rank

                        the reason is column 'context_value' not only contains equipment id even we select column 'feature' as 
                        equipment_id or process_chamber
                        '''
                        head_rank = int(pd_the_most_recent_pmcm[pd_the_most_recent_pmcm['context_value'] == equip]\
                                            ['ranking'].iloc[0])

                        pd_checking = df[(df['ranking'] <= tail_rank) & \
                                         (df['ranking'] > head_rank) & \
                                         (df['mfg_process_step'] == str(self.step_list[index]))]

                        pd_checking['final_rank'] = pd_checking['ranking'].rank(ascending=1)

                        final_depth = pd_checking.shape[0]
                        if final_depth <= self.depth:
                            pd_filter = pd_filter.drop(['ranking'], axis=1)
                            pd_filter['reason'] = 'PM/CM, ' + 'occurred time:' + str(pd_filter['datetime'].iloc[0])[0:11]
                            pd_filter = pd_filter.drop_duplicates()

                            tmp_df[index] = pd_filter

                        else:
                            pd_filter = pd_filter.drop(['ranking'], axis=1)
                            pd_filter['reason'] = ''
                            pd_filter = pd_filter.drop_duplicates()

                            tmp_df[index] = pd_filter

            pd_final = pd.concat(tmp_df).reset_index(drop=True)

            return pd_final
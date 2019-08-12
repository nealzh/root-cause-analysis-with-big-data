import unittest
from mu_f10ds_test_trv.test_trv import TestTravelerQuery
import pandas as pd
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

class Test_trv(unittest.TestCase):
    def test_traveler(self):
        fab = 7
        lot_list = ['J273427.007', 'J447397.007', 'H975967.007', 'J372787.007', 'J472587.007', 'J315287.007', 'J363897.007', 'H995577.007', 'J265967.007']
        current_step = '8401-DF NITR QUAL'
        query_session = '2018-12-30 00:00:00'
        teradata_config = {
            'server': 'SGTERAPROD09',
            'user': 'hdfsf10w',
            'password': 'F10Hdp#01'
        }
        trv_instance = TestTravelerQuery(fab=fab, lot_list=lot_list, current_step=current_step, query_session=query_session, teradata_config=teradata_config)
        df = trv_instance.test_trv()
        print df
        self.assertEqual(137, df.shape[0])
        print('successful')


if __name__ == '__main__':
    unittest.main()
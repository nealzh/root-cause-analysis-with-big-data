import unittest
from sigma_query import SigmaQuery


class TestUtil(unittest.TestCase):
    def test_sigma_query(self):
        fab = 10
        tz = 'Asia/Singapore'
        host = 'fslhdppname3.imfs.micron.com',
        port = 9090
        cluster_type = 'prod'
        columns = {
            'lot': {
                'cf:EQUIPMENT_ID': 'equipment_id',
                'cf:PROCESS_ID': 'process_id',
                'cf:GERM_RECIPE - RUN_PROC_DATA||1': 'recipe',
                'cf:RETICLE_1 - DG_AD (PROCESS_JOB)||1': 'reticle',
                'cf:MFG_PROCESS_STEP': 'mfg_process_step'},

            'wafer': {
                'cf:WAFER_TYPE': 'wafer_type',
                'cf:WAFER_SCRIBE': 'wafer_scribe',
                'cf:PROCESS_CHAMBER - WAFER_ATTR||1': 'process_chamber',
                'cf:RETICLE_1 - DG_AD||1': 'reticle',
                'cf:TRACK Process Data/RESIST_1||1': 'resist',
                'cf:PROCESS_POSITION - WAFER_ATTR||1': 'process_position'
            }
        }



        sigma_query_lot_instance = SigmaQuery(fab=fab, data_type='lot', host=host, port=port,
                                              cluster_type=cluster_type, columns=columns['lot'],
                                              include_timestamp=True)
        print(sigma_query_lot_instance.sigma_query(lot_id='0000000'))



if __name__ == '__main__':
    unittest.main()
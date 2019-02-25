import unittest
from functions import *

class TestUtil(unittest.TestCase):
    def test_format_datetime_string(self):
        step_normal = '1210-52 WL PHOTO SEM CD'
        step_bdis = 'BDIS_4200-28 W0 OXIDE DEP'
        step_d3 = 'D3_1001-21_L06B_21_AD_PIDSC_1001'
        self.assertEqual(get_loop_id(step_normal), '52')
        self.assertEqual(get_loop_id(step_bdis), '28')
        self.assertEqual(get_loop_id(step_d3), '21')




if __name__ == '__main__':
    unittest.main()
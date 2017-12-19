# -*- coding: utf-8 -*-

import re
import time

_dir_path = '\\'.join(re.split('[\\\/]', __file__)[:-3])
#_data_dir_path = _dir_path + '\\data'
#_package_dir_path = _dir_path + '\\package'
#_tests_dir_path = _dir_path + '\\tests'
#_logs_dir_path = _dir_path + '\\logs'

# 年月日时间戳，时间比较，时间段合并
def time_str(_tmp_date=''):
    _tmp_current_date = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))
    if _tmp_date in ['present', 'TODAY']:
        return _tmp_current_date
    if _tmp_date != '':
        _tmp_date = str(_tmp_date)
        if re.search(r'^[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]$', _tmp_date):
            if _tmp_date <= _tmp_current_date:
                return _tmp_date
    return '0000-00-00'
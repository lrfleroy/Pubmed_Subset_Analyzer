# -*- coding: utf-8 -*-
import urllib2
import time
import re
from package.docproc import doc

class Page:
    #statics
    __headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'
    }
    __log_dir_path = '\\'.join(re.split('[\\\/]', __file__)[:-3]) + '\\logs\\crawler_logs'
    __error_log_file_path = __log_dir_path + '\\Page.' + str(time.strftime('%Y-%m-%d', time.localtime(time.time()))) + '.error_log'

    def __init__(self, _url=''):
        self._url = _url
        self._type = ''
        self._id = ''
        self._contain = ''

        self.__log = []
        self.__error_log = []

    def __deal_error_log(self):
        if len(self.__error_log) != 0:
            _tmp_log_file = doc.LogFile('Page.error')
            _tmp_log_file.add(self.__error_log)
            del _tmp_log_file
            del self.__error_log[:]
        return

    def request_html(self):
        _attempts = 0
        success = False
        while _attempts < 3 and not success:
            _attempts += 1
            time.sleep(0.3)
            try:
                _request = urllib2.Request(url=self._url, headers=self.__headers)
                _socket = urllib2.urlopen(_request, timeout=3)
                self._contain = _socket.read()
                self.__deal_error_log()
                return self._contain
            #except urllib2.HTTPError:
            #    print 'HTTPError'
            #    continue
            except Exception, e:
                _error_info = str(time.asctime()) + ' Page.request_html(): '
                _error_info += 'URL=' + str(self._url) + ' attempts=' + str(_attempts) + ' ' + str(e)
                #print _error_info
                self.__error_log.append(_error_info)
        self.__deal_error_log()
        return

    def write_2_file(self, _path):
        _tmp_html_doc = doc.File(_path)
        if self._contain == '':
            self.request_html()
        _tmp_html_doc.write(self._contain)
        return

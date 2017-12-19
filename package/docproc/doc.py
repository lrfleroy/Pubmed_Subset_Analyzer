# -*- coding: utf-8 -*-
import os
import re
import time
import shutil
import json

import datetime

from package.backstage import setting

class File:
    def __init__(self, _file_path):
        self.dir = ''
        self.path = ''
        self.__path_check(_file_path)
        self.name = ''
        self.type = ''
        self.__path_parser()
        self.contain = ''

    def __path_check(self, _file_path):
        _file_path = str(_file_path)
        self.dir = '\\'.join(re.split('[/\\\]', _file_path)[:-1])
        if os.path.exists(self.dir):
            self.path = _file_path
            return True
        else:
            print _file_path + ': File don\'t exists.'
            #self.path = _file_path
            return False

    def __path_parser(self):
        self.name = re.split('[/\\\]', self.path)[-1]
        self.type = self.name.split('.')[-1]

    def __fresh_contain(self):
        self.contain = ''
        return

    def read(self):
        if self.type == 'list':
            self.contain = ListConnector(self.path).read()
        elif self.type == 'json':
            self.contain = JsonConnector(self.path).read()
        elif self.type in ['html', 'med']:
            self.contain = HtmlConnector(self.path).read()
        else:
            self.contain = TxtConnector(self.path).read()
        return self.contain

    def write(self, _contain):
        self.__fresh_contain()
        if self.type == 'list':
            return ListConnector(self.path).write(_contain)
        elif self.type == 'json':
            return JsonConnector(self.path).write(_contain)
        elif self.type in ['html', 'med']:
            return HtmlConnector(self.path).write(_contain)
        else:
            return TxtConnector(self.path).write(_contain)

    def add(self, _contain):
        self.__fresh_contain()
        if self.type in ['list']:
            return ListConnector(self.path).add(_contain)
        elif self.type in ['json']:
            return JsonConnector(self.path).add(_contain)
        elif self.type in ['html', 'med']:
            return HtmlConnector(self.path).add(_contain)
        else:
            return TxtConnector(self.path).add(_contain)

    def clear(self):
        self.__fresh_contain()
        _tmp_file = open(self.path, 'w')
        _tmp_file.close()
        return not os.path.getsize(self.path)

    def delete(self):
        self.__fresh_contain()
        os.remove(self.path)
        return not os.path.exists(self.path)

    def copy_from(self, _target_file_path):
        self.__fresh_contain()
        #print self.path
        if os.path.isfile(_target_file_path):
            return shutil.copyfile(_target_file_path, self.path)
        else:
            print _target_file_path + ' don\'t exist!'
            return

    def move_from(self, _target_file_path):
        self.__fresh_contain()
        if os.path.isfile(_target_file_path):
            return shutil.move(_target_file_path, self.path)
        else:
            print _target_file_path + ' don\'t exist!'
            return

class TxtConnector:
    def __init__(self, _file_path):
        self._path = _file_path
        self._contain = ''

    def read(self):
        return self.read_doc_2_str()

    def write(self, _tmp_str, _file_flow_sig='w'):
        return self.rewrite_str_2_doc(_tmp_str)

    def add(self, _tmp_str):
        return self.add_str_2_doc(_tmp_str)

    def set_contain(self, _tmp_str):
        self._contain = _tmp_str

    def __update_contain(self):
        _tmp_file = open(self._path, 'r')
        self._contain = _tmp_file.read()
        _tmp_file.close()

    def __is_contain_empty(self):
        _tmp_sig = self._contain == ''
        if _tmp_sig:
            print 'nothing is write!'
        return _tmp_sig

    def write_contain(self, _file_flow_sig='w'):
        if self.__is_contain_empty():
            return False
        _tmp_file = open(self._path, _file_flow_sig)
        _tmp_file.write(self._contain + '\n')
        _tmp_file.close()
        return

    def add_contain(self):
        return self.write_contain('a')

    def read_doc_2_str(self):
        _tmp_file = open(self._path, 'r')
        _tmp_str = _tmp_file.read()
        _tmp_file.close()
        return _tmp_str

    def rewrite_str_2_doc(self, _tmp_str, _file_flow_sig='w'):
        _tmp_file = open(self._path, _file_flow_sig)
        _tmp_file.write(str(_tmp_str) + '\n')
        _tmp_file.close()
        return

    def add_str_2_doc(self, _tmp_str):
        return self.rewrite_str_2_doc(_tmp_str, 'a')

class ListConnector():
    def __init__(self, _file_path):
        self._path = _file_path

    def read(self):
        _contain_list = []
        _tmp_file = open(self._path, 'r')
        for _line in _tmp_file:
            _contain_list.append(_line.strip())
        _tmp_file.close()
        return _contain_list

    def write(self, _tmp_list, _file_flow_sig='w'):
        _tmp_file = open(self._path, _file_flow_sig)
        for _line in _tmp_list:
            _tmp_file.write(str(_line).strip() + '\n')
        _tmp_file.close()
        return

    def add(self, _tmp_list):
        return self.write(_tmp_list, 'a')

class HtmlConnector(TxtConnector):
    def __init__(self, _file_path):
        TxtConnector.__init__(self, _file_path)

    #def Parser(self):
    #    pass

class JsonConnector():
    def __init__(self, _file_path):
        self._path = _file_path

    def read(self):
        _contain_dict = {}
        _tmp_file = open(self._path, 'r')
        _contain_dict = json.loads(_tmp_file.read())
        _tmp_file.close()
        return _contain_dict

    def write(self, _tmp_dict, _file_flow_sig='w'):
        _tmp_str = json.dumps(_tmp_dict)
        _tmp_file = open(self._path, _file_flow_sig)
        _tmp_file.write(_tmp_str.strip() + '\n')
        _tmp_file.close()
        return

    def add(self, _tmp_dict):
        return self.write(_tmp_dict, 'w')


class LogFile(File):
    _logs_root_path = setting._dir_path + '\\logs'
    def __init__(self, _module_name):
        _date = str(datetime.datetime.now().date())
        File.__init__(self, self._logs_root_path + '\\' + str(_module_name) + '.' + _date + '.log')

class DataDoc():
    _data_main_dir_path = setting._dir_path + '\\data'
    _default_dir_path = '\\tmp_data'
    def __init__(self, _file_name, _file_type='', _subdir_path='', _dir_path=''):
        self._name = ''
        self._type = ''
        self._subdir_path = ''
        self._dir_path = ''
        self._path =''
        self._body = ''
        #文件名和文件类型
        if _file_type != '':
            self._name = _file_name
            self._type = _file_type
        else:
            _tmp_file_name_list = _file_name.split('.')
            if len(_tmp_file_name_list) > 1:
                self._name = '.'.join(_tmp_file_name_list[:-1])
                self._type = _tmp_file_name_list[-1]
            else:
                self._name = _file_name
                self._type = ''
        #文件目录
        if _subdir_path == '':
            self._subdir_path = self._default_dir_path
        else:
            self._subdir_path = _subdir_path
        if _dir_path == '':
            self._dir_path = self._data_main_dir_path + self._subdir_path
        else:
            self._dir_path = _dir_path
        #文件路径
        self._update_path()
        #文件句柄
        self._body = File(self._path)

    def _update_path(self):
        self._path = self._dir_path + '\\' + self._name + '.' + self._type

    def get_path(self):
        return self._path

    def read(self):
            return self._body.read()
    def write(self, _tmp_data):
        return self._body.write(_tmp_data)
    def add(self, _tmp_data):
        return self._body.add(_tmp_data)
    def clear(self):
        return self._body.clear()

    def delete(self):
        return self._body.delete()

    def copy_from(self, _target_file_path):
        return self._body.copy_from(_target_file_path)

    def move_from(self, _target_file_path):
        return self._body.move_from(_target_file_path)



class ListDoc(DataDoc):
    __subdir_path = ''
    def __init__(self, _tmp_data_dir_path='', _start_time='', _end_time='', _file_type='list'):
        self._start_time = self._is_time_str(_start_time)
        self._end_time = self._is_time_str(_end_time)
        self.__check_data_dir(_tmp_data_dir_path)
        self.__update_name()
        DataDoc.__init__(self, self._name, _file_type, self.__subdir_path, self._dir_path)
        self._contain_list = []

    #年月日时间戳，时间比较，时间段合并
    def _is_time_str(self, _tmp_date):
        _tmp_current_date = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))
        if _tmp_date in ['present', 'TODAY']:
            return _tmp_current_date
        if _tmp_date != '':
            _tmp_date = str(_tmp_date)
            if re.search(r'^[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]$', _tmp_date):
                if _tmp_date <= _tmp_current_date:
                    return _tmp_date
        return '0000-00-00'

    def __check_data_dir(self, _tmp_data_dir_path):
        _tmp_dir_list = re.split('[/\\\]', _tmp_data_dir_path)
        if len(_tmp_dir_list) >= 2:
            _up_dir_path = '\\'.join(re.split('[/\\\]', _tmp_data_dir_path)[:-1])
            self._dataset_name = re.split('[/\\\]', _tmp_data_dir_path)[-1]
        else:
            _up_dir_path = self._data_main_dir_path + self._default_dir_path
            self._dataset_name = 'unknown'
            _tmp_data_dir_path = _up_dir_path + '\\' + self._dataset_name
            print _tmp_data_dir_path + ' will be saved in ' + self._default_dir_path + '!'
        _tmp_dir_path = _up_dir_path + '\\list'
        if not os.path.isdir(_up_dir_path):
            os.mkdir(_up_dir_path)
        if not os.path.isdir(_tmp_data_dir_path):
            os.mkdir(_tmp_data_dir_path)
        if not os.path.isdir(_tmp_dir_path):
            os.mkdir(_tmp_dir_path)
        self._dir_path = _tmp_dir_path
        return True

    def __update_name(self):
        #list文件采用 数据文件夹名+时间段 命名
        if self._start_time > self._end_time:
            self._end_time = self._start_time
        self._name = self._dataset_name + '.' + self._start_time + '_' + self._end_time

    def __set_path(self, _file_name, _dir_path=''):
        if _dir_path == '':
            self._name = str(_file_name)
            DataDoc._update_path(self)


class SRAList(ListDoc):
    __subdir_path = '\\download\\sra\\sra'
    def __init__(self, _start_time='', _end_time=''):
        _tmp_data_dir_path = self._data_main_dir_path + self.__subdir_path
        ListDoc.__init__(self, _tmp_data_dir_path, _start_time, _end_time, 'list')

class PMIDList(ListDoc):
    __subdir_path = '\\download\\pubmed\\med'
    def __init__(self, _tmp_data_dir_path=None, _start_time='', _end_time=''):
        if _tmp_data_dir_path == None:
            _tmp_data_dir_path = self._data_main_dir_path + self.__subdir_path
        else:
            _tmp_data_dir_path = _tmp_data_dir_path
        print _tmp_data_dir_path
        ListDoc.__init__(self, _tmp_data_dir_path, _start_time, _end_time)

class HtmlDoc(DataDoc):
    __subdir_path = ''
    def __init__(self, _file_name, _file_type='html', _target_url='about:blank', _subdir_path='', _dir_path=''):
        DataDoc.__init__(self, str(_file_name), _file_type, _subdir_path, _dir_path)
        self._url = _target_url

    def __set_path(self, _file_name, _dir_path=''):
        if _dir_path == '':
            self._name = str(_file_name)
            DataDoc._update_path(self)

    def download_html(self, _target_url=''):
        from package.crawler import page
        if _target_url == '':
            _target_url = self._url
        _page = page.Page(_target_url)
        self._contain = _page.request_html()
        if self._contain != None:
            self.write(self._contain)
        return self._contain


class MedDoc(HtmlDoc):
    __subdir_path = '\\download\\pubmed\\med'
    def __init__(self, _pmid, _dir_path=''):
        self._pmid = str(_pmid)
        self.__update_url()
        HtmlDoc.__init__(self, self._pmid, 'med', self._url, self.__subdir_path, _dir_path)

    def __set_path(self, _pmid, _dir_path=''):
        if _dir_path == '':
            self._name = str(_pmid) + '.med'
            HtmlDoc._update_path(self)

    def __update_url(self):
        self._url = 'https://www.ncbi.nlm.nih.gov/pubmed/' + self._pmid + '?report=medline&format=text'
        return self._url

    def set_pmid(self, _pmid):
        self.__set_path(_pmid)
        self.__update_url()

    def read(self):
        if not os.path.isfile(self._path):
            self.download_html()
        return self._body.read()
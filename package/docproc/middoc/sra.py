# -*- coding: utf-8 -*-

import re
import os
from bs4 import BeautifulSoup

from package.backstage import setting
from package.docproc import doc
from package.dbconn import dbtable
from package.docproc.middoc import website

class SRP():
    def __init__(self, _sra=None):
        self._data_number = 0
        self._update_date = None
        #self._sra = _sra
        website.WebsiteDict().update_database()
        self._SRA_data_url_header = dbtable.DBOpera('Website').search_dbtable(['DURL'], {'TI':'SRP'})[0][0]

    #Step3： 更新SRA Study信息
    def update_sra_study_info(self, _update_date='present'):
        self._update_date = setting.time_str(_update_date)

        _sig = 0
        _study_list = dbtable.DBOpera('SRP').search_dbtable(['DISTINCT SRPID'])
        for _study_id in _study_list:
            if _study_id[0] == 'SRP041285':
                _sig = 1
            if _sig == 0:
                continue
            print _study_id
            _study_id = str(_study_id[0])
            # 取study_list，获取链接，分离pmid
            _pmid_list = self.__get_pmid_from_srp_page(_study_id)
            if _pmid_list == None:
                continue
            if len(set(_pmid_list)) > 1:
                print '>>>>pmid more than 1'
                print _pmid_list
            # 得到有pmid的study_list, 获取og+st组合, 更新文献标注
            self.__create_tag_2_Article(_study_id, _pmid_list[0])

        #self.__update_srp_update_date()

    def __get_pmid_from_srp_page(self, _study_id):

        _url = self._SRA_data_url_header + _study_id
        _data_subdir_path = '\\download\\sra\\srp'
        _html_file_path = setting._dir_path + '\\data' + _data_subdir_path + '\\' + _url + '.html'
        if os.path.isfile(_html_file_path):
            _study_html = open(_html_file_path, 'r').read()
        else:
            _study_html = doc.HtmlDoc(_study_id, 'html', _url, _data_subdir_path).download_html()
            if _study_html == None:
                return None
        _soup = BeautifulSoup(_study_html, 'html.parser')
        data_para_header = _soup.find('span', string=re.compile(r'^External Link:'))
        _pmid_list = []
        if data_para_header:
            # print data_para_header
            # Pubmed URL is in <strong> after the <span>
            _the_strong_tag = data_para_header.find_next_sibling('strong')

            _tag_dict = {'article': [], 'otheratag': [], 'other': []}
            _log_doc = doc.LogFile('srp_get_pmid')
            _log_doc.add('> ' + str(_study_id) + '\n')
            # strong是否有内容
            _the_sub_tag_list = _the_strong_tag.find_all(True)
            if len(_the_strong_tag.string) != 0:
                if re.search(r'^/PUBMED:', _the_strong_tag.string):
                    _pmid = _the_strong_tag.string.split(':')[1]
                    _pmid_list.append(str(_pmid))
                elif len(_the_sub_tag_list) == 0:
                    print 'Other Strong: ' + str(_the_strong_tag)

            for _a_tag in _the_strong_tag.find_all('a', attrs={'class': 'pubmed_id'}):
                for _pmid in _a_tag['rel']:
                    _pmid_list.append(str(_pmid))
            for _sub_tag in _the_sub_tag_list:
                if _sub_tag.content == 'Website name':
                    continue
                if re.search(r'^<a ', str(_sub_tag)):
                    if re.search(r'class="pubmed_id"', str(_sub_tag)):
                        continue
                    if re.search(r'[./]doi[./]', str(_sub_tag)):
                        _tag_dict['article'].append(_sub_tag['rel'])
                        continue
                    if re.search(r'article', str(_sub_tag)):
                        print '>>>article>>'
                        print _sub_tag
                        _tag_dict['article'].append(_sub_tag)
                    print '>>>Other <a>'
                    print _sub_tag
                    _tag_dict['otheratag'].append(_sub_tag)
                    continue
                _tag_dict['other'].append(_sub_tag)
                print '### Other Tag<>'
                print _sub_tag
                _log_doc.add(_sub_tag.string + '\n')
            del _log_doc
        if _pmid_list == []:
            _pmid_list = None
        return _pmid_list

    def __create_tag_2_Article(self, _study_id, _pmid):
        _OS_id_list_list = dbtable.DBOpera('SRP').search_dbtable(['OGID', 'STID'], {'SRPID':_study_id})
        _OGID_LIST = []
        _STID_LIST = []
        for _OS_id_list in _OS_id_list_list:
            _OGID_LIST.append(str(_OS_id_list[0]))
            _STID_LIST.append(str(_OS_id_list[1]))
        ##pmid-list处理
        _dbtable = dbtable.DBOpera('Article')
        _db_pmid = _dbtable.search_dbtable(['PMID'], {'PMID':_pmid})
        if len(_db_pmid) < 1:
            _dbtable.insert_dbtable({'PMID':_pmid,
                                     'PSIG':'1',
                                     'SIDL':_study_id,
                                     'ROGL':'|'.join(_OGID_LIST),
                                     'RSTL':'|'.join(_STID_LIST)}
                                    )



    def __update_srp_update_date(self):
        dbtable.DBOpera('Website').update_dbtable({'UDT':self._update_date}, {'TI':'SRP'})

class SRPItem:
    SRP_id = ''
    OGID = ''
    STID = ''
    SRA_list = []
    def __init__(self, _SRAItem=None):
        if _SRAItem == None:
            self.__create_item_2_SRP_table()
        self._SRAItem = _SRAItem
        if self.__is_new_srp_og_st():
            self.__create_item_2_SRP_table()
        else:
            SRPItem.SRA_list.append(str(self._SRAItem['SRAID']))


    def __is_new_srp_og_st(self):
        #print SRPItem.SRP_id + ' ' + SRPItem.OGID + ' ' + SRPItem.STID
        if SRPItem.SRP_id != str(self._SRAItem['SRPID']):
            return True
        if SRPItem.OGID != str(self._SRAItem['OGID']):
            return True
        if SRPItem.STID != str(self._SRAItem['STID']):
            return True
        return False


    def __create_item_2_SRP_table(self):
        if SRPItem.SRP_id == '':
            SRPItem.SRP_id = str(self._SRAItem['SRPID'])
            SRPItem.OGID = str(self._SRAItem['OGID'])
            SRPItem.STID = str(self._SRAItem['STID'])
            SRPItem.SRA_list = [str(self._SRAItem['SRAID'])]
        else:
            #print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
            _SRA_str = '|'.join(SRPItem.SRA_list)
            _dbtable = dbtable.DBOpera('SRP')
            _SID = _dbtable.search_dbtable(['SID'], {'SRPID':SRPItem.SRP_id, 'OGID':SRPItem.OGID, 'STID':SRPItem.STID})
            if len(_SID) == 0:
                 _dbtable.insert_dbtable({'SRPID':SRPItem.SRP_id, 'OGID':SRPItem.OGID, 'STID':SRPItem.STID, 'SRAL':_SRA_str})
            else:
                _dbtable.update_dbtable({'SRAL':_SRA_str}, {'SID':_SID[0][0]})
            del _dbtable

            if self._SRAItem == None:
                return

            SRPItem.SRP_id = str(self._SRAItem['SRPID'])
            SRPItem.OGID = str(self._SRAItem['OGID'])
            SRPItem.STID = str(self._SRAItem['STID'])
            SRPItem.SRA_list = [str(self._SRAItem['SRAID'])]



class SRA():
    def __init__(self):
        self._data_number = 0
        self._update_date = None
        website.WebsiteDict().update_database()
        #self._SRA_website_id = dbtable.DBOpera('Website').search_dbtable(['WID'], {'TI':'SRA'})[0][0]

    #Step2： 更新sra experiment列表
    def write_sra_result_file_2_database(self, _file_path, _update_date='present'):
        self._update_date = setting.time_str(_update_date)
        _doc = doc.SRAList(setting.time_str(), self._update_date)
        _doc.copy_from(_file_path)

        _file = open(_doc.get_path(), 'r')
        _sig = 0
        _count = 0
        for _line in _file:
            _line = _line.strip()
            if _line == '':
                continue
            if _sig == 0:
                if re.search(r'^"Experiment Accession"', _line):
                    sig = 1
                    continue
            _sra_item = SRAItem(_line)
            #_sra_item = SRAItem(_line, self._SRA_website_id)
            _count += 1
            #print str(_sra_item._items_dict)
            del _sra_item
        print str(_count) + ' SRA experiment has been processed.'
        del _doc

        #self.__update_sra_update_date()

    #def __update_sra_update_date(self):
    #   dbtable.DBOpera('Website').update_dbtable({'UDT':self._update_date}, {'WID':self._SRA_website_id})

class SRAItem():
    _sra_tag_list = ['SRAID', 'SRPID', 'OGTI', 'OGID', 'STTI', 'STID']
    _sra_list_tag_list = []
    def __init__(self, _tmp_str=None):
        if _tmp_str == None:
            self.__create_item_2_SRP_table_tail()
        self._items_dict = {}
        for _tag in self._sra_tag_list:
            self._items_dict[_tag] = None
        self.__SRA_file_line_parser(_tmp_str)

    def get_SRAID(self):
        return self._items_dict['SRAID']

    def __SRA_file_line_parser(self, _tmp_str):
        _tmp_list = _tmp_str.strip()[1:-1].split('","')
        self._items_dict['SRAID'] = _tmp_list[0]    # sra_result文件：Experiment Accession
        self._items_dict['OGTI'] = _tmp_list[2]     # sra_result文件：Organism Name
        self.__create_item_2_Organism_table()
        self._items_dict['STTI'] = _tmp_list[14]    # sra_result文件：Library Strategy
        self.__create_item_2_Strategy_table()
        self._items_dict['SRPID'] = _tmp_list[5]      # sra_result文件：Study Accession
        self.__create_item_2_SRP_table()

        #self.__create_item_2_Data_table()


    #创建Organism数据表条目
    def __create_item_2_Organism_table(self):
        _dbtable = dbtable.DBOpera('Organism')
        self._items_dict['OGID'] = _dbtable.search_dbtable(['OGID'], {'TI':self._items_dict['OGTI']})
        if len(self._items_dict['OGID']) < 1:
            _key_list = ['OGTI']
            _column_list = ['TI']
            _dbtable.insert_dbtable(self._items_dict, _key_list, _column_list)
        self._items_dict['OGID'] = _dbtable.search_dbtable(['OGID'], {'TI':self._items_dict['OGTI']})[0][0]
        del _dbtable

    #创建Strategy数据表条目
    def __create_item_2_Strategy_table(self):
        _dbtable = dbtable.DBOpera('Strategy')
        self._items_dict['STID'] = _dbtable.search_dbtable(['STID'], {'TI':self._items_dict['STTI']})
        if len(self._items_dict['STID']) != 1:
            _key_list = ['STTI']
            _column_list = ['TI']
            _dbtable.insert_dbtable(self._items_dict, _key_list, _column_list)
        self._items_dict['STID'] = _dbtable.search_dbtable(['STID'], {'TI':self._items_dict['STTI']})[0][0]
        del _dbtable

    def __create_item_2_SRP_table(self):
        _SRP = SRPItem(self._items_dict)
        del _SRP

    def __create_item_2_SRP_table_tail(self):
        _SRP = SRPItem()
        del _SRP

    #创建Data数据表条目
    ##后续tag时需要更新以下两个参数
    # Process sig: Tagged:0/1. The result equals add_together(items).
    #-Pubmed id
    def __create_item_2_Data_table(self):
        _dbtable = dbtable.DBOpera('Data')
        self._items_dict['DID'] = _dbtable.search_dbtable(['DID'], {'RID':self._items_dict['RID']})
        if len(self._items_dict['DID']) != 1:
            _key_list = ['PSIG', 'WID', 'RID', 'SRPID', 'OGID', 'STID']
            _dbtable.insert_dbtable(self._items_dict, _key_list)
        self._items_dict['DID'] = _dbtable.search_dbtable(['DID'], {'RID':self._items_dict['RID']})[0][0]
        del _dbtable

class _SRAItem():
    _sra_tag_list = ['DID', 'PSIG', 'WID', 'RID', 'SID', 'OGTI', 'OGID', 'STTI', 'STID', 'PMID']
    _sra_list_tag_list = []
    def __init__(self, _tmp_str, _WID):
        self._items_dict = {}
        for _tag in self._sra_tag_list:
            self._items_dict[_tag] = ''
        self._items_dict['WID'] = _WID
        self.__SRA_file_line_parser(_tmp_str)

    def get_RDID(self):
        return self._items_dict['RID']

    def __SRA_file_line_parser(self, _tmp_str):
        _tmp_list = _tmp_str.strip()[1:-1].split('","')
        self._items_dict['PSIG'] = '0'
        self._items_dict['RID'] = _tmp_list[0]     # sra_result文件：Experiment Accession
        self._items_dict['SID'] = _tmp_list[5]      # sra_result文件：Study Accession
        self._items_dict['OGTI'] = _tmp_list[2]     # sra_result文件：Organism Name
        self.__create_item_2_Organism_table()
        self._items_dict['STTI'] = _tmp_list[14]    # sra_result文件：Library Strategy
        self.__create_item_2_Strategy_table()
        self.__create_item_2_Data_table()


    #创建Organism数据表条目
    def __create_item_2_Organism_table(self):
        _dbtable = dbtable.DBOpera('Organism')
        self._items_dict['OGID'] = _dbtable.search_dbtable(['OGID'], {'TI':self._items_dict['OGTI']})
        if len(self._items_dict['OGID']) != 1:
            _key_list = ['OGTI']
            _column_list = ['TI']
            _dbtable.insert_dbtable(self._items_dict, _key_list, _column_list)
        self._items_dict['OGID'] = _dbtable.search_dbtable(['OGID'], {'TI':self._items_dict['OGTI']})[0][0]
        del _dbtable

    #创建Strategy数据表条目
    def __create_item_2_Strategy_table(self):
        _dbtable = dbtable.DBOpera('Strategy')
        self._items_dict['STID'] = _dbtable.search_dbtable(['STID'], {'TI':self._items_dict['STTI']})
        if len(self._items_dict['STID']) != 1:
            _key_list = ['STTI']
            _column_list = ['TI']
            _dbtable.insert_dbtable(self._items_dict, _key_list, _column_list)
        self._items_dict['STID'] = _dbtable.search_dbtable(['STID'], {'TI':self._items_dict['STTI']})[0][0]
        del _dbtable

    #创建Data数据表条目
    ##后续tag时需要更新以下两个参数
    # Process sig: Tagged:0/1. The result equals add_together(items).
    #-Pubmed id
    def __create_item_2_Data_table(self):
        _dbtable = dbtable.DBOpera('Data')
        self._items_dict['DID'] = _dbtable.search_dbtable(['DID'], {'RID':self._items_dict['RID']})
        if len(self._items_dict['DID']) != 1:
            _key_list = ['PSIG', 'WID', 'RID', 'SID', 'OGID', 'STID']
            _dbtable.insert_dbtable(self._items_dict, _key_list)
        self._items_dict['DID'] = _dbtable.search_dbtable(['DID'], {'RID':self._items_dict['RID']})[0][0]
        del _dbtable
# -*- coding: utf-8 -*-
import re

from package.docproc import doc
from package.dbconn import dbtable
from package.backstage import setting

class PubMed():
    def __init__(self, _list_source=None):
        if _list_source == None:
            self._set_name = 'pubmed'
        else:
            self._set_name = str(_list_source)
        self._data_path = setting._dir_path + '\\data\\download\\' + self._set_name + '\\med'
        self._data_number = 0
        self._update_date = None

    #Step1： 更新pmid列表
    def write_medline_file_2_database(self, _file_path, _update_date='present'):

        self._update_date = setting.time_str(_update_date)
        _doc = doc.PMIDList(self._data_path, setting.time_str(), self._update_date)
        _doc.copy_from(_file_path)

        _file = open(_doc.get_path(), 'r')
        _count = 0
        for _pmid in _file:
            _pmid = _pmid.strip()
            #print _pmid
            self.__create_item_2_medline_dbtable(_pmid)
            _count += 1
        print str(_count) + ' medline items saved to Medline table in database.'
        del _doc

    #Step4： 更新全文HTML
    def download_full_text(self):
        _dbtable = dbtable.DBOpera('Medline')
        _dbtable_rows = _dbtable.count_rows()
        _rows_per_page = 100
        #分页
        for _page_id in range(int(_dbtable_rows) / _rows_per_page + 1):
            _start_num = _page_id * _rows_per_page
            _result_list = _dbtable.search_dbtable(['PMID', 'AURL'],
                                                 {'id':['>', str(_start_num)]},
                                                 'ORDER BY id ASC LIMIT ' + str(_rows_per_page)
                                                 )
            for _result in _result_list:
                _pmid = _result[0]
                #print '>>>>>>'
                #print _pmid
                #print '>>>>>>'
                _Article_url_list = _result[1].split('|')
                _doc = doc.HtmlDoc(_pmid, 'html', _Article_url_list[0], '', setting._dir_path + '\\data\\download\\pubmed\\html')
                _doc.download_html()
                #解析html到全文
                del _doc

        del _dbtable


    def update_dbtable_article_2_medline(self):
        _pmid_list = dbtable.DBOpera('Medline').search_dbtable(['PMID'])
        for _pmid in _pmid_list:
            self.__create_item_2_medline_dbtable(_pmid)


    def update_dbtable_medline_2_article(self):
        _dbtable = dbtable.DBOpera('Medline')
        _dbtable_rows = _dbtable.count_rows()
        _rows_per_page = 100
        #分页
        for _page_id in range(int(_dbtable_rows) / _rows_per_page + 1):
            _start_num = _page_id * _rows_per_page
            _result_list = _dbtable.search_dbtable(['PMID'],
                                                 {'id':['>', str(_start_num)]},
                                                 'ORDER BY id ASC LIMIT ' + str(_rows_per_page)
                                                 )
            for _result in _result_list:
                self.__create_item_2_medline_dbtable(_result[0])


    def __create_item_2_medline_dbtable(self, _pmid):
        if self.__is_pmid(_pmid):
            _med_item = MedlineItem(_pmid)
            del _med_item

    def __create_item_2_article_dbtable(self, _pmid):
        _dbtable = dbtable.DBOpera('Article')
        _db_pmid = _dbtable.search_dbtable(['PMID'], {'PMID':_pmid})
        if len(_db_pmid) < 1:
            _dbtable.insert_dbtable({'PMID':_pmid,
                                     'PSIG':'0'}
                                    )

    def __is_pmid(self, _pmid_str):
        return re.search(r'^[0-9]+$', _pmid_str)



class MedlineItem():
    _pubmed_tag_list = ['PMID', 'PSIG', 'TI', 'AB', 'AU', 'FAU', 'JT', 'PJID', 'JID', 'CRDT', 'PMC', 'AID', 'LID', 'AURL']
    _pubmed_list_tag_list = ['AU', 'FAU', 'AID', 'LID', 'AURL']
    #按pmid为依据将medline条目存储到数据库中
    def __init__(self, _pmid):
        self._items_dict = {}
        for _tag in self._pubmed_tag_list:
            self._items_dict[_tag] = []
        #self._items_dict['PMID'].append(str(_pmid))
        self._items_dict['PSIG'].append('1')
        self.doc = doc.MedDoc(str(_pmid))
        self.text = self.doc.read()
        self.__med_file_parser()

        self.__create_item_2_database()

    #取全文链接
    ##除去以下3种模式，需要进一步探究其他全文链接模式
    def __get_fulltext_url(self):
        _doi_url_header = 'https://doi.org/'
        _pii_url_header = 'https://linkinghub.elsevier.com/retrieve/pii/'
        _PMC_url_header = 'https://www.ncbi.nlm.nih.gov/pmc/articles/'
        _tmp_set = set([])
        for _id in self._items_dict['LID']:
            _tmp_set.add(_id)
        for _id in self._items_dict['AID']:
            _tmp_set.add(_id)
        self._items_dict['AURL'] = []
        _sig = 0
        for _id in self._items_dict['PMC']:
            _sig += 1
            self._items_dict['AURL'].append(_PMC_url_header + _id)
        for _id in _tmp_set:
            if _id[-5:] == '[doi]':
                _sig += 1
                self._items_dict['AURL'].append(_doi_url_header + _id[:-6])
        for _id in _tmp_set:
            if _id[-5:] == '[pii]':
                _sig += 1
                self._items_dict['AURL'].append(_pii_url_header + _id[:-6])
        #self._items_dict['AURL'] = '|'.join(self._items_dict['AURL'])
        #print self._items_dict['AURL']
        #if _sig == 0:
            #print self._items_dict['PMID']
            #print _tmp_set
        return

    #解析med文件
    def __med_file_parser(self):
        _tag = ''
        _str = ''
        _sig = 0
        _save_sig = 0
        for _line in self.text.split('\n'):
            if len(_line) < 5:
                continue
            if _line[4:6] == '- ':
                if _tag in self._pubmed_tag_list:
                    self._items_dict[_tag].append(_str)
                _tag = _line[:4].strip()
                if _tag in self._pubmed_tag_list:
                    _str = _line[6:].strip()
                    _save_sig = 1
                    continue
                else:
                    _save_sig = 0
            if _save_sig == 1:
                _str += ' ' + _line.strip()
        if _tag in self._pubmed_tag_list:
            self._items_dict[_tag].append(_str)
        self.__get_fulltext_url()
        #所有属性转为string
        for _tag, _contain in self._items_dict.items():
            self._items_dict[_tag] = '|'.join(_contain)
            if _tag not in self._pubmed_list_tag_list and len(_contain) > 1:
                print self._items_dict['PMID']
                print _tag
                print _contain
                print 'Medline file is misunderstand.'
        #print self._items_dict
        return
        #Example 1:
        #_column_list = ['PMID', 'PSIG', 'TI', 'AU', 'JID', 'CRDT', 'LID', 'AID', 'AURL']
        #_insert_mysqlorder = self.__gen_insert_mysqlorder('Medline', self._items_dict, _column_list)
        # _insert_mysqlorder = "INSERT INTO Medline(PMID,PSIG,TI,AU,JID,CRDT,LID,AID,AURL) " \
        #               "VALUES('" + self._items_dict['PMID'] + "'" \
        #                       ",'0'" \
        #                       ",'" + self._items_dict['TI'] + "'" \
        #                       ",'" + self._items_dict['AU'] + "'" \
        #                       ",'" + _JID + "'" \
        #                      ",'" + self._items_dict['CRDT'] + "'" \
        #                       ",'" + self._items_dict['LID'] + "'" \
        #                       ",'" + self._items_dict['AID'] + "'" \
        #                       ",'" + self._items_dict['AURL'] + "');"

        #Example 2:
        #_key_list = ['JT', 'JID']
        #_column_list = ['JT', 'PJID']
        #_insert_mysqlorder = self.__gen_insert_mysqlorder('Medline', self._items_dict, _key_list, _column_list)
        #_insert_mysqlorder = "INSERT INTO Journal(JT,PJID) " \
        #                     "VALUES('" + self._items_dict['JT'] + "'," + self._items_dict['JID'] + ");"

    #创建杂志信息表条目，将JID属性更新为数据库中的JID值
    def __create_item_2_Journal_table(self):
        #medline中的JID字段在数据库中的字段更为PJID
        self._items_dict['PJID'] = self._items_dict['JID']
        _tmp_dict = {'JT':self._items_dict['JT'], 'PJID':self._items_dict['PJID']}
        _Journal_table = dbtable.DBOpera('Journal')
        self._items_dict['JID'] = _Journal_table.search_dbtable(['JID'], {'JT':_tmp_dict['JT']})
        #print '>>>'
        if len(self._items_dict['JID']) != 1:
            #print '>>>>>>>'
            #print self._items_dict['JID']
            _key_list = ['JT', 'PJID']
            #print self._items_dict
            _Journal_table.insert_dbtable(self._items_dict, _key_list)
        #print self._items_dict
        self._items_dict['JID'] = _Journal_table.search_dbtable(['JID'], {'JT':_tmp_dict['JT']})[0][0]
        del _Journal_table

    #写medline信息表条目
    def __create_item_2_database(self):
        self.__create_item_2_Journal_table()
        _Medline_table = dbtable.DBOpera('Medline')
        _pmid = _Medline_table.search_dbtable(['PMID'], {'PMID':self._items_dict['PMID']})
        if len(_pmid) == 0:
            _key_list = ['PMID', 'PSIG', 'TI', 'AU', 'FAU', 'JID', 'CRDT', 'AURL', 'AB']
            _Medline_table.insert_dbtable(self._items_dict, _key_list)
        del _Medline_table
        return True


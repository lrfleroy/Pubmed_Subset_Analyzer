# -*- coding: utf-8 -*-
from package.docproc import doc
from package.dbconn import dbtable

class WebsiteDict():
    def __init__(self, _dict_file=''):
        self._items_dict = {}
        if _dict_file == '':
            self.__load_default()
        self.update_database()

    #加载已知网站信息
    ##全文链接网站未增加
    ##全文网站未增加
    ##数据网站未增加
    def __load_default(self):
        self._items_dict['PubMed'] = {'TI':'PubMed',
                                      'UDT':'',
                                      'URL':'https://www.ncbi.nlm.nih.gov/pubmed',
                                      'SURL':'https://www.ncbi.nlm.nih.gov/pubmed/?term=',
                                      'DURL':'https://www.ncbi.nlm.nih.gov/pubmed/?term='}
        self._items_dict['SRA'] = {'TI':'SRA',
                                   'UDT':'',
                                   'URL':'https://www.ncbi.nlm.nih.gov/sra',
                                   'SURL':'https://www.ncbi.nlm.nih.gov/sra/?term=',
                                   'DURL':'https://www.ncbi.nlm.nih.gov/sra/?term='}
        self._items_dict['SRP'] = {'TI':'SRP',
                                   'UDT':'',
                                   'URL':'',
                                   'SURL':'',
                                   'DURL':'https://trace.ncbi.nlm.nih.gov/Traces/sra/?study='}
        #self._items_dict[''] = {'UDT':'', 'URL':'', 'SURL':'', 'DURL':''}

    def update_database(self):
        for _website_name, _info_dict in self._items_dict.items():
            _website_item = WebsiteItem(_website_name, _info_dict)
            del _website_item



class WebsiteItem:
    def __init__(self, _website_name, _info_dict):
        self._items_dict = {'TI':_website_name}
        self._items_dict.update(_info_dict)
        self.__create_item_2_Website_table()

    def __create_item_2_Website_table(self):
        _dbtable = dbtable.DBOpera('Website')
        self._items_dict['WID'] = _dbtable.search_dbtable(['WID'], {'TI':self._items_dict['TI']})
        if len(self._items_dict['WID']) != 1:
            _dbtable.insert_dbtable(self._items_dict)
        self._items_dict['WID'] = _dbtable.search_dbtable(['WID'], {'TI': self._items_dict['TI']})[0][0]
        del _dbtable
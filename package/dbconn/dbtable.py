# -*- coding: utf-8 -*-
import MySQLdb

class DBOpera():
    def __init__(self, _dbtable_name):
        self._table_name = _dbtable_name
        self._dbtable = DBTable(self._table_name)

    def __del__(self):
        del self._dbtable

    def column_list(self):
        _tmp_list = []
        for _column_info in self._dbtable.operate_db_table('SHOW COLUMNS FROM ' + self._table_name + ';'):
            _tmp_list.append(_column_info[0])
        return _tmp_list

    def data_number(self):
        _count = self.search_dbtable(['count(*)'])[0][0]
        return _count

    def show_dbtable_info(self):
        print 'Table: ' + self._table_name
        print 'Column: ' + str(self.column_list()) + ''
        print 'Data number: ' + str(self.data_number()) + ''

    # 查询数据表的通用函数
    def search_dbtable(self, _result_tag_list=None, _search_dict=None, _other_limits=None):
        _mysqlorder = self._dbtable.gen_search_mysqlorder(self._table_name, _result_tag_list, _search_dict, _other_limits)
        _result_list = self._dbtable.operate_db_table(_mysqlorder)
        return _result_list

    #修改数据库表条目
    def update_dbtable(self, _data_dict, _search_dict=None):
        _mysqlorder = self._dbtable.gen_update_mysqlorder(self._table_name, _data_dict, _search_dict)
        self._dbtable.operate_db_table(_mysqlorder)
        return

    #修改数据库表条目
    def delete_dbtable(self, _search_dict=None):
        _mysqlorder = self._dbtable.gen_delete_mysqlorder(self._table_name, _search_dict)
        self._dbtable.operate_db_table(_mysqlorder)
        return


    #插入单条数据库表条目
    def insert_dbtable(self, _data_dict, _key_list=None, _column_list=None):
        _mysqlorder = self._dbtable.gen_insert_mysqlorder(self._table_name, _data_dict, _key_list, _column_list)
        self._dbtable.operate_db_table(_mysqlorder)
        return

    #批量插入数据库表条目
    def inserts_dbtable(self, _column_list, _data_list_list):
        _mysqlorder = self._dbtable.gen_inserts_mysqlorder(self._table_name, _column_list, _data_list_list)
        self._dbtable.operate_db_table(_mysqlorder)
        return

    def count_rows(self):
        _mysqlorder = "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_NAME = '" + self._table_name + "';"
        _number = self._dbtable.operate_db_table(_mysqlorder)
        return _number[0][0] if len(_number) != 0 else '0'

class DBTable():
    _DBTable_list = ['Medline',  'Article', 'Journal', 'Data', 'Website', 'Ontology', 'Organism', 'Strategy']
    def __init__(self, _tmp_table_name='tmp'):
        self._db_conn = self.__connect_db()
        self._table_name = _tmp_table_name
        self.__check_dbtable()
        self._data_list = []

    def __del__(self):
        self._db_conn.close()

    def __db_table_init(self):
        if self._table_name == 'Medline':
            return self.__Medline_table_info()
        elif self._table_name == 'Article':
            return self.__Article_table_info()
        elif self._table_name == 'Journal':
            return self.__Journal_table_info()
        elif self._table_name == 'Data':
            return self.__Data_table_info()
        elif self._table_name == 'SRP':
            return self.__SRP_table_info()
        elif self._table_name == 'Website':
            return self.__Website_table_info()
        elif self._table_name == 'Ontology':
            return self.__Ontology_table_info()
        elif self._table_name == 'Organism':
            return self.__Organism_table_info()
        elif self._table_name == 'Strategy':
            return self.__Strategy_table_info()
        else:
            return self.__tmp_table_info()

    def __connect_db(self):
        return MySQLdb.connect(
        host='localhost',
        port = 3306,
        user='root',
        passwd='leroy',
        db='PSA'
        )

    def __check_dbtable(self):
        _order = "SELECT * FROM information_schema.tables WHERE table_name = '" + self._table_name + "'"
        #print self.operate_db_table(_order)
        if len(self.operate_db_table(_order)) == 0:
            self.operate_db_table(self.__db_table_init())


    def operate_db_table(self, _mysql_order='show tables;'):
        _tmp_cur = self._db_conn.cursor()
        #print '>>>'
        #print _mysql_order
        #print '>>>'
        _tmp_cur.execute(_mysql_order)
        _result_list = _tmp_cur.fetchall()
        _tmp_cur.close()
        self._db_conn.commit()
        return _result_list


    def gen_search_mysqlorder(self, _dbtable_name, _result_tag_list=None, _search_dict=None, _other_limit=None):
        _order_str = "SELECT"
        if _result_tag_list == None:
            _order_str += ' *'
        else:
            _order_str += ' ' + ','.join(_result_tag_list)
            _order_str += " FROM " + str(_dbtable_name)
        if _search_dict != None:
            _order_str += ' WHERE '
            _search_list = []
            for _column_name, _search_word in _search_dict.items():
                if type(_search_word) == type(''):
                    _search_list.append(" " + str(_column_name) + " = '" + MySQLdb.escape_string(str(_search_word)) + "'")
                if type(_search_word) == type([]):
                    _search_list.append(" " + str(_column_name) + " " + _search_word[0] + " '" + MySQLdb.escape_string(str(_search_word[1])) + "'")
            _order_str += ' and'.join(_search_list)
        if _other_limit != None:
            _order_str += ' ' + str(_other_limit)
        _order_str += ';'
        return _order_str

    #将字典写入1个数据库条目
    def gen_update_mysqlorder(self, _table_name, _data_dict, _search_dict=None):
        _order_str = "UPDATE " + _table_name + " SET "
        _sig = 0
        for _column, _value in _data_dict.items():
            if _sig == 0:
                _sig = 1
            else:
                _order_str += ", "
            _order_str += str(_column) + " = '" + MySQLdb.escape_string(str(_value)) + "'"
        if _search_dict != None:
            _order_str += " WHERE "
            _sig = 0
            for _column, _value in _search_dict.items():
                if _sig == 0:
                    _sig = 1
                else:
                    _order_str += ", "
                _order_str += str(_column) + " = '" + MySQLdb.escape_string(str(_value)) + "'"
        _order_str += ';'
        return _order_str


    #将字典写入1个数据库条目
    def gen_delete_mysqlorder(self, _table_name, _search_dict=None):
        _order_str = "DELETE FROM " + _table_name
        if _search_dict != None:
            _order_str += " WHERE "
            _sig = 0
            for _column, _value in _search_dict.items():
                if _sig == 0:
                    _sig = 1
                else:
                    _order_str += ", "
                _order_str += str(_column) + " = '" + str(_value) + "'"
        _order_str += ';'
        return _order_str

    #将字典写入1个数据库条目
    def gen_insert_mysqlorder(self, _dbtable_name, _data_dict, _key_list=None, _column_list=None):
        if _key_list == None:
            _key_list = _data_dict.keys()
        _order_str = "INSERT INTO " + _dbtable_name + "("
        if _column_list == None:
            _order_str += ",".join(_key_list)
        else:
            _order_str += ",".join(_column_list)
        _order_str += ") VALUES("
        _sig = 0
        for _column in _key_list:
            if _sig == 0:
                _sig = 1
            else:
                _order_str += ","
            _order_str += "'" + MySQLdb.escape_string(str(_data_dict[_column])) + "'"
        _order_str += ");"
        return _order_str

    #将字典写入1个数据库条目
    def gen_inserts_mysqlorder(self, _table_name, _column_list, _data_list_list):
        _order_str = "INSERT INTO " + _table_name + "("
        _order_str += ",".join(_column_list)
        _order_str += ") VALUES("
        _sig = 0
        for _data_list in _data_list_list:
            if _sig == 0:
                _sig = 1
            else:
                _order_str += "), ("
            for _i in range(len(_data_list)):
                _data_list[_i] = MySQLdb.escape_string(_data_list[_i])
            _order_str += "'" + "','".join(_data_list) + "'"
        _order_str += ");"
        return _order_str


    #将字典写入1个数据库条目
    def gen_create_table_mysqlorder(self, _table_name, _column_list, _column_dict):
        _order_str = "CREATE TABLE IF NOT EXISTS " + _table_name + " ("
        _sig = 0
        for _column in _column_list:
            if _sig == 0:
                _sig = 1
            else:
                _order_str += ", "
            _order_str += _column + " "
            _order_str += " ".join(_column_dict[_column])
        _order_str += ", PRIMARY KEY (" + _column_list[0] + ")"
        _order_str += ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        return _order_str

    def __tmp_table_info(self):
        _column_list = ['id', 'sig', 'date', 'title', 'timestamp', 'description']
        _column_dict = {'id':["INT","NOT NULL","AUTO_INCREMENT"],   #
                        'sig':["INT","DEFAULT '0'"],                #
                        'date':["DATE","DEFAULT '1900-01-01'"],     #
                        'title':["VARCHAR(200)","DEFAULT NULL"],    #
                        'timestamp':["VARCHAR(12)","DEFAULT NULL"], #
                        'description':["TEXT","DEFAULT NULL"]       #
                        }
        _order_str = self.gen_create_table_mysqlorder('tmp', _column_list, _column_dict)
        return _order_str

    def __Medline_table_info(self):
        _column_list = ['ID', 'PMID', 'PSIG', 'TI', 'AU', 'FAU', 'JID', 'CRDT', 'AURL', 'AB']
        _column_dict = {'ID': ["INT", "NOT NULL", "AUTO_INCREMENT"],  ##Article id
                        'PMID':["INT","NOT NULL"],              #Pubmed id
                        'PSIG':["CHAR(3)","DEFAULT NULL"],      # Process sig: Download:0. The result equals add_together(items).
                        'TI':["VARCHAR(511)","DEFAULT NULL"],   # Title
                        'AU':["VARCHAR(1023)","DEFAULT NULL"],   # Author
                        'FAU':["VARCHAR(2047)","DEFAULT NULL"],  # Full Author
                        'JID':["INT","DEFAULT NULL"],           #-Journal id
                        'CRDT':["VARCHAR(31)","DEFAULT NULL"],  # Create Date
                        'AURL':["VARCHAR(511)","DEFAULT NULL"], # Article URL
                        'AB':["TEXT","DEFAULT NULL"]            # Abstract
                        }
        _order_str = self.gen_create_table_mysqlorder('Medline', _column_list, _column_dict)
        return _order_str

    def __Article_table_info(self):
        _table_name = 'Article'
        _column_list = ['ID', 'PMID', 'PSIG', 'SIDL', 'ROGL', 'RSTL', 'POGL', 'PSTL', 'POTL']
        _column_dict = {'ID':["INT","NOT NULL","AUTO_INCREMENT"], ##Article id
                        'PMID':["INT","NOT NULL"],              #Pubmed id
                        'PSIG':["CHAR(3)","DEFAULT NULL"],      # Process sig: Tagged:0/1, predicted:0/2. The result equals add_together(items).
                        'SIDL':["VARCHAR(200)","DEFAULT NULL"], # Study id List
                        'ROGL':["VARCHAR(200)","DEFAULT NULL"], #=Real Organism id List
                        'RSTL':["VARCHAR(200)","DEFAULT NULL"], #=Real Strategy id List
                        'POGL':["VARCHAR(200)","DEFAULT NULL"], #=Predict Organism id List
                        'PSTL':["VARCHAR(200)","DEFAULT NULL"], #=Predict Strategy id List
                        'POTL':["VARCHAR(200)","DEFAULT NULL"]  #=Predict Ontology id List
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str

    def __Journal_table_info(self):
        _table_name = 'Journal'
        _column_list = ['JID', 'PJID', 'JT', 'SJT', 'URL']
        _column_dict = {'JID':["INT","NOT NULL","AUTO_INCREMENT"],  ##Journal id
                        'PJID':["INT","DEFAULT NULL"],              # Pubmed Journal id
                        'IF':["INT","DEFAULT NULL"],                # Infect number
                        'AN':["INT","DEFAULT NULL"],                # Artical number
                        'JT':["VARCHAR(511)","DEFAULT NULL"],       # Journal Title
                        'SJT':["VARCHAR(31)","DEFAULT NULL"],       # simple Journal Title
                        'URL':["VARCHAR(255)","DEFAULT NULL"]       # Main Page URL
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str

    def __Data_table_info(self):
        _table_name = 'Data'
        _column_list = ['DID', 'PSIG', 'WID', 'RID', 'SID', 'OGID', 'STID', 'PMID']
        _column_dict = {'DID':["INT","NOT NULL","AUTO_INCREMENT"],  ##Data id
                        'PSIG':["CHAR(3)","DEFAULT NULL"],          # Process sig: Tagged:0/1. The result equals add_together(items).
                        'WID':["INT","NOT NULL"],                   #-Website id
                        'RID':["VARCHAR(31)","DEFAULT NULL"],       # Real Data id
                        'SID':["VARCHAR(31)","DEFAULT NULL"],       # SRA Study id
                        'OGID':["INT","DEFAULT NULL"],              #-Organism id
                        'STID':["INT","DEFAULT NULL"],              #-Strategy id
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str

    def __SRP_table_info(self):
        _table_name = 'SRP'
        _column_list = ['SID', 'SRPID', 'SRAL', 'OGID', 'STID']
        _column_dict = {'SID':["INT","NOT NULL","AUTO_INCREMENT"],  ##Study id
                        'SRPID':["VARCHAR(31)","DEFAULT NULL"],     # SRP Study id
                        'SRAL':["TEXT","DEFAULT NULL"],             # SRA list
                        'OGID':["INT","DEFAULT NULL"],              #-Organism id
                        'STID':["INT","DEFAULT NULL"],              #-Strategy id
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str

    def __Website_table_info(self):
        _table_name = 'Website'
        _column_list = ['WID', 'TI', 'OID', 'UDT', 'URL', 'SURL', 'DURL']
        _column_dict = {'WID':["INT","NOT NULL","AUTO_INCREMENT"],  ##Website id
                        'TI':["VARCHAR(127)","DEFAULT NULL"],       # Title
                        'OID':["INT","DEFAULT NULL"],               #-Ontology id
                        'UDT':["DATE","DEFAULT '0000-00-00'"],      # Update Date
                        'URL':["VARCHAR(255)","DEFAULT NULL"],      # Main Page URL
                        'SURL':["VARCHAR(255)","DEFAULT NULL"],     # Search Page URL
                        'DURL':["VARCHAR(255)","DEFAULT NULL"]      # Data Page URL Header
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str

    def __Ontology_table_info(self):
        _table_name = 'Ontology'
        _column_list = ['OID', 'MOID', 'TI', 'PL']
        _column_dict = {'OID':["INT","NOT NULL","AUTO_INCREMENT"],  ##Ontology id
                        'MOID':["INT","DEFAULT NULL"],              # Mother Ontology id
                        'TI':["VARCHAR(127)","DEFAULT NULL"],       # Title
                        'PL':["TEXT","DEFAULT NULL"]                # Phrase List
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str

    def __Organism_table_info(self):
        _table_name = 'Organism'
        _column_list = ['OGID', 'OID', 'TI']
        _column_dict = {'OGID':["INT","NOT NULL","AUTO_INCREMENT"], # Organism id
                        'OID':["INT","DEFAULT NULL"],               #-Ontology id
                        'TI':["VARCHAR(127)","DEFAULT NULL"]        # Title
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str

    def __Strategy_table_info(self):
        ##Strategy id
        #-Ontology id
        # Title
        _table_name = 'Strategy'
        _column_list = ['STID', 'OID', 'TI']
        _column_dict = {'STID':["INT","NOT NULL","AUTO_INCREMENT"], # Strategy id
                        'OID':["INT","DEFAULT NULL"],               #-Ontology id
                        'TI':["VARCHAR(127)","DEFAULT NULL"]        # Title
                        }
        _order_str = self.gen_create_table_mysqlorder(_table_name, _column_list, _column_dict)
        return _order_str
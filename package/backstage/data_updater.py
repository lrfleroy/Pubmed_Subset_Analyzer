# -*- coding: utf-8 -*-


#pubmed数据
#list

#med数据
#全文数据
'''
from package.docproc.middoc import med

_pubmed = med.PubMed('sra')
#_tmp_path = 'E:\Pubmed_Subset_Analyzer\Pubmed_subset_analyzer\data\middoc\sra_result_pmid.list'
#_pubmed.write_medline_file_2_database(_tmp_path, '00000000')   # Step 1
_pubmed.download_full_text()                                    # Step 4
del _pubmed
'''


#sra数据          # Step 2

from package.docproc.middoc import sra

_sra = sra.SRA()
_tmp_path = 'E:\\Pubmed_Subset_Analyzer\\Pubmed_subset_analyzer\\data\\download\\sra\\list\\sra_result.csv'
_sra.write_sra_result_file_2_database(_tmp_path, '20171201')
del _sra

#srp数据          # Step 3

#_srp = sra.SRP()
#_srp.update_sra_study_info('20171201')
#del _srp


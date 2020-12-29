#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re, string, sys
import pandas as pd
from unidecode  import unidecode
import pyodbc
from collections import defaultdict


# In[2]:


def cleansing_special_characters(txt):
    seps = [' ',';',':','.','`','~',',','*','#','@','|','\\','-','_','?','%','!','^','(',')','[',']','{','}','$','=','+','"','<','>',"'",' AND ', ' and ']
    default_sep = seps[0]
    txt = str(txt)
    for sep in seps[1:]:
        if sep == " AND " or sep == " and ":
            txt = txt.upper()
            txt = txt.replace(sep, ' & ')
        else:
            txt = txt.upper()
            txt = txt.replace(sep, default_sep)
    try :
        list(map(int,txt.split()))
        txt = 'NUMBERS'
    except:
        pass
    txt = re.sub(' +', ' ', txt)
    temp_list = [i.strip() for i in txt.split(default_sep)]
    temp_list = [i for i in temp_list if i]
    return " ".join(temp_list)


###########################################################

punctuation = re.compile('[%s]' % re.escape(string.punctuation))

class fingerprinter(object):
    
    # __init__function
    
    def __init__(self, string):
        self.string = self._preprocess(string)
        
    
    # strip leading, trailing spaces and to lower case
    def _preprocess(self, string):
        return punctuation.sub('',string.strip().lower())
    
        
    def _latinize(self, string):
        return unidecode(string)
#         return unidecode(string.decode('utf-8'))
    
    def _unique_preserve_order(self,seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    
    #-####################################################
    def get_fingerprint(self):
        return self._latinize(' '.join(self._unique_preserve_order(sorted(self.string.split()))))
    
    ######################################################
    def get_ngram_fingerprint(self, n=1):
        return self._latinize(''.join(self._unique_preserve_order(sorted([self.string[i:i + n] for i in range(len(self.string) - n +1)]))))
    
    


# In[ ]:


# conn = pyodbc.connect('Driver={SQL Server Native Client 11.0}; Server=192.168.1.47; uid=seair; pwd=se@ir1234; Database = tempEXIM; Trusted_Connection = No')


# In[5]:

if __name__ == '__main__':

	# filename = 'Indo_buyer_tbuuuuuu-xlsx.xlsx'
	# df = pd.read_sql("""select distinct BUYER_NAME as SUPPLIER_NAME from DB_Indonesia..Indonesia_Exp2019 where LEN(BUYER_NAME)>10 union 
	# select distinct BUYER_NAME as SUPPLIER_NAME from DB_Indonesia..Indonesia_Exp2020 where LEN(BUYER_NAME)>10  """,conn)
	df = pd.read_excel(sys.argv[1])

	#preprocess the column
	df['Clean'] = df['SUPPLIER_NAME'].apply(cleansing_special_characters)

	# step 1 cleanining
	###########################################################################################

	# ##for n_gram fingerprint algorithm
	###########################################################################################

	df['n_gram_fingerprint_n2'] = df['Clean'].apply(lambda x : fingerprinter(x.replace(" ","")).get_ngram_fingerprint(n=2))

	## generate tag_id for every unique generated n_gram_fingerprint
	d = defaultdict(lambda: len(d))
	df['tag_idn']=[d[x] for x in df['n_gram_fingerprint_n2']]

	###########################################################################################

	#drop n_gram column
	df.drop(columns=['n_gram_fingerprint_n2'], inplace=True)

	# make copy to create group of tag_id
	df1 = df[['SUPPLIER_NAME','tag_idn']]


	# drop SUPPLIER_NAME column , we have tag_id's now
	df.drop(columns=['SUPPLIER_NAME'], inplace=True)

	# group df with tag_id with selecting minimum 
	#group = df.groupby('tag_id').min().reset_index()
	group = df.loc[df["Clean"].str.len().groupby(df["tag_idn"]).idxmax()]

	# join both the data frames group(unique) and main data
	df_merge = pd.merge(df1,group, on=['tag_idn'])

	# sterp 2 cleaning
	##########################################################################################
	##for fingerprint algorithm
	###########################################################################################
	df_merge['fingerprint'] = df_merge['Clean'].apply(lambda x : fingerprinter(x).get_fingerprint())

	## generate tag_id for every unique generated n_gram_fingerprint
	d = defaultdict(lambda: len(d))
	df_merge['tag_idf']=[d[x] for x in df_merge['fingerprint']]
	                                      
	# drop fingerprint column
	df_merge.drop(columns=['fingerprint'], inplace=True)
	                                                  
	df2 = df_merge[['tag_idf','Clean']]
	group1 = df2.loc[df2["Clean"].str.len().groupby(df2["tag_idf"]).idxmax()]
	df_merge1 = pd.merge(df_merge,group1, on=['tag_idf'])
	df_merge1.index.name = 'Serial No.'
	##############################################################################################


	# rearrange columns
	# # output excel file
	df_merge1.to_excel(sys.argv[1].strip('.xlsx')+'_Cleaned' +".xlsx")
	# print('File {} Generated'.format(filename.strip('.xlsx')+'_Cleaned'))


	# In[4]:


	# df_merge1


	# In[ ]:





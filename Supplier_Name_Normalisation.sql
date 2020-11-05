execute sp_execute_external_script
@LANGUAGE = N'Python',@script = N'
import re, string
import pandas as pd
from unidecode  import unidecode
import pyodbc
from collections import defaultdict

def cleansing_special_characters(txt):
    seps = [" ",";",":",".",",","*","#","@","|","\\","-","_","?","%","!","^","(",")","$","=","+",""","<",">","""," AND ", " and "]
    default_sep = seps[0]
    txt = str(txt)
    for sep in seps[1:]:
        if sep == " AND " or sep == " and ":
            txt = txt.upper()
            txt = txt.replace(sep, " & ")
        else:
            txt = txt.upper()
            txt = txt.replace(sep, default_sep)
    try :
        list(map(int,txt.split()))
        txt = "NUMBERS"
    except:
        pass
    txt = re.sub(" +", " ", txt)
    temp_list = [i.strip() for i in txt.split(default_sep)]
    temp_list = [i for i in temp_list if i]
    return " ".join(temp_list)


###########################################################

punctuation = re.compile("[%s]" % re.escape(string.punctuation))

class fingerprinter(object):
    
    # __init__function
    
    def __init__(self, string):
        self.string = self._preprocess(string)
        
    
    # strip leading, trailing spaces and to lower case
    def _preprocess(self, string):
        return punctuation.sub("",string.strip().lower())
    
        
    def _latinize(self, string):
        return unidecode(string)
#         return unidecode(string.decode("utf-8"))
    
    def _unique_preserve_order(self,seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    
#####################################################
    def get_fingerprint(self):
        return self._latinize(" ".join(self._unique_preserve_order(sorted(self.string.split()))))
    
######################################################
    def get_ngram_fingerprint(self, n=1):
        return self._latinize("".join(self._unique_preserve_order(sorted([self.string[i:i + n] for i in range(len(self.string) - n +1)]))))




df = pd.DataFrame(supp)

#preprocess the column
df["Clean"] = df["SUPPLIER_NAME"].apply(cleansing_special_characters)

##for fingerprint algorithm
df["n_gram_fingerprint_n2"] = df["Clean"].apply(lambda x : fingerprinter(x.replace(" ","")).get_ngram_fingerprint(n=2))

## generate tag_id for every unique generated n_gram_fingerprint
d = defaultdict(lambda: len(d))
df["tag_id"]=[d[x] for x in df["n_gram_fingerprint_n2"]]

#drop n_gram column
df = df[["tag_id","SUPPLIER_NAME","Clean"]]

# drop clean column , to merge with clean grouped tag_ids
df1 = df[["tag_id","SUPPLIER_NAME"]]

# drop SUPPLIER_NAME column , we have tag_ids now
df = df[["tag_id","Clean"]]
# group df with tag_id with selecting minimum 
group = df.groupby("tag_id").min().reset_index()

# join both the data frames group(unique) and main data
df_merge = pd.merge(df1,group, on=["tag_id"])

# rearrange columns
df_merge = df_merge[["tag_id","SUPPLIER_NAME","Clean"]]
OutputDataSet = df_merge   
',
@input_data_1 = N'select  distinct SUPPLIER_NAME from DB_Suppliers..Indonesia_Imp2020',
@input_data_1_name = N'supp'
with result sets (([tag_id] int,SUPPLIER_NAME nvarchar(MAX),[CLEANED_SUPPLIER_NAME] nvarchar(MAX)))
import pandas as pd

df= pd.read_csv(r'C:\Users\zriyad\Desktop\CDR_Trace\src\data\cdr\format3\file1.csv',delimiter=";",low_memory=False, dtype={'last_nbi_ECGI_SAI': str,'first_nbi_ECGI_SAI':str})
nbi =  pd.read_csv(r'C:\Users\zriyad\Desktop\CDR_Trace\src\data\cdr\format3\network_equipment_cells.csv',dtype={'ECGI/SAI': str})


l1 = pd.DataFrame()
l2 = pd.DataFrame()

l1['nbi_ECGI_SAI'] = df['last_nbi_ECGI_SAI']
l2['nbi_ECGI_SAI'] = df['first_nbi_ECGI_SAI']

l12 = pd.concat([l1,l2])

ls = l12['nbi_ECGI_SAI'].unique()
l3 = nbi['ECGI/SAI'].unique()

set1 = set(ls)
set2 = set(l3)

common_elements = set1.intersection(set2)
print("Common elements:", common_elements)

# Finding uncommon elements in list1
uncommon_list1 = set1.difference(set2)
print("Uncommon elements in list1:", uncommon_list1)



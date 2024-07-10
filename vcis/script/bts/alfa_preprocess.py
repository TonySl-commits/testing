from vcis.bts.preproccess import CDR_Preprocess
from vcis.bts.integration import CDR_Integration
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.oracle_spark.insert_to_oracle import oracleInsert


utils = CDR_Utils()
properties = CDR_Properties()
preprocess = CDR_Preprocess()
integration = CDR_Integration()
oracle_insert = oracleInsert()
twog,threeg,LTE = utils.read_cdr_bts()

twog,threeg,LTE = preprocess.add_tech_column(twog,threeg,LTE)
twog,threeg,LTE = preprocess.rename_columns(twog,threeg,LTE)
print(properties.column_mapping)

df = integration.merge_cdr(twog,threeg,LTE)
df = preprocess.add_default_columns(df,'alfa')
df = preprocess.add_rnc_column(df)
df = preprocess.add_lcid_column(df)
df = preprocess.add_frequency_column(df)
df = preprocess.round_coordinates(df)
df = preprocess.fix_coordinates(df)
df = preprocess.select_columns(df)
df = preprocess.fix_columns_type(df)
print(df.head())
oracle_insert.insert_to_oracle(df,properties.unique_oracle_number)
df.to_excel('test.xlsx',index=False)
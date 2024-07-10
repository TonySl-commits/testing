import pandas as pd

main = pd.read_excel("C:/Users/joseph.as/Desktop/bts_tables.xlsx")
data2 = pd.read_excel("C:/Users/joseph.as/Desktop/CDRVscode/data/saves/3900.xlsx")
data3 = pd.read_excel("C:/Users/joseph.as/Desktop/CDRVscode/data/saves/1700.xlsx")
data4 = pd.read_excel("C:/Users/joseph.as/Desktop/CDRVscode/data/saves/800.xlsx")
data5 = pd.read_excel("C:/Users/joseph.as/Desktop/CDRVscode/data/saves/3900_2.xlsx")
data = pd.read_excel("C:/Users/joseph.as/Desktop/CDRVscode/data/saves/final.xlsx")
chunks = pd.concat([data,data2,data3,data4,data5])
df = pd.merge(main, chunks, on=['location_latitude','location_longitude','location_azimuth'], how='left')
print(df.shape)
print(df.head())
df.to_excel("C:/Users/joseph.as/Desktop/CDRVscode/data/saves/full_bts.xlsx")

from vcis.cdr_data_generation.cdr_data_generation import CDR_data_generation


# geo_ids = ['731d9099-78a7-2220-93d5-1c59c01e1b0a','2789ab01-2c2c-2102-c15e-2cbbaff48013','EC390C45-D482-28AC-544E-415ED9A17EF8','e52f2344-efdd-2ecd-d440-d080bccd98dc','9A12319F-248C-275A-CBFD-2880927B9D34','a90a0370-4ee8-4c0c-b66b-7448c3d489c3','5af0ffd2-17dc-23c2-9537-3090432e2152','108f7a20-b9d0-217a-943a-8beab9cde90e','9ccf3f04-d8f7-28db-9bca-e92b4da989ac','7F9737F2-4C24-20E9-57AC-9F480A87ADA9','ca755b4b-3f1e-4df8-a9e1-411d9ddeeac7']
geo_ids = ['731d9099-78a7-2220-93d5-1c59c01e1b0a']

start_date = '2022-06-18'
end_date = '2023-06-18'
tuples_list = []
cdr_data_generation = CDR_data_generation()

for geo_id in geo_ids:
    geo_id , imsi_id = cdr_data_generation.cdr_data_generation(passed_geo_id=geo_id, passed_start_date=start_date, passed_end_date=end_date)
    tuples_list.append((geo_id,imsi_id))

print(tuples_list)

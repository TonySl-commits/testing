import pandas as pd

df_common = pd.read_csv('df_common.csv')

print(df_common['main_id'].nunique())
for main_id in df_common['main_id'].unique():
    print("____"*10)
    print(main_id)
    print("____")
    devices_list = df_common[df_common['main_id'] == main_id]['device_id'].unique().tolist()
    devices_list.append(main_id)
    print("____"*10)

groups = []
for main_id in df_common['main_id'].unique():
    devices_list = df_common[df_common['main_id'] == main_id]['device_id'].unique().tolist()
    devices_list.append(main_id)
    groups.append(devices_list)

print(groups)

def merge_similar_groups(groups, tolerance=0.1):
    """
    Merge groups that are similar based on a tolerance level for device_id comparison.
    """
    merged_groups = []
    while groups:
        base_group = groups.pop(0)
        base_set = set(base_group)
        i = 0
        while i < len(groups):
            compare_set = set(groups[i])
            if len(base_set.intersection(compare_set)) / len(base_set.union(compare_set)) > tolerance:
                base_set.update(compare_set)
                groups.pop(i)
            else:
                i += 1
        merged_groups.append(list(base_set))
    return merged_groups

# Merge similar groups and print the result
merged_groups = merge_similar_groups(groups)
print("Merged Groups:")
for group in merged_groups:
    print(group)



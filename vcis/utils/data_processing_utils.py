import pandas as pd

class DataProcessing():
    def __init__(self):
        pass

    def remove_unnecessary_columns(self, df, remove_nas=True, remove_zeros=False, remove_columns=[], verbose=False):
        """
        Removes columns from a DataFrame where all values are either None or 0, or specified in the remove_columns.
        
        Parameters:
            df (pd.DataFrame): The DataFrame to preprocess.
        
        Returns:
            pd.DataFrame: A DataFrame with the unnecessary columns removed.
        """

        null_columns = []
        zero_columns = []        

        if len(remove_columns) > 0:
            df = df.drop(columns=remove_columns)

        if remove_nas:
            # Determine which columns should be dropped
            null_columns = [col for col in df.columns if (df[col].isna()).all()]
            
            # Drop these columns from the DataFrame
            df = df.drop(columns=null_columns)

        if remove_zeros:
            # Determine which columns should be dropped
            zero_columns = [col for col in df.columns if (df[col] == 0).all()]
            
            # Drop these columns from the DataFrame
            df = df.drop(columns=zero_columns)

        if verbose:
            print(f"Removed columns: {remove_columns}")
            print(f"Removed NAs: {null_columns}")
            print(f"Removed zeros: {zero_columns}")
        
        return df
    
    def print_table(self, df, info=False):
        space = ' ' * 10
        print(f"★★★★★★★★★★ {space} DATAFRAME {space} ★★★★★★★★★★")
        print(f"★★★★★★★★★★ {space} COLUMNS {space} ★★★★★★★★★★\n", list(df.columns), "\n")

        if info:
            print(f"★★★★★★★★★★ {space} DATAFRAME INFO {space} ★★★★★★★★★★")
            print(df.info())

        if len(df.columns) > 5:
            # Print the columns of the dataframe 5 columns at a time
            for i in range(0, len(df.columns), 5):
                print(df.iloc[:, i:i+5])
        else: 
            print(df)

        print("★★★★★★★★★★★★★★★★★★★★" * 5, "\n\n\n")
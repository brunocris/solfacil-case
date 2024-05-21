import pandas as pd

from os import remove
from src.config.env import TMP_DIR
from airflow.utils.log.logging_mixin import LoggingMixin

class BaseModule(LoggingMixin):

    dag_name:str = None
    fields:dict = None
    temp_file_extn:str = '.csv'

    def __init__(self):
        self.dir_path = TMP_DIR
        self.temp_file_path = f"{self.dir_path}{self.dag_name}{self.temp_file_extn}"

    def extract(self):
        raise NotImplementedError("Extract method not defined.")
    
    def load(self):
        raise NotImplementedError("Load method not defined.")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """"

        This method transforms the extracted DataFrame following the recipe on the
        fields dictionary. Given a source column, there are two options:

        1. String:

            If the value of the key is of type str, then just renames the column to
            the name in that value:

            self.fields = {
                "source_column": "target_column"
            }

            source_column is renamed to target_column before being load.

        2. Dict:

            If, on the other hand, the type is a dict, then there are a couple of options:

            - rename: new name for the column, using the rename method.
                      Doc: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html#pandas.DataFrame.rename

            - fn: function to be applied to all values of the column, using the apply method.
                  Doc: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.apply.html#pandas.DataFrame.apply

            - type: type to cast the values from the column to, following the astype method.
                    Doc: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html#pandas.DataFrame.astype

            - replace: the values of the column inside the DataFrame are dynamically replaced
                       following the value rule. Doc: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.replace.html
                       
            - date_format: transforms a string to datetime using the value (date format), using the 
                           to_datetime method. Doc: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas-to-datetime

            - fillna: method to use to fill null values, using the fillna method.
                      Doc: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.fillna.html#pandas.DataFrame.fillna
        
        
        After transforming all columns, returns the new DataFrame with all the desired columns. If one of the keys of the fields dictionary
        wasn't extracted (i.g. when the documents you've extracted in a NoSQL engine don't contain that key), this methods creates it as a
        empty column.
                      
        """
        
        if df.empty():
            self.log.info("DataFrame is empty. Nothing to transform - Skipping.")
            return df

        new_df = df.copy()

        for col in self.fields:
            col_val = self.fields[col]
            self.log.info(f'Transforming field {col}: {col_val}')

            if isinstance(col_val, str):
                col_dest_name = col_val

            elif isinstance(col_val, dict):
                col_dest_name = col_val.get('rename', col)
                col_fn = col_val.get('fn', None)
                col_cast_type = col_val.get('type', None)
                col_replace = col_val.get('replace', None)
                col_date_format = col_val.get('date_format', None)
                col_fillna = col_val.get('fillna', None)

            else:
                raise TypeError(f'Not able to parse fields for {col_val}, should be dict or str.')

            try:
                # replace must be the first step
                if col_replace:
                    new_df[col] = new_df[col].replace(col_replace)

                if col_fn:
                    new_df[col] = new_df[col].apply(col_fn)

                if col_cast_type:
                    new_df[col] = new_df[col].astype(col_cast_type)
                
                if col_date_format:
                    new_df[col] = pd.to_datetime(new_df[col], format=col_date_format)
                
                if col_fillna:
                    new_df[col] = new_df[col].fillna(col_fillna)

                # rename must be the last step
                if col_dest_name:
                    new_df = new_df.rename(columns={col: col_dest_name})

            except KeyError:
                # this exception was added to work with NoSQL, where columns may not appear in the documents
                self.log.info(f'Non-optional column {col} is not available in df creating with `None`.')
                new_df[col_dest_name] = None

        # fill not available columns in df
        columns_not_avaiable = list(set(self.columns) - set(new_df.columns))
        self.log.info(f'Columns not available in df {columns_not_avaiable}')
        new_df[columns_not_avaiable] = None

        return new_df[self.columns]

    def delete_temp_file(self):
        """

        Deletes the temporary file (when it uses the default name, self.temp_file_path).

        """
    
        try:
            remove(self.temp_file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Couldn't find temporary file in {self.temp_file_path}.")
            
        self.log.info(f'Temporary file removed in path: {self.temp_file_path}')

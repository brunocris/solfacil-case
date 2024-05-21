import json
import pandas as pd

from hashlib import sha256
from src.config.env import TMP_DIR
from src.modules.base.module import BaseModule
from hooks import DisneyApiHook, S3Hook
from airflow.exceptions import AirflowException


class DisneyRaw(BaseModule):

    temp_file_extn = '.json'
    bucket = 'raw'
    dag_name = f"disney_api_{bucket}_dag"

    def __init__(self):
        self.disney_hook = DisneyApiHook()
        self.s3_hook = S3Hook(self.bucket)
    
    def extract(self):
        characters = self.disney_hook.get_characters
        with open(self.temp_file_path, 'w') as f:
            f.write(json.dumps(characters))

        self.log.info(f"Data successfully extracted to '{self.temp_file_path}'")
    
    def load(self):
        try:
            self.s3_hook.upload_file(
                self.temp_file_path,
                'disney-api/characters/characters.json'
            )
        except Exception as e:
            raise AirflowException(f"Could not upload file to amazon S3: {e}")


class DisneyCurated(BaseModule):
    
    temp_file_extn = '.json'
    bucket = 'curated'
    files_to_send = []
    dag_name = f"disney_api_{bucket}_dag"

    fields = {
      "_id": {"rename":"id", "type":"int64"},
      "films": "films",
      "shortFilms": "short_films",
      "tvShows": "tv_shows",
      "videoGames": "video_games",
      "parkAttractions": "park_attractions",
      "allies": "allies",
      "enemies": "enemies",
      "sourceUrl": "source_url",
      "name": {"rename":"name", "fn":lambda x: sha256(x.encode()).hexdigest()}, # Encrypt the column 'name'
      "imageUrl": "image_url",
      "createdAt": {"rename":"created_at", "date_format":"%Y-%m-%dT%H:%M:%S"},
      "updatedAt": {"rename":"updated_at", "date_format":"%Y-%m-%dT%H:%M:%S"},
      "url": "url",
      "__v": "version"
    }

    def __init__(self):
        self.disney_hook = DisneyApiHook()
        self.s3_hook = S3Hook(self.bucket)

    def extract(self):
        self.s3_hook.download_file(
            'disney-api/characters/characters.json',
            self.temp_file_path
        )
    
    def _transform(self):
        # Opening file from temporary dir
        with open(self.temp_file_path, 'r', encoding='utf-8') as f:
            temp_json = json.load(f)

        for character in temp_json:
            # Creating normalized DataFrame from JSON
            df = self._normalize_json_data(character)
            # Apply transformation by fields attribute
            self.transform(df)

            # Parquet file named by "_id"
            filename = f"{character['_id']}.parquet"
            # Path to store temporary parquet before sending
            filepath = f"{TMP_DIR}{self.dag_name}/{filename}"
            self.files_to_send.append((filepath, filename))

            df.to_parquet(filepath)

    def _normalize_json_data(data):
        # Convert dictionary to DataFrame
        df = pd.DataFrame.from_dict(data, orient='index').T

        # Normalize DataFrame
        for key,value in data.items():
            if isinstance(value, list):
                df = df.explode(str(key))

        # Reset index
        df_normalized = df.reset_index(drop=True, inplace=True)

        return df_normalized

    def load(self):
        # Applying transformations and data partition
        self._transform()
        # Send parquet files to curated bucket in s3 with organized dir path
        for file in self.files_to_send:
            self.s3_hook.upload_file(
                file[0], # Origin file path from tuple
                'disney-api/characters/' + file[1] # Parquet file name from tuple plus prefix
            )


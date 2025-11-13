from utils.job_runner import JobRunner
from utils.loader_local import LoaderLocal
from utils.transformation import Transformation
from utils.writer_local import WriterLocal
import pandas as pd


class ValidationJob(JobRunner):
    def __init__(self):
        self.in_path_list = [
            "/home/onyxia/work/hackathon_mobilites_2025/data/raw/validations-reseau-ferre-nombre-validations-par-jour-1er-trimestre.parquet",
            "/home/onyxia/work/hackathon_mobilites_2025/data/raw/validations-reseau-ferre-nombre-validations-par-jour-2eme-trimestre.parquet",
            "/home/onyxia/work/hackathon_mobilites_2025/data/raw/validations-reseau-ferre-nombre-validations-par-jour-3eme-trimestre.parquet",
            "/home/onyxia/work/hackathon_mobilites_2025/data/raw/validations-reseau-ferre-nombre-validations-par-jour-2eme-trimestre.parquet"
        ]
        self.out_path = "/home/onyxia/work/hackathon_mobilites_2025/data/interim/validation_pourcentage.parquet"

    def process(self):
        dataframes = []
        for file_path in self.in_path_list:
            df = LoaderLocal.loader_parquet(file_path)
            dataframes.append(df)
        df_concat = pd.concat(dataframes, ignore_index=True)

        agg_df = df.groupby('ville').agg(
            total_ventes=('ventes', 'sum'),
            premier_responsable=('responsable', lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan)
        ).reset_index()

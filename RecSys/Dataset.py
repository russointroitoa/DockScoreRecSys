import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import re

class DataManager(object):

    def __init__(self):
        self.proteins = pd.read_csv("/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/Manager/protein_time_len.csv", header=None)

        self.proteins = list(self.proteins[0].values[:27])

        cols_name = "Rank,Formula,MW,LogP,HPScore,HMScore,HSScore,Average,Name"
        self.cols_name = cols_name.split(",")

    def check_rows(self, filename):
        file_path = os.path.join(self.folder_name, filename)
        use_cols = ["Formula", "HPScore", "HMScore", "HSScore", "Average", "Name"]
        df = pd.read_csv(file_path, header=None, names=self.cols_name, usecols=use_cols)

        df = df.iloc[df.drop("Name", axis=1).drop_duplicates().index]

        wrong_indices = []

        for i, f, hp, hm, hs, a, n in zip(df.index, df.Formula, df.HPScore, df.HMScore, df.HSScore, df.Average,
                                          df.Name):
            try:
                isinstance(str(f), str)  # Check Formula is a string
                isinstance(float(hp), float)  #  Check HPScore is a float
                isinstance(float(hm), float)  #  Check HMScore is a float
                isinstance(float(hs), float)  #  Check HSScore is a float
                isinstance(float(a), float)  #  Check Average is a float
                isinstance(int(n), int)  #  Check Name is a int
            except:
                wrong_indices.append(i)

        # print(f"Wrong values detected {len(wrong_indices)}")

        df = df.drop(wrong_indices)

        # Check if Nans
        assert all(~df.isna().sum().values), "Nans detected"

        df["Name"] = df.Name.astype(int) # TODO Checkare se è int o no: dovrebbe esserlo per tutti
        df["HPScore"] = df.HPScore.astype(np.float32)
        df["HMScore"] = df.HMScore.astype(np.float32)
        df["HSScore"] = df.HSScore.astype(np.float32)
        df["Average"] = df.Average.astype(np.float32)

        return df, len(wrong_indices)

    def getDataFrame(self, folder_name, all=False):
        walker = os.walk(folder_name)
        walker = next(walker)

        compressed_db_name = folder_name.split("/")[-1].replace(".csv", "")
        compressed_db_name = int("".join(re.findall(r'\d+', compressed_db_name)))

        self.folder_name = walker[0]
        self.filenames = walker[2]

        df = None
        total_wrong = 0

        p_bar = tqdm(self.filenames)
        for f in p_bar:
            p_bar.set_postfix_str(f)
            protein_name = f[-8:-4]
            if protein_name in self.proteins:
                # temp_df = pd.read_csv(file_path, header=None, names=cols_name, usecols=use_cols)
                temp_df, n_wrong = self.check_rows(f)

                if all:
                    rename_cols = {n: n + "_" + protein_name for n in ["HPScore", "HMScore", "HSScore", "Average"]}
                    temp_df = temp_df.rename(columns=rename_cols)
                else:
                    rename_cols = {"Average":protein_name}
                    temp_df = temp_df.drop(["HPScore", "HMScore", "HSScore"], axis=1)
                    temp_df = temp_df.rename(columns=rename_cols)

                if df is None:
                    df = temp_df.copy()
                else:
                    temp_df = temp_df.drop("Formula", axis=1)
                    df = pd.merge(df, temp_df, on="Name", how="inner")

            total_wrong += n_wrong
            p_bar.set_description(f"Wrong Indices Detected: {total_wrong}")

        df = df.reset_index(drop=True)

        # Add DB_NAME as column
        df['DB'] = [compressed_db_name] * df.shape[0]
        return df

    def getURM(self, folder_name):
        df = self.getDataFrame(folder_name)
        columns = [x for x in df.columns if "Average" in x]
        df = df[columns].copy()
        return df.values.T

    def get_total_df(self, result_folder):
        all_dfs = None

        walker = next(os.walk(result_folder))
        base_path = walker[0]

        for fold in walker[1]:
            temp_df = self.getDataFrame(os.path.join(base_path, fold))
            if all_dfs is None:
                all_dfs = temp_df
            else:
                pd.concat([all_dfs, temp_df])

        return all_dfs
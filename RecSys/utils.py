import pandas as pd
import numpy as np
import pickle
import os
from datetime import datetime


def split_train_test(URM, test_idx_row, seen=[]):
    assert test_idx_row < URM.shape[0], "Test row index is out of range"

    test_row = URM[test_idx_row, :]
    train_urm = URM.copy()
    train_urm[test_idx_row, :] = 0

    if len(seen)> 0:
        train_urm[test_idx_row, seen] = test_row[seen]

    return train_urm, test_row, seen


def save_dict(d):
    res_path = "/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/RecSys/Results"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f'result_{timestamp}'
    if not os.path.isdir(os.path.join(res_path, name)):
        os.mkdir(os.path.join(res_path, name))

    with open(os.path.join(res_path, name, 'eval'), 'wb') as f:
        pickle.dump(d, f, pickle.HIGHEST_PROTOCOL)

    print(f"Saved in {name}")


def load_dict(folder_name):
    res_path = "/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/RecSys/Results"
    filename = os.path.join(res_path, folder_name, 'eval')
    with open(filename, 'rb') as f:
        return pickle.load(f)
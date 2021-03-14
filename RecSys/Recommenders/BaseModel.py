import pandas as pd
import numpy as np

from utils import split_train_test

class BaseModel(object):

    def __init__(self, urm, test_row_idx):

        self.urm = urm
        self.test_row_idx = test_row_idx
        self.train_urm, self.test_row, _ = split_train_test(self.urm, self.test_row_idx)

        self.n_items = self.train_urm.shape[1]


    def fit(self,):
        pass

    def predict(self, filter_seen=True):
        pass

    def get_test_row(self, filter_seen=True):
        "Return the test row (filtered by seen)"
        if filter_seen:
            self.test_row[self.train_urm[self.test_row_idx, :] != 0] = -np.inf
            return self.test_row
        else:
            return self.test_row

    def update_train_urm(self, recommendations):
        self.train_urm[self.test_row_idx, recommendations] = self.urm[self.test_row_idx, recommendations]

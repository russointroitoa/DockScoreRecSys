import pandas as pd
import numpy as np

import sys
# sys.path.append("../")
from Recommenders.BaseModel import BaseModel


class TopPopular(BaseModel):

    def __init__(self, urm, test_row_idx):
        super(TopPopular, self).__init__(urm, test_row_idx)

        self.train_rows = list(range(self.train_urm.shape[0]))
        self.train_rows.remove(self.test_row_idx)

    def fit(self, ):
        """
        Non ha tanto senso togliere l'ultima riga perché quando poi faccio l'update dell'urm, non conto lo stesso
        i valori aggiunti. Meglio settare la riga di test a 0 e tenere conto di tale riga nella prediction, altrimenti
        non cambia mai il valore della predizione

        rows_indices = list(range(self.URM_train.shape[0]))
        rows_indices.remove(self.test_idx_row)
        self.URM_train = self.URM_train[rows_indices, :]
        """
        pass

    def predict(self, filter_seen=True):
        recommendations = np.average(self.train_urm[self.train_rows, :], axis=0)

        if filter_seen:
            recommendations[self.train_urm[self.test_row_idx, :] != 0] = -np.inf

        return np.argsort(recommendations)[::-1]

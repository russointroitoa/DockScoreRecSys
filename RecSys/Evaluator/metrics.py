import pandas as pd
import numpy as np

"""
Metrics for single user
"""

class Metric(object):

    def __init__(self,):
        self.name = None

    def get_value(self, test_row, recommendations, at_percentage):
        self.test_row = test_row
        self.recommendations = recommendations
        self.sorted_test_row = np.argsort(self.test_row)[::-1]
        pass

    def convert_percentage_list(self, at_percentage):
        n_items = self.test_row.shape[0]
        return [int( p * n_items) for p in at_percentage]


class Precision(Metric):

    def __init__(self,):
        super(Precision, self).__init__()
        self.name = "Precision"

    def get_value(self, test_row, recommendations, at_percentage):
        self.test_row = test_row
        self.recommendations = recommendations
        self.sorted_test_row = np.argsort(self.test_row)[::-1]

        res = []
        for p in self.convert_percentage_list(at_percentage):
            is_relevant = np.isin(self.recommendations, self.sorted_test_row[:p])
            precision_at_p = np.average(is_relevant)
            res.append(precision_at_p)
        return res


class Average_Precision(Metric):

    def __init__(self,):
        super(Average_Precision).__init__()
        self.name = "Average_Precision"

    def get_value(self, test_row, recommendations, at_percentage):
        self.test_row = test_row
        self.recommendations = recommendations
        self.sorted_test_row = np.argsort(self.test_row)[::-1]
        res = []

        for p in self.convert_percentage_list(at_percentage):
            # TODO Convertire at_percentage in lista di valori
            is_relevant = np.isin(self.recommendations, self.sorted_test_row[:p])
            p_at_k = is_relevant * np.cumsum(is_relevant) / (1 + np.arange(is_relevant.shape[0]))
            a_p = np.sum(p_at_k) / np.min([p, is_relevant.shape[0]])
            res.append(a_p)

        return res

"""
class Rank_Weighted_Precision(Metric):

    def __init__(self, test_row, recommendations):
        super(Rank_Weighted_Precision, self).__init__(test_row, recommendations)
        self.name = "Rank_Weighted_Precision"

    def get_value(self, at_percentage):

"""


class AUC(Metric):

    def __init__(self):
        super(AUC, self).__init__()
        self.name = "AUC"

    def get_value(self, test_row, recommendations, at_percentage):
        self.test_row = test_row
        self.recommendations = recommendations
        self.sorted_test_row = np.argsort(self.test_row)[::-1]
        res = []

        for p in self.convert_percentage_list(at_percentage):
            is_relevant = np.isin(self.recommendations, self.sorted_test_row[:p])

            ranks = np.arange(is_relevant.shape[0])
            pos_ranks = ranks[is_relevant]
            neg_ranks = ranks[~is_relevant]

            if len(neg_ranks) == 0:
                return [1.0 for _ in range(is_relevant.shape[0])]

            if len(pos_ranks) > 0:
                for pos_pred in pos_ranks:
                    pass


class ROC_AUC(Metric):

    def __init__(self, ):
        super(ROC_AUC, self).__init__()
        self.name = "ROC_AUC"

    def get_value(self, test_row, recommendations, at_percentage):
        self.test_row = test_row
        self.recommendations = recommendations
        self.sorted_test_row = np.argsort(self.test_row)[::-1]
        res = []

        for p in self.convert_percentage_list(at_percentage):
            is_relevant = np.isin(self.recommendations, self.sorted_test_row[:p])
            ranks = np.arange(is_relevant.shape[0])
            pos_ranks = ranks[is_relevant]
            neg_ranks = ranks[~is_relevant]
            auc_score = 0.0

            if len(neg_ranks) == 0:
                return 1.0

            if len(pos_ranks) > 0:
                for pos_pred in pos_ranks:
                    auc_score += np.sum(pos_pred < neg_ranks, dtype=np.float32)
                auc_score /= (pos_ranks.shape[0] * neg_ranks.shape[0])
            res.append(auc_score)

        return res


class Recall(Metric):

    def __init__(self,):
        super(Recall, self).__init__()
        self.name = "Recall"

    def get_value(self, test_row, recommendations, at_percentage):
        self.test_row = test_row
        self.recommendations = recommendations
        self.sorted_test_row = np.argsort(self.test_row)[::-1]
        res = []

        for p in self.convert_percentage_list(at_percentage):
            is_relevant = np.isin(self.recommendations, self.sorted_test_row[:p])

            recall = np.sum(is_relevant, dtype=np.float32) / p
            res.append(recall)

        return res
import numpy as np
import pickle

from Evaluator.metrics import *

class Evaluator(object):

    def __init__(self):
        self.result_dict ={}
        self.metrics = [Precision(), Average_Precision(), Recall(), ROC_AUC()]
        self.at_percentage = None

        # Initialize Result Dict
        for m in self.metrics:
            self.result_dict[m.name] = {}

    def compute_metrics(self, test_row, recommendations, at_percentage, i):
        #for m in self.metrics:
        #    self.result_dict[m.name].append(m.get_value(test_row, recommendations, at_percentage))

        if self.at_percentage is None:
            self.at_percentage = at_percentage

        for m in self.metrics:
            if not f"run_{i}" in self.result_dict[m.name]:
                self.result_dict[m.name][f"run_{i}"] = []

            self.result_dict[m.name][f"run_{i}"].append(m.get_value(test_row, recommendations, at_percentage))



    def get_results(self):
        return self.result_dict

    def get_final_results(self):
        final_result = {k : {r : np.average(self.result_dict[k][r], axis=0) for r in self.result_dict[k]} for k in self.result_dict.keys()}
        final_result['at_Percentage'] = self.at_percentage

        return final_result
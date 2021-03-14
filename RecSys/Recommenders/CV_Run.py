"""
# TODO
- definire per bene compute_metrics
- Estendere le classi per TopPopular, CBF e altri recommenders
- definire compute_final_results
- Trovare un modo per passarli alla dashboard

"""
import numpy as np
from tqdm import tqdm
from Evaluator.Evaluator import Evaluator


class CV_Run(object):

    def __init__(self, model, urm, N, Ks, MAX_SUGGESTIONS=100000):
        """

        :param model:
        :param urm:
        :param N:
        :param Ks: list of percentage. Percentage on which evaluate test_row
        :param MAX_SUGGESTIONS:
        :return:
        """
        self.model = model
        self.urm = urm
        self.N = N
        self.Ks = Ks
        self.MAX_SUGGESTIONS = MAX_SUGGESTIONS
        self.evaluator= Evaluator()

    def run(self,):
        for test_row_idx in tqdm(range(self.urm.shape[0])):

            model = self.model(self.urm, test_row_idx)

            for i, _ in enumerate(tqdm(range(0, self.MAX_SUGGESTIONS, self.N), leave=False)):
                model.fit()
                recommendations = model.predict()[:self.N]
                test_row = model.get_test_row()

                # self.compute_metrics(test_row, recommendations, metric_list, self.Ks)
                self.evaluator.compute_metrics(test_row, recommendations, self.Ks, i)
                model.update_train_urm(recommendations)

    def single_run(self,):
        for test_row_idx in tqdm(range(self.urm.shape[0])):

            model = self.model(self.urm, test_row_idx)
            model.fit()
            recommendations = model.predict()[:self.N]
            test_row = model.get_test_row()

            self.evaluator.compute_metrics(test_row, recommendations, self.Ks, 0)

    def get_results(self):
        return self.evaluator.get_results()
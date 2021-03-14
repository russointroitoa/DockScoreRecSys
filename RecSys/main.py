import numpy as np
import os

from Dataset import DataManager
from Recommenders.CV_Run import CV_Run
from Recommenders.TopPopular import TopPopular

path = "/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/DockAndScore/Results/20Atoms_4RotBonds_diverse_relax"
dm = DataManager()

base_path = "/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/RecSys/Temp/URMs"
db_name = "20Atoms_4RotBonds"
urm_path = os.path.join(base_path, db_name)

if not os.path.isfile(urm_path + ".npy"):
    print("Collecting Data ..")
    urm = dm.getURM(path)
else:
    print("Get URM")
    urm = np.load(urm_path + ".npy")

if not os.path.isfile(urm_path + ".npy"):
    np.save(urm_path, urm)


N = 10000
Ks = [0.01, 0.02, 0.1]
cv = CV_Run(TopPopular, urm, N, Ks)

cv.run()

print(cv.get_results())
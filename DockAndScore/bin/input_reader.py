import os

class Reader(object):

    def __init__(self, filepath):
        self.path = filepath

        self.params = {}
        with open(self.path, 'r') as infile:
            for line in infile:
                line = line.strip()
                key_value = line.split()
                if len(key_value) == 2:
                    self.params[key_value[0]] = key_value[1]
                elif len(key_value) == 1:
                    print("Error in input parameter file")

        self.check_params()

    def check_params(self):
        self.params['cpu_workers'] = int(self.params['cpu_workers'])
        self.params["database"] = os.path.abspath(self.params["database"])
        self.params["protein"] = os.path.abspath(self.params["protein"])
        self.params["chunk_size"] = int(self.params["chunk_size"])
        if self.params["n_molecules"].isnumeric():
            self.params['n_molecules'] = int(self.params['n_molecules'])
        else:
            self.params['n_molecules'] = "ALL"
        return self.params

    def get_params(self):
        return self.params

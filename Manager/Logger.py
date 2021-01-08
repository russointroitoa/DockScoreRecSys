import os
import shutil
import pandas as pd

class Logger(object):

    def __init__(self):
        """
        Job Log:
            Header: job_id,input_name,status
            Status:
                - S : sumbitted
                - C : cancelled
                - T : terminated
                - F : failed
        """
        self.abs_dir = os.path.dirname(__file__)
        self.input_folder = os.path.join(self.abs_dir, "data_transfer", "input")
        self.log_file = os.path.join(self.input_folder, "job_submission_log.csv")

        self.input_params_folder = os.path.join(self.input_folder, "input_parameters")

        # Initialize Logger

        if not os.path.isfile(self.log_file):
            with open(self.log_file, "w") as f:
                f.write("jobId,input_name,status\n")

        self.status_dict = {
            1 : "S",
            2 : "S",
            4 : "S",
            8 : "S",
            16 : "T",
            32 : "F",
            64 : "C",
        }

        self.status_folder = {
            "R" : "ready",
            "S" : "submitted",
            "T" : "terminated",
            "F" : "cancelled",
            "C" : "cancelled"
        }

    def add(self, job_id, input_parameters_name):  # TODO Ci pensa Logger
        with open(self.log_file, "a") as f:
            f.write(f"{job_id},{input_parameters_name},S" + "\n")

        self.moveInputFile(input_parameters_name, "R", "S")


    def moveInputFile(self, input_parameters_name, old_status, new_status):
        src_path = os.path.join(self.input_params_folder, self.status_folder[old_status], input_parameters_name)
        dst_path = os.path.join(self.input_params_folder, self.status_folder[new_status], input_parameters_name)
        shutil.move(src_path, dst_path)

    def update(self, jobs_json):
        log = pd.read_csv(self.log_file)
        for i, j, name, s in zip(log.index, log.jobId, log.input_name, log.status):
            try:
                state = [x for x in jobs_json if x["Id"] == j][0]["State"]
            except IndexError:
                print(f"Job {j} doesn't exist")

            if s != self.status_dict[state]:
                log.loc[i, "status"] = self.status_dict[state]
                self.moveInputFile(name, s, self.status_dict[state])

        log.to_csv(self.log_file, index=False)

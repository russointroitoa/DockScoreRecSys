import sys
import json
import os
import time
from pathlib import Path
from io import StringIO

import paramiko
from paramiko import SSHClient
from scp import SCPClient

import heappeac as hp
import datetime
import re

from Manager.Logger import Logger

class HEAppE_Manager(object):

    def __init__(self,):
        # Setting HEAppE Client
        configuration = hp.Configuration()
        self.username = "computationuser"
        self.password = "bSwxMi7WBqxxbMSfzE9&2020"
        configuration.host = "https://heappe.it4i.cz/covid"
        self.api_instance = hp.ApiClient(configuration)

        self.jm = hp.api.JobManagementApi(self.api_instance)
        self.ft = hp.api.FileTransferApi(self.api_instance)
        self.ulm = hp.api.UserAndLimitationManagementApi(self.api_instance)

        self.file_abs = os.path.dirname(__file__)
        self.data_transfer_path = os.path.join(self.file_abs, "data_transfer")
        self.input_folder = os.path.join(self.data_transfer_path, "input")
        self.output_folder = os.path.join(self.data_transfer_path, "output")
        print("HEAppE instance Python client prepared")

        self.logger = Logger()

    def authentication(self):
        # Authentication user to HEAppE instance
        print(f"Authenticating {self.username}...")
        cred = {
            "_preload_content": False,
            "body": {
                "Credentials": {
                    "Password": self.password,
                    "Username": self.username
                }
            }
        }

        ulm = hp.api.UserAndLimitationManagementApi(self.api_instance)
        r = ulm.heappe_user_and_limitation_management_authenticate_user_password_post(**cred)
        session_code = json.loads(r.data)
        print(f"Session code: {session_code}")
        return session_code

    def createJob(self, session_code, input_parameters_name, output_folder_name, maxCores=96):
        # Job specification with tasks
        print("Creating job template...")
        # jm = hp.api.JobManagementApi(self.api_instance)

        job_spec_body = {
            "_preload_content": False,
            "body": {
                "JobSpecification": {
                    "Name": "ScoreExtractionJob",
                    "Project": "DD-20-11",

                    # "WaitingLimit": 0,
                    # "NotificationEmail": "string",
                    # "PhoneNumber": "string",
                    # "NotifyOnAbort": true,
                    # "NotifyOnFinish": true,
                    # "NotifyOnStart": true,
                    "ClusterId": 2,
                    "FileTransferMethodId": 2,
                    "EnvironmentVariables": [],
                    "Tasks": [
                        {
                            "Name": "ScoreExtractionTask",
                            "MinCores": 1,
                            "MaxCores": maxCores,  # for 4 nodes (4x24Cores per node)
                            "WalltimeLimit": 86400,     # 24 hours
                            # "RequiredNodes": "string",
                            "Priority": 4,
                            # "JobArrays": "string",
                            # "IsExclusive": true,
                            # "IsRerunnable": true,
                            # "StandardInputFile": "string",
                            "StandardOutputFile": "stdout",
                            "StandardErrorFile": "stderr",
                            "ProgressFile": "stdprog",
                            "LogFile": "stdlog",
                            # "ClusterTaskSubdirectory": "string",
                            "ClusterNodeTypeId": 7,
                            "CommandTemplateId": 5,
                            # "CpuHyperThreading": true,

                            # "EnvironmentVariables": [], #Option1: Without outputs in CovidComputation folder

                            "EnvironmentVariables": [
                                {
                                    "Name": "EXEC_OUTPUT_FOLDER",
                                    "Value": output_folder_name
                                }
                            ],  # Option2: Outputs in /scratch/temp/CovidComputation/{output_folder_name}
                            # "DependsOn": [],
                            "TemplateParameterValues": [
                                {
                                    "CommandParameterIdentifier": "MPICores",
                                    "ParameterValue": "4"
                                },  # for calling mpi -4 in execution
                                {
                                    "CommandParameterIdentifier": "INPUT_FILENAME",
                                    "ParameterValue": input_parameters_name
                                }
                            ]
                        }
                    ]
                },
                "SessionCode": session_code
            }
        }
        r = self.jm.heappe_job_management_create_job_post(**job_spec_body)
        r_data = json.loads(r.data)
        job_id = r_data["Id"]
        tasks = r_data["Tasks"]
        print(f"Job ID: {job_id}")
        return job_id, tasks

    def loadInput(self, session_code, job_id, tasks, input_parameters_name):
        # Copying files from input folder to job folder on HPC cluster
        #print("Preparation of copying files ...")
        # ft = hp.api.FileTransferApi(api_instance)
        ft_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "SessionCode": session_code
            }
        }
        r = self.ft.heappe_file_transfer_get_file_transfer_method_post(**ft_body)
        r_data = json.loads(r.data)

        print("Copying files..")
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_username = r_data["Credentials"]["UserName"]
        pkey_file = StringIO(r_data["Credentials"]["PrivateKey"])
        pkey = paramiko.rsakey.RSAKey.from_private_key(pkey_file)
        ssh.connect(r_data["ServerHostname"], username=ssh_username, pkey=pkey)
        base_path = r_data["SharedBasepath"]
        filenames = ["stdout", "stderr"]

        with SCPClient(ssh.get_transport()) as scp:
            for task in tasks:
                task_id = str(task["Id"])
                input_path = os.path.join(self.input_folder, f"input_parameters/ready/{input_parameters_name}")
                scp.put(input_path, os.path.join(base_path, task_id))

                print(f"Copied: {input_parameters_name} --> {os.path.join(base_path, task_id)}")

        ft_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "UsedTransferMethod": r_data,
                "SessionCode": session_code
            }
        }
        r = self.ft.heappe_file_transfer_end_file_transfer_post(**ft_body)
        r_data = json.loads(r.data)

        self.logger.add(job_id, input_parameters_name)

        print("Copying input files finished.")

    def submitJob(self, session_code, job_id):
        # Submittion of job
        # Status codes:
        #  1  - Configuring
        #  2  - Submited
        #  4  - Queued
        #  8  - Running
        #  16 - Finished
        #  32 - Failed
        #  64 - Canceled

        print(f"Submitting job {job_id}...")
        submit_body = {
            "_preload_content": False,
            "body":
                {
                    "CreatedJobInfoId": job_id,
                    "SessionCode": session_code
                }
        }
        r = self.jm.heappe_job_management_submit_job_post(**submit_body)
        r_data = json.loads(r.data)

    def getJobStatusLoop(self, session_code, job_id):
        print(f"Waiting for job {job_id} to finish...")

        gcji_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "SessionCode": session_code
            }
        }

        isAlreadyRunning = False
        while True:
            r = self.jm.heappe_job_management_get_current_info_for_job_post(**gcji_body)
            r_data = json.loads(r.data)
            state = r_data["State"]
            if r_data["State"] == 16:
                print(f"The job has finished.")
                break
            if r_data["State"] == 32:
                print(f"The job has failed.")
                break
            if r_data["State"] == 64:
                print(f"The job has canceled.")
                break
            if r_data["State"] == 8:
                if not isAlreadyRunning:
                    start_time = time.time()
                    isAlreadyRunning = True
                print(f"Waiting for job {job_id} to finish... current state: {state}. Time: {datetime.timedelta(seconds=(time.time() - start_time))} min")
            else:
                print(f"Waiting for job {job_id} to finish... current state: {state}")
            time.sleep(30)

        print(f"Total Time {datetime.timedelta(seconds=(time.time() - start_time))} min")

    def getJobStatus(self,  session_code, job_id):
        status_dict = {
            4 : "Queued",
            8 : "Running",
            16 : "Finished",
            32 : "Failed",
            64 : "Canceled",
        }


        gcji_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "SessionCode": session_code
            }
        }

        r = self.jm.heappe_job_management_get_current_info_for_job_post(**gcji_body)
        r_data = json.loads(r.data)

        state = r_data["State"]
        elapsed_time = self.elapsed_time(r_data)

        return f"{status_dict[state]} : {elapsed_time}"

    def getSubmittedJobs(self, session_code):
        # check status of submitted job
        # jm = hp.api.JobManagementApi(api_instance)
        list_job_for_curr_user_body = {
            "_preload_content": False,
            "body": {
                "SessionCode": session_code
            }
        }

        r = self.jm.heappe_job_management_list_jobs_for_current_user_post(**list_job_for_curr_user_body)
        r_data = json.loads(r.data)
        return r_data

    def getListChangedFilesForJob(self, session_code, job_id):
        ft_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "SessionCode": session_code
            }
        }
        print("Waiting for output files ..")
        r = self.ft.heappe_file_transfer_list_changed_files_for_job_post(**ft_body)
        r_data = json.loads(r.data)

        return r_data


    def getSSHConnection(self, session_code, job_id):

        # SSH Connection
        ft_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "SessionCode": session_code
            }
        }

        r = self.ft.heappe_file_transfer_get_file_transfer_method_post(**ft_body)
        r_data = json.loads(r.data)

        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_username = r_data["Credentials"]["UserName"]
        pkey_file = StringIO(r_data["Credentials"]["PrivateKey"])
        pkey = paramiko.rsakey.RSAKey.from_private_key(pkey_file)
        ssh.connect(r_data["ServerHostname"], username=ssh_username, pkey=pkey)
        base_path = r_data["SharedBasepath"]

        return ssh, base_path

    def downloadOutput(self, session_code, job_id):
        """
        If final results are available, take only them; otherwise, store rank output in a folder in output
        :param session_code:
        :param job_id:
        :return:
        """
        out_files = self.getListChangedFilesForJob(session_code, job_id)
        out_files = [os.path.normpath(f) for f in out_files]

        print(f"Output files: {' '.join(out_files)}")

        filenames = [f for f in out_files if "final_results_" in f]
        out_folder = "/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/DockAndScore/Results" # TODO Absolute Path

        ssh, base_path = self.getSSHConnection(session_code, job_id)

        # SCP Transfer
        with SCPClient(ssh.get_transport()) as scp:
            input_params = [f for f in out_files if "input_parameters_" in f][0]
            db_name = os.path.basename(input_params).replace("input_parameters_", "")
            db_name = db_name.split("_")[:-1]
            db_name = "_".join(db_name)

            if len(filenames) == 1:
                print("Get only final results")
                fn = filenames[0]
                print(f"{os.path.basename(fn)} --> {os.path.join(out_folder, db_name, os.path.basename(fn))}")
                scp.get(os.path.join(base_path, fn), os.path.join(out_folder, db_name, os.path.basename(fn)))
            else:
                print("Final Results not available. Store ranks output")
                for fn in out_files:
                    local_path = os.path.join(self.output_folder, {job_id})
                    Path(os.path.dirname(os.path.join(local_path, fn))).mkdir(parents=True, exist_ok=True)
                    print(f"{os.path.join(base_path, fn)} --> {os.path.join(local_path, fn)}")

                    scp.get(os.path.join(base_path, fn), os.path.join(local_path, fn))


    def getStdOutputFiles(self, session_code, job_id):
        # TODO
        pass

    def cancelJob(self, session_code, job_id):
        # Cancel Job
        cancel_job_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "SessionCode": session_code,
            }
        }

        r = self.jm.heappe_job_management_cancel_job_post(**cancel_job_body)
        r_data = json.loads(r.data)
        print(r_data)


    def removeJob(self, session_code, job_id):
        # Remove job after execution on HPC Cluster
        print(f"Removing job {job_id}...")
        ft_body = {
            "_preload_content": False,
            "body": {
                "SubmittedJobInfoId": job_id,
                "SessionCode": session_code
            }
        }
        r = self.jm.heappe_job_management_delete_job_post(**ft_body)
        r_data = json.loads(r.data)
        print(r_data)


    def getCompressedDBNameProtein(self, input_parameters_name):
        i = input_parameters_name.replace("input_parameters_", "").replace(".txt", "")
        i = i.split("_")
        protein_name = i[-1]
        compressed_db_name = [re.search('[A-Z0-9]*', s).group(0) for s in i[:2]] + [s[0] for s in i[2:-1]]
        compressed_db_name = "".join(compressed_db_name)
        return "_".join([compressed_db_name, protein_name])

    def runSumbitJob(self, session_code, input_parameters_name, maxCores=96):
        """
        NEED AUTHENTICATION and an input_parameters_{something}.txt file
        :param session_code:
        :param input_parameters_name:
        :return:
        """
        output_folder_name = self.getCompressedDBNameProtein(input_parameters_name)

        job_id, tasks = self.createJob(session_code, input_parameters_name, output_folder_name, maxCores=maxCores)
        self.loadInput(session_code, job_id, tasks, input_parameters_name)
        self.submitJob(session_code, job_id)
        return job_id


    def updateLogStatus(self, session_code):
        r_data = self.getSubmittedJobs(session_code)
        self.logger.update(r_data)

    def sumbitJobsListProteinsSameDB(self, session_code, db_name, protein_list, ):
        input_list = self.getListInputsFromListOfProtein(db_name, protein_list)

        for i in input_list:
            self.runSumbitJob(session_code, i)



    # Input handle functions

    def getListInputsFromListOfProtein(self, db_name, protein_list):
        input_files = [self.getInputFileName(db_name, p) for p in protein_list]
        return input_files

    def getInputFileName(self, db_name, protein_name):
        filename = f"input_parameters_{db_name.split('.mol2')[0]}_{protein_name}.txt"
        return filename

    def getReadyInputFilePath(self, db_name, protein_name):
        filename = self.getInputFileName(db_name, protein_name)
        path = os.path.join(self.input_folder, "input_parameters", "ready", filename)
        return path

    def create_input_file(self, db_name, protein_name, n_molecules="ALL"):
        filepath = self.getReadyInputFilePath(db_name, protein_name)

        db_path = os.path.join("/scratch/work/project/dd-20-11/data/mol2DB", db_name)
        protein_path = os.path.join("/scratch/work/project/dd-20-11/data/srcFiles/proteins", protein_name)

        # TODO Magari fare un check qui per vedere se il file è già stato creato in precedenza
        f = open(filepath, "w")
        f.write("cpu_workers 22\n")
        f.write(f"database {db_path}\n")
        f.write(f"protein {protein_path}\n")
        f.write(f"chunk_size 500\n")
        f.write(f"n_molecules {n_molecules}")



    # Get Job Status information functions

    def elapsed_time(self, desc_dict):
        start_time = datetime.datetime.fromisoformat(desc_dict["StartTime"])
        end_time = desc_dict["EndTime"]
        if end_time is None:
            now = datetime.datetime.now().replace(microsecond=0)
            elapsed = (now - start_time)
            print(f"Running: {elapsed}")
        else:
            end_time = datetime.datetime.fromisoformat(end_time)
            elapsed = end_time - start_time
            print(f"Terminated: {elapsed}")
        return f"{elapsed}"

import os
import sys
import math
from multiprocessing import Pool, Queue, Event, Process
import time
import shutil
import subprocess

sys.path.insert(1, os.path.dirname(__file__))

from input_reader import Reader

class Executor_Queue(object):

    def __init__(self, params_filename, n_proc, start_pos, end_pos):
        self.input_filename = params_filename
        self.params = self.read_params()

        self.cpu_workers = self.params['cpu_workers']
        self.db_path = self.params['database']
        self.protein = self.params['protein']
        self.chunk_size = self.params['chunk_size']
        self.n_molecules = self.params['n_molecules']

        if self.n_molecules == "ALL":
            self.n_molecules = math.inf

            # Queue Initialization
            self.queue = Queue()
        else:
            self.n_molecules = int(self.n_molecules)
            self.n_molecules = int(self.n_molecules / n_proc)

            # Queue Initialization
            self.queue = Queue(self.n_molecules)  # Max number of molecules that has to be processed

        self.start_pos = start_pos
        self.end_pos = end_pos

        # self.home_dir = os.path.dirname(__file__)    # TODO Controllare se va bene
        self.home_dir = os.getcwd()

        self.output_dir = os.path.join(self.home_dir, "output")
        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)

        self.chunk_dir = os.path.join(self.output_dir, "chunk")
        if not os.path.isdir(self.chunk_dir):
            os.mkdir(self.chunk_dir)

        self.db_name = self.db_path.split("/")[-1].replace('.mol2', '')
        self.protein_name = self.protein.split("/")[-1].replace('.mol2', '')
        result_filename = f"results_{self.db_name}_protein_{self.protein_name}.csv"
        self.result_file_path = os.path.join(self.output_dir, result_filename)
        if not os.path.isfile(self.result_file_path):
            with open(self.result_file_path, 'w') as res:
                # res.write("Rank,Formula,MW,LogP,HPScore,HMScore,HSScore,Average,Name\n") # Header
                pass
        self.completed_queue = Queue()
        
    def read_params(self):
        r = Reader(self.input_filename)
        return r.get_params()

    def create_mol2_file(self, molecule, mol_filename):
        # Create mol_filename directory:
        path = os.path.join(self.chunk_dir, mol_filename)
        os.mkdir(path)

        # create .mol2 file
        # print("Creating .mol2 file..")

        file_name_path = os.path.join(path, f'{mol_filename}.mol2')
        mol_file = open(file_name_path, "w")
        mol_file.write(molecule)
        mol_file.close()

        # print("Done")

    def call_dock_and_score(self, molecule, protein):
        # Change dir
        path = os.path.join(self.chunk_dir, molecule)
        os.chdir(path)

        start_time = time.time()
        # Call dock_and_score
        # bash_path = os.path.join(home_dir, "dock_and_score.bash")
        try:
            subprocess.run(["/scratch/work/project/dd-20-11/bin/dock_and_score_mpi.bash", molecule, protein])
            end_time = time.time()
        except Exception as e:
            print(f"Error Dock_and_Score --> {e}")
            exit()
        #print(f"Parent {os.getppid()} --> Process {os.getpid()} processes moleucule {molecule}: d&s time = {end_time - start_time}")
        return end_time - start_time    
    
    def add_to_completed(self, folder_name):
        self.completed_queue.put(folder_name)

    def process_single_mol(self, m):
        mol_name = m.split("\n", 2)[1]
        os.chdir(self.home_dir)
        self.create_mol2_file(m, f"{mol_name}")
        time_elapsed = self.call_dock_and_score(f"{mol_name}", self.protein_name)
        self.add_to_completed(f"{mol_name}")
        return time_elapsed

    # Worker

    def worker(self,):
        processed = 0
        total_time = 0.0
        while True:
            item = self.queue.get(True)
            if item == "STOP":
                print(f"{os.getpid()} processes STOP signal, stop execution. Processed {processed} with avg. time {float(total_time / processed)}")
                break

            time = self.process_single_mol(item)
            processed += 1
            total_time += time

    # Writer

    def write_score(self, folder):                  # TODO
        with open(self.result_file_path, 'a') as result:
            # Create Path to the folder/BestPose_XScore file
            bestPose_path = os.path.join(self.chunk_dir, folder, "bestPose_XScore.log")
            try:
                score_file = open(bestPose_path, "r")
                _, score = score_file.readline(), score_file.readline()
                score = score.replace("|", "").split()

                if  score[1] != '0.0' and score[1] != '0.00' and len(score) == 9:
                    score = ",".join(score)
                    result.write(score + "\n")

                    score_file.close()
                    shutil.rmtree(os.path.join(self.chunk_dir, folder))
                    return 1

                elif len(score) != 9:
                    shutil.rmtree(os.path.join(self.chunk_dir, folder))
                    return 0

                elif float(score[1]) == 0.0:
                    # Dock and Score fails, removing the folder
                    shutil.rmtree(os.path.join(self.chunk_dir, folder))
                    return 0

            except Exception as e:
                # print(f"Best Pose of ligand {folder} cannot be read: {e} --> Removing Folder..")
                shutil.rmtree(os.path.join(self.chunk_dir, folder))
                return 0

    def writer_worker(self,):
        count = 0
        while True:
            folder = self.completed_queue.get(True)
            if folder == "DONE":
                break
            self.write_score(folder)
            count += 1
            
            if (count % self.chunk_size) == 0:
                print(f"Writer: Molecules processed: {count}")
        
    def run(self):
        """
        There are three groups of processes:
        - Reader: it corresponds to the main process which reads the input database. Each times an entire molecule is read, it is added to the shared queue
        - Pool of Processes: this is a pool of processes which process each molecule in the queue and call dock_and_score. They terminate when STOP is read
        - Writer: it uses the completed_queue to check the molecules processed and write their scores in a single file.
        :return:
        """
        start_time = time.time()
        count = 0
        curr_molecule = ""

        pool = Pool(self.cpu_workers, self.worker, ())
        print("Workers Started..")

        # Starting Deamon Worker
        writer = Process(target=self.writer_worker, args=())
        # writer.daemon = True
        writer.start()
        print("Writer Started..")

        with open(self.db_path, "r") as db:
            db.seek(self.start_pos)

            # Initialization
            line = db.readline()    # First <TRIPOS>MOLECULE
            curr_molecule += line

            while db.tell() < self.end_pos and count < self.n_molecules:
                line = db.readline()

                if line == "": # EOF
                    break

                if "@<TRIPOS>MOLECULE\n" == line:   # An entire molecule has been read, it's added to the queue
                    count += 1
                    self.queue.put(curr_molecule)

                    # Reset curr_molecule buffer
                    curr_molecule = ""

                curr_molecule += line

            for _ in range(self.cpu_workers):       # STOP WORKERS
                print("Stopping Workers. Send STOP signal")
                self.queue.put("STOP")

        pool.close()
        pool.join()

        print("Pool terminates")

        # Pool processes all molecules, wait until writer terminates
        self.completed_queue.put("DONE")
        writer.join()
        print("Deamon terminates")
        print(f"Executor Execution Time: {time.time() - start_time}")

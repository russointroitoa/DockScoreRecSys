import os
import random
import sys
import math
from multiprocessing import Pool
import time
import subprocess
import multiprocessing
import shutil

sys.path.insert(1, os.path.dirname(__file__))

from input_reader import Reader


class DB_Executor(object):

    def __init__(self, params_filename, n_proc, start_pos, end_pos):
        logger = multiprocessing.log_to_stderr()
        logger.setLevel(multiprocessing.SUBDEBUG)

        self.input_filename = params_filename
        self.params = self.read_params()

        self.cpu_workers = self.params['cpu_workers']
        self.db_path = self.params['database']
        self.protein = self.params['protein']
        self.chunk_size = self.params['chunk_size']
        self.n_molecules = self.params['n_molecules']

        if self.n_molecules == "ALL":
            self.n_molecules = math.inf
        else:
            self.n_molecules = int(self.n_molecules)
            self.n_molecules = int(self.n_molecules/n_proc)

        self.start_pos = start_pos
        self.end_pos = end_pos

        #self.home_dir = os.path.dirname(__file__)    # TODO Controllare se va bene
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

    def read_params(self):
        r = Reader(self.input_filename)
        return r.get_params()

    def create_mol2_file(self, molecule, mol_filename):
        # Create mol_filename directory:
        path = os.path.join(self.chunk_dir, mol_filename)
        os.mkdir(path)

        # create .mol2 file
        # print("Creating .mol2 file..")
        molecule = "@<TRIPOS>MOLECULE\n" + molecule

        file_name_path = os.path.join(path, f'{mol_filename}.mol2')
        mol_file = open(file_name_path, "w")
        mol_file.write(molecule)
        mol_file.close()

        # print("Done")

    def call_dock_and_score(self, molecule, protein):
        #molecule_name = molecule.split(".mol2")[0]

        # Change dir
        path = os.path.join(self.chunk_dir, molecule)
        os.chdir(path)
        
        start_time = time.time()
        # Call dock_and_score
        # bash_path = os.path.join(home_dir, "dock_and_score.bash")
        try:
            # os.system(f"dock_and_score_db.bash {molecule_name} {protein}")    # Local dock_and_score.bash
            # os.system(f"/scratch/work/project/dd-20-11/bin/dock_and_score_mpi.bash {molecule} {protein}")
            subprocess.run(["/scratch/work/project/dd-20-11/bin/dock_and_score_mpi.bash", molecule, protein])
            end_time = time.time()
        except:
            exit()
        print(f"Parent {os.getppid()} --> Process {os.getpid()} processes moleucule {molecule}: d&s time = {end_time - start_time}  ;  Offset from pool beginning = {end_time - self.start_time_pool}")

    def process_molecules(self, molecules):
        self.start_time_pool = time.time()
        with Pool(processes=self.cpu_workers) as p:
            p.map(self.process_single_mol, molecules, chunksize=int(len(molecules) / self.cpu_workers))
        
        print(f"Total Time for parallelized loop = {time.time() - self.start_time_pool}")
        """
        total_time = 0.0
        for m in molecules:
            start_time = time.time()
            self.process_single_mol(m, protein)
            total_time += time.time() - start_time

        total_time /= len(molecules)
        print(f"Average time for {len(molecules)} equal to {total_time}")        
        """

    def process_single_mol(self, m):
        mol_name = m.partition("\n")[0]
        os.chdir(self.home_dir)
        self.create_mol2_file(m, f"ligand_{mol_name}")
        self.call_dock_and_score(f"ligand_{mol_name}", self.protein_name)
    
    def extract_chunk_result(self, ):

        # Add scores to result file
        with open(self.result_file_path, 'a') as result:
            for subdir, dir, files in os.walk(self.chunk_dir):
                for f in files:
                    if "bestPose_XScore" in f:
                        score_file = open(os.path.join(subdir, f), 'r')
                        try:
                            _, score = score_file.readline(), score_file.readline()
                            score = score.replace("|", "").split()
                            if not score[1] == "0.0":
                                score = ",".join(score)
                                result.write(score + "\n")
                          
                        except:
                            pass
                        score_file.close()

        # Remove everything in chunk directory
        shutil.rmtree(self.chunk_dir)
        os.mkdir(self.chunk_dir)

    def run(self,):
        chunk_count = 0
        total_count = 0
        lines_read = ""

        with open(self.db_path, 'r') as db:
            db.seek(self.start_pos)

            while db.tell() != self.end_pos:
                line = db.readline()

                if line == "":  # EOF
                    break

                if "@<TRIPOS>MOLECULE\n" == line:
                    if total_count % self.chunk_size == 0:
                        print(f"Molecules read: {total_count}")

                    chunk_count += 1
                    total_count += 1

                    if chunk_count - 1 == self.chunk_size or total_count - 1 == self.n_molecules:

                        # Process molecules
                        molecules = lines_read.split("@<TRIPOS>MOLECULE\n")
                        molecules = molecules[1:]

                        # Processing
                        self.process_molecules(molecules)

                        # Extract chunk results
                        self.extract_chunk_result()

                        # End Processing
                        lines_read = ""

                        if total_count - 1 == self.n_molecules:
                            print(f"Molecules read: {total_count - 1}") # It's -1 since we have already increased the total_count variable
                            break

                        chunk_count = 1

                lines_read += line

            # Process last molecules
            if lines_read != "":
                molecules = lines_read.split("@<TRIPOS>MOLECULE\n")
                molecules = molecules[1:]
                print(f"Last {len(molecules)} molecules. Processing..")

                # Processing
                self.process_molecules(molecules)

                # Extract chunk results
                self.extract_chunk_result()
                print(f"Molecules read: {total_count}")

       

import sys
import os
from mpi4py import MPI
import math
import numpy as np

sys.path.insert(1, os.path.dirname(__file__))

from input_reader import Reader
# from executor import DB_Executor
from executor_queue import Executor_Queue as DB_Executor

def check_next_mol(position, db_path, rank):
    with open(db_path, 'r') as f:
        f.seek(position)
        prev_byte_pos = f.tell()
        line = f.readline()

        while (line != "@<TRIPOS>MOLECULE\n") and (line != ""):
            prev_byte_pos = f.tell()
            line = f.readline()

        new_pos = f.tell() - (f.tell() - prev_byte_pos)

    return new_pos

if __name__ == "__main__":

    #assert MPI.COMM_WORLD.Get_size() > 1

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    n_proc = comm.Get_size()

    curr_dir = os.getcwd()
    output_dir = os.path.join(curr_dir, "output")

    if rank == 0:
        os.mkdir(output_dir)

    print(f"Rank {rank} starts")

    # Read input_params file
    params_filename = sys.argv[1]
    params_filename = os.path.abspath(params_filename)
    reader = Reader(params_filename)
    params = reader.get_params()

    if params['n_molecules'] == "ALL":
        mols_per_rank = "ALL" # math.inf
    else:
        mols_per_rank = int(params['n_molecules'] / n_proc)

    db_path = params['database']
    filesize = os.path.getsize(db_path)
    part_size = int(filesize / n_proc)

    # Compute initial positions
    start_pos = part_size * rank
    end_pos = part_size * (rank + 1)

    if rank == n_proc - 1:
        end_pos = filesize

    # Check initial pos of the next molecule for both start_point and end_point
    start_pos = check_next_mol(start_pos, db_path, rank)
    end_pos = check_next_mol(end_pos, db_path, rank)

    pos_range = (start_pos, end_pos)

    # Check correctness
    #assert(start_pos <= end_pos), "Error: start_pos > end_pos "    # TODO Gestire l'eccezione in mpi: Gestire se start_pos == end_pos

    # Check consistency among processes --> Gather positions
    positions = comm.gather(pos_range, root=0)

    if rank == 0:
        print(positions)
        for i in range(len(positions) - 1):
            if positions[i][1] != positions[i+1][0]:
                print(f"Rank {i} end != Rank {i+1} start. Set Rank {i+1} start = Rank {i} end")
                positions[i+1][0] = positions[i][1]

    pos_range = comm.scatter(positions, root=0)


    # Each process create its own directory in output and call the other script
    rank_dir = os.path.join(output_dir, f"rank_{rank}")
    os.mkdir(rank_dir)

    os.chdir(rank_dir)

    executor = DB_Executor(params_filename, n_proc, start_pos, end_pos)
    executor.run()

    comm.Barrier()

    # Write rank results in one file
    # Local Results Path
    db_name = params['database'].split("/")[-1].replace('.mol2', '')
    protein_name = params['protein'].split("/")[-1].replace('.mol2', '')
    rank_result_filename = f"results_{db_name}_protein_{protein_name}.csv"
    rank_res_path = os.path.join(rank_dir, "output" , rank_result_filename)

    res_filesize =  os.path.getsize(rank_res_path)

    displacements = None
    sizes = comm.gather(res_filesize, root=0)

    if rank == 0:
        sizes.insert(0,0)
        sizes.pop()
        displacements = list(np.cumsum(sizes))
        print(displacements)

    disp = comm.scatter(displacements, root=0)

    amode = MPI.MODE_WRONLY | MPI.MODE_CREATE
    final_path = os.path.join(output_dir, f"final_results_{db_name}_protein_{protein_name}.csv")
    
    fh = MPI.File.Open(comm, final_path, amode)
    fh.Seek(disp, MPI.SEEK_SET)    

    with open(rank_res_path, 'r') as rank_f:
        for line in rank_f:
            ba = bytearray()
            ba.extend(map(ord, line))
            fh.Write(ba)

    fh.Close()

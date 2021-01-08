import sys
import os
from mpi4py import MPI
import math
import numpy as np

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

def read_input_params(input_params, rank):
    params = {}
    with open(input_params, 'r') as infile:
        for line in infile:
            line = line.strip()
            key_value = line.split()
            if len(key_value) == 2:
                params[key_value[0]] = key_value[1]
            #else:
            #    params[key_value[0]] = None

    if rank == 0:
        print(params)
        if not os.path.isfile(params['database']):
            print("Database path wrong, insert a valid one")

        #if not os.path.isfile(params['protein']):   # TODO USARLO
        #    print("Protein path wrong, insert a valid one")

    return params

def transform_input_params(params):
    params["database"] = os.path.abspath(params["database"])
    params["protein"] = os.path.abspath(params["protein"])
    params["chunk_size"] = int(params["chunk_size"])
    if params["n_molecules"].isnumeric():
        params['n_molecules'] = int(params['n_molecules'])
    else:
        params['n_molecules'] = "ALL"
    return params

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
    params = read_input_params(params_filename, rank)
    params = transform_input_params(params)

    if params['n_molecules'] == "ALL":
        mols_per_rank = 1000000000 # math.inf
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

    # TODO Aggiustare il calcolo di mol_per_rank

    os.system(f"python {os.path.join('/scratch/work/project/dd-20-11/bin/', 'extract_scores.py')} {params['database']} {params['protein']} {params['chunk_size']} {mols_per_rank} {start_pos} {end_pos} {params['cpu_workers']}")

    # TODO Gestire lettura intera database

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
    final_path = os.path.join(output_dir, "final_results.csv")
    fh = MPI.File.Open(comm, final_path, amode)

    with open(rank_res_path, 'r') as rank_f:
        #res = bytearray(rank_f.read().encode())
        res = bytearray()
        res.extend(map(ord, rank_f.read()))

    fh.Seek(disp, MPI.SEEK_SET)
    fh.Write(res)

    fh.Close()

import os
import random
import sys

global home_dir
global output_dir
global chunk_dir
global result_file_path

def create_mol2_file(molecule, mol_filename):
    # Create mol_filename directory:
    path = os.path.join(chunk_dir, mol_filename)
    os.mkdir(path)

    # create .mol2 file
    # print("Creating .mol2 file..")
    molecule = "@<TRIPOS>MOLECULE\n" + molecule

    file_name_path = os.path.join(path, f'{mol_filename}.mol2')
    mol_file = open(file_name_path, "w")
    mol_file.write(molecule)
    mol_file.close()

    # print("Done")


def call_dock_and_score(molecule, protein):
    molecule_name = molecule.split(".mol2")[0]

    # Change dir
    path = os.path.join(chunk_dir, molecule)
    os.chdir(path)

    # Call dock_and_score
    #bash_path = os.path.join(home_dir, "dock_and_score.bash")
    try:
        # os.system(f"dock_and_score_db.bash {molecule_name} {protein}")    # Local dock_and_score.bash
        os.system(f"/scratch/work/project/dd-20-11/bin/dock_and_score_mpi.bash {molecule_name} {protein}")
    except:
        exit()

def clean_files(mol_name):
    # Remove files
    os.system(f"rm -r {mol_name}.mol2 GeoDockPoses log  srcFiles xscoreOut")


def process_molecules(molecules, protein):
    for m in molecules:  # Parallelizable loop   # TODO Parallelize
        mol_name = m.partition("\n")[0]
        os.chdir(home_dir)
        create_mol2_file(m, f"ligand_{mol_name}")
        call_dock_and_score(f"ligand_{mol_name}", protein)
        clean_files(f"ligand_{mol_name}")


def extract_chunk_result():

    # Add scores to result file
    with open(result_file_path, 'a') as result:
        for subdir, dir, files in os.walk(chunk_dir):
            for f in files:
                if "bestPose_XScore" in f:
                    score_file = open(os.path.join(subdir, f), 'r')
                    try:
                        _, score = score_file.readline(), score_file.readline()
                        score = ",".join(score.replace("|", "").split())
                        result.write(score + "\n")
                    except:
                        pass
                    score_file.close()

    # Remove everything in chunk directory
    os.system(f"rm -r {chunk_dir}/*")


if __name__ == "__main__":
    # argvs
    db_path = sys.argv[1]
    protein = sys.argv[2]
    chunk_size = int(sys.argv[3])
    n_molecules = int(sys.argv[4])
    start_pos = int(sys.argv[5])
    end_pos = int(sys.argv[6])
    cpu_workers = int(sys.argv[7])    

    """
    assert (db_path != ""), "Error: insert a path to a database" # TODO Checkare se il path esiste e prendere il path assoluto
    assert (protein != ""), "Error: insert a protein to be tested" # TODO come sopra
    assert (isinstance(chunk_size, int)), "Error: Chunk_size must be a number"
    assert (isinstance(n_molecules, int)), "Error: Number of molecules must be a number" # TODO gestire il caso di voler leggere tutto il dataset

    
    n_molecules = 450  # Number of molecules that we want to process
    chunk_size = 100  # Chunk of molecules
    db_filename = "datasetMol2b/ligands.mol2"
    protein = "1c1b"
    """

    home_dir = os.getcwd()
    output_dir = os.path.join(home_dir, "output")

    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    chunk_dir = os.path.join(output_dir, "chunk")
    if not os.path.isdir(chunk_dir):
        os.mkdir(chunk_dir)

    # Create result_file
    db_name = db_path.split("/")[-1].replace('.mol2', '')
    protein_name = protein.split("/")[-1].replace('.mol2', '')
    result_filename = f"results_{db_name}_protein_{protein_name}.csv"
    result_file_path = os.path.join(output_dir, result_filename)
    if not os.path.isfile(result_file_path):
        with open(result_file_path, 'w') as res:
            #res.write("Rank,Formula,MW,LogP,HPScore,HMScore,HSScore,Average,Name\n") # Header
            pass

    # Make the bash script runnable
    # os.system("chmod u+x dock_and_score.bash")

    chunk_count = 0
    total_count = 0
    lines_read = ""

    # Read database
    db = open(db_path, 'r')
    db.seek(start_pos)

    with open(db_path, 'r') as db:
        db.seek(start_pos)

        while db.tell() != end_pos:
            line = db.readline()

            if line == "":   # EOF
                break

            if "@<TRIPOS>MOLECULE\n" == line:
                if total_count % chunk_size == 0:
                    print(f"Molecules read: {total_count}")

                chunk_count += 1
                total_count += 1

                if chunk_count - 1 == chunk_size or total_count - 1 == n_molecules:
                    
                    # Process molecules     
                    molecules = lines_read.split( "@<TRIPOS>MOLECULE\n")
                    molecules = molecules[1:]

                    # Processing
                    process_molecules(molecules, protein_name)

                    # Extract chunk results
                    extract_chunk_result()

                    # End Processing
                    lines_read = ""

                    if total_count - 1 == n_molecules:
                        break

                    chunk_count = 1

            lines_read += line

        # Process last molecules
        if lines_read != "":
            molecules = lines_read.split("@<TRIPOS>MOLECULE\n")
            molecules = molecules[1:]
            print(len(molecules))

            # Processing
            process_molecules(molecules, protein_name)

            # Extract chunk results
            extract_chunk_result()
            print(f"Molecules read: {total_count}")

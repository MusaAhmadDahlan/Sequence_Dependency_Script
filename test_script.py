import pandas as pd
import networkx as nx
import logging
import unittest
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO)

def read_csv_files(dependency_file, prog_name_file):
    try:
        df_jobs = pd.read_csv(dependency_file)
        jobs = list(df_jobs[['STEP_SEQ_ID', 'STEP_DEP_ID']].itertuples(index=False, name=None))
    except Exception as e:
        logging.error(f"Error reading dependency CSV file: {e}")
        raise

    try:
        df_sequence_names = pd.read_csv(prog_name_file)
        sequence_names = dict(df_sequence_names[['STEP_SEQ_ID', 'STEP_PROG_NAME']].values)
    except Exception as e:
        logging.error(f"Error reading sequence name CSV file: {e}")
        raise

    return jobs, sequence_names

def build_dependency_tree_networkx(jobs):
    G = nx.DiGraph()
    for sequence, dependency in jobs:
        G.add_edge(dependency, sequence)

    try:
        execution_order = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible as e:
        logging.error(f"The graph is not a DAG (contains cycles): {e}")
        raise ValueError("The graph is not a DAG (contains cycles).")

    return execution_order

def main(dependency_file, prog_name_file):
    try:
        jobs, sequence_names = read_csv_files(dependency_file, prog_name_file)
    except Exception as e:
        logging.error(f"Failed to read and process the CSV files: {e}")
        return

    try:
        execution_order = build_dependency_tree_networkx(jobs)
    except Exception as e:
        logging.error(f"Failed to determine execution order: {e}")
        return

    print("Order of Execution:")
    for job in execution_order:
        if job != 0:
            print(sequence_names[job])

class TestDependencyTree(unittest.TestCase):

    def setUp(self):
        # Create mock CSV data
        self.dependency_csv = BytesIO("""
                                    STEP_SEQ_ID,STEP_DEP_ID
                                    1,0
                                    2,1
                                    3,2
                                    4,2
                                    5,3
                                    5,4
                                    6,3
                                    """.strip().encode())

        self.prog_name_csv = BytesIO("""
                                    STEP_SEQ_ID,STEP_PROG_NAME
                                    1,PKGIDS_CMMN_UTILITY.PROCIDS_JOB_START
                                    2,pkgids_ptf_hrchy_processing.Procids_delete_job_set_nbr
                                    3,PKGIDS_PTF_EXTR.ext_static_ptf_table
                                    4,PKGIDS_PTF_EXTR.ext_eff_ptf_table
                                    5,pkgids_ptf_hrchy_processing.procids_get_tree_a
                                    6,pkgids_ptf_hrchy_processing.procids_get_tree_b
                                    """.strip().encode())

    def test_basic_execution_order_networkx(self):
        jobs, sequence_names = read_csv_files(self.dependency_csv, self.prog_name_csv)
        execution_order = build_dependency_tree_networkx(jobs)
        expected_orders = [
            [0, 1, 2, 3, 4, 5, 6],
            [0, 1, 2, 4, 3, 6, 5],
            [0, 1, 2, 4, 3, 5, 6],
            [0, 1, 2, 3, 4, 6, 5],
        ]
        self.assertIn(execution_order, expected_orders)

    def test_handling_cycles_networkx(self):
        jobs = [(1, 0), (2, 1), (3, 2), (1, 3)]
        with self.assertRaises(ValueError):
            build_dependency_tree_networkx(jobs)

    def test_multiple_roots_networkx(self):
        jobs = [(1, 0), (2, 0), (3, 1), (4, 2)]
        execution_order = build_dependency_tree_networkx(jobs)
        expected_orders = [
            [0, 1, 2, 3, 4],
            [0, 1, 3, 2, 4]
        ]
        self.assertIn(execution_order, expected_orders)

    def test_disconnected_components_networkx(self):
        jobs = [(1, 0), (2, 0), (4, 3), (5, 4)]
        execution_order = build_dependency_tree_networkx(jobs)
        expected_orders = [
            [0, 1, 2, 3, 4, 5],
            [0, 3, 1, 2, 4, 5]
        ]
        self.assertIn(execution_order, expected_orders)

if __name__ == '__main__':
    # Uncomment for unit test or
    # run in terminal -> python -m unittest test_script.py
    # unittest.main(exit=False)

    # execute the main function
    dependency_file = "DEPENDENCY_RULES.csv"
    prog_name_file = "PROG_NAME.csv"
    main(dependency_file, prog_name_file)

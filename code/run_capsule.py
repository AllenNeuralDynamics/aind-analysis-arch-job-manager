""" top level run script """

"""Temporarily put here. Will be move to job assignment script."""

import hashlib
import itertools
import json
import glob
import logging
import numpy as np
import os
import sys
from tqdm import tqdm
import random

from util.docDB_io import get_existing_job_hashes_from_docDB


from aind_dynamic_foraging_models.generative_model import ForagerCollection

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
LOCAL_NWB_ROOT = f"{SCRIPT_DIR}/../data/foraging_nwb_bonsai"

logger = logging.getLogger()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        #logging.FileHandler(f"{SCRIPT_DIR}/../results/run_capsule.log"),
        logging.StreamHandler(),
    ],
)
logger.addHandler(logging.StreamHandler())

def get_all_nwbs(nwb_root=LOCAL_NWB_ROOT):
    # Use glob to get all nwbs
    nwbs = glob.glob(f"{nwb_root}/*.nwb")
    logger.info(f"Found {len(nwbs)} nwbs")
    return [os.path.basename(nwb) for nwb in nwbs]

def get_all_analysis_specs():
    """Define analysis specs"""
    # -- All MLE agents from aind-dynamic-foraging-models --
    df_all_agents = ForagerCollection().get_all_foragers()
    analysis_specs = [
        {
            "analysis_name": "MLE fitting",
            "analysis_ver": "first version @ 0.10.0",
            "analysis_libs_to_track_ver": ["aind_dynamic_foraging_models"],
            "analysis_args": {
                "agent_class": agent_class,
                "agent_kwargs": agent_kwargs,
                "fit_kwargs": {
                    "DE_kwargs": {"polish": True, "seed": 42},
                    "k_fold_cross_validation": 10,
                },
            },
        }
        for agent_class, agent_kwargs, preset_name 
        in df_all_agents[["agent_class_name", "agent_kwargs", "preset_name"]].values
        if preset_name in ["Rescorla-Wagner", "Bari2019", "Hattori2019", "Win-Stay-Lose-Shift"]
    ]
    
    # -- TODO: Add more analysis specs here --
    
    logger.info(f"Found {len(analysis_specs)} analyses!")
    return analysis_specs


def generate_all_jobs() -> list:
    """Generate all possible job dictionaries."""
    nwbs = get_all_nwbs(LOCAL_NWB_ROOT)
    analysis_specs = get_all_analysis_specs()

    all_job_dicts = []

    # Generate all job_dicts by combining nwb and analysis_spec
    for nwb, analysis_spec in tqdm(
        itertools.product(nwbs, analysis_specs),
        desc="Generating all jobs",
        total=len(nwbs) * len(analysis_specs),
        ):
        job_dict = {
            "nwb_name": nwb,
            "analysis_spec": analysis_spec,
        }
        job_hash = hash_dict(json.dumps(job_dict))
        job_dict["job_hash"] = job_hash  # Add hash to job_dict
        all_job_dicts.append(job_dict)
        
    logger.info(f"Generated {len(all_job_dicts)} total jobs. {'-'*20}")
    return all_job_dicts


def assign_jobs(job_dicts, n_workers):
    """Assign jobs to pipeline workers by putting job jsons to folders under /root/capsule/results/
    
    Parameters:
        job_dicts (list): list of job_dicts
        n_workers (int): number of workers in the pipeline (specified in CO pipeline)
    """
    n_jobs = len(job_dicts)
    n_workers = np.min([n_workers, n_jobs])
    jobs_for_each_worker = np.array_split(job_dicts, n_workers)
    for n_worker, jobs_this in tqdm(
        enumerate(jobs_for_each_worker), 
        desc="Assigning jobs to workers",
        total=len(jobs_for_each_worker)
    ):
        os.makedirs(f"{SCRIPT_DIR}/../results/{n_worker}", exist_ok=True)
        for job_dict in jobs_this:
            with open(
                f"{SCRIPT_DIR}/../results/{n_worker}/{job_dict['job_hash']}.json", "w"
            ) as f:
                json.dump(job_dict, f, indent=4)
    logger.info(f"Assigned pending {n_jobs} jobs to {n_workers} workers. {'-'*20}")
    print(f"Assigned pending {n_jobs} jobs to {n_workers} workers.")  # Print to the console for CO pipeline run

def hash_dict(job_dict):
    return hashlib.sha256(job_dict.encode("utf-8")).hexdigest()


if __name__ == "__main__":

    # Parse input arguments
    import argparse

    # create a parser object
    parser = argparse.ArgumentParser()

    # add the corresponding parameters
    parser.add_argument('--n_workers', dest='n_workers')
    parser.add_argument('--if_random_job_order', dest='if_random_job_order')
    parser.add_argument('--max_jobs', dest='max_jobs')

    # return the data in the object and save in args
    args = parser.parse_args()
    print(args)
    
    if_random_job_order = bool(int(args.if_random_job_order or "1"))

    # -- Upload new jobs to docDB --
    all_job_dicts = generate_all_jobs()  # All jobs
    existing_job_hashes = get_existing_job_hashes_from_docDB()  # Existing jobs
    
    all_job_hashes = [job["job_hash"] for job in all_job_dicts]  # All job hashes
    
    # Remove jobs that are already in docDB
    new_job_hashes = list(set(all_job_hashes) - set(existing_job_hashes))
    new_job_dicts = [job for job in all_job_dicts if job["job_hash"] in new_job_hashes]
    n_skipped_jobs = len(all_job_dicts) - len(new_job_dicts)
    
    if new_job_dicts:
        if if_random_job_order:
            random.shuffle(new_job_dicts)
        
        job_dicts_to_assign = new_job_dicts[:int(args.max_jobs or "10")]
        assign_jobs(job_dicts_to_assign, n_workers=int(args.n_workers or "20"))
        
        logger.info(
            f"{n_skipped_jobs} already existed. {'-'*20}\n"
            f"Added {len(new_job_dicts)} new jobs from all {len(all_job_dicts)} jobs"
        )
    else:
        logger.info(f"No new jobs to assign. {'-'*20}")

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

from util.docDB_io import batch_get_new_jobs, batch_add_jobs_to_docDB, get_pending_jobs

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
LOCAL_NWB_ROOT = f"{SCRIPT_DIR}/../data/foraging_nwb_bonsai"

logger = logging.getLogger()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(f"{SCRIPT_DIR}/../results/run_capsule.log"),
        logging.StreamHandler(),
    ],
)
logger.addHandler(logging.StreamHandler())

def get_all_nwbs(nwb_root=LOCAL_NWB_ROOT):
    # Use glob to get all nwbs
    nwbs = glob.glob(f"{nwb_root}/*.nwb")
    return [os.path.basename(nwb) for nwb in nwbs]

def get_all_analysis_specs():
    # TODO: Get analysis specs from ForagerCollection
    analysis_specs = [
        {
            "analysis_name": "MLE fitting",
            "analysis_ver": "first version @ 0.10.0",
            "analysis_libs_to_track_ver": ["aind_dynamic_foraging_models"],
            "analysis_args": {
                "agent_class": "ForagerLossCounting",
                "agent_kwargs": {"win_stay_lose_switch": True, "choice_kernel": "none"},
                "fit_kwargs": {
                    "DE_kwargs": {"polish": True, "seed": 42},
                    "k_fold_cross_validation": 2,
                },
            },
        },
    ]
    return analysis_specs

def get_new_jobs() -> list:
    """Based on all nwb files, analysis specs, and existing jobs, generate new jobs.
    
    Returns:
        list: list of new_job_dicts
    """
    nwbs = get_all_nwbs(LOCAL_NWB_ROOT)
    analysis_specs = get_all_analysis_specs()

    all_job_dicts = []

    # -- Generate all job_dicts by combining nwb and analysis_spec --
    for nwb, analysis_spec in itertools.product(nwbs, analysis_specs):
        job_dict = {
            "nwb_name": nwb,
            "analysis_spec": analysis_spec,
        }
        job_hash = hash_dict(json.dumps(job_dict))
        job_dict["job_hash"] = job_hash  # Add hash to job_dict
        all_job_dicts.append(job_dict)
        
    # -- Batch check if job already exists on docDB --
    new_job_dicts = batch_get_new_jobs(all_job_dicts)
    n_existing = len(all_job_dicts) - len(new_job_dicts)
    logger.info(
        f"Found {len(new_job_dicts)} new jobs from all {len(all_job_dicts)} jobs; {n_existing} already existed."
    )
    return new_job_dicts

def assign_jobs(job_dicts, n_workers):
    """Assign jobs to pipeline workers by putting job jsons to folders under /root/capsule/results/
    
    Parameters:
        job_dicts (list): list of job_dicts
        n_workers (int): number of workers in the pipeline (specified in CO pipeline)
    """
    n_jobs = len(job_dicts)
    n_workers = np.min([n_workers, n_jobs])
    jobs_for_each_worker = np.array_split(job_dicts, n_workers)
    for n_worker, jobs_this in enumerate(jobs_for_each_worker):
        os.makedirs(f"{SCRIPT_DIR}/../results/{n_worker}", exist_ok=True)
        for job_dict in jobs_this:
            with open(
                f"{SCRIPT_DIR}/../results/{n_worker}/{job_dict['job_hash']}.json", "w"
            ) as f:
                json.dump(job_dict, f, indent=4)
    logger.info(f"Assigned pending {n_jobs} jobs to {n_workers} workers.")

def hash_dict(job_dict):
    return hashlib.sha256(job_dict.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    # -- Get all new jobs --
    new_job_dicts = get_new_jobs()
    
    if new_job_dicts:
        # -- Batch add new jobs to docDB --
        batch_add_jobs_to_docDB(new_job_dicts)
    else:
        logger.info("No new jobs to add to docDB.")
    
    # -- Trigger all pending jobs from docDB in the downstream pipeline --        
    pending_jobs = get_pending_jobs()  # Could be newly added jobs or existing pending jobs
    if pending_jobs:
        job_dicts = [job["job_dict"] for job in pending_jobs]
        
        try:
            n_workers = int(sys.argv[1])  # Number of workers defined in the pipeline
        except:
            n_workers = 10  # Default number of workers
            
        assign_jobs(job_dicts, n_workers)
    else:
        logger.info("No pending jobs to assign.")

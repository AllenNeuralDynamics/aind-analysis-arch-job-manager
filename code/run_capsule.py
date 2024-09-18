""" top level run script """

"""Temporarily put here. Will be move to job assignment script."""

import hashlib
import itertools
import json
import glob
from tqdm import tqdm
import logging

from util.docDB_io import check_if_job_exists, add_jobs_to_docDB

LOCAL_NWB_ROOT = "/root/capsule/data/foraging_nwb_bonsai"

logger = logging.getLogger()
logging.basicConfig(format="%(asctime)s - %(message)s", 
                    datefmt="%Y-%m-%d %H:%M:%S",
                    level=logging.INFO
)
logger.addHandler(logging.StreamHandler())

def get_all_nwbs(nwb_root=LOCAL_NWB_ROOT):
    # Use glob to get all nwbs
    return glob.glob(f"{nwb_root}/*.nwb")

def get_all_analysis_specs():
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

def generate_job_json():
    nwbs = get_all_nwbs(LOCAL_NWB_ROOT)
    analysis_specs = get_all_analysis_specs()

    n_job_specs_all = len(nwbs) * len(analysis_specs)
    n_already_exists = 0
    new_job_dicts = []

    for nwb, analysis_spec in tqdm(
        itertools.product(nwbs, analysis_specs),
        total=n_job_specs_all,
        desc="Generating job jsons",
    ):
        # -- Generate job_dict and compute hash --
        job_dict = {
            "nwb_name": nwb,
            "analysis_spec": analysis_spec,
        }
        job_hash = hash_dict(json.dumps(job_dict))

        # -- Check if the job_hash already exists on docDB --
        if check_if_job_exists(job_hash):
            n_already_exists += 1
            continue

        # -- If not, create a new record on docDB and write the job_dict to a json file --
        job_dict["job_hash"] = job_hash  # Add hash to job_dict
        new_job_dicts.append(job_dict)

        with open(f"/root/capsule/results/{job_hash}.json", "w") as f:
            json.dump(job_dict, f, indent=4)
        
    # -- Batch add new jobs to docDB --
    logger.info(f"Adding {len(new_job_dicts)} new jobs to docDB.")
    add_jobs_to_docDB(new_job_dicts)

    logger.info(
        f"Processed {n_job_specs_all} job specs, {len(new_job_dicts)} new jobs, {n_already_exists} already exist."
    )

def hash_dict(job_dict):
    return hashlib.sha256(job_dict.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    generate_job_json()

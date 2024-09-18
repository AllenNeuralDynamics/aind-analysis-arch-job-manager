""" top level run script """

"""Temporarily put here. Will be move to job assignment script."""

import hashlib
import itertools
import json
import glob
from tqdm import tqdm

LOCAL_NWB_ROOT = "/root/capsule/data/foraging_nwb_bonsai"

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
                "agent_class": "ForagerQLearning",
                "agent_kwargs": {
                    "number_of_learning_rate": 1,
                    "number_of_forget_rate": 1,
                    "choice_kernel": "one_step",
                    "action_selection": "softmax",
                },
                "fit_kwargs": {
                    "DE_kwargs": {"polish": True, "seed": 42},
                    "k_fold_cross_validation": 2,
                },
            },
        },
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
    
    for nwb, analysis_spec in tqdm(itertools.product(nwbs, analysis_specs), "generate job json"):
        # Generate job_dict and compute hash
        job_dict = {
            "nwb_name": nwb,
            "analysis_spec": analysis_spec,
        }
        job_hash = hash_dict(json.dumps(job_dict))
        job_dict["job_hash"] = job_hash  # Add hash to job_dict
        with open(f"/root/capsule/result/{job_hash}.json", "w") as f:
            json.dump(job_dict, f, indent=4)


def hash_dict(job_dict):
    return hashlib.sha256(job_dict.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    generate_job_json()
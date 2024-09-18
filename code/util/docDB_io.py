import logging
from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"
credentials.collection = "job_manager"

logger = logging.getLogger(__name__)

def check_if_job_exists(job_hash) -> bool:
    """Check if job_hash exists in the database

    Parameters
    ----------
    job_hash : _type_
        _description_
    
    Returns
    -------
    bool
    """
    with DocumentDbSSHClient(credentials) as client:
        response = client.collection.find({"job_hash": job_hash})

    return response is not None

def add_jobs_to_docDB(job_dicts):
    """Add all jobs in job_list to the database
    """
    with DocumentDbSSHClient(credentials) as client:
        # Update job status and log
        response = client.collection.insert_many(
            {
                "job_hash": job_dict["job_hash"],
                "job_dict": job_dict,
                "status": "pending",
            } for job_dict in job_dicts
        )
    
    return response

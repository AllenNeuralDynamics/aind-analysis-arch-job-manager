import logging
from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"
credentials.collection = "job_manager"

logger = logging.getLogger(__name__)

def get_pending_jobs(retry_failed, retry_running) -> list:
    """Get all pending jobs from the database
    """
    reg_ex = "pending"
    if retry_failed:
        reg_ex += "|failed"
    if retry_running:
        reg_ex += "|running"
    
    print(reg_ex)
    logger.info(f"Fetch {reg_ex} jobs")
    with DocumentDbSSHClient(credentials) as client:
        pending_jobs = list(
            client.collection.find({"status": {"$regex": reg_ex}}, {"job_dict": 1, "_id": 0})
        )
        # Update the status of those jobs to 'pending'
        client.collection.update_many(
            {"status": {"$regex": reg_ex}},  # Match the documents based on regex
            {"$set": {"status": "pending"}}  # Set the 'status' field to 'pending'
        )
    logger.info("Done!")
    return pending_jobs

def batch_get_new_jobs(job_dicts: list) -> list:
    """Remove existing jobs from job_dicts
    """
    job_hashs = [job_dict["job_hash"] for job_dict in job_dicts]

    logger.info("Fetch existing job hashs...")
    with DocumentDbSSHClient(credentials) as client:
        matched_records = client.collection.find(
            {"job_hash": {"$in": job_hashs}}, {"job_hash": 1, "_id": 0}
        )
        matched_job_hashs = [record["job_hash"] for record in matched_records]
    logger.info("Done!")

    return [
        job_dict for job_dict in job_dicts 
        if job_dict["job_hash"] not in matched_job_hashs
    ]

def batch_add_jobs_to_docDB(job_dicts):
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

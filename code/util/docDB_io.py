import logging
from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials
from pymongo import UpdateOne

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
    logger.info(f"Fetch {reg_ex} jobs from the database. {'-'*20}")
    with DocumentDbSSHClient(credentials) as client:
        pending_jobs = list(
            client.collection.find({"status": {"$regex": reg_ex}}, {"job_dict": 1, "_id": 0})
        )
        # Update the status of those jobs to 'pending'
        client.collection.update_many(
            {"status": {"$regex": reg_ex}},  # Match the documents based on regex
            {"$set": {"status": "pending"}}  # Set the 'status' field to 'pending'
        )
    logger.info("Fetch {reg_ex} jobs done! {len(pending_jobs)} found. {'-'*20}")
    return pending_jobs


def get_existing_job_hashes(batch_size=10000):
    """Retrieve all existing job hashes in batches."""
    existing_hashes = set()
    last_id = None

    with DocumentDbSSHClient(credentials) as client:
        while True:
            query = {}
            if last_id:
                query["_id"] = {"$gt": last_id}

            cursor = (
                client.collection.find(query, {"job_hash": 1})
                .sort("_id")
                .limit(batch_size)
            )
            batch = list(cursor)

            if not batch:
                break

            # Add the job hashes from this batch to the set
            for job in batch:
                existing_hashes.add(job['job_hash'])

            # Update last_id to the _id of the last document in this batch
            last_id = batch[-1]['_id']

    return existing_hashes


def batch_add_jobs_to_docDB(job_dicts):
    """Add all jobs in job_list to the database
    """
    logging.info(f"Adding {len(job_dicts)} jobs to the database. {'-'*20}")
    with DocumentDbSSHClient(credentials) as client:
        # Update job status and log
        response = client.collection.insert_many(
            {
                "job_hash": job_dict["job_hash"],
                "job_dict": job_dict,
                "status": "pending",
            } for job_dict in job_dicts
        )
    logging.info(f"Done! {'-'*20}")
    return response

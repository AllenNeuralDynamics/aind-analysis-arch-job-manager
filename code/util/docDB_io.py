import logging
from aind_data_access_api.document_db import MetadataDbClient

# Analysis db
analysis_docDB_dft = MetadataDbClient(
    host="api.allenneuraldynamics.org",
    database="analysis",
    collection="dynamic-foraging-analysis",
)

logger = logging.getLogger(__name__)

def get_existing_job_hashes_from_docDB():
    """Retrieve all existing job hashes in batches."""
    logger.info("Retrieving existing job hashes from docDB...")
    records = analysis_docDB_dft.retrieve_docdb_records(
        filter_query={},
        projection={"_id": 1},
        paginate=True,
        paginate_batch_size=10000,   
    )
    existing_hashes = [record["_id"] for record in records]
    logger.info(f"Found {len(existing_hashes)} existing job hashes in docDB.")
    return existing_hashes
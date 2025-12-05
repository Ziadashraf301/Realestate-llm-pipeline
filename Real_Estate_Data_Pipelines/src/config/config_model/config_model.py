from pydantic import BaseModel, Field

class RealEstateConfigModel(BaseModel):
    # BigQuery
    GCP_PROJECT_ID: str
    BQ_RAW_DATASET_ID: str
    BQ_RAW_TABLE_ID: str
    BQ_MART_DATASET_ID: str
    BQ_MART_TABLE_ID: str

    # Scraper settings
    MAX_PAGES: int = Field(gt=0, description="Max number of pages to scrape")
    LOG_DIR: str

    # Milvus vector DB
    MILVUS_HOST: str
    MILVUS_PORT: str
    VECTOR_BATCH_SIZE: int = Field(gt=0)

    # LLM Models
    EMBEDDING_MODEL: str
    GENERATION_MODEL: str
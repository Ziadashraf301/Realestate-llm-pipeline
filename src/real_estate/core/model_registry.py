"""
MLflow Model Registry & Artifact Resolver.
Loads versioned ONNX models and tokenizers from the MLflow Model Registry or local cache.
Strict production guarantees: zero mock random fallbacks, zero transformers imports.
"""

from pathlib import Path
from typing import Tuple
import mlflow
from real_estate.core.settings import settings
from real_estate.core.logger import logger


def resolve_model_artifacts(model_type: str) -> Tuple[Path, Path]:
    """
    Resolves ONNX model and Rust-backed tokenizer paths.
    First checks local filesystem cache; if not found, pulls the versioned artifact
    from the MLflow Model Registry.

    Args:
        model_type: 'embedding' or 'reranker'

    Returns:
        Tuple of (model_path, tokenizer_path)

    Raises:
        FileNotFoundError: If the model artifacts cannot be resolved from disk or MLflow.
    """
    if model_type == "embedding":
        local_model_path = Path(settings.ONNX_EMBEDDING_MODEL_PATH)
        local_tok_path = local_model_path.parent / "tokenizer.json"
        mlflow_model_name = "multilingual-e5-small-int8"
    elif model_type == "reranker":
        local_model_path = Path(settings.ONNX_RERANKER_MODEL_PATH)
        local_tok_path = local_model_path.parent / "reranker_tokenizer.json"
        mlflow_model_name = "bge-reranker-base-int8"
    else:
        raise ValueError(f"Unknown model_type: {model_type}")

    # 1. Local path check
    if local_model_path.exists() and local_tok_path.exists():
        logger.info(
            "model_artifacts_resolved_from_local_cache",
            model_type=model_type,
            model_path=str(local_model_path),
            tokenizer_path=str(local_tok_path)
        )
        return local_model_path, local_tok_path

    # 2. MLflow Model Registry download
    logger.info(
        "model_not_found_locally_attempting_mlflow_download",
        model_type=model_type,
        mlflow_tracking_uri=settings.MLFLOW_TRACKING_URI,
        model_name=mlflow_model_name
    )

    try:
        mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
        artifact_uri = f"models:/{mlflow_model_name}/Production"
        download_dir = Path(mlflow.artifacts.download_artifacts(artifact_uri=artifact_uri))

        downloaded_model = download_dir / local_model_path.name
        downloaded_tok = download_dir / local_tok_path.name

        if downloaded_model.exists() and downloaded_tok.exists():
            logger.info(
                "model_artifacts_downloaded_from_mlflow_registry",
                model_type=model_type,
                artifact_uri=artifact_uri
            )
            return downloaded_model, downloaded_tok

    except Exception as e:
        logger.warning(
            "mlflow_registry_download_failed",
            model_type=model_type,
            error=str(e)
        )

    # 3. If neither succeeds, raise an explicit, fail-fast configuration error
    raise FileNotFoundError(
        f"Production model artifacts for '{model_type}' not found! "
        f"Expected model at '{local_model_path}' and tokenizer at '{local_tok_path}', "
        f"or registered in MLflow at 'models:/{mlflow_model_name}/Production'. "
        f"Zero mock fallbacks are permitted in production."
    )

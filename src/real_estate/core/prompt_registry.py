"""
MLflow Prompt Registry & Universal External Artifact Loader.
Loads versioned prompt templates and few-shot examples from external YAML artifacts or MLflow server.
Strictly ZERO hardcoded prompt strings in Python code across the entire platform.
"""

from pathlib import Path
from typing import Dict, Any, List
import yaml
import mlflow
from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.schemas.intent import ExtractedQueryIntent

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


class MLflowPromptRegistry:
    """Manages prompt versions for Intent Extraction and Advisor Recommendation."""

    _cached_configs: Dict[str, Dict[str, Any]] = {}
    _cached_intent_prompt: str | None = None

    @classmethod
    def load_prompt_config(cls, prompt_name: str, yaml_filename: str) -> Dict[str, Any]:
        """
        Loads prompt configuration:
        1. Primary: Tries downloading registered prompt artifact from MLflow Model Registry.
        2. Fallback: Loads versioned YAML prompt file from local prompts directory.
        """
        if prompt_name in cls._cached_configs:
            return cls._cached_configs[prompt_name]

        # 1. Attempt MLflow Prompt Registry download
        try:
            mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
            mlflow_artifact = mlflow.artifacts.download_artifacts(
                artifact_uri=f"models:/{prompt_name}/Production/prompt.yaml"
            )
            with open(mlflow_artifact, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                cls._cached_configs[prompt_name] = config
                logger.info("prompt_loaded_from_mlflow_registry", prompt_name=prompt_name)
                return config
        except Exception:
            pass

        # 2. Fallback to versioned YAML file
        local_file = PROMPTS_DIR / yaml_filename
        if local_file.exists():
            with open(local_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                cls._cached_configs[prompt_name] = config
                logger.info("prompt_loaded_from_yaml_fallback", prompt_name=prompt_name, file=str(local_file))
                return config

        raise FileNotFoundError(
            f"Prompt artifact '{prompt_name}' not found in MLflow or at '{local_file}'!"
        )

    # -------------------------------------------------------------------------
    # 1. Intent Extraction Prompt
    # -------------------------------------------------------------------------
    @classmethod
    def get_intent_prompt(cls) -> str:
        """Assembles intent extraction prompt with system template, few-shots, and Milvus JSON schema."""
        if cls._cached_intent_prompt:
            return cls._cached_intent_prompt

        config = cls.load_prompt_config(
            prompt_name=settings.MLFLOW_INTENT_PROMPT_NAME,
            yaml_filename="intent_extraction.yaml"
        )
        system_template = config.get("system_template", "").strip()
        few_shots = config.get("few_shot_examples", [])

        examples_text = ""
        for idx, ex in enumerate(few_shots, 1):
            examples_text += f"\n[مثال {idx}]:\nطلب العميل: \"{ex['query']}\"\nالمخرجات المتطابقة: {ex['intent']}\n"

        schema_json = ExtractedQueryIntent.model_json_schema()

        assembled = (
            f"{system_template}\n\n"
            f"أمثلة توجيهية (Few-Shot Prompting):\n{examples_text}\n\n"
            f"يجب أن تكون مخرجاتك كائن JSON فقط مطابق لمخطط Pydantic التالي:\n{schema_json}"
        )

        cls._cached_intent_prompt = assembled
        return cls._cached_intent_prompt

    @classmethod
    def get_few_shot_examples(cls) -> List[Dict[str, Any]]:
        """Returns intent few-shot training examples."""
        config = cls.load_prompt_config(
            prompt_name=settings.MLFLOW_INTENT_PROMPT_NAME,
            yaml_filename="intent_extraction.yaml"
        )
        return config.get("few_shot_examples", [])

    # -------------------------------------------------------------------------
    # 2. Advisor Recommendation Generation Prompt
    # -------------------------------------------------------------------------
    @classmethod
    def get_advisor_system_instruction(cls) -> str:
        """Returns the system role instruction for llama.cpp and Gemini."""
        config = cls.load_prompt_config(
            prompt_name=settings.MLFLOW_ADVISOR_PROMPT_NAME,
            yaml_filename="advisor_recommendation.yaml"
        )
        return config.get("system_instruction", "أنت مستشار عقاري مصري خبير ومحايد ومعتمد.").strip()

    @classmethod
    def build_advisor_user_prompt(cls, query: str, context_text: str) -> str:
        """Formats the grounded recommendation user prompt using the external template."""
        config = cls.load_prompt_config(
            prompt_name=settings.MLFLOW_ADVISOR_PROMPT_NAME,
            yaml_filename="advisor_recommendation.yaml"
        )
        user_template = config.get("user_template", "").strip()
        return user_template.format(query=query, context_text=context_text)


# Backward-compatible alias
FEW_SHOT_INTENT_EXAMPLES = MLflowPromptRegistry.get_few_shot_examples()

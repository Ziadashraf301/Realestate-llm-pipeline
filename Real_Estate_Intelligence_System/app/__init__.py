from flask import Flask
from app.routes import register_routes
from Real_Estate_Data_Pipelines.src.config import config
from app.services.search_service import initialize_embedding_model
from app.services.generation_service import configure_gemini

def create_app():
    app = Flask(__name__)

    # Initialize services
    initialize_embedding_model(config)
    configure_gemini(config)

    # Register blueprints
    register_routes(app)

    return app

from flask import Flask
from Real_Estate_Intelligence_System.app.routes import register_routes
from Real_Estate_Data_Pipelines.src.config import config
from Real_Estate_Intelligence_System.app.services.search_service import initialize_embedding_model
from Real_Estate_Intelligence_System.app.services.generation_service import configure_gemini

def create_app():
    
    app = Flask(__name__)

    # Initialize services
    initialize_embedding_model(config)
    configure_gemini(config)

    # Register blueprints
    register_routes(app)

    return app

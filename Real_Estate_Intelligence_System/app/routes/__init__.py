from Real_Estate_Intelligence_System.app.routes.search_routes import search_bp
from Real_Estate_Intelligence_System.app.routes.generate_routes import generate_bp

def register_routes(app):
    app.register_blueprint(search_bp)
    app.register_blueprint(generate_bp)

    @app.route('/')
    def index():
        from flask import render_template
        return render_template('ui.html')

    @app.route('/health')
    def health():
        return {"status": "ok"}

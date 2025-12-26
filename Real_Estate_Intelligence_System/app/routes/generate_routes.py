from flask import Blueprint, request, jsonify
from Real_Estate_Intelligence_System.app.services.generation_service import generate_summary_gemini

generate_bp = Blueprint('generate_bp', __name__)

@generate_bp.route('/generate', methods=['POST'])
def generate_summary():
    data = request.json
    query = data.get('query', '')
    properties = data.get('properties', [])
    try:
        summary = generate_summary_gemini(query, properties)
        return jsonify({'success': True, 'summary': summary})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
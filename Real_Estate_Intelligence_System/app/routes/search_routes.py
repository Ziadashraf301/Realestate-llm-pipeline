from flask import Blueprint, request, jsonify
from app.services.search_service import search_properties, get_collection_count

search_bp = Blueprint('search_bp', __name__)

@search_bp.route('/search', methods=['POST'])
def search():
    data = request.json
    try:
        results = search_properties(data)
        return jsonify({'success': True, 'results': results, 'count': len(results)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@search_bp.route('/stats', methods=['GET'])
def stats():
    try:
        count = get_collection_count()
        return jsonify({'success': True, 'total_properties': count})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
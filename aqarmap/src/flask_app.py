"""
Flask Web UI for Real Estate Vector Search + Gemini Summarization (Arabic fields)
"""

from flask import Flask, render_template, request, jsonify
from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer
import tensorflow as tf
tf.get_logger().setLevel('ERROR')
import google.generativeai as genai
from markdown import markdown
import os
from pathlib import Path

app = Flask(__name__)

# =========================
# Configuration
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / 'config'

# Load Gemini API key from config or environment
try:
    import json
    config_file = CONFIG_DIR / 'api_config.json'
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
            GEMINI_API_KEY = config.get('GEMINI_API_KEY')
    else:
        GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
except Exception as e:
    print(f"⚠️ Warning: Could not load API config: {e}")
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

if not GEMINI_API_KEY:
    raise ValueError("❌ GEMINI_API_KEY not found. Set it in config/api_config.json or as environment variable")

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)

# Load Table Configuration
TABLE_CONFIG_PATH = CONFIG_DIR / 'table_config.json'
try:
    with open(TABLE_CONFIG_PATH, 'r', encoding='utf-8') as f:
        table_config = json.load(f)
    print(f"✅ Table config loaded from: {TABLE_CONFIG_PATH}")

    EMBEDDING_MODEL = table_config.get('EMBEDDING_MODEL')

except FileNotFoundError:
    print(f"⚠️ WARNING: table_config.json not found at {TABLE_CONFIG_PATH}")
    print("   Using default fallback values...")
    EMBEDDING_MODEL = 'paraphrase-multilingual-MiniLM-L12-v2'

# Validate Configuration
if not all([EMBEDDING_MODEL]):
    raise ValueError("❌ Missing required configuration values in table_config.json")

os.environ['EMBEDDING_MODEL'] = EMBEDDING_MODEL


# =========================
# Global variables
# =========================
model = None
collection = None


def initialize():
    """Initialize embedding model and Milvus connection"""
    global model, collection

    print("🚀 Loading embedding model...")
    model = SentenceTransformer(EMBEDDING_MODEL)

    print("🔌 Connecting to Milvus...")
    connections.connect(alias="default", host='localhost', port='19530')

    collection = Collection("real_estate_properties")
    collection.load()
    print("✅ Initialization complete!")


# =========================
# Routes
# =========================
@app.route('/')
def index():
    """Render Arabic search UI"""
    return render_template('ui.html')


@app.route('/search', methods=['POST'])
def search():
    """Perform vector search with optional filters"""
    try:
        data = request.json
        query = data.get('query', '')
        n_results = data.get('n_results', 10)

        query = query.strip()

        listing_type = data.get('listing_type') 
        location = data.get('location')
        min_price = data.get('min_price')
        max_price = data.get('max_price')
        min_bedrooms = data.get('min_bedrooms')

        # Build Milvus filter expression
        filters = []
        if listing_type:
            filters.append(f'listing_type == "{listing_type}"')
        if min_price:
            filters.append(f'price_egp >= {min_price}')
        if max_price:
            filters.append(f'price_egp <= {max_price}')

        if location:
            location_map = {
                'alexandria': 'alexandria',
                'الإسكندرية': 'alexandria',
                'اسكندرية': 'alexandria',
                'الاسكندرية': 'alexandria',
                'cairo': 'cairo', 
                'القاهرة': 'cairo',
                'قاهرة': 'cairo'
            }
            normalized_location = location_map.get(location.lower(), location)
            filters.append(f'location == "{normalized_location}"')

        if min_bedrooms:
            filters.append(f'bedrooms >= {min_bedrooms}')

        filter_expr = ' && '.join(filters) if filters else None
        print("🧩 Filter expression:", filter_expr)

        # Generate query embedding
        query_embedding = model.encode(query, convert_to_numpy=True, normalize_embeddings=True)

        # Milvus vector search
        search_params = {
            "metric_type": "COSINE",
            "params": {"nprobe": 64}
        }

        results = collection.search(
            data=[query_embedding.tolist()],
            anns_field="embedding",
            param=search_params,
            limit=n_results * 2,
            expr=filter_expr,
            output_fields=[
                "property_id", "title", "location", "property_type",
                "listing_type", "price_egp", "bedrooms", "bathrooms",
                "area_sqm", "url", "text"
            ]
        )

        formatted_results = []
        for hit in results[0]:
            entity = hit.entity
            similarity = max(0, 1 - hit.distance)

            formatted_results.append({
                'property_id': entity.get('property_id'),
                'title': entity.get('title'),
                'location': entity.get('location'),
                'property_type': entity.get('property_type'),
                'listing_type': entity.get('listing_type'),
                'price_egp': entity.get('price_egp'),
                'bedrooms': entity.get('bedrooms'),
                'bathrooms': entity.get('bathrooms'),
                'area_sqm': entity.get('area_sqm'),
                'url': entity.get('url'),
                'text': entity.get('text'),
                'distance': hit.distance,
                'similarity': round(similarity, 3)
            })

        formatted_results = formatted_results[:n_results]

        return jsonify({'success': True, 'results': formatted_results, 'count': len(formatted_results)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/generate', methods=['POST'])
def generate_summary():
    """Generate conversational AI summary using Gemini"""
    try:
        data = request.json
        query = data.get('query', '')
        properties = data.get('properties', [])
        
        if not properties:
            return jsonify({'success': False, 'error': 'No properties provided'}), 400

        # Format top 20 properties for better context
        top_properties = properties[:20]
        context_text = "\n\n".join([
            f"العقار {i+1} (تطابق {p['similarity']*100:.1f}%):\n"
            f"• العنوان: {p['title']}\n"
            f"• الموقع: {p['location']}\n"
            f"• السعر: {p['price_egp']:,} جنيه\n"
            f"• الغرف: {p.get('bedrooms', 'غير محدد')} | الحمامات: {p.get('bathrooms', 'غير محدد')} | المساحة: {p.get('area_sqm', 'غير محدد')} م²\n"
            f"• النوع: {p['listing_type']} - {p['property_type']}\n"
            f"• الوصف: {p.get('text', 'لا يوجد')[:450]}..."
            for i, p in enumerate(top_properties)
        ])

        # Conversational prompt - more natural and helpful
        prompt = f"""أنت مستشار عقاري محترف وودود. عميل يبحث عن عقار وقال لك:
        
        "{query}"

        وجدت له أفضل 3 عقارات مطابقة:

        {context_text}

        الآن تحدث معه بطريقة طبيعية وودية:

        1. ابدأ بجملة ترحيبية قصيرة وأخبره أنك وجدت له خيارات مناسبة.

        2. ناقش أفضل 2-3 عقارات بأسلوب سردي طبيعي. لكل عقار:
        - اذكر أهم مميزاته
        - أشر إلى أي عيوب أو نقاط يجب الانتباه لها
        - اشرح لماذا قد يكون مناسباً أو غير مناسب له

        3. أعطه نصيحة عملية واحدة أو اثنتين مفيدة.

        **مهم جداً**: 
        - تحدث بأسلوب محادثة طبيعي كأنك تجلس معه في مكتبك
        - لا تستخدم قوائم نقطية أو جداول أو أرقام
        - اكتب فقرات قصيرة ومتصلة
        - كن صريحاً وواقعياً
        - استخدم رموز تعبيرية (emojis) بشكل بسيط لجعل النص أكثر حيوية

        اجعل ردك قصيراً (200-300 كلمة فقط) ومباشراً."""

        # Use Gemini 2.0 Flash for faster responses
        model_ai = genai.GenerativeModel("gemini-2.0-flash-exp")
        
        response = model_ai.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.8,  
                max_output_tokens=800,  
            )
        )
        
        summary = response.text
        
        # Convert markdown to HTML for better formatting
        summary = markdown(summary)
        
        return jsonify({'success': True, 'summary': summary})

    except Exception as e:
        print(f"Error generating summary: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/stats')
def stats():
    """Return Milvus collection stats"""
    try:
        count = collection.num_entities
        return jsonify({'success': True, 'total_properties': count})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    initialize()
    app.run(debug=True, host='0.0.0.0', port=5000)
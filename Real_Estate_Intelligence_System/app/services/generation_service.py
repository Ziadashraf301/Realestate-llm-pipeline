import os
import google.generativeai as genai
from markdown import markdown

def configure_gemini(cfg):
    GEMINI_API_KEY = cfg.GEMINI_API_KEY or os.getenv("GEMINI_API_KEY")
    if not GEMINI_API_KEY:
        raise ValueError("❌ GEMINI_API_KEY not found")
    genai.configure(api_key=GEMINI_API_KEY)

def generate_summary_gemini(query, properties):
    if not properties:
        return "لا توجد خصائص لتوليد الملخص"
    
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
    
    # Conversational prompt
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

    model_ai = genai.GenerativeModel("gemini-2.0-flash")
    response = model_ai.generate_content(
        prompt,
        generation_config=genai.types.GenerationConfig(
            temperature=0.8,
            max_output_tokens=800
        )
    )

    return markdown(response.text)

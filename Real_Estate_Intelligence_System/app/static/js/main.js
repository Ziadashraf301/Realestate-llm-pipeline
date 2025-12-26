async function loadStats() {
    try {
        const response = await fetch('/stats');
        const data = await response.json();
        if (data.success) {
            document.getElementById('total-properties').textContent = data.total_properties.toLocaleString();
        }
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

async function searchProperties() {
    const query = document.getElementById('search-query').value;
    if (!query.trim()) {
        alert('الرجاء إدخال نص البحث');
        return;
    }

    const resultsSection = document.getElementById('results-section');
    const searchBtn = document.getElementById('search-btn');

    searchBtn.disabled = true;
    searchBtn.textContent = '⏳ جاري البحث...';
    resultsSection.innerHTML = '<div class="loading">⏳ جاري البحث في قاعدة البيانات...</div>';

    const searchData = {
        query,
        n_results: parseInt(document.getElementById('n-results').value),
        listing_type: document.getElementById('listing-type').value || null,
        location: document.getElementById('location').value || null,
        min_price: document.getElementById('min-price').value ? parseFloat(document.getElementById('min-price').value) : null,
        max_price: document.getElementById('max-price').value ? parseFloat(document.getElementById('max-price').value) : null,
        min_bedrooms: document.getElementById('min-bedrooms').value ? parseInt(document.getElementById('min-bedrooms').value) : null
    };

    try {
        const response = await fetch('/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(searchData)
        });

        const data = await response.json();
        if (data.success) {
            document.getElementById('search-count').textContent = data.count;

            // Sort by similarity (highest first)
            const sortedResults = data.results.sort((a, b) => b.similarity - a.similarity);

            // Show AI summary first, then results
            await displayResultsWithSummary(query, sortedResults);
        } else {
            resultsSection.innerHTML = `<div class="no-results">❌ خطأ: ${data.error}</div>`;
        }
    } catch (error) {
        resultsSection.innerHTML = `<div class="no-results">❌ خطأ في الاتصال: ${error.message}</div>`;
    } finally {
        searchBtn.disabled = false;
        searchBtn.textContent = '🔍 بحث';
    }
}

async function displayResultsWithSummary(query, results) {
    const resultsSection = document.getElementById('results-section');

    if (results.length === 0) {
        resultsSection.innerHTML = `<div class="no-results">😔 لا توجد نتائج مطابقة</div>`;
        return;
    }

    // Create summary placeholder first
    const summaryBox = document.createElement('div');
    summaryBox.className = 'summary-box';
    summaryBox.innerHTML = `
        <h3>
            <span>🤖</span>
            <span>تحليل ذكي من Gemini AI</span>
        </h3>
        <div class="content">
            <div class="summary-loading">
                <div class="spinner"></div>
                <span>جاري تحليل النتائج وتقديم توصيات مخصصة...</span>
            </div>
        </div>
    `;

    resultsSection.innerHTML = '';
    resultsSection.appendChild(summaryBox);

    // Add results header
    const resultsHeader = document.createElement('div');
    resultsHeader.className = 'results-header';
    resultsHeader.innerHTML = `
        <h2>📊 النتائج المطابقة (${results.length})</h2>
        <div class="sort-info">مرتبة حسب درجة التطابق ⬇️</div>
    `;
    resultsSection.appendChild(resultsHeader);

    // Display property cards
    results.forEach((property, index) => {
        const card = createPropertyCard(property, index + 1);
        resultsSection.appendChild(card);
    });

    // Generate AI summary (async)
    generateSummary(query, results, summaryBox);
}

async function generateSummary(query, results, summaryBox) {
    try {
        const response = await fetch('/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query, properties: results })
        });

        const data = await response.json();
        if (data.success) {
            summaryBox.querySelector('.content').innerHTML = data.summary;
        } else {
            summaryBox.querySelector('.content').innerHTML = `
                <p style="color: #dc3545;">⚠️ فشل توليد التحليل: ${data.error}</p>
            `;
        }
    } catch (err) {
        summaryBox.querySelector('.content').innerHTML = `
            <p style="color: #dc3545;">⚠️ خطأ أثناء الاتصال بالذكاء الاصطناعي: ${err.message}</p>
        `;
    }
}

function createPropertyCard(property, rank) {
    const card = document.createElement('div');
    card.className = 'property-card';

    const similarityClass = property.similarity >= 0.7 ? 'high' :
        property.similarity >= 0.5 ? 'medium' : 'low';

    card.innerHTML = `
        <div class="property-rank">${rank}</div>

        <div class="property-header">
            <div class="property-title">
                ${property.title || 'عقار بدون عنوان'}
                <div class="similarity-badge ${similarityClass}">
                    ⭐ تطابق ${(property.similarity * 100).toFixed(1)}%
                </div>
            </div>
            <div class="property-price">
                ${property.price_egp ? property.price_egp.toLocaleString() : '0'} جنيه
            </div>
        </div>

        <div class="property-details">
            <div class="detail-item">🏠 ${property.property_type || 'غير محدد'}</div>
            <div class="detail-item">📋 ${property.listing_type || 'غير محدد'}</div>
            ${property.bedrooms ? `<div class="detail-item">🛏️ ${property.bedrooms} غرف</div>` : ''}
            ${property.bathrooms ? `<div class="detail-item">🚿 ${property.bathrooms} حمام</div>` : ''}
            ${property.area_sqm ? `<div class="detail-item">📐 ${property.area_sqm} م²</div>` : ''}
        </div>

        <div class="property-description">
            ${property.text ? property.text.substring(0, 200) + '...' : 'لا يوجد وصف'}
        </div>

        <div class="property-footer">
            <div class="property-location">
                <span>📍</span>
                <span>${property.location || 'غير محدد'}</span>
            </div>
            ${property.url ? `<a href="${property.url}" target="_blank" class="property-link">عرض التفاصيل ←</a>` : ''}
        </div>
    `;

    return card;
}

document.getElementById('search-query').addEventListener('keypress', e => {
    if (e.key === 'Enter') searchProperties();
});

loadStats();
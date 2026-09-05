/**
 * Master Discovery Client & State Manager.
 * Orchestrates JWT Session Guard, Unified Agentic RAG Calls,
 * Intent Chips Rendering, Cache Telemetry, and Favorites Persistence.
 */

document.addEventListener('DOMContentLoaded', () => {
    // 1. Auth Guard
    const token = localStorage.getItem('re_token');
    if (!token) {
        window.location.href = '/login';
        return;
    }

    // Set user profile in header
    const rawUser = localStorage.getItem('re_user');
    if (rawUser) {
        try {
            const user = JSON.parse(rawUser);
            document.getElementById('username-val').textContent = `${user.username} (${user.role})`;
        } catch (e) {}
    }

    // Logout Action
    document.getElementById('btn-logout').addEventListener('click', () => {
        localStorage.removeItem('re_token');
        localStorage.removeItem('re_user');
        window.location.href = '/login';
    });

    // Elements
    const queryInput = document.getElementById('query-input');
    const searchBtn = document.getElementById('search-btn');
    const bypassCheckbox = document.getElementById('bypass-cache-checkbox');
    const loadingSpinner = document.getElementById('loading-spinner');
    const advisorSection = document.getElementById('advisor-section');
    const advisorHtml = document.getElementById('advisor-html');
    const cacheTierBadge = document.getElementById('cache-tier-badge');
    const resultsSection = document.getElementById('results-section');
    const resultsCount = document.getElementById('results-count');
    const latencyVal = document.getElementById('latency-val');
    const propertiesGrid = document.getElementById('properties-grid');
    const intentContainer = document.getElementById('intent-chips-container');
    const chipsList = document.getElementById('chips-list');

    // Drawer Elements
    const btnOpenFavorites = document.getElementById('btn-open-favorites');
    const btnCloseDrawer = document.getElementById('btn-close-drawer');
    const drawerOverlay = document.getElementById('drawer-overlay');
    const favoritesList = document.getElementById('favorites-list');
    const favoritesCount = document.getElementById('favorites-count');

    // Favorites State
    let favorites = JSON.parse(localStorage.getItem('re_favorites') || '[]');
    updateFavoritesBadge();

    // Favorites Drawer Toggle
    btnOpenFavorites.addEventListener('click', () => {
        renderFavoritesDrawer();
        drawerOverlay.classList.add('open');
    });

    btnCloseDrawer.addEventListener('click', () => {
        drawerOverlay.classList.remove('open');
    });

    drawerOverlay.addEventListener('click', (e) => {
        if (e.target === drawerOverlay) {
            drawerOverlay.classList.remove('open');
        }
    });

    // Suggestions Click Handlers
    document.querySelectorAll('.quick-pill, .suggestion-pill').forEach(pill => {
        pill.addEventListener('click', () => {
            queryInput.value = pill.dataset.query;
            executeSearch();
        });
    });

    searchBtn.addEventListener('click', executeSearch);
    queryInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') executeSearch();
    });

    async function executeSearch() {
        const query = queryInput.value.trim();
        if (!query) return;

        // UI Loading State
        loadingSpinner.style.display = 'block';
        advisorSection.classList.add('hidden');
        resultsSection.classList.add('hidden');
        intentContainer.classList.add('hidden');
        chipsList.innerHTML = '';
        searchBtn.disabled = true;

        try {
            const resp = await fetch('/api/v1/rag', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${token}`
                },
                body: JSON.stringify({
                    query: query,
                    n_results: 10,
                    bypass_cache: bypassCheckbox ? bypassCheckbox.checked : false
                })
            });

            if (resp.status === 401) {
                // Session expired or invalid
                localStorage.removeItem('re_token');
                window.location.href = '/login';
                return;
            }

            let data;
            try {
                data = await resp.json();
            } catch (e) {
                data = { detail: resp.statusText || 'خطأ في معالجة الاستجابة' };
            }

            if (resp.ok && data.success) {
                renderIntentChips(data.intent);
                renderCacheTelemetry(data);
                renderAdvisorRecommendation(data.recommendation);
                renderPropertyResults(data.properties, data.latency_ms);
            } else {
                const errMsg = typeof data.detail === 'string' ? data.detail : (data.message || JSON.stringify(data.detail) || 'حدث خطأ أثناء معالجة البحث.');
                alert(`⚠️ ${errMsg}`);
            }
        } catch (err) {
            console.error('Search error:', err);
            alert(`تعذر الاتصال بالخادم: ${err.message || 'يرجى التأكد من تشغيل السيرفر'}`);
        } finally {
            loadingSpinner.style.display = 'none';
            searchBtn.disabled = false;
        }
    }

    function renderIntentChips(intent) {
        if (!intent) return;
        chipsList.innerHTML = '';
        let hasChips = false;

        const cityLabels = { 'alexandria': 'الإسكندرية', 'cairo': 'القاهرة', 'giza': 'الجيزة' };
        const typeLabels = { 'Apartment': 'شقة', 'Villa': 'فيلا', 'Duplex': 'دوبلكس', 'Chalet': 'شاليه', 'Studio': 'استوديو' };

        if (intent.city) {
            const displayCity = cityLabels[intent.city.toLowerCase()] || intent.city;
            chipsList.innerHTML += `<span class="criteria-chip">🏙️ ${displayCity}</span>`;
            hasChips = true;
        }
        if (intent.district) {
            chipsList.innerHTML += `<span class="criteria-chip">📍 ${intent.district}</span>`;
            hasChips = true;
        }
        if (intent.listing_type) {
            const listType = intent.listing_type === 'Rent' || intent.listing_type === 'ايجار' ? 'إيجار' : 'للبيع';
            chipsList.innerHTML += `<span class="criteria-chip">🏷️ ${listType}</span>`;
            hasChips = true;
        }
        if (intent.property_type) {
            const propType = typeLabels[intent.property_type] || intent.property_type;
            chipsList.innerHTML += `<span class="criteria-chip">🏠 ${propType}</span>`;
            hasChips = true;
        }
        if (intent.max_price && Number(intent.max_price) > 0) {
            chipsList.innerHTML += `<span class="criteria-chip">💰 حتى ${Number(intent.max_price).toLocaleString()} ج.م</span>`;
            hasChips = true;
        }
        if (intent.min_bedrooms && Number(intent.min_bedrooms) > 0) {
            chipsList.innerHTML += `<span class="criteria-chip">🛏️ ${intent.min_bedrooms} غرف+</span>`;
            hasChips = true;
        }

        if (hasChips) {
            intentContainer.classList.remove('hidden');
        }
    }

    function renderCacheTelemetry(data) {
        // Technical cache telemetry hidden from end-user UI for clean luxury experience
    }

    function renderAdvisorRecommendation(htmlContent) {
        if (htmlContent) {
            advisorHtml.innerHTML = htmlContent;
            advisorSection.classList.remove('hidden');
        }
    }

    function renderPropertyResults(properties, latencyMs) {
        resultsCount.textContent = properties.length;
        if (latencyVal) latencyVal.textContent = '';
        propertiesGrid.innerHTML = '';

        if (!properties || properties.length === 0) {
            propertiesGrid.innerHTML = '<p style="color: var(--text-muted); grid-column: 1/-1; text-align: center; padding: 40px 0;">لم يتم العثور على عقارات مطابقة حالياً.. جرب البحث بمواصفات أخرى.</p>';
            resultsSection.classList.remove('hidden');
            return;
        }

        const icons = ['🏢', '🏡', '🏖️', '🏠', '🏘️'];

        properties.forEach((p, index) => {
            const isFav = favorites.some(f => f.id === p.id);
            const icon = icons[index % icons.length];
            const priceText = Number(p.price_egp) > 0 ? `${Number(p.price_egp).toLocaleString()} ج.م` : 'السعر عند الاستفسار';
            const listingText = (p.listing_type === 'Rent' || p.listing_type === 'ايجار') ? 'للإيجار' : 'للبيع';

            const card = document.createElement('div');
            card.className = 'property-card';
            card.innerHTML = `
                <div class="card-img-placeholder">
                    <span>${icon}</span>
                    <span class="badge-type">${escapeHtml(p.property_type)} • ${listingText}</span>
                    <button class="fav-btn-card ${isFav ? 'active' : ''}" data-id="${p.id}" title="إضافة للمفضلة">
                        ${isFav ? '❤️' : '🤍'}
                    </button>
                </div>

                <div class="card-body">
                    <div class="property-price">${priceText}</div>
                    <h4 class="property-title">${escapeHtml(p.title)}</h4>
                    
                    <div class="property-location">
                        <span>📍</span> ${escapeHtml(p.location)}
                    </div>

                    <div class="property-features">
                        ${p.bedrooms ? `<div class="feature-item"><span>🛏️</span> ${p.bedrooms} غرف</div>` : ''}
                        ${p.bathrooms ? `<div class="feature-item"><span>🚿</span> ${p.bathrooms} حمام</div>` : ''}
                        ${p.area_sqm ? `<div class="feature-item"><span>📐</span> ${p.area_sqm} م²</div>` : ''}
                    </div>

                    <p class="property-desc">${escapeHtml(p.description || 'عقار مميز بموقع متميز ومواصفات عالية الجودة.')}</p>

                    <div class="card-actions">
                        ${p.url ? `<a href="${p.url}" target="_blank" rel="noopener noreferrer" class="btn-view-details">عرض التفاصيل الكاملة ←</a>` : '<span></span>'}
                    </div>
                </div>
            `;

            const favBtn = card.querySelector('.fav-btn-card');
            favBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                toggleFavorite(p, favBtn);
            });

            propertiesGrid.appendChild(card);
        });

        resultsSection.classList.remove('hidden');
    }

    function toggleFavorite(property, btnElement) {
        const index = favorites.findIndex(f => f.id === property.id);
        if (index > -1) {
            favorites.splice(index, 1);
            btnElement.classList.remove('active');
            btnElement.textContent = '🤍';
        } else {
            favorites.push(property);
            btnElement.classList.add('active');
            btnElement.textContent = '❤️';
        }
        localStorage.setItem('re_favorites', JSON.stringify(favorites));
        updateFavoritesBadge();
    }

    function updateFavoritesBadge() {
        if (favoritesCount) favoritesCount.textContent = favorites.length;
    }

    function renderFavoritesDrawer() {
        favoritesList.innerHTML = '';
        if (favorites.length === 0) {
            favoritesList.innerHTML = '<p style="color: var(--text-muted); text-align: center; margin-top: 40px;">لا توجد عقارات في المفضلة حالياً.</p>';
            return;
        }

        favorites.forEach(item => {
            const el = document.createElement('div');
            el.className = 'fav-item';
            el.innerHTML = `
                <button class="fav-remove" data-id="${item.id}" title="إزالة">✕</button>
                <h4>${escapeHtml(item.title)}</h4>
                <div class="fav-price">${Number(item.price_egp).toLocaleString()} ج.م</div>
                <div style="font-size: 0.8rem; color: var(--text-muted); margin-top: 4px;">
                    📍 ${escapeHtml(item.location)} | 📐 ${item.area_sqm || '-'} م²
                </div>
            `;

            el.querySelector('.fav-remove').addEventListener('click', () => {
                favorites = favorites.filter(f => f.id !== item.id);
                localStorage.setItem('re_favorites', JSON.stringify(favorites));
                updateFavoritesBadge();
                renderFavoritesDrawer();
                // Also update any heart button on active grid
                const cardBtn = document.querySelector(`.card-fav-btn[data-id="${item.id}"]`);
                if (cardBtn) {
                    cardBtn.classList.remove('active');
                    cardBtn.textContent = '🤍';
                }
            });

            favoritesList.appendChild(el);
        });
    }

    function escapeHtml(text) {
        if (!text) return '';
        return String(text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
});
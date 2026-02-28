"""
Cave Leclerc Blagnac — Comparateur Vivino
Streamlit app : scrape les vins Leclerc Blagnac + notes Vivino
"""

import streamlit as st
import requests
import re
import time
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Cave Leclerc Blagnac × Vivino",
    page_icon="🍷",
    layout="wide",
)

STORE_CODE = "1431"  # Leclerc Blagnac

VIVINO_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'fr-FR,fr;q=0.9',
    'Referer': 'https://www.vivino.com/',
}

# ─────────────────────────────────────────────────────────────────────────────
# CSS personnalisé
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=DM+Mono&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

.main-title {
    font-family: 'Playfair Display', serif;
    font-size: 2.4rem;
    font-weight: 900;
    color: #1A0810;
}
.main-title span { color: #C9A84C; }
.subtitle { color: #8B6B72; font-size: 0.85rem; letter-spacing: 0.08em; text-transform: uppercase; }

.metric-card {
    background: #1A0810;
    border-radius: 8px;
    padding: 1.2rem 1.5rem;
    text-align: center;
}
.metric-card .value {
    font-family: 'DM Mono', monospace;
    font-size: 1.8rem;
    color: #C9A84C;
}
.metric-card .label {
    font-size: 0.7rem;
    color: #8B6B72;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.wine-card {
    background: white;
    border-radius: 8px;
    padding: 1rem 1.4rem;
    margin-bottom: 0.6rem;
    border-left: 4px solid #6B1A2A;
    box-shadow: 0 2px 8px rgba(26,8,16,.07);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
}
.wine-card.top1 { border-left-color: #C9A84C; background: #fffdf4; }
.wine-card.top2 { border-left-color: #9C9C9C; }
.wine-card.top3 { border-left-color: #CD7F32; }

.wine-rank {
    font-family: 'DM Mono', monospace;
    font-size: 1.4rem;
    min-width: 2.5rem;
    text-align: center;
}

.wine-info { flex: 1; }
.wine-name-text {
    font-weight: 600;
    font-size: 0.95rem;
    color: #1A0810;
}
.wine-vivino-name { font-size: 0.75rem; color: #8B6B72; font-style: italic; }

.wine-price {
    font-family: 'DM Mono', monospace;
    font-size: 1.1rem;
    color: #1A0810;
    min-width: 70px;
    text-align: right;
}

.wine-rating { min-width: 120px; text-align: center; }
.stars { color: #C9A84C; font-size: 1rem; letter-spacing: 1px; }
.rating-num { font-family: 'DM Mono'; font-size: 0.85rem; color: #1A0810; }
.reviews { font-size: 0.65rem; color: #8B6B72; }
.no-rating { font-size: 0.75rem; color: #ccc; font-style: italic; }

.ratio-wrap { min-width: 130px; }
.ratio-bar-bg {
    background: rgba(107,26,42,.1);
    border-radius: 3px;
    height: 6px;
    overflow: hidden;
    margin-top: 4px;
}
.ratio-bar-fill {
    height: 100%;
    background: linear-gradient(90deg, #6B1A2A, #C9A84C);
    border-radius: 3px;
}
.ratio-num-text { font-family: 'DM Mono'; font-size: 0.8rem; color: #6B1A2A; }

.badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    font-size: 0.65rem;
    font-family: 'DM Mono';
}
.badge-deal { background: rgba(201,168,76,.15); color: #8B6030; border: 1px solid rgba(201,168,76,.4); }
.badge-top  { background: rgba(107,26,42,.08);  color: #6B1A2A; border: 1px solid rgba(107,26,42,.2); }

div[data-testid="stMetric"] label { font-size: 0.7rem !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER LECLERC (via Playwright ou demo)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_leclerc_wines() -> list[dict]:
    """Lance Playwright pour scraper les vins Leclerc Blagnac."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        st.warning("⚠️ Playwright non installé → données de démonstration. `pip install playwright && playwright install chromium`")
        return get_demo_wines()

    wines = []
    api_data = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='fr-FR',
            )
            page = context.new_page()

            # Intercepter les réponses API du catalogue
            def on_response(response):
                if 'ccu.e.leclerc' in response.url and response.status == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict) and 'products' in data:
                            api_data.extend(data['products'])
                    except:
                        pass

            page.on('response', on_response)

            page.goto(
                f"https://www.e.leclerc/cat/vins-rouges?oaf-sign-code={STORE_CODE}",
                wait_until='networkidle', timeout=30000
            )

            time.sleep(2)

            if api_data:
                wines = parse_api_products(api_data)
            else:
                wines = scrape_dom(page)

            # Scroll pour charger plus
            for _ in range(3):
                prev = len(wines)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1.5)
                new = scrape_dom(page)
                existing_names = {w['name'] for w in wines}
                wines += [w for w in new if w['name'] not in existing_names]
                if len(wines) == prev:
                    break

            browser.close()

    except Exception as e:
        st.error(f"Erreur Playwright : {e}")
        return get_demo_wines()

    return wines if wines else get_demo_wines()


def parse_api_products(products):
    wines = []
    for p in products:
        try:
            name = (p.get('label') or p.get('name') or '').strip()
            if not name:
                continue
            price_raw = p.get('price', {})
            price = price_raw.get('price') or price_raw.get('selling') or 0 if isinstance(price_raw, dict) else price_raw
            image = ''
            imgs = p.get('images', [])
            if imgs:
                image = imgs[0].get('url', '') if isinstance(imgs[0], dict) else imgs[0]
            wines.append({
                'name': name,
                'price': float(str(price).replace(',', '.').replace('€', '').strip() or 0),
                'image': image,
                'url': f"https://www.e.leclerc/pr/{p.get('slug', p.get('code', ''))}",
            })
        except:
            pass
    return wines


def scrape_dom(page):
    wines = []
    for sel in ['app-product-card', '.product-card', 'article[class*="product"]']:
        cards = page.query_selector_all(sel)
        if cards:
            for card in cards:
                try:
                    name_el = card.query_selector('.product-label, .product-title, [class*="product-name"], h2, h3')
                    name = name_el.inner_text().strip() if name_el else ''
                    price_el = card.query_selector('.product-price, [class*="price"], .selling-price')
                    price_text = price_el.inner_text().strip() if price_el else '0'
                    m = re.search(r'(\d+[.,]\d{2}|\d+)', price_text.replace('\xa0', ''))
                    price = float(m.group(1).replace(',', '.')) if m else 0
                    img_el = card.query_selector('img')
                    image = (img_el.get_attribute('src') or img_el.get_attribute('data-src') or '') if img_el else ''
                    link_el = card.query_selector('a')
                    href = link_el.get_attribute('href') or '' if link_el else ''
                    url = f"https://www.e.leclerc{href}" if href.startswith('/') else href
                    if name and price > 0:
                        wines.append({'name': name, 'price': price, 'image': image, 'url': url})
                except:
                    pass
            break
    return wines


# ─────────────────────────────────────────────────────────────────────────────
# VIVINO
# ─────────────────────────────────────────────────────────────────────────────

def search_vivino(wine_name: str) -> dict | None:
    clean = re.sub(r'\s+\d{4}\s*$', '', wine_name)
    clean = re.sub(r'(?i)(rouge|blanc|rosé)\s*$', '', clean).strip()
    query = ' '.join(clean.split()[:5])

    try:
        resp = requests.get(
            'https://www.vivino.com/api/explore/explore',
            params={
                'language': 'fr', 'wine_type_ids[]': 1,
                'q': query, 'order_by': 'match',
            },
            headers=VIVINO_HEADERS, timeout=8
        )
        if resp.status_code == 200:
            records = resp.json().get('explore_vintage', {}).get('records', [])
            if records:
                v = records[0].get('vintage', {})
                w = v.get('wine', {})
                stats = v.get('statistics', w.get('statistics', {}))
                return {
                    'vivino_name': w.get('name', ''),
                    'vivino_year': v.get('year', ''),
                    'rating': stats.get('ratings_average', 0),
                    'ratings_count': stats.get('ratings_count', 0),
                    'vivino_url': f"https://www.vivino.com{w.get('seo_name', '')}",
                }
    except:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# DEMO DATA
# ─────────────────────────────────────────────────────────────────────────────

def get_demo_wines():
    return [
        {'name': 'Côtes du Rhône Villages Sablet 2021',          'price': 8.50,  'image': '', 'url': ''},
        {'name': 'Saint-Émilion Grand Cru 2019',                  'price': 29.90, 'image': '', 'url': ''},
        {'name': 'Bourgogne Pinot Noir Louis Jadot 2020',         'price': 15.90, 'image': '', 'url': ''},
        {'name': 'Pic Saint-Loup Ermitage du Pic 2019',           'price': 12.50, 'image': '', 'url': ''},
        {'name': 'Minervois La Livinière 2020',                   'price': 9.90,  'image': '', 'url': ''},
        {'name': 'Château Pichon Baron 2018',                     'price': 45.90, 'image': '', 'url': ''},
        {'name': 'Beaujolais Villages Georges Duboeuf 2022',      'price': 6.90,  'image': '', 'url': ''},
        {'name': 'Médoc Haut-Médoc Grand Cru Bourgeois 2018',     'price': 18.90, 'image': '', 'url': ''},
        {'name': 'Languedoc Terrasses du Larzac 2020',            'price': 11.50, 'image': '', 'url': ''},
        {'name': 'Côtes de Bordeaux Château Penin 2019',          'price': 7.90,  'image': '', 'url': ''},
    ]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS AFFICHAGE
# ─────────────────────────────────────────────────────────────────────────────

def build_stars(rating):
    html = ''
    for i in range(1, 6):
        if rating >= i:          html += '★'
        elif rating >= i - 0.5: html += '½'
        else:                    html += '☆'
    return html


def wine_card_html(wine, rank, max_ratio):
    card_class = 'top1' if rank == 1 else 'top2' if rank == 2 else 'top3' if rank == 3 else ''
    rank_icon = '🥇' if rank == 1 else '🥈' if rank == 2 else '🥉' if rank == 3 else f'#{rank}'

    name_html = f'<a href="{wine["url"]}" target="_blank" style="color:#1A0810;text-decoration:none">{wine["name"]}</a>' if wine.get('url') else wine['name']

    vivino_sub = ''
    if wine.get('vivino_name') and wine['vivino_name'] != wine['name']:
        vivino_sub = f'<div class="wine-vivino-name">Vivino : {wine["vivino_name"]} {wine.get("vivino_year","")}</div>'

    rating = wine.get('rating')
    if rating:
        rating_html = f'''
            <div class="wine-rating">
              <div class="stars">{build_stars(rating)}</div>
              <div class="rating-num">{rating:.2f} / 5</div>
              <div class="reviews">{wine.get("ratings_count",0):,} avis'.replace(',', '\xa0')</div>
            </div>'''
    else:
        rating_html = '<div class="wine-rating no-rating">non trouvé sur Vivino</div>'

    ratio = wine.get('ratio', 0)
    pct = min(100, (ratio / max_ratio) * 100) if max_ratio > 0 else 0
    ratio_html = f'''
        <div class="ratio-wrap">
          <div class="ratio-num-text">{ratio:.3f}</div>
          <div class="ratio-bar-bg"><div class="ratio-bar-fill" style="width:{pct:.1f}%"></div></div>
        </div>''' if ratio else '<div class="no-rating">—</div>'

    badges = ''
    if ratio > 0 and rank <= 5:    badges += '<span class="badge badge-deal">🔥 Top ratio</span> '
    if rating and rating >= 4.2:   badges += '<span class="badge badge-top">★ Top noté</span>'
    if wine.get('vivino_url'):
        badges += f' <a href="{wine["vivino_url"]}" target="_blank" style="font-size:.7rem;color:#8B6B72;text-decoration:none">→ Vivino</a>'

    return f'''
    <div class="wine-card {card_class}">
      <div class="wine-rank">{rank_icon}</div>
      <div class="wine-info">
        <div class="wine-name-text">{name_html}</div>
        {vivino_sub}
        <div style="margin-top:.3rem">{badges}</div>
      </div>
      {rating_html}
      <div class="wine-price">{wine["price"]:.2f} €'.replace('.', ',')</div>
      {ratio_html}
    </div>'''


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

st.markdown('<div class="main-title">Cave <span>Leclerc Blagnac</span> × Vivino</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Comparateur qualité / prix — Vins rouges</div>', unsafe_allow_html=True)
st.markdown('<br>', unsafe_allow_html=True)

# ── Sidebar filtres ──────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔧 Filtres")
    search = st.text_input("🔍 Recherche", placeholder="Ex: Bordeaux, Rhône…")
    price_max = st.slider("💶 Prix max (€)", 0, 200, 200, step=5)
    rating_min = st.select_slider(
        "⭐ Note Vivino min",
        options=[0.0, 3.0, 3.5, 3.8, 4.0, 4.2, 4.5],
        value=0.0,
        format_func=lambda x: "Toutes" if x == 0 else f"≥ {x} ★"
    )
    sort_by = st.selectbox(
        "↕ Trier par",
        ["Meilleur ratio ★/€", "Meilleure note", "Prix croissant", "Prix décroissant"]
    )
    st.divider()
    demo_mode = st.checkbox("🎭 Mode démo (sans scraping)", value=False,
                             help="Utiliser des données fictives pour tester l'interface")
    scrape_btn = st.button("🔄 Lancer / Rafraîchir", use_container_width=True, type="primary")
    st.caption("Le scraping peut prendre 1-2 minutes")

# ── Session state ────────────────────────────────────────────
if 'wines' not in st.session_state:
    st.session_state.wines = []

if scrape_btn or (not st.session_state.wines):
    st.session_state.wines = []

    if demo_mode:
        raw_wines = get_demo_wines()
    else:
        with st.status("🔍 Scraping Leclerc Blagnac…", expanded=True) as status:
            st.write("Lancement de Playwright…")
            raw_wines = scrape_leclerc_wines()
            st.write(f"✅ {len(raw_wines)} vins récupérés")

            st.write("🍷 Interrogation de Vivino…")
            enriched = []
            prog = st.progress(0)
            for i, wine in enumerate(raw_wines):
                vd = search_vivino(wine['name'])
                if vd and vd['rating']:
                    wine.update(vd)
                    wine['ratio'] = round((vd['rating'] / wine['price']) * 10, 3) if wine['price'] > 0 else 0
                else:
                    wine.update({'rating': None, 'ratings_count': 0, 'ratio': 0, 'vivino_name': '', 'vivino_url': ''})
                enriched.append(wine)
                prog.progress((i + 1) / len(raw_wines))
                time.sleep(0.3)

            st.session_state.wines = enriched
            status.update(label=f"✅ Terminé — {len(enriched)} vins analysés", state="complete")

    if demo_mode:
        # Enrichir les démos aussi
        enriched = []
        with st.spinner("Vivino en cours…"):
            for wine in raw_wines:
                vd = search_vivino(wine['name'])
                if vd and vd['rating']:
                    wine.update(vd)
                    wine['ratio'] = round((vd['rating'] / wine['price']) * 10, 3) if wine['price'] else 0
                else:
                    wine.update({'rating': None, 'ratings_count': 0, 'ratio': 0, 'vivino_name': '', 'vivino_url': ''})
                enriched.append(wine)
                time.sleep(0.2)
        st.session_state.wines = enriched

# ── Filtrer & trier ──────────────────────────────────────────
wines = st.session_state.wines

if wines:
    filtered = [
        w for w in wines
        if w['price'] <= price_max
        and (rating_min == 0 or (w.get('rating') and w['rating'] >= rating_min))
        and (not search or search.lower() in w['name'].lower() or search.lower() in (w.get('vivino_name') or '').lower())
    ]

    if sort_by == "Meilleur ratio ★/€":    filtered.sort(key=lambda x: x.get('ratio') or 0, reverse=True)
    elif sort_by == "Meilleure note":       filtered.sort(key=lambda x: x.get('rating') or 0, reverse=True)
    elif sort_by == "Prix croissant":       filtered.sort(key=lambda x: x['price'])
    elif sort_by == "Prix décroissant":     filtered.sort(key=lambda x: x['price'], reverse=True)

    # ── Métriques ──
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🍷 Vins affichés", len(filtered))
    with col2:
        avg_p = sum(w['price'] for w in filtered) / len(filtered) if filtered else 0
        st.metric("💶 Prix moyen", f"{avg_p:.2f} €")
    with col3:
        rated = [w for w in filtered if w.get('rating')]
        avg_r = sum(w['rating'] for w in rated) / len(rated) if rated else 0
        st.metric("⭐ Note moy. Vivino", f"{avg_r:.2f} / 5" if avg_r else "—")
    with col4:
        best = max(filtered, key=lambda x: x.get('ratio') or 0) if filtered else None
        st.metric("🏆 Meilleur ratio", f"{best['ratio']:.2f}" if best and best.get('ratio') else "—")

    st.divider()

    # ── Export CSV ──
    with st.expander("📥 Exporter en CSV"):
        df = pd.DataFrame([{
            'Nom': w['name'],
            'Prix (€)': w['price'],
            'Note Vivino': w.get('rating', ''),
            'Nb avis': w.get('ratings_count', ''),
            'Ratio ★/€': w.get('ratio', ''),
            'URL Leclerc': w.get('url', ''),
            'URL Vivino': w.get('vivino_url', ''),
        } for w in filtered])
        st.download_button("⬇️ Télécharger CSV", df.to_csv(index=False, sep=';').encode('utf-8-sig'),
                           "vins_leclerc_blagnac.csv", "text/csv")

    # ── Tableau ──
    max_ratio = max((w.get('ratio') or 0 for w in filtered), default=1)

    st.markdown("### 🏅 Classement qualité / prix")

    for i, wine in enumerate(filtered):
        st.markdown(wine_card_html(wine, i + 1, max_ratio), unsafe_allow_html=True)

else:
    st.info("👈 Cliquez sur **Lancer / Rafraîchir** dans le panneau gauche pour démarrer l'analyse.")
    st.markdown("""
    **Ce que fait cette app :**
    1. 🔍 Scrape les vins rouges disponibles chez **Leclerc Blagnac** (code magasin 1431)
    2. 🍷 Récupère la note et les avis **Vivino** pour chaque vin
    3. 📊 Calcule le **ratio qualité/prix** = note ÷ prix × 10
    4. 🏅 Classe les vins pour trouver la **meilleure affaire**
    """)

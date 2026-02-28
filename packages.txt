"""
Cave Leclerc Blagnac — Comparateur Vivino
Scrape les vins disponibles chez Leclerc Blagnac et compare avec les notes Vivino
"""

from flask import Flask, jsonify, render_template, request
from playwright.sync_api import sync_playwright
import requests
import json
import re
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

STORE_CODE = "1431"  # Leclerc Blagnac
LECLERC_BASE_URL = f"https://www.e.leclerc/cat/vins-rouges#oaf-sign-code={STORE_CODE}"

# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER LECLERC (Playwright)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_leclerc_wines(max_pages: int = 5) -> list[dict]:
    """
    Utilise Playwright pour rendre la page Angular et extraire les produits.
    L'URL avec #oaf-sign-code=1431 filtre automatiquement sur le magasin Blagnac.
    """
    wines = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            viewport={'width': 1280, 'height': 800},
            locale='fr-FR'
        )
        page = context.new_page()
        
        # Intercepter les requêtes API pour récupérer les données brutes
        api_data = []
        
        def handle_response(response):
            """Capture les réponses API produits"""
            if 'ccu.e.leclerc' in response.url and response.status == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and 'products' in data:
                        api_data.extend(data['products'])
                    elif isinstance(data, list):
                        api_data.extend(data)
                except:
                    pass
        
        page.on('response', handle_response)
        
        logger.info(f"Navigation vers {LECLERC_BASE_URL}")
        
        try:
            page.goto(
                f"https://www.e.leclerc/cat/vins-rouges?oaf-sign-code={STORE_CODE}",
                wait_until='networkidle',
                timeout=30000
            )
        except Exception as e:
            logger.warning(f"Timeout initial (normal): {e}")
        
        # Attendre que les produits s'affichent
        try:
            page.wait_for_selector('app-product-card, .product-card, [class*="product-card"]', timeout=15000)
        except:
            logger.warning("Sélecteur product-card non trouvé, on continue...")
        
        time.sleep(2)
        
        # Si on a capturé des données API, les utiliser en priorité
        if api_data:
            logger.info(f"✅ {len(api_data)} produits via API interceptée")
            wines = parse_api_products(api_data)
        else:
            # Fallback: scraping DOM
            logger.info("Scraping DOM...")
            wines = scrape_dom(page)
        
        # Pagination - scroller pour charger plus de produits
        page_num = 1
        while page_num < max_pages:
            prev_count = len(wines)
            
            # Scroller vers le bas pour déclencher lazy load
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1.5)
            
            # Chercher bouton "page suivante"
            next_btn = page.query_selector('button[aria-label*="suivant"], .pagination-next, [class*="next-page"]')
            if next_btn:
                next_btn.click()
                time.sleep(2)
                new_wines = scrape_dom(page)
                wines.extend([w for w in new_wines if w['name'] not in [x['name'] for x in wines]])
            
            if len(wines) == prev_count:
                break
            page_num += 1
        
        browser.close()
    
    logger.info(f"Total vins récupérés: {len(wines)}")
    return wines


def parse_api_products(products: list) -> list[dict]:
    """Parse les produits depuis l'API interceptée"""
    wines = []
    for p in products:
        try:
            name = (p.get('label') or p.get('name') or p.get('title') or '').strip()
            if not name:
                continue
            
            price_raw = p.get('price', {})
            if isinstance(price_raw, dict):
                price = price_raw.get('price') or price_raw.get('selling') or 0
            else:
                price = price_raw or 0
            
            image = ''
            imgs = p.get('images', [])
            if imgs and isinstance(imgs, list):
                image = imgs[0].get('url', '') if isinstance(imgs[0], dict) else imgs[0]
            
            wines.append({
                'name': name,
                'price': float(str(price).replace(',', '.').replace('€', '').strip() or 0),
                'image': image,
                'url': f"https://www.e.leclerc/pr/{p.get('slug', p.get('code', ''))}",
                'source': 'api'
            })
        except Exception as e:
            logger.warning(f"Erreur parse produit: {e}")
    
    return wines


def scrape_dom(page) -> list[dict]:
    """Scraping fallback depuis le DOM"""
    wines = []
    
    # Sélecteurs possibles selon la version de la page
    selectors = [
        'app-product-card',
        '.product-card',
        '[class*="ProductCard"]',
        '[data-testid="product-card"]',
        'article[class*="product"]'
    ]
    
    cards = []
    for sel in selectors:
        cards = page.query_selector_all(sel)
        if cards:
            logger.info(f"Trouvé {len(cards)} cartes avec sélecteur: {sel}")
            break
    
    for card in cards:
        try:
            # Nom
            name_el = card.query_selector(
                '.product-label, .product-title, [class*="product-name"], '
                '[class*="ProductName"], h2, h3'
            )
            name = name_el.inner_text().strip() if name_el else ''
            
            # Prix
            price_el = card.query_selector(
                '.product-price, [class*="price"], [class*="Price"], '
                '.price-tag, .selling-price'
            )
            price_text = price_el.inner_text().strip() if price_el else '0'
            price_match = re.search(r'(\d+[.,]\d{2}|\d+)', price_text.replace('\xa0', ''))
            price = float(price_match.group(1).replace(',', '.')) if price_match else 0
            
            # Image
            img_el = card.query_selector('img')
            image = ''
            if img_el:
                image = img_el.get_attribute('src') or img_el.get_attribute('data-src') or ''
            
            # URL produit
            link_el = card.query_selector('a')
            url = ''
            if link_el:
                href = link_el.get_attribute('href') or ''
                url = f"https://www.e.leclerc{href}" if href.startswith('/') else href
            
            if name and price > 0:
                wines.append({
                    'name': name,
                    'price': price,
                    'image': image,
                    'url': url,
                    'source': 'dom'
                })
        except Exception as e:
            logger.debug(f"Erreur carte: {e}")
    
    return wines


# ─────────────────────────────────────────────────────────────────────────────
# API VIVINO
# ─────────────────────────────────────────────────────────────────────────────

VIVINO_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'fr-FR,fr;q=0.9',
    'Referer': 'https://www.vivino.com/',
}


def search_vivino(wine_name: str) -> dict | None:
    """Recherche un vin sur Vivino et retourne sa note"""
    # Nettoyer le nom pour la recherche
    clean_name = re.sub(r'\s+\d{4}\s*$', '', wine_name)  # Retirer millésime final
    clean_name = re.sub(r'(?i)(rouge|blanc|rosé|moelleux)\s*$', '', clean_name).strip()
    
    # Raccourcir si trop long
    words = clean_name.split()
    search_query = ' '.join(words[:5]) if len(words) > 5 else clean_name
    
    try:
        resp = requests.get(
            'https://www.vivino.com/api/explore/explore',
            params={
                'language': 'fr',
                'country_codes[]': 'fr',
                'price_range_max': 300,
                'price_range_min': 0,
                'wine_type_ids[]': 1,  # vin rouge
                'q': search_query,
                'order_by': 'match',
            },
            headers=VIVINO_HEADERS,
            timeout=8
        )
        
        if resp.status_code == 200:
            data = resp.json()
            records = data.get('explore_vintage', {}).get('records', [])
            
            if records:
                # Prendre le premier résultat le plus pertinent
                best = records[0]
                vintage = best.get('vintage', {})
                wine = vintage.get('wine', {})
                stats = vintage.get('statistics', wine.get('statistics', {}))
                
                return {
                    'vivino_name': wine.get('name', ''),
                    'vivino_vintage': vintage.get('year', ''),
                    'rating': stats.get('ratings_average', 0),
                    'ratings_count': stats.get('ratings_count', 0),
                    'vivino_url': f"https://www.vivino.com{wine.get('seo_name', '')}",
                    'vivino_image': (vintage.get('image', {}) or {}).get('location', '')
                }
    
    except Exception as e:
        logger.warning(f"Erreur Vivino pour '{wine_name}': {e}")
    
    return None


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES FLASK
# ─────────────────────────────────────────────────────────────────────────────

_cache = {}

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/wines')
def get_wines():
    """Retourne tous les vins avec leur note Vivino et ratio qualité/prix"""
    
    if 'wines' in _cache:
        logger.info("Cache hit ✅")
        return jsonify(_cache['wines'])
    
    # 1. Scraper Leclerc
    logger.info("🔍 Scraping Leclerc Blagnac...")
    wines = scrape_leclerc_wines()
    
    if not wines:
        # Mode démo si scraping échoue
        logger.warning("Scraping échoué - données de démonstration")
        wines = get_demo_wines()
    
    # 2. Enrichir avec Vivino
    logger.info(f"🍷 Enrichissement Vivino pour {len(wines)} vins...")
    enriched = []
    
    for i, wine in enumerate(wines):
        logger.info(f"[{i+1}/{len(wines)}] {wine['name'][:50]}")
        
        vivino_data = search_vivino(wine['name'])
        
        if vivino_data and vivino_data['rating'] > 0:
            wine.update(vivino_data)
            # Ratio qualité/prix (note Vivino sur 5 / prix en €, ×10 pour lisibilité)
            wine['ratio'] = round((vivino_data['rating'] / wine['price']) * 10, 3) if wine['price'] > 0 else 0
        else:
            wine['rating'] = None
            wine['ratings_count'] = 0
            wine['ratio'] = 0
            wine['vivino_name'] = ''
            wine['vivino_url'] = ''
        
        enriched.append(wine)
        time.sleep(0.3)  # Respecter le rate limit Vivino
    
    # Trier par ratio décroissant
    enriched.sort(key=lambda x: x['ratio'] or 0, reverse=True)
    
    _cache['wines'] = enriched
    return jsonify(enriched)


@app.route('/api/refresh')
def refresh():
    """Vider le cache et re-scraper"""
    _cache.clear()
    return jsonify({'status': 'cache cleared'})


@app.route('/api/vivino/<path:wine_name>')
def vivino_search(wine_name):
    """Recherche manuelle d'un vin sur Vivino"""
    result = search_vivino(wine_name)
    return jsonify(result or {})


def get_demo_wines():
    """Données de démonstration si Playwright n'est pas installé"""
    return [
        {'name': 'Château Pichon Baron 2018', 'price': 45.90, 'image': '', 'url': ''},
        {'name': 'Côtes du Rhône Villages Sablet 2021', 'price': 8.50, 'image': '', 'url': ''},
        {'name': 'Saint-Émilion Grand Cru 2019', 'price': 29.90, 'image': '', 'url': ''},
        {'name': 'Bourgogne Pinot Noir Louis Jadot 2020', 'price': 15.90, 'image': '', 'url': ''},
        {'name': 'Pic Saint-Loup Ermitage du Pic 2019', 'price': 12.50, 'image': '', 'url': ''},
        {'name': 'Minervois La Livinière 2020', 'price': 9.90, 'image': '', 'url': ''},
        {'name': 'Pomerol Château La Conseillante 2017', 'price': 89.00, 'image': '', 'url': ''},
        {'name': 'Beaujolais Villages Georges Duboeuf 2022', 'price': 6.90, 'image': '', 'url': ''},
        {'name': 'Médoc Haut-Médoc Grand Cru Bourgeois 2018', 'price': 18.90, 'image': '', 'url': ''},
        {'name': 'Côtes de Provence rouge Miraval 2021', 'price': 22.00, 'image': '', 'url': ''},
    ]


if __name__ == '__main__':
    print("🍷 Cave Leclerc Blagnac — Comparateur Vivino")
    print("📡 Accéder à http://localhost:5000")
    app.run(debug=True, port=5000)

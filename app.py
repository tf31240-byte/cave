 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/app.py b/app.py
index a68b69666090b99b6a8f5c8404c79e0f9c6c9469..55312a7b6deda664a50ea16e3cb058fefde67d32 100644
--- a/app.py
+++ b/app.py
@@ -1,45 +1,53 @@
 """
 Cave Leclerc Blagnac — Comparateur Vivino
 Scrape les vins disponibles chez Leclerc Blagnac et compare avec les notes Vivino
 """
 
-from flask import Flask, jsonify, render_template, request
+from flask import Flask, jsonify, render_template
 from playwright.sync_api import sync_playwright
 import requests
 import json
 import re
 import time
 import logging
+import os
+from requests.exceptions import RequestException
 
 logging.basicConfig(level=logging.INFO)
 logger = logging.getLogger(__name__)
 
-app = Flask(__name__)
+app = Flask(__name__, template_folder='.')
 
 STORE_CODE = "1431"  # Leclerc Blagnac
 LECLERC_BASE_URL = f"https://www.e.leclerc/cat/vins-rouges#oaf-sign-code={STORE_CODE}"
+PRICE_HISTORY_FILE = 'price_history.json'
+MAX_PRICE_HISTORY_POINTS = 30
+
+
+def normalize_wine_name(name: str) -> str:
+    return (name or '').strip().casefold()
 
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
@@ -80,95 +88,115 @@ def scrape_leclerc_wines(max_pages: int = 5) -> list[dict]:
         
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
-                wines.extend([w for w in new_wines if w['name'] not in [x['name'] for x in wines]])
+                known_names = {normalize_wine_name(x.get('name', '')) for x in wines}
+                wines.extend([w for w in new_wines if normalize_wine_name(w.get('name', '')) not in known_names])
             
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
+                'ean': str(p.get('ean') or p.get('gtin') or p.get('code') or ''),
                 'source': 'api'
             })
         except Exception as e:
             logger.warning(f"Erreur parse produit: {e}")
     
     return wines
 
 
+def deduplicate_wines(wines: list[dict]) -> list[dict]:
+    """Déduplique les vins par nom (insensible à la casse) en conservant le prix le plus bas."""
+    by_name = {}
+    for wine in wines:
+        name = (wine.get('name') or '').strip()
+        if not name:
+            continue
+
+        key = normalize_wine_name(name)
+        current = by_name.get(key)
+        if current is None or wine.get('price', 0) < current.get('price', 0):
+            by_name[key] = wine
+
+    deduped = list(by_name.values())
+    logger.info(f"Déduplication: {len(wines)} → {len(deduped)} vins")
+    return deduped
+
+
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
@@ -177,195 +205,329 @@ def scrape_dom(page) -> list[dict]:
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
+                ean_match = re.search(r'(\d{8,14})$', url or '')
                 wines.append({
                     'name': name,
                     'price': price,
                     'image': image,
                     'url': url,
+                    'ean': ean_match.group(1) if ean_match else '',
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
 
 
+VIVINO_MAX_RETRIES = 3
+VIVINO_RETRY_BACKOFF_S = 0.6
+VIVINO_COOLDOWN_S = 180
+_vivino_state = {
+    'blocked_until': 0.0,
+    'reason': '',
+}
+
+
+def _is_vivino_blocked() -> bool:
+    return time.time() < _vivino_state['blocked_until']
+
+
+def _mark_vivino_blocked(reason: str, cooldown_s: int = VIVINO_COOLDOWN_S) -> None:
+    _vivino_state['blocked_until'] = time.time() + cooldown_s
+    _vivino_state['reason'] = reason
+    logger.warning(f"Vivino temporairement indisponible ({reason}) pendant {cooldown_s}s")
+
+
+def _empty_vivino_payload(unavailable: bool = False) -> dict:
+    return {
+        'rating': None,
+        'ratings_count': 0,
+        'ratio': 0,
+        'vivino_name': '',
+        'vivino_url': '',
+        'vivino_unavailable': unavailable
+    }
+
+
 def search_vivino(wine_name: str) -> dict | None:
-    """Recherche un vin sur Vivino et retourne sa note"""
-    # Nettoyer le nom pour la recherche
-    clean_name = re.sub(r'\s+\d{4}\s*$', '', wine_name)  # Retirer millésime final
+    """Recherche un vin sur Vivino et retourne sa note."""
+    if _is_vivino_blocked():
+        return None
+
+    clean_name = re.sub(r'\s+\d{4}\s*$', '', wine_name)
     clean_name = re.sub(r'(?i)(rouge|blanc|rosé|moelleux)\s*$', '', clean_name).strip()
-    
-    # Raccourcir si trop long
     words = clean_name.split()
     search_query = ' '.join(words[:5]) if len(words) > 5 else clean_name
-    
-    try:
-        resp = requests.get(
-            'https://www.vivino.com/api/explore/explore',
-            params={
-                'language': 'fr',
-                'country_codes[]': 'fr',
-                'price_range_max': 300,
-                'price_range_min': 0,
-                'wine_type_ids[]': 1,  # vin rouge
-                'q': search_query,
-                'order_by': 'match',
-            },
-            headers=VIVINO_HEADERS,
-            timeout=8
-        )
-        
-        if resp.status_code == 200:
-            data = resp.json()
-            records = data.get('explore_vintage', {}).get('records', [])
-            
-            if records:
-                # Prendre le premier résultat le plus pertinent
+
+    for attempt in range(1, VIVINO_MAX_RETRIES + 1):
+        try:
+            resp = requests.get(
+                'https://www.vivino.com/api/explore/explore',
+                params={
+                    'language': 'fr',
+                    'country_codes[]': 'fr',
+                    'price_range_max': 300,
+                    'price_range_min': 0,
+                    'wine_type_ids[]': 1,
+                    'q': search_query,
+                    'order_by': 'match',
+                },
+                headers=VIVINO_HEADERS,
+                timeout=8
+            )
+
+            if resp.status_code == 200:
+                data = resp.json()
+                records = data.get('explore_vintage', {}).get('records', [])
+                if not records:
+                    return None
+
                 best = records[0]
                 vintage = best.get('vintage', {})
                 wine = vintage.get('wine', {})
                 stats = vintage.get('statistics', wine.get('statistics', {}))
-                
                 return {
                     'vivino_name': wine.get('name', ''),
                     'vivino_vintage': vintage.get('year', ''),
                     'rating': stats.get('ratings_average', 0),
                     'ratings_count': stats.get('ratings_count', 0),
                     'vivino_url': f"https://www.vivino.com{wine.get('seo_name', '')}",
                     'vivino_image': (vintage.get('image', {}) or {}).get('location', '')
                 }
-    
-    except Exception as e:
-        logger.warning(f"Erreur Vivino pour '{wine_name}': {e}")
-    
+
+            if resp.status_code == 429:
+                _mark_vivino_blocked('rate-limit (429)')
+                return None
+
+            if resp.status_code in {500, 502, 503, 504}:
+                logger.warning(f"Vivino erreur HTTP {resp.status_code} (tentative {attempt}/{VIVINO_MAX_RETRIES})")
+            else:
+                logger.warning(f"Vivino réponse inattendue HTTP {resp.status_code} pour '{wine_name}'")
+                return None
+
+        except RequestException as e:
+            logger.warning(f"Erreur réseau Vivino pour '{wine_name}' (tentative {attempt}/{VIVINO_MAX_RETRIES}): {e}")
+
+        if attempt < VIVINO_MAX_RETRIES:
+            time.sleep(VIVINO_RETRY_BACKOFF_S * attempt)
+
+    _mark_vivino_blocked('pannes réseau/HTTP répétées', cooldown_s=90)
     return None
 
 
+def _price_history_key(wine: dict) -> str:
+    ean = str(wine.get('ean') or '').strip()
+    if ean:
+        return f"ean:{ean}"
+    return f"name:{normalize_wine_name(wine.get('name', ''))}"
+
+
+def load_price_history() -> dict:
+    if not os.path.exists(PRICE_HISTORY_FILE):
+        return {}
+
+    try:
+        with open(PRICE_HISTORY_FILE, 'r', encoding='utf-8') as f:
+            data = f.read().strip()
+            if not data:
+                return {}
+            parsed = json.loads(data)
+            return parsed if isinstance(parsed, dict) else {}
+    except Exception as e:
+        logger.warning(f"Historique prix illisible, réinitialisation: {e}")
+        return {}
+
+
+def save_price_history(history: dict) -> None:
+    tmp_file = f"{PRICE_HISTORY_FILE}.tmp"
+    with open(tmp_file, 'w', encoding='utf-8') as f:
+        f.write(json.dumps(history, ensure_ascii=False))
+    os.replace(tmp_file, PRICE_HISTORY_FILE)
+
+
+def apply_price_history(wines: list[dict]) -> list[dict]:
+    """Enrichit les vins avec évolution du prix et persiste l'historique local."""
+    history = load_price_history()
+    today = time.strftime('%Y-%m-%d')
+
+    for wine in wines:
+        key = _price_history_key(wine)
+        entry = history.setdefault(key, {'name': wine.get('name', ''), 'history': []})
+        entry['name'] = wine.get('name', entry.get('name', ''))
+
+        current_price = wine.get('price') or 0
+        points = entry.get('history', [])
+
+        previous_price = points[-1]['price'] if points else None
+
+        if current_price > 0:
+            if points and points[-1]['date'] == today:
+                points[-1]['price'] = current_price
+            else:
+                points.append({'date': today, 'price': current_price})
+            entry['history'] = points[-MAX_PRICE_HISTORY_POINTS:]
+
+        wine['price_previous'] = previous_price
+        if previous_price is not None and current_price > 0:
+            delta = round(current_price - previous_price, 2)
+            wine['price_delta'] = delta
+            wine['price_delta_pct'] = round((delta / previous_price) * 100, 2) if previous_price else 0
+            if delta > 0.05:
+                wine['price_trend'] = 'up'
+            elif delta < -0.05:
+                wine['price_trend'] = 'down'
+            else:
+                wine['price_trend'] = 'stable'
+        else:
+            wine['price_delta'] = 0
+            wine['price_delta_pct'] = 0
+            wine['price_trend'] = 'unknown'
+
+    save_price_history(history)
+    return wines
+
+
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
+
+    wines = deduplicate_wines(wines)
+    wines = apply_price_history(wines)
     
     # 2. Enrichir avec Vivino
     logger.info(f"🍷 Enrichissement Vivino pour {len(wines)} vins...")
     enriched = []
     
+    vivino_cache = {}
+
+    vivino_unavailable = _is_vivino_blocked()
+    unavailable_payload = _empty_vivino_payload(unavailable=True)
+    empty_payload = _empty_vivino_payload(unavailable=False)
+
     for i, wine in enumerate(wines):
         logger.info(f"[{i+1}/{len(wines)}] {wine['name'][:50]}")
-        
-        vivino_data = search_vivino(wine['name'])
-        
+
+        if vivino_unavailable:
+            wine.update(unavailable_payload)
+            enriched.append(wine)
+            continue
+
+        cache_key = normalize_wine_name(wine.get('name', ''))
+        vivino_data = vivino_cache.get(cache_key)
+        if vivino_data is None:
+            vivino_data = search_vivino(wine['name'])
+            vivino_cache[cache_key] = vivino_data
+            vivino_unavailable = _is_vivino_blocked()
+
         if vivino_data and vivino_data['rating'] > 0:
             wine.update(vivino_data)
-            # Ratio qualité/prix (note Vivino sur 5 / prix en €, ×10 pour lisibilité)
             wine['ratio'] = round((vivino_data['rating'] / wine['price']) * 10, 3) if wine['price'] > 0 else 0
+            wine['vivino_unavailable'] = False
         else:
-            wine['rating'] = None
-            wine['ratings_count'] = 0
-            wine['ratio'] = 0
-            wine['vivino_name'] = ''
-            wine['vivino_url'] = ''
-        
+            wine.update(unavailable_payload if vivino_unavailable else empty_payload)
+
         enriched.append(wine)
-        time.sleep(0.3)  # Respecter le rate limit Vivino
+        if i < len(wines) - 1 and not vivino_unavailable:
+            time.sleep(0.3)
     
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
-        {'name': 'Château Pichon Baron 2018', 'price': 45.90, 'image': '', 'url': ''},
-        {'name': 'Côtes du Rhône Villages Sablet 2021', 'price': 8.50, 'image': '', 'url': ''},
-        {'name': 'Saint-Émilion Grand Cru 2019', 'price': 29.90, 'image': '', 'url': ''},
-        {'name': 'Bourgogne Pinot Noir Louis Jadot 2020', 'price': 15.90, 'image': '', 'url': ''},
-        {'name': 'Pic Saint-Loup Ermitage du Pic 2019', 'price': 12.50, 'image': '', 'url': ''},
-        {'name': 'Minervois La Livinière 2020', 'price': 9.90, 'image': '', 'url': ''},
-        {'name': 'Pomerol Château La Conseillante 2017', 'price': 89.00, 'image': '', 'url': ''},
-        {'name': 'Beaujolais Villages Georges Duboeuf 2022', 'price': 6.90, 'image': '', 'url': ''},
-        {'name': 'Médoc Haut-Médoc Grand Cru Bourgeois 2018', 'price': 18.90, 'image': '', 'url': ''},
-        {'name': 'Côtes de Provence rouge Miraval 2021', 'price': 22.00, 'image': '', 'url': ''},
+        {'name': 'Château Pichon Baron 2018', 'price': 45.90, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Côtes du Rhône Villages Sablet 2021', 'price': 8.50, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Saint-Émilion Grand Cru 2019', 'price': 29.90, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Bourgogne Pinot Noir Louis Jadot 2020', 'price': 15.90, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Pic Saint-Loup Ermitage du Pic 2019', 'price': 12.50, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Minervois La Livinière 2020', 'price': 9.90, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Pomerol Château La Conseillante 2017', 'price': 89.00, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Beaujolais Villages Georges Duboeuf 2022', 'price': 6.90, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Médoc Haut-Médoc Grand Cru Bourgeois 2018', 'price': 18.90, 'image': '', 'url': '', 'ean': ''},
+        {'name': 'Côtes de Provence rouge Miraval 2021', 'price': 22.00, 'image': '', 'url': '', 'ean': ''},
     ]
 
 
 if __name__ == '__main__':
     print("🍷 Cave Leclerc Blagnac — Comparateur Vivino")
     print("📡 Accéder à http://localhost:5000")
     app.run(debug=True, port=5000)
 
EOF
)

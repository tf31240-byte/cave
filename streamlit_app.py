"""
Cave Leclerc Blagnac — Comparateur Vivino
- Filtre par type de vin (Rouge / Blanc / Rosé / Mousseux)
- Notes Vivino via recherche DuckDuckGo (pas d'API, pas de blocage IP)
- Vérification du millésime
"""

import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import time
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Cave Leclerc Blagnac × Vivino",
    page_icon="🍷",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────

STORE_CODE = "1431"
MAX_PAGES  = 10

# Cache
CACHE_DIR        = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
# Vivino : pas de TTL — refresh manuel uniquement
LECLERC_CACHE_TTL = 6 * 3600       # 6h — le stock peut changer

# Types de vins disponibles sur Leclerc
WINE_TYPES = {
    "🔴 Rouge":     "vins-rouges",
    "⚪ Blanc":     "vins-blancs",
    "🌸 Rosé":      "vins-roses",
    "🍾 Mousseux":  "vins-mousseux-et-petillants",
}

def leclerc_url(wine_type_slug: str, page: int = 1) -> str:
    base = f"https://www.e.leclerc/cat/{wine_type_slug}"
    if page > 1:
        return f"{base}?page={page}#oaf-sign-code={STORE_CODE}"
    return f"{base}#oaf-sign-code={STORE_CODE}"


# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=DM+Mono&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.main-title { font-family:'Playfair Display',serif; font-size:2.2rem; font-weight:900; color:#1A0810; }
.main-title span { color:#C9A84C; }
.subtitle { color:#8B6B72; font-size:.82rem; letter-spacing:.08em; text-transform:uppercase; }
.wine-card {
    background:white; border-radius:8px; padding:.9rem 1.3rem; margin-bottom:.5rem;
    border-left:4px solid #6B1A2A; box-shadow:0 2px 8px rgba(26,8,16,.07);
    display:flex; align-items:center; justify-content:space-between; gap:1rem;
}
.wine-card.top1 { border-left-color:#C9A84C; background:#fffdf4; }
.wine-card.top2 { border-left-color:#9C9C9C; }
.wine-card.unavailable { opacity:0.55; filter:grayscale(40%); }
.wine-card.top3 { border-left-color:#CD7F32; }
.wine-card.vintage-warn { border-right: 3px solid #f59e0b; }
.wine-rank { font-family:'DM Mono',monospace; font-size:1.3rem; min-width:2.5rem; text-align:center; }
.wine-info { flex:1; }
.wine-name-text { font-weight:600; font-size:.9rem; color:#1A0810; }
.wine-sub { font-size:.72rem; color:#8B6B72; font-style:italic; }
.wine-price { font-family:'DM Mono',monospace; font-size:1.05rem; color:#1A0810; min-width:68px; text-align:right; }
.wine-rating { min-width:120px; text-align:center; }
.stars { color:#C9A84C; font-size:.95rem; letter-spacing:1px; }
.rating-num { font-family:'DM Mono'; font-size:.82rem; color:#1A0810; }
.reviews { font-size:.62rem; color:#8B6B72; }
.no-rating { font-size:.72rem; color:#ccc; font-style:italic; }
.ratio-wrap { min-width:130px; }
.ratio-bar-bg { background:rgba(107,26,42,.1); border-radius:3px; height:6px; overflow:hidden; margin-top:4px; }
.ratio-bar-fill { height:100%; background:linear-gradient(90deg,#6B1A2A,#C9A84C); border-radius:3px; }
.ratio-num-text { font-family:'DM Mono'; font-size:.78rem; color:#6B1A2A; }
.badge { display:inline-block; padding:.15rem .5rem; border-radius:3px; font-size:.62rem; font-family:'DM Mono'; margin-right:.2rem; }
.badge-deal { background:rgba(201,168,76,.15); color:#8B6030; border:1px solid rgba(201,168,76,.4); }
.badge-top  { background:rgba(107,26,42,.08);  color:#6B1A2A; border:1px solid rgba(107,26,42,.2); }
.badge-year { background:rgba(245,158,11,.1);  color:#92400e; border:1px solid rgba(245,158,11,.4); }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PARSEUR HTML LECLERC
# ─────────────────────────────────────────────────────────────────────────────

def extract_year_from_name(name: str) -> int | None:
    """Extrait le millésime (année) depuis le nom Leclerc. Ex: '...2022...' → 2022"""
    m = re.search(r"\b(19[5-9]\d|20[0-3]\d)\b", name)
    return int(m.group(1)) if m else None


def parse_cards_from_html(html: str) -> list[dict]:
    soup  = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("app-product-card")
    wines = []

    for card in cards:
        # Nom
        label = card.find(class_="product-label")
        name  = label.get_text(strip=True) if label else ""
        if not name:
            continue

        # Prix — via block-price-and-availability (robuste SSR + CSR)
        price = 0.0
        blk   = card.find(class_=lambda c: c and "block-price-and-availability" in c.split())
        if blk:
            m = re.search(r"(\d+)€,(\d{2})", blk.get_text(strip=True))
            if m:
                try:
                    price = float(f"{m.group(1)}.{m.group(2)}")
                except ValueError:
                    pass
        # Fallback price-unit / price-cents
        if price == 0:
            ue = card.find_all(class_=lambda c: c and "price-unit"  in c.split())
            ce = card.find_all(class_=lambda c: c and "price-cents" in c.split())
            if ue and ce:
                try:
                    price = float(f"{ue[0].get_text(strip=True)}.{ce[0].get_text(strip=True).lstrip(',').strip()}")
                except ValueError:
                    pass

        # URL
        link = card.find("a", href=True)
        href = link["href"] if link else ""
        url  = href if href.startswith("http") else f"https://www.e.leclerc{href}"

        # EAN
        ean_m = re.search(r"offer_m-(\d{13})-\d+", str(card))
        ean   = ean_m.group(1) if ean_m else ""
        if not ean:
            m2 = re.search(r"-(\d{13})$", url)
            ean = m2.group(1) if m2 else ""

        # Image
        img   = card.find("img")
        image = ""
        if img:
            image = (img.get("src") or img.get("data-src") or
                     (img.get("data-srcset","").split()[0] if img.get("data-srcset") else "") or "")

        wines.append({
            "name":    name,
            "price":   price,
            "url":     url,
            "ean":     ean,
            "image":   image,
            "vintage": extract_year_from_name(name),
        })

    return wines


def get_nb_pages(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    nums = []
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            nums.append(int(m.group(1)))
    return max(nums) if nums else 1


# ─────────────────────────────────────────────────────────────────────────────
# VIVINO — RECHERCHE WEB (DuckDuckGo)
# Même approche que votre ancien code JS :
#   query = "{nom} {appellation} vivino rating"
#   → on cherche sur le web, on filtre les URLs vivino.com
#   → on extrait note + avis depuis le snippet
#   → on vérifie le millésime
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# CACHE — Leclerc (stock + prix) et Vivino (notes)
# ─────────────────────────────────────────────────────────────────────────────

def _vivino_cache_path() -> Path:
    return CACHE_DIR / "vivino.json"

def _leclerc_cache_path(slug: str) -> Path:
    return CACHE_DIR / f"leclerc_{slug}.json"

def load_vivino_cache() -> dict:
    """Charge le cache Vivino. Clé = query normalisée, valeur = {rating, ...}"""
    p = _vivino_cache_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_vivino_cache(cache: dict) -> None:
    try:
        _vivino_cache_path().write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass

def load_leclerc_cache(slug: str) -> dict | None:
    """Retourne le cache Leclerc si valide (< TTL), sinon None."""
    p = _leclerc_cache_path(slug)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        age  = time.time() - data.get("cached_at", 0)
        if age < LECLERC_CACHE_TTL:
            return data
    except Exception:
        pass
    return None

def save_leclerc_cache(slug: str, wines: list[dict]) -> None:
    data = {
        "cached_at": time.time(),
        "slug":      slug,
        "wines":     wines,
    }
    try:
        _leclerc_cache_path(slug).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass

def check_availability(cached_wines: list[dict], current_eans: set) -> list[dict]:
    """
    Compare les EANs actuels avec le cache.
    Marque 'available': False pour les vins absents du Leclerc aujourd'hui.
    """
    for w in cached_wines:
        w["available"] = w.get("ean", "") in current_eans
    return cached_wines

def fmt_cache_age(cached_at: float) -> str:
    """Formate l'âge du cache en texte lisible."""
    age = time.time() - cached_at
    if age < 60:        return "à l'instant"
    if age < 3600:      return f"il y a {int(age/60)} min"
    if age < 86400:     return f"il y a {int(age/3600)} h"
    return f"il y a {int(age/86400)} j"



def build_vivino_query(wine_name: str) -> str:
    """
    Construit la query de recherche Vivino depuis le nom Leclerc.
    Règles :
    - Garder BIO (c'est une fiche Vivino distincte)
    - Supprimer Magnum (format bouteille)
    - Supprimer l'année
    - Normaliser les MAJUSCULES
    - Tronquer sur Cuvée/Vieilles Vignes/Grande à partir du 3e mot
    """
    nom = re.split(r",\s*|\s+-\s+", wine_name)[0].strip()

    # Format bouteille
    nom = re.sub(r"^Magnum\s+", "", nom, flags=re.I).strip()

    # Année
    nom = re.sub(r"\b(19|20)\d{2}\b", "", nom).strip().strip("-").strip()

    # Tout en MAJUSCULES → Title Case
    if re.match(r"^[A-Z][A-Z\s\'\-]+$", nom):
        nom = nom.title()

    # Tronquer les sous-cuvées à partir du 3e mot
    cut_at = {"Cuvée", "Cuvee", "Vieilles", "Vieille", "Grande"}
    words = nom.split()
    if len(words) > 2:
        for i, w in enumerate(words[2:], start=2):
            if w in cut_at:
                nom = " ".join(words[:i]).strip()
                break

    # Appellation
    app_m = re.search(r"-\s*([\w\s\-]+?)\s*(?:AOP|IGP|AOC|Vin de France)", wine_name, re.I)
    appellation = app_m.group(1).strip() if app_m else ""

    parts = [nom]
    if appellation and appellation.lower() not in nom.lower():
        parts.append(appellation)

    return " ".join(parts)


def parse_vivino_search_html(html: str, wine_vintage: int | None) -> dict | None:
    """
    Parse le HTML rendu de la page de recherche Vivino.
    Extrait note, nb avis, URL et millésime du premier résultat.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # ── Note ──────────────────────────────────────────────────────────────────
    rating = None

    # 1. Classe React hashée *averageValue*
    for el in soup.find_all(class_=lambda c: c and "averageValue" in c):
        try:
            v = round(float(el.get_text(strip=True).replace(",", ".")), 1)
            if 2.5 <= v <= 5.0:
                rating = v
                break
        except ValueError:
            pass

    # 2. data-average attribute
    if not rating:
        el = soup.find(attrs={"data-average": True})
        if el:
            try:
                v = round(float(str(el["data-average"]).replace(",", ".")), 1)
                if 2.5 <= v <= 5.0:
                    rating = v
            except (ValueError, KeyError):
                pass

    # 3. JSON-LD / inline JSON
    if not rating:
        m = re.search(r'"ratings_average"\s*:\s*([\d.]+)', html)
        if not m:
            m = re.search(r'"ratingValue"\s*:\s*"?([\d.,]+)"?', html)
        if m:
            try:
                v = round(float(m.group(1).replace(",", ".")), 1)
                if 2.5 <= v <= 5.0:
                    rating = v
            except ValueError:
                pass

    if not rating:
        return None

    # ── Nb avis ───────────────────────────────────────────────────────────────
    count = 0

    # 1. Classe React *numRatings*
    for el in soup.find_all(class_=lambda c: c and "numRatings" in c):
        digits = re.sub(r"[^\d]", "", el.get_text())
        if digits:
            count = int(digits)
            break

    # 2. Texte "X notes/avis/ratings" dans la page
    if not count:
        m = re.search(r"([\d\s\xa0,\.]+)\s*(?:notes?|avis|ratings?)", html, re.I)
        if m:
            digits = re.sub(r"[^\d]", "", m.group(1))
            if digits:
                count = int(digits)

    # 3. JSON inline ratings_count
    if not count:
        m = re.search(r'"ratings_count"\s*:\s*(\d+)', html)
        if m:
            count = int(m.group(1))

    # ── URL du premier résultat ───────────────────────────────────────────────
    best_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/wines/[\w\-]+", href) and "search" not in href:
            best_url = href if href.startswith("http") else f"https://www.vivino.com{href}"
            break

    # ── Millésime ─────────────────────────────────────────────────────────────
    vivino_year = None
    ym = re.search(r"\b(20[0-2]\d|19[5-9]\d)\b", best_url)
    if ym:
        vivino_year = int(ym.group(1))
    if not vivino_year:
        # Chercher dans les paramètres year= ou dans le texte
        ym2 = re.search(r"year[=:]\s*(20[0-2]\d|19[5-9]\d)", html)
        if not ym2:
            ym2 = re.search(r"\b(20[0-2]\d|19[5-9]\d)\b", html[:8000])
        if ym2:
            vivino_year = int(ym2.group(1))

    vintage_match = None
    if wine_vintage and vivino_year:
        vintage_match = (wine_vintage == vivino_year)
    elif not wine_vintage:
        vintage_match = True

    return {
        "rating":        rating,
        "ratings_count": count,
        "vivino_url":    best_url,
        "vivino_year":   vivino_year,
        "vintage_match": vintage_match,
    }


def get_selenium_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import os

    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,800")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    for binary in ["/usr/bin/chromium", "/usr/bin/chromium-browser",
                   "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable"]:
        if os.path.exists(binary):
            opts.binary_location = binary
            break

    for drv in ["/usr/bin/chromedriver", "/usr/lib/chromium/chromedriver",
                "/usr/lib/chromium-browser/chromedriver"]:
        if os.path.exists(drv):
            return webdriver.Chrome(service=Service(drv), options=opts)

    return webdriver.Chrome(options=opts)


def scrape_and_enrich(wine_type_slug: str, force_leclerc: bool = False,
                       force_vivino: bool = False, log=None) -> list[dict]:
    """
    1. Cache Leclerc : si valide et force_leclerc=False, utilise le cache
       Sinon scrape Leclerc + détecte les vins disparus
    2. Cache Vivino  : si note en cache et force_vivino=False, réutilise
       Sinon navigue sur la page de recherche Vivino
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # ── Charger les caches ───────────────────────────────────────────────────
    vivino_cache    = load_vivino_cache()
    leclerc_cached  = load_leclerc_cache(wine_type_slug)
    now             = time.time()

    # ── Leclerc ─────────────────────────────────────────────────────────────
    if leclerc_cached and not force_leclerc:
        # Cache valide : on vérifie quand même la disponibilité actuelle
        if log: log(f"📦 Cache Leclerc ({fmt_cache_age(leclerc_cached['cached_at'])}) — vérification stock…")
        cached_wines = leclerc_cached["wines"]

        # Scrape léger (page 1 seulement) pour obtenir les EANs actuels
        if log: log("🌐 Vérification disponibilité…")
        current_eans = set()
        try:
            driver = get_selenium_driver()
            driver.get(leclerc_url(wine_type_slug, 1))
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card"))
                )
            except Exception:
                pass
            time.sleep(2)
            html = driver.page_source
            nb_pages = min(get_nb_pages(html), MAX_PAGES)
            page1_wines = parse_cards_from_html(html)
            current_eans = {w["ean"] for w in page1_wines}
            # Pages suivantes si le cache en avait plus d'une
            for p in range(2, nb_pages + 1):
                driver.get(leclerc_url(wine_type_slug, p))
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card"))
                    )
                except Exception:
                    pass
                time.sleep(1.5)
                for w in parse_cards_from_html(driver.page_source):
                    current_eans.add(w["ean"])
        except Exception as e:
            if log: log(f"⚠️ Vérification stock échouée : {e}")
        finally:
            try: driver.quit()
            except: pass

        all_wines = check_availability(cached_wines, current_eans)
        n_dispo   = sum(1 for w in all_wines if w.get("available", True))
        n_indispo = len(all_wines) - n_dispo
        if log: log(f"✅ {n_dispo} dispo, {n_indispo} indisponible(s) à Blagnac")
        vivino_needed = [w for w in all_wines
                         if force_vivino or build_vivino_query(w["name"]) not in vivino_cache
                         ]  # pas de TTL Vivino — refresh manuel
    else:
        # Pas de cache ou refresh forcé : scrape complet
        if log: log("🚀 Démarrage Chromium…")
        driver    = get_selenium_driver()
        all_wines = []
        seen_eans = set()

        try:
            if log: log("🌐 Chargement Leclerc…")
            driver.get(leclerc_url(wine_type_slug, 1))
            try:
                WebDriverWait(driver, 25).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card"))
                )
            except Exception:
                pass
            time.sleep(2)

            html     = driver.page_source
            wines_p1 = parse_cards_from_html(html)
            nb_pages = min(get_nb_pages(html), MAX_PAGES)
            for w in wines_p1:
                if w["ean"] not in seen_eans:
                    seen_eans.add(w["ean"])
                    all_wines.append(w)
            if log: log(f"✅ Page 1 : {len(wines_p1)} vins — {nb_pages} page(s)")

            for p in range(2, nb_pages + 1):
                if log: log(f"🌐 Page {p}/{nb_pages}…")
                driver.get(leclerc_url(wine_type_slug, p))
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card"))
                    )
                except Exception:
                    pass
                time.sleep(2)
                wines_p = parse_cards_from_html(driver.page_source)
                new = [w for w in wines_p if w["ean"] not in seen_eans]
                if not new:
                    break
                for w in new:
                    seen_eans.add(w["ean"])
                all_wines.extend(new)
                if log: log(f"✅ Page {p} : {len(new)} vins (total {len(all_wines)})")

        finally:
            try: driver.quit()
            except: pass

        # Tous disponibles (on vient de scraper)
        for w in all_wines:
            w["available"] = True

        save_leclerc_cache(wine_type_slug, all_wines)
        if log: log(f"💾 Cache Leclerc sauvegardé ({len(all_wines)} vins)")
        vivino_needed = all_wines  # Tous ont besoin de Vivino

    # ── Vivino ───────────────────────────────────────────────────────────────
    # Injecter les données cache pour les vins déjà connus
    for wine in all_wines:
        key = build_vivino_query(wine["name"])
        cached_v = vivino_cache.get(key)
        if cached_v and cached_v.get("rating") is not None and not force_vivino:
            wine.update({k: v for k, v in cached_v.items() if k != "cached_at"})
            wine["ratio"] = (
                round((cached_v["rating"] / wine["price"]) * 10, 3)
                if wine.get("price", 0) > 0 and cached_v.get("rating") else 0
            )
        else:
            # Pas encore en cache Vivino
            wine.setdefault("rating", None)
            wine.setdefault("ratings_count", 0)
            wine.setdefault("ratio", 0)
            wine.setdefault("vivino_url", "")
            wine.setdefault("vivino_year", None)
            wine.setdefault("vintage_match", None)

    # Vins à scraper sur Vivino
    vivino_todo = [w for w in all_wines
                   if force_vivino or build_vivino_query(w["name"]) not in vivino_cache
                   ]  # pas de TTL Vivino — refresh manuel

    if vivino_todo:
        if log: log(f"🍷 Recherche Vivino ({len(vivino_todo)} vins à scraper)…")
        driver2 = get_selenium_driver()
        found   = 0
        EMPTY   = {"rating": None, "ratings_count": 0, "ratio": 0,
                   "vivino_url": "", "vivino_year": None, "vintage_match": None}

        try:
            for i, wine in enumerate(vivino_todo):
                query      = build_vivino_query(wine["name"])
                vintage    = wine.get("vintage")
                search_url = (
                    "https://www.vivino.com/search/wines"
                    f"?q={requests.utils.quote(query)}&language=fr"
                )
                try:
                    driver2.get(search_url)
                    try:
                        WebDriverWait(driver2, 8).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR,
                                 "[class*='averageValue'],[class*='average__number'],[data-average]")
                            )
                        )
                    except Exception:
                        pass
                    time.sleep(1)
                    vd = parse_vivino_search_html(driver2.page_source, vintage)
                except Exception:
                    vd = None

                if vd:
                    wine.update(vd)
                    wine["ratio"] = (
                        round((vd["rating"] / wine["price"]) * 10, 3)
                        if wine.get("price", 0) > 0 else 0
                    )
                    # Mettre en cache
                    vivino_cache[query] = {**vd, "cached_at": now}
                    found += 1
                    if log: log(f"  ✅ {wine['name'][:38]} → ★{vd['rating']} ({vd['ratings_count']} avis)")
                else:
                    wine.update(EMPTY)
                    # Mettre quand même en cache (vide) pour éviter de re-scraper
                    vivino_cache[query] = {"rating": None, "ratings_count": 0,
                                           "vivino_url": "", "vivino_year": None,
                                           "vintage_match": None, "cached_at": now}

                if (i + 1) % 10 == 0 or i == len(vivino_todo) - 1:
                    if log: log(f"  🍷 {i+1}/{len(vivino_todo)} — {found} notes")
                time.sleep(0.5)

        finally:
            try: driver2.quit()
            except: pass

        save_vivino_cache(vivino_cache)
        if log: log(f"💾 Cache Vivino sauvegardé")
    else:
        if log: log(f"✅ Notes Vivino depuis le cache (0 scraping)")

    return all_wines


# AFFICHAGE
# ─────────────────────────────────────────────────────────────────────────────

def build_stars(r: float) -> str:
    return "".join("★" if r >= i else ("½" if r >= i - .5 else "☆") for i in range(1, 6))


def wine_card_html(wine: dict, rank: int, max_ratio: float) -> str:
    card_cls    = {1:"top1", 2:"top2", 3:"top3"}.get(rank, "")
    vintage_warn = wine.get("vintage_match") is False
    if vintage_warn:
        card_cls = (card_cls + " vintage-warn").strip()
    if not wine.get("available", True):
        card_cls = (card_cls + " unavailable").strip()

    rank_icon = {1:"🥇", 2:"🥈", 3:"🥉"}.get(rank, f"#{rank}")

    # ── Nom avec lien Leclerc ───────────────────────────────────────────────
    name_html = (
        f'<a href="{wine["url"]}" target="_blank" '
        f'style="color:#1A0810;text-decoration:none;font-weight:500">'
        f'{wine["name"]}</a>'
        if wine.get("url") else wine["name"]
    )
    vintage_tag = (
        f' <span style="color:#8B6B72;font-size:.72rem">{wine["vintage"]}</span>'
        if wine.get("vintage") else ""
    )

    # ── Alerte millésime ────────────────────────────────────────────────────
    subs = ""
    if wine.get("vivino_year") and wine.get("vintage") and wine["vivino_year"] != wine["vintage"]:
        subs = (
            f'<div style="font-size:.7rem;color:#c17a00;margin-top:.2rem">'
            f'⚠️ Vivino : millésime {wine["vivino_year"]} (Leclerc : {wine["vintage"]})</div>'
        )

    # ── Indisponible ────────────────────────────────────────────────────────
    avail_tag = ""
    if not wine.get("available", True):
        avail_tag = '<span style="font-size:.68rem;color:#b0003a;margin-left:.4rem">⛔ indispo à Blagnac</span>'

    # ── Liens Leclerc + Vivino côte à côte ─────────────────────────────────
    links = []
    if wine.get("url"):
        links.append(
            f'<a href="{wine["url"]}" target="_blank" '
            f'style="font-size:.68rem;color:#2563eb;text-decoration:none;'
            f'border:1px solid #2563eb;border-radius:3px;padding:1px 5px">🛒 Leclerc</a>'
        )
    if wine.get("vivino_url"):
        links.append(
            f'<a href="{wine["vivino_url"]}" target="_blank" '
            f'style="font-size:.68rem;color:#7B2D8B;text-decoration:none;'
            f'border:1px solid #7B2D8B;border-radius:3px;padding:1px 5px">🍷 Vivino</a>'
        )
    links_html = (
        f'<div style="display:flex;gap:.4rem;margin-top:.35rem;flex-wrap:wrap">'
        + "".join(links) + "</div>"
    ) if links else ""

    # ── Badges ──────────────────────────────────────────────────────────────
    badges = ""
    ratio = wine.get("ratio") or 0
    rating = wine.get("rating")
    if ratio > 0 and rank <= 5:  badges += '<span class="badge badge-deal">🔥 Top ratio</span> '
    if rating and rating >= 4.2: badges += '<span class="badge badge-top">★ Top noté</span>'

    # ── Note Vivino ─────────────────────────────────────────────────────────
    if rating:
        cnt = wine.get("ratings_count") or 0
        cnt_str = f"{cnt:,}".replace(",", "\u202f") if cnt else "—"
        rating_html = (
            f'<div class="wine-rating">'
            f'<div class="stars">{build_stars(rating)}</div>'
            f'<div class="rating-num">{rating:.1f} / 5</div>'
            f'<div class="reviews">{cnt_str} avis</div>'
            f'</div>'
        )
    else:
        rating_html = '<div class="wine-rating no-rating">non trouvé<br>sur Vivino</div>'

    # ── Ratio ────────────────────────────────────────────────────────────────
    pct = min(100, (ratio / max_ratio) * 100) if max_ratio > 0 else 0
    ratio_html = (
        f'<div class="ratio-wrap">'
        f'<div class="ratio-num-text">{ratio:.3f}</div>'
        f'<div class="ratio-bar-bg">'
        f'<div class="ratio-bar-fill" style="width:{pct:.1f}%"></div>'
        f'</div></div>'
        if ratio else '<div class="no-rating">—</div>'
    )

    price_fmt = f'{wine["price"]:.2f}'.replace(".", ",") + " €"

    return (
        f'<div class="wine-card {card_cls}">'
        f'<div class="wine-rank">{rank_icon}</div>'
        f'<div class="wine-info">'
        f'<div class="wine-name-text">{name_html}{vintage_tag}{avail_tag}</div>'
        f'{subs}'
        f'{links_html}'
        f'<div style="margin-top:.25rem">{badges}</div>'
        f'</div>'
        f'{rating_html}'
        f'<div class="wine-price">{price_fmt}</div>'
        f'{ratio_html}'
        f'</div>'
    )



# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    '<div class="main-title">Cave <span>Leclerc Blagnac</span> × Vivino</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="subtitle">Comparateur qualité / prix — disponible en magasin</div>',
    unsafe_allow_html=True,
)
st.markdown("<br>", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🍾 Type de vin")
    wine_type_label = st.selectbox(
        "Catégorie",
        list(WINE_TYPES.keys()),
        label_visibility="collapsed",
    )
    wine_type_slug = WINE_TYPES[wine_type_label]

    st.markdown("### 🔧 Filtres")
    search     = st.text_input("🔍 Recherche", placeholder="Bordeaux, Rhône, Corbières…")
    price_max  = st.slider("💶 Prix max (€)", 0, 200, 200, step=5)
    rating_min = st.select_slider(
        "⭐ Note Vivino min",
        options=[0.0, 3.0, 3.5, 3.8, 4.0, 4.2, 4.5], value=0.0,
        format_func=lambda x: "Toutes" if x == 0 else f"≥ {x} ★",
    )
    only_vintage_ok = st.checkbox("✅ Millésime confirmé uniquement", value=False)
    only_available  = st.checkbox("🏪 Disponibles à Blagnac uniquement", value=True)
    sort_by = st.selectbox("↕ Trier par", [
        "Meilleur ratio ★/€", "Meilleure note", "Prix croissant", "Prix décroissant",
    ])
    st.divider()

    # ── Statut cache ──────────────────────────────────────────────────────────
    lec_cache = load_leclerc_cache(WINE_TYPES.get(wine_type_label, ""))
    viv_cache = load_vivino_cache()
    if lec_cache:
        st.caption(f"📦 Leclerc : {fmt_cache_age(lec_cache['cached_at'])}")
    else:
        st.caption("📦 Leclerc : pas de cache")
    st.caption(f"🍷 Vivino : {len(viv_cache)} vins en cache")

    scrape_btn    = st.button("🔄 Charger / Vérifier stock", use_container_width=True, type="primary")
    refresh_vivino = st.button("🍷 Rafraîchir notes Vivino", use_container_width=True)
    st.caption(f"📍 Leclerc Blagnac · magasin {STORE_CODE}")

# ── Session state ──────────────────────────────────────────────────────────────
if "wines" not in st.session_state:
    st.session_state.wines = []
if "loaded_type" not in st.session_state:
    st.session_state.loaded_type = None

# Recharger si on change de type de vin
if wine_type_slug != st.session_state.loaded_type:
    st.session_state.wines = []

if scrape_btn or refresh_vivino or not st.session_state.wines:
    st.session_state.wines = []

    with st.status(f"🔍 Chargement {wine_type_label}…", expanded=True) as status:
        log_box = st.empty()
        logs    = []
        def log(msg):
            logs.append(msg)
            log_box.markdown("\n\n".join(logs[-8:]))

        try:
            raw_wines = scrape_and_enrich(
                wine_type_slug,
                force_leclerc = scrape_btn,           # forcer re-scrape Leclerc
                force_vivino  = refresh_vivino,        # forcer re-scrape Vivino
                log=log,
            )
        except Exception as e:
            st.error(
                f"❌ Erreur Selenium : {e}\n\n"
                "**Vérifiez que `packages.txt` contient :**\n```\nchromium\nchromium-driver\n```"
            )
            st.stop()

        if not raw_wines:
            st.error("Aucun produit récupéré — réessayez dans quelques instants.")
            st.stop()

        n_rated    = sum(1 for w in raw_wines if w.get("rating"))
        n_dispo    = sum(1 for w in raw_wines if w.get("available", True))
        n_indispo  = len(raw_wines) - n_dispo

        log(f"✅ {n_dispo} vins dispo, {n_indispo} indisponible(s), {n_rated} notes Vivino")

        st.session_state.wines       = raw_wines
        st.session_state.loaded_type = wine_type_slug
        status.update(
            label=f"✅ {n_dispo} vins en rayon · {n_rated} notes Vivino",
            state="complete",
        )



# ── Affichage ──────────────────────────────────────────────────────────────────
wines = st.session_state.wines

if wines:
    filtered = [
        w for w in wines
        if w["price"] <= price_max
        and (rating_min == 0 or (w.get("rating") and w["rating"] >= rating_min))
        and (not search or search.lower() in w["name"].lower())
        and (not only_vintage_ok or w.get("vintage_match") is True)
        and (not only_available or w.get("available", True))
    ]

    sort_key = {
        "Meilleur ratio ★/€": lambda x: -(x.get("ratio") or 0),
        "Meilleure note":      lambda x: -(x.get("rating") or 0),
        "Prix croissant":      lambda x:  x["price"],
        "Prix décroissant":    lambda x: -x["price"],
    }
    filtered.sort(key=sort_key[sort_by])

    # Métriques
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("🍷 Vins affichés", len(filtered))
    with c2:
        avg_p = sum(w["price"] for w in filtered if w["price"]) / max(len(filtered), 1)
        st.metric("💶 Prix moyen", f"{avg_p:.2f} €".replace(".", ","))
    with c3:
        rated = [w for w in filtered if w.get("rating")]
        avg_r = sum(w["rating"] for w in rated) / max(len(rated), 1) if rated else 0
        st.metric("⭐ Note moy. Vivino", f"★ {avg_r:.1f}" if rated else "—")
    with c4:
        best = max(filtered, key=lambda x: x.get("ratio") or 0, default=None)
        st.metric("🏆 Meilleur ratio", f"{best['ratio']:.2f}" if best and best.get("ratio") else "—")

    # Export CSV
    with st.expander("📥 Exporter en CSV"):
        df = pd.DataFrame([{
            "Nom":              w["name"],
            "Millésime":        w.get("vintage", ""),
            "Prix (€)":         w["price"],
            "EAN":              w.get("ean", ""),
            "Note Vivino":      w.get("rating", ""),
            "Nb avis":          w.get("ratings_count", ""),
            "Millésime Vivino": w.get("vivino_year", ""),
            "Millésime OK":     w.get("vintage_match", ""),
            "Ratio ★/€":       w.get("ratio", ""),
            "URL Leclerc":      w.get("url", ""),
            "URL Vivino":       w.get("vivino_url", ""),
        } for w in filtered])
        st.download_button(
            "⬇️ Télécharger CSV",
            df.to_csv(index=False, sep=";").encode("utf-8-sig"),
            f"vins_leclerc_{wine_type_slug}.csv", "text/csv",
        )

    # Légende millésime
    n_bad = sum(1 for w in filtered if w.get("vintage_match") is False)
    if n_bad:
        st.warning(
            f"⚠️ **{n_bad} vins** ont un millésime différent entre Leclerc et Vivino "
            f"(bordure orange). Vérifiez manuellement sur Vivino.",
            icon=None,
        )

    st.divider()
    st.markdown("### 🏅 Classement qualité / prix")
    max_ratio = max((w.get("ratio") or 0 for w in filtered), default=1)
    for i, wine in enumerate(filtered):
        st.markdown(wine_card_html(wine, i + 1, max_ratio), unsafe_allow_html=True)

else:
    st.info("👈 Sélectionnez un type de vin et cliquez sur **Lancer / Rafraîchir**.")

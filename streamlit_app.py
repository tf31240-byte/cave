"""
Cave Leclerc Blagnac × Vivino
─────────────────────────────
Cache JSON persistant sur disque → survit aux déconnexions.
Scraping Vivino : page recherche → URL /w/{id} → fiche JSON-LD (rating + count).
"""

import re, json, time
import streamlit as st
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
STORE_CODE        = "1431"
MAX_PAGES         = 15
# Leclerc : cache 12h (on met à jour manuellement via le bouton de toute façon)
LECLERC_CACHE_TTL = 12 * 3600
# Vivino : pas de TTL — refresh manuel uniquement

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

WINE_TYPES = {
    "🔴 Rouge":    "vins-rouges",
    "⚪ Blanc":    "vins-blancs",
    "🌸 Rosé":     "vins-roses",
    "🍾 Mousseux": "vins-mousseux-et-petillants",
}

# Essaie 96 articles par page pour aller plus vite (moins de pages à charger)
LECLERC_PAGE_SIZE = 96

st.set_page_config(
    page_title="Cave Leclerc Blagnac × Vivino",
    page_icon="🍷",
    layout="wide",
    initial_sidebar_state="collapsed",   # fermé par défaut sur mobile
)

# ═══════════════════════════════════════════════════════════════════════════
# CSS  (desktop + mobile responsive)
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=DM+Mono&family=DM+Sans:wght@300;400;500&display=swap');

/* ── Base ── */
html,body,[class*="css"]{font-family:'DM Sans',sans-serif}
.main-title{font-family:'Playfair Display',serif;font-size:clamp(1.4rem,4vw,2.2rem);
  font-weight:900;color:#1A0810;line-height:1.1}
.main-title span{color:#C9A84C}
.subtitle{color:#8B6B72;font-size:.82rem;letter-spacing:.08em;text-transform:uppercase}

/* ── Carte vin ── */
.wine-card{
  background:white;border-radius:10px;
  padding:.8rem 1rem;margin-bottom:.5rem;
  border-left:4px solid #6B1A2A;
  box-shadow:0 2px 10px rgba(26,8,16,.08);
  display:grid;
  grid-template-columns:2.2rem 1fr auto auto auto;
  align-items:center;gap:.6rem}
.wine-card.top1{border-left-color:#C9A84C;background:#fffdf4}
.wine-card.top2{border-left-color:#9C9C9C}
.wine-card.top3{border-left-color:#CD7F32}
.wine-card.vintage-warn{border-right:3px solid #f59e0b}
.wine-card.unavailable{opacity:.4;filter:grayscale(70%)}

.wine-rank{font-family:'DM Mono',monospace;font-size:1.2rem;text-align:center}
.wine-info{min-width:0}
.wine-name{font-weight:700;font-size:.9rem;color:#1A0810;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wine-sub{font-size:.7rem;color:#8B6B72;margin-top:.1rem}
.wine-links{display:flex;gap:.35rem;margin-top:.3rem;flex-wrap:wrap}
.lnk{font-size:.65rem;text-decoration:none;border-radius:4px;
  padding:2px 8px;border:1px solid;white-space:nowrap;font-family:'DM Mono'}
.lnk-lec{color:#2563eb;border-color:#2563eb}
.lnk-viv{color:#7B2D8B;border-color:#7B2D8B}

.wine-rating{text-align:center;min-width:90px}
.stars{color:#C9A84C;font-size:.9rem;letter-spacing:1px;display:block}
.r-num{font-family:'DM Mono';font-size:.85rem;font-weight:700;color:#1A0810}
.r-cnt{font-size:.6rem;color:#8B6B72}
.no-rat{font-size:.68rem;color:#ccc;font-style:italic;text-align:center;min-width:80px}

.wine-price{font-family:'DM Mono',monospace;font-size:1rem;
  font-weight:700;color:#1A0810;text-align:right;white-space:nowrap}

.ratio-wrap{min-width:100px}
.ratio-num{font-family:'DM Mono';font-size:.75rem;color:#6B1A2A;font-weight:600}
.ratio-bar{background:rgba(107,26,42,.1);border-radius:3px;height:5px;
  overflow:hidden;margin-top:3px}
.ratio-fill{height:100%;background:linear-gradient(90deg,#6B1A2A,#C9A84C);border-radius:3px}

.badge{display:inline-block;padding:.1rem .4rem;border-radius:3px;
  font-size:.58rem;font-family:'DM Mono';margin-right:.15rem;margin-top:.2rem}
.b-deal{background:rgba(201,168,76,.15);color:#8B6030;border:1px solid rgba(201,168,76,.4)}
.b-top{background:rgba(107,26,42,.08);color:#6B1A2A;border:1px solid rgba(107,26,42,.2)}

/* ── MOBILE : < 640px ── */
@media (max-width:640px){
  .wine-card{
    grid-template-columns:1.8rem 1fr;
    grid-template-rows:auto auto auto;
    gap:.3rem}
  .wine-rating{grid-column:1/3;display:flex;
    align-items:center;gap:.6rem;justify-content:flex-start;min-width:0}
  .stars{display:inline}
  .wine-price{grid-column:1/3;text-align:left;font-size:.95rem}
  .ratio-wrap{display:none}          /* caché sur mobile */
  .wine-name{white-space:normal}
}

/* ── Sort pills ── */
.sort-bar{display:flex;gap:.4rem;flex-wrap:wrap;margin-bottom:.6rem}
.sort-pill{display:inline-block;padding:.3rem .8rem;border-radius:20px;
  font-size:.78rem;cursor:pointer;border:1.5px solid #6B1A2A;
  color:#6B1A2A;background:white;white-space:nowrap;font-family:'DM Mono'}
.sort-pill.active{background:#6B1A2A;color:white}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════════════════════════

def _lec_path(slug): return CACHE_DIR / f"leclerc_{slug}.json"
def _viv_path():     return CACHE_DIR / "vivino.json"

def load_leclerc_cache(slug: str) -> dict | None:
    p = _lec_path(slug)
    if not p.exists(): return None
    try:
        d = json.loads(p.read_text("utf-8"))
        if time.time() - d.get("cached_at", 0) < LECLERC_CACHE_TTL:
            return d
    except Exception: pass
    return None

def save_leclerc_cache(slug: str, wines: list) -> None:
    _lec_path(slug).write_text(
        json.dumps({"cached_at": time.time(), "slug": slug, "wines": wines},
                   ensure_ascii=False, indent=2), "utf-8")

def load_vivino_cache() -> dict:
    p = _viv_path()
    if p.exists():
        try: return json.loads(p.read_text("utf-8"))
        except Exception: pass
    return {}

def save_vivino_cache(cache: dict) -> None:
    _viv_path().write_text(json.dumps(cache, ensure_ascii=False, indent=2), "utf-8")

def fmt_age(ts: float) -> str:
    if not ts: return "importé"
    age = time.time() - ts
    if age < 60:    return "à l'instant"
    if age < 3600:  return f"il y a {int(age/60)} min"
    if age < 86400: return f"il y a {int(age/3600)} h"
    return f"il y a {int(age/86400)} j"


# ═══════════════════════════════════════════════════════════════════════════
# SCRAPING LECLERC
# ═══════════════════════════════════════════════════════════════════════════

def leclerc_url(slug: str, page: int = 1) -> str:
    """URL avec pageSize=96 pour réduire le nb de pages à charger."""
    base = f"https://www.e.leclerc/cat/{slug}"
    params = f"pageSize={LECLERC_PAGE_SIZE}&page={page}"
    return f"{base}?{params}#oaf-sign-code={STORE_CODE}"

def _parse_price(card) -> float:
    blk = card.find(class_=lambda c: c and "block-price-and-availability" in c.split())
    if blk:
        m = re.search(r"(\d+)€,(\d{2})", blk.get_text())
        if m: return float(f"{m.group(1)}.{m.group(2)}")
    ue = card.find_all(class_=lambda c: c and "price-unit"  in c.split())
    ce = card.find_all(class_=lambda c: c and "price-cents" in c.split())
    if ue and ce:
        try:
            return float(f"{ue[0].get_text(strip=True)}.{ce[0].get_text(strip=True).lstrip(',').strip()}")
        except ValueError: pass
    return 0.0

def parse_cards(html: str) -> list:
    wines = []
    for card in BeautifulSoup(html, "html.parser").find_all("app-product-card"):
        lbl = card.find(class_="product-label")
        name = lbl.get_text(strip=True) if lbl else ""
        if not name: continue
        lnk  = card.find("a", href=True)
        href = lnk["href"] if lnk else ""
        url  = href if href.startswith("http") else f"https://www.e.leclerc{href}"
        em   = re.search(r"offer_m-(\d{13})-\d+", str(card))
        ean  = em.group(1) if em else ""
        if not ean:
            m2 = re.search(r"-(\d{13})$", url)
            ean = m2.group(1) if m2 else ""
        img = card.find("img")
        image = ""
        if img:
            image = img.get("src") or img.get("data-src") or \
                    img.get("data-srcset", "").split()[0] or ""
        ym = re.search(r"\b(19[5-9]\d|20[0-3]\d)\b", name)
        wines.append({"name": name, "price": _parse_price(card),
                      "url": url, "ean": ean, "image": image,
                      "vintage": int(ym.group(1)) if ym else None})
    return wines

def get_nb_pages(html: str) -> int:
    nums = [int(m.group(1))
            for a in BeautifulSoup(html, "html.parser").find_all("a", href=True)
            if (m := re.search(r"[?&]page=(\d+)", a["href"]))]
    return max(nums) if nums else 1


# ═══════════════════════════════════════════════════════════════════════════
# VIVINO — query + parsing
# ═══════════════════════════════════════════════════════════════════════════

def build_query(wine_name: str) -> str:
    nom = re.split(r",\s*|\s+-\s+", wine_name)[0].strip()
    nom = re.sub(r"^(Magnum|Demi-bouteille)\s+", "", nom, flags=re.I).strip()
    nom = re.sub(r"\b(19|20)\d{2}\b", "", nom).strip().strip("-").strip()
    if re.match(r"^[A-Z][A-Z\s'\-]+$", nom): nom = nom.title()
    cut = {"Cuvée", "Cuvee", "Vieilles", "Vieille", "Grande"}
    words = nom.split()
    for i, w in enumerate(words[2:], 2):
        if w in cut: nom = " ".join(words[:i]); break
    m = re.search(r"-\s*([\w\s\-]+?)\s*(?:AOP|IGP|AOC|Vin de France)", wine_name, re.I)
    app = m.group(1).strip() if m else ""
    parts = [nom]
    if app and app.lower() not in nom.lower(): parts.append(app)
    return " ".join(parts)


def vivino_url_from_search(html: str) -> str | None:
    """
    Trouve la 1ère URL de fiche vin dans la page de recherche Vivino.
    Les URLs Vivino sont de la forme /fr/nom-du-vin/w/12345
    → on cherche le pattern /w/{id} (avec id numérique).
    """
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Pattern réel Vivino : /fr/wine-name/w/12345 ou /wine-name/w/12345
        if re.search(r"/w/\d+", href) and "search" not in href:
            return href if href.startswith("http") else f"https://www.vivino.com{href}"
    return None


def parse_wine_jsonld(html: str) -> dict:
    """
    Extrait note + nb avis depuis la fiche Vivino via JSON-LD aggregateRating.
    C'est la source la plus fiable — injectée côté serveur, toujours présente.
    """
    rating, count = None, 0
    soup = BeautifulSoup(html, "html.parser")

    # ── 1. JSON-LD <script type="application/ld+json"> ─────────────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data  = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                ag = item.get("aggregateRating", {})
                if not ag: continue
                rv = ag.get("ratingValue")
                rc = ag.get("ratingCount") or ag.get("reviewCount")
                if rv:
                    v = round(float(str(rv).replace(",", ".")), 1)
                    if 2.5 <= v <= 5.0: rating = v
                if rc:
                    count = int(re.sub(r"[^\d]", "", str(rc)) or 0)
                if rating: break
        except Exception: pass
        if rating: break

    # ── 2. JSON inline (ratings_average / ratings_count) ───────────────────
    if not rating:
        m = re.search(r'"ratings_average"\s*:\s*([\d.]+)', html)
        if m:
            v = round(float(m.group(1)), 1)
            if 2.5 <= v <= 5.0: rating = v
    if not count:
        m = re.search(r'"ratings_count"\s*:\s*(\d+)', html)
        if m: count = int(m.group(1))

    # ── 3. Classes React hashées (dernier recours) ──────────────────────────
    if not rating:
        for el in soup.find_all(class_=lambda c: c and "averageValue" in c):
            try:
                v = round(float(el.get_text(strip=True).replace(",", ".")), 1)
                if 2.5 <= v <= 5.0: rating = v; break
            except ValueError: pass
    if not count:
        for el in soup.find_all(class_=lambda c: c and "numRatings" in c):
            d = re.sub(r"[^\d]", "", el.get_text())
            if d: count = int(d); break

    return {"rating": rating, "ratings_count": count}


# ═══════════════════════════════════════════════════════════════════════════
# SELENIUM
# ═══════════════════════════════════════════════════════════════════════════

def make_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import os
    opts = Options()
    for arg in ["--headless", "--no-sandbox", "--disable-dev-shm-usage",
                "--disable-gpu", "--window-size=1280,900",
                "--disable-blink-features=AutomationControlled"]:
        opts.add_argument(arg)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    for b in ["/usr/bin/chromium", "/usr/bin/chromium-browser",
              "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable"]:
        if os.path.exists(b): opts.binary_location = b; break
    for d in ["/usr/bin/chromedriver", "/usr/lib/chromium/chromedriver",
              "/usr/lib/chromium-browser/chromedriver"]:
        if os.path.exists(d): return webdriver.Chrome(service=Service(d), options=opts)
    return webdriver.Chrome(options=opts)


def scrape_leclerc_full(slug: str, log=None) -> list:
    """Scrape toutes les pages Leclerc avec pageSize=96."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    driver = make_driver()
    wines, seen = [], set()
    try:
        url1 = leclerc_url(slug, 1)
        if log: log(f"🌐 Chargement {url1}…")
        driver.get(url1)
        try: WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card")))
        except Exception: pass
        time.sleep(2)
        html = driver.page_source
        nb   = min(get_nb_pages(html), MAX_PAGES)
        for w in parse_cards(html):
            if w["ean"] not in seen: seen.add(w["ean"]); wines.append(w)
        if log: log(f"✅ Page 1 : {len(wines)} vins — {nb} page(s)")
        for p in range(2, nb + 1):
            if log: log(f"🌐 Page {p}/{nb}…")
            driver.get(leclerc_url(slug, p))
            try: WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card")))
            except Exception: pass
            time.sleep(2)
            new = [w for w in parse_cards(driver.page_source) if w["ean"] not in seen]
            if not new: break
            for w in new: seen.add(w["ean"])
            wines.extend(new)
            if log: log(f"✅ Page {p} : +{len(new)} (total {len(wines)})")
    finally:
        try: driver.quit()
        except: pass
    return wines


def check_availability(slug: str, cached_wines: list, log=None) -> list:
    """
    Vérifie les EANs actuellement en rayon à Blagnac.
    Plus léger qu'un scrape complet : on veut juste savoir quels vins sont dispo.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    current_eans = set()
    driver = make_driver()
    try:
        for p in range(1, MAX_PAGES + 1):
            driver.get(leclerc_url(slug, p))
            try: WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card")))
            except Exception: pass
            time.sleep(1.5)
            page_w = parse_cards(driver.page_source)
            if not page_w: break
            current_eans.update(w["ean"] for w in page_w)
            if p == 1 and get_nb_pages(driver.page_source) == 1: break
    except Exception as e:
        if log: log(f"⚠️ Vérif. stock échouée : {e}")
    finally:
        try: driver.quit()
        except: pass

    for w in cached_wines:
        w["available"] = w.get("ean", "") in current_eans
    nok = sum(1 for w in cached_wines if w.get("available"))
    if log: log(f"✅ {nok} dispo, {len(cached_wines)-nok} indispo à Blagnac")
    return cached_wines


def fetch_vivino(driver, wine_name: str, vintage: int | None) -> dict:
    """
    2 navigations : page recherche → URL fiche (/w/{id}) → fiche JSON-LD.
    Fix clé : le pattern /w/\\d+ au lieu de /wines/... (erreur précédente).
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    EMPTY = {"rating": None, "ratings_count": 0,
             "vivino_url": "", "vivino_year": None, "vintage_match": None}
    query = build_query(wine_name)

    # ── Étape 1 : page de recherche → URL de la fiche ──────────────────────
    try:
        driver.get(f"https://www.vivino.com/search/wines"
                   f"?q={requests.utils.quote(query)}&language=fr")
        # Attendre qu'une carte vin soit visible
        try: WebDriverWait(driver, 9).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "[class*='wineCard'],[class*='wine-card'],[class*='averageValue'],"
                 "[href*='/w/']")))
        except Exception: pass
        time.sleep(1)
        wine_url = vivino_url_from_search(driver.page_source)
    except Exception:
        return EMPTY

    if not wine_url:
        return EMPTY

    # ── Étape 2 : fiche du vin → JSON-LD ───────────────────────────────────
    try:
        driver.get(wine_url)
        try: WebDriverWait(driver, 9).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "script[type='application/ld+json']")))
        except Exception: pass
        time.sleep(1)
        d = parse_wine_jsonld(driver.page_source)
    except Exception:
        return EMPTY

    if not d.get("rating"):
        return EMPTY

    # Millésime depuis l'URL finale (?year=2023)
    vy = None
    m  = re.search(r"[?&]year=(\d{4})", driver.current_url)
    if m: vy = int(m.group(1))

    vmatch = None
    if vintage and vy: vmatch = (vintage == vy)
    elif not vintage:  vmatch = True

    return {"rating": d["rating"], "ratings_count": d["ratings_count"],
            "vivino_url": wine_url, "vivino_year": vy, "vintage_match": vmatch}


# ═══════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════

def load_wines_from_cache(slug: str) -> list:
    """
    Charge les vins depuis le cache Leclerc + cache Vivino.
    Retourne une liste enrichie sans aucun scraping.
    Appelé au premier chargement de la page.
    """
    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache()
    if not lc:
        return []
    wines = lc["wines"]
    for w in wines:
        key = build_query(w["name"])
        cv  = vc.get(key, {})
        w.setdefault("available", True)
        if cv.get("rating") is not None:
            for k, v in cv.items():
                if k != "cached_at": w[k] = v
            w["ratio"] = round((cv["rating"] / w["price"]) * 10, 3) \
                         if w.get("price", 0) > 0 else 0
        else:
            w.setdefault("rating", None)
            w.setdefault("ratings_count", 0)
            w.setdefault("ratio", 0)
            w.setdefault("vivino_url", "")
            w.setdefault("vivino_year", None)
            w.setdefault("vintage_match", None)
    return wines


def run_check_stock(slug: str, log=None) -> list:
    """
    Vérifie uniquement la disponibilité Leclerc (bouton 🔄).
    Recharge le cache Leclerc si expiré, sinon juste EANs actuels.
    Notes Vivino = cache, pas de scraping Vivino.
    """
    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache()
    now = time.time()

    if lc:
        if log: log(f"📦 Cache Leclerc ({fmt_age(lc['cached_at'])}) — vérif. stock…")
        wines = lc["wines"]
        wines = check_availability(slug, wines, log=log)
        # Mettre à jour le cache avec les nouvelles disponibilités
        save_leclerc_cache(slug, wines)
    else:
        if log: log("🚀 Pas de cache — scrape Leclerc complet…")
        wines = scrape_leclerc_full(slug, log=log)
        for w in wines: w["available"] = True
        save_leclerc_cache(slug, wines)
        if log: log(f"💾 Cache Leclerc sauvegardé ({len(wines)} vins)")

    # Injecter Vivino depuis le cache
    for w in wines:
        key = build_query(w["name"])
        cv  = vc.get(key, {})
        if cv.get("rating") is not None:
            for k, v in cv.items():
                if k != "cached_at": w[k] = v
            w["ratio"] = round((cv["rating"] / w["price"]) * 10, 3) \
                         if w.get("price", 0) > 0 else 0
        else:
            w.setdefault("rating", None)
            w.setdefault("ratings_count", 0)
            w.setdefault("ratio", 0)
            w.setdefault("vivino_url", "")
            w.setdefault("vivino_year", None)
            w.setdefault("vintage_match", None)
    return wines


def run_refresh_vivino(slug: str, log=None) -> list:
    """
    Re-scrape Vivino pour TOUS les vins (bouton 🍷).
    Lent (~3s/vin) mais récupère notes + nb avis + URL.
    """
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("🚀 Pas de cache Leclerc — scrape complet…")
        wines = scrape_leclerc_full(slug, log=log)
        for w in wines: w["available"] = True
        save_leclerc_cache(slug, wines)
    else:
        wines = lc["wines"]
        for w in wines: w.setdefault("available", True)

    vc  = load_vivino_cache()
    now = time.time()

    if log: log(f"🍷 Scraping Vivino — {len(wines)} vins (search → fiche JSON-LD)…")
    driver = make_driver()
    found  = 0
    try:
        for i, wine in enumerate(wines):
            vd  = fetch_vivino(driver, wine["name"], wine.get("vintage"))
            wine.update(vd)
            wine["ratio"] = round((vd["rating"] / wine["price"]) * 10, 3) \
                            if vd.get("rating") and wine.get("price", 0) > 0 else 0
            key = build_query(wine["name"])
            vc[key] = {**vd, "cached_at": now}
            if vd.get("rating"):
                found += 1
                cnt_s = f"{vd['ratings_count']:,}".replace(",", "\u202f") \
                        if vd["ratings_count"] else "—"
                if log: log(f"  ✅ {wine['name'][:42]}\n"
                            f"     ★ {vd['rating']} · {cnt_s} avis")
            if (i + 1) % 10 == 0 or i == len(wines) - 1:
                if log: log(f"  🍷 {i+1}/{len(wines)} — {found} notes trouvées")
            time.sleep(0.3)
    finally:
        try: driver.quit()
        except: pass

    save_vivino_cache(vc)
    if log: log(f"💾 Cache Vivino sauvegardé ({found}/{len(wines)} notes)")
    return wines


# ═══════════════════════════════════════════════════════════════════════════
# RENDU HTML
# ═══════════════════════════════════════════════════════════════════════════

def stars(r: float) -> str:
    return "".join("★" if r >= i else ("½" if r >= i-.5 else "☆") for i in range(1, 6))

def fmt_count(n: int) -> str:
    if not n: return "—"
    return f"{n:,}".replace(",", "\u202f")


def wine_card_html(wine: dict, rank: int, max_ratio: float) -> str:
    cls = {1:"top1", 2:"top2", 3:"top3"}.get(rank, "")
    if wine.get("vintage_match") is False: cls = (cls + " vintage-warn").strip()
    if not wine.get("available", True):    cls = (cls + " unavailable").strip()

    icon = {1:"🥇", 2:"🥈", 3:"🥉"}.get(rank, f"<span style='font-size:.75rem'>#{rank}</span>")

    # Nom (lien Leclerc direct)
    name = wine["name"]
    if wine.get("url"):
        name_html = (f'<a href="{wine["url"]}" target="_blank" '
                     f'style="color:#1A0810;text-decoration:none">{name}</a>')
    else:
        name_html = name

    yr = (f' <span style="color:#8B6B72;font-size:.68rem;font-weight:400">'
          f'{wine["vintage"]}</span>') if wine.get("vintage") else ""
    unavail = (' <span style="font-size:.62rem;color:#dc2626">⛔ indispo</span>'
               if not wine.get("available", True) else "")

    # Alerte millésime
    mil = ""
    if wine.get("vivino_year") and wine.get("vintage") and wine["vivino_year"] != wine["vintage"]:
        mil = (f'<div class="wine-sub" style="color:#c17a00">'
               f'⚠️ Vivino={wine["vivino_year"]} / Leclerc={wine["vintage"]}</div>')

    # Liens 🛒 Leclerc et 🍷 Vivino
    links = []
    if wine.get("url"):
        links.append(f'<a href="{wine["url"]}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>')
    if wine.get("vivino_url"):
        links.append(f'<a href="{wine["vivino_url"]}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>')
    links_html = (f'<div class="wine-links">' + "".join(links) + "</div>") if links else ""

    # Badges
    ratio  = wine.get("ratio") or 0
    rating = wine.get("rating")
    badges = ""
    if ratio > 0 and rank <= 5:  badges += '<span class="badge b-deal">🔥 Top ratio</span>'
    if rating and rating >= 4.2: badges += '<span class="badge b-top">★ Top noté</span>'

    # Note Vivino
    if rating:
        cnt = wine.get("ratings_count") or 0
        rating_col = (f'<div class="wine-rating">'
                      f'<span class="stars">{stars(rating)}</span>'
                      f'<span class="r-num">{rating:.1f}</span>'
                      f'<span class="r-cnt">{fmt_count(cnt)} avis</span>'
                      f'</div>')
    else:
        rating_col = '<div class="no-rat">—<br>Vivino</div>'

    # Ratio (barre)
    pct = min(100, (ratio / max_ratio) * 100) if max_ratio > 0 else 0
    ratio_col = (
        f'<div class="ratio-wrap">'
        f'<div class="ratio-num">{ratio:.3f}</div>'
        f'<div class="ratio-bar"><div class="ratio-fill" style="width:{pct:.1f}%"></div></div>'
        f'</div>'
    ) if ratio else '<div class="ratio-wrap" style="color:#ccc;font-size:.72rem">—</div>'

    price_s = f'{wine["price"]:.2f} €'.replace(".", ",")

    return (f'<div class="wine-card {cls}">'
            f'<div class="wine-rank">{icon}</div>'
            f'<div class="wine-info">'
            f'<div class="wine-name">{name_html}{yr}{unavail}</div>'
            f'{mil}{links_html}'
            f'<div>{badges}</div>'
            f'</div>'
            f'{rating_col}'
            f'<div class="wine-price">{price_s}</div>'
            f'{ratio_col}'
            f'</div>')


# ═══════════════════════════════════════════════════════════════════════════
# APP STREAMLIT
# ═══════════════════════════════════════════════════════════════════════════

st.markdown('<div class="main-title">Cave <span>Leclerc Blagnac</span> × Vivino</div>',
            unsafe_allow_html=True)
st.markdown('<div class="subtitle">Comparateur qualité / prix · Blagnac</div>',
            unsafe_allow_html=True)

# ── SESSION STATE ─────────────────────────────────────────────────────────
# On utilise un flag explicite pour ne pas re-déclencher sur chaque filtre/tri
for k, v in [("wines", []), ("loaded_slug", None), ("data_ready", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── SIDEBAR ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🍾 Type de vin")
    wine_label = st.selectbox("Type", list(WINE_TYPES), label_visibility="collapsed")
    slug       = WINE_TYPES[wine_label]

    st.divider()
    st.markdown("### 🔄 Mise à jour")

    # Statut cache
    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache()
    n_note  = sum(1 for v in vc.values() if v.get("rating"))
    n_count = sum(1 for v in vc.values() if (v.get("ratings_count") or 0) > 0)
    n_url   = sum(1 for v in vc.values() if v.get("vivino_url"))

    st.caption(f"📦 **Leclerc** : {fmt_age(lc['cached_at']) if lc else '❌ pas de cache'}")
    st.caption(f"🍷 **Vivino** : {n_note} notes · {n_count} nb avis · {n_url} URLs")

    st.info(
        "💡 **Les données sont en cache sur le serveur.**\n\n"
        "Si vous avez déjà cliqué sur un bouton, revenez plus tard : "
        "les données se chargent instantanément depuis le cache.",
        icon=None,
    )

    btn_stock  = st.button("🔄 Vérifier disponibilité",
                           use_container_width=True, type="primary",
                           help="Vérifie les vins en rayon. Vivino depuis le cache.")
    btn_vivino = st.button("🍷 Rafraîchir notes Vivino",
                           use_container_width=True,
                           help="Re-scrape Vivino : lent ~3s/vin mais récupère note + nb avis + URL.")
    st.caption(f"📍 Leclerc Blagnac · magasin {STORE_CODE}")

    st.divider()
    st.markdown("### 🔧 Filtres")
    search   = st.text_input("🔍 Recherche", placeholder="Bordeaux, Guigal…")
    price_max = st.slider("💶 Prix max (€)", 0, 200, 200, step=5)
    rating_min = st.select_slider("⭐ Note min",
        options=[0.0, 3.0, 3.5, 3.8, 4.0, 4.2, 4.5], value=0.0,
        format_func=lambda x: "Toutes" if x == 0 else f"≥ {x} ★")
    only_vintage = st.checkbox("✅ Millésime confirmé", False)
    only_dispo   = st.checkbox("🏪 Dispos à Blagnac", True)

# ── CHARGEMENT / SCRAPING ─────────────────────────────────────────────────
# Changement de type de vin → reset
if slug != st.session_state.loaded_slug:
    st.session_state.wines     = []
    st.session_state.data_ready = False

# Premier chargement sans bouton : essayer le cache disque silencieusement
if not st.session_state.data_ready and not btn_stock and not btn_vivino:
    cached = load_wines_from_cache(slug)
    if cached:
        st.session_state.wines      = cached
        st.session_state.loaded_slug = slug
        st.session_state.data_ready  = True

# Bouton "Vérifier disponibilité"
if btn_stock:
    st.session_state.wines = []
    st.session_state.data_ready = False
    with st.status("🔄 Vérification du stock…", expanded=True) as status:
        logs, log_box = [], st.empty()
        def log(msg): logs.append(msg); log_box.markdown("\n\n".join(logs[-10:]))
        try:
            raw = run_check_stock(slug, log=log)
        except Exception as e:
            st.error(f"❌ Erreur Selenium : {e}\n\n"
                     "Vérifiez `packages.txt` :\n```\nchromium\nchromium-driver\n```")
            st.stop()
        n_dispo  = sum(1 for w in raw if w.get("available", True))
        n_rated  = sum(1 for w in raw if w.get("rating"))
        st.session_state.wines       = raw
        st.session_state.loaded_slug = slug
        st.session_state.data_ready  = True
        status.update(label=f"✅ {n_dispo} vins dispo · {n_rated} notes Vivino", state="complete")

# Bouton "Rafraîchir Vivino"
if btn_vivino:
    st.session_state.wines = []
    st.session_state.data_ready = False
    with st.status("🍷 Scraping Vivino…", expanded=True) as status:
        logs, log_box = [], st.empty()
        def log(msg): logs.append(msg); log_box.markdown("\n\n".join(logs[-10:]))
        try:
            raw = run_refresh_vivino(slug, log=log)
        except Exception as e:
            st.error(f"❌ Erreur Selenium : {e}")
            st.stop()
        n_rated  = sum(1 for w in raw if w.get("rating"))
        n_counts = sum(1 for w in raw if (w.get("ratings_count") or 0) > 0)
        st.session_state.wines       = raw
        st.session_state.loaded_slug = slug
        st.session_state.data_ready  = True
        status.update(
            label=f"✅ {n_rated} notes Vivino · {n_counts} nb avis",
            state="complete")

wines = st.session_state.wines
if not wines:
    st.markdown("<br>", unsafe_allow_html=True)
    st.info("👈 Ouvrez le menu et cliquez sur **Vérifier disponibilité** pour charger les vins.")
    st.stop()

# ── FILTRE ────────────────────────────────────────────────────────────────
filtered = [w for w in wines
    if (w.get("price") or 0) <= price_max
    and (rating_min == 0 or (w.get("rating") and w["rating"] >= rating_min))
    and (not search or search.lower() in w["name"].lower())
    and (not only_vintage or w.get("vintage_match") is True)
    and (not only_dispo or w.get("available", True))]

# ── TRI (pills radio inline) ──────────────────────────────────────────────
SORTS = {
    "★/€ Ratio":     lambda x: -(x.get("ratio") or 0),
    "⭐ Note":        lambda x: -(x.get("rating") or 0),
    "💶 Prix ↑":     lambda x:  x.get("price") or 999,
    "💶 Prix ↓":     lambda x: -(x.get("price") or 0),
    "💰 Moins cher": lambda x:  x.get("price") or 999,
}

sort_cols = st.columns(len(SORTS))
if "sort_key" not in st.session_state:
    st.session_state.sort_key = "★/€ Ratio"

for col, (label, _) in zip(sort_cols, SORTS.items()):
    with col:
        active = st.session_state.sort_key == label
        if st.button(label, key=f"sort_{label}",
                     type="primary" if active else "secondary",
                     use_container_width=True):
            st.session_state.sort_key = label

# Dédoublonner les labels similaires
SORTS_CLEAN = {
    "★/€ Ratio":     lambda x: -(x.get("ratio") or 0),
    "⭐ Note":        lambda x: -(x.get("rating") or 0),
    "💶 Prix ↑":     lambda x:  x.get("price") or 999,
    "💶 Prix ↓":     lambda x: -(x.get("price") or 0),
    "💰 Moins cher": lambda x:  x.get("price") or 999,
}
filtered.sort(key=SORTS_CLEAN.get(st.session_state.sort_key, SORTS_CLEAN["★/€ Ratio"]))

# ── ONGLETS ───────────────────────────────────────────────────────────────
tab_rank, tab_data, tab_export = st.tabs(["🏅 Classement", "📊 Données & Cache", "📥 Export"])

# ── CLASSEMENT ────────────────────────────────────────────────────────────
with tab_rank:
    c1, c2, c3, c4 = st.columns(4)
    prices = [w["price"] for w in filtered if w.get("price")]
    rated  = [w["rating"] for w in filtered if w.get("rating")]
    best   = max(filtered, key=lambda x: x.get("ratio") or 0, default=None)
    with c1: st.metric("🍷 Vins", len(filtered))
    with c2: st.metric("💶 Prix moy.",
        f"{sum(prices)/len(prices):.2f} €".replace(".", ",") if prices else "—")
    with c3: st.metric("⭐ Note moy.",
        f"★ {sum(rated)/len(rated):.2f}" if rated else "—")
    with c4: st.metric("🏆 Top ratio",
        f"{best['ratio']:.3f}" if best and best.get("ratio") else "—")

    n_bad = sum(1 for w in filtered if w.get("vintage_match") is False)
    if n_bad:
        st.warning(f"⚠️ {n_bad} vins avec millésime différent Leclerc / Vivino (bordure orange).")

    st.divider()
    if not filtered:
        st.info("Aucun vin ne correspond aux filtres.")
    else:
        max_ratio = max((w.get("ratio") or 0 for w in filtered), default=1)
        for i, w in enumerate(filtered):
            st.markdown(wine_card_html(w, i + 1, max_ratio), unsafe_allow_html=True)

# ── DONNÉES ───────────────────────────────────────────────────────────────
with tab_data:
    st.markdown("#### Tous les vins")
    df_w = pd.DataFrame([{
        "Nom":         w["name"],
        "Millésime":   w.get("vintage") or "",
        "Prix (€)":    w.get("price") or 0,
        "Note":        w.get("rating") or "",
        "Nb avis":     w.get("ratings_count") or "",
        "Ratio":       w.get("ratio") or "",
        "Dispo":       "✅" if w.get("available", True) else "⛔",
        "Mil. OK":     {True:"✅", False:"⚠️", None:"—"}.get(w.get("vintage_match"), "—"),
        "Leclerc":     w.get("url") or "",
        "Vivino":      w.get("vivino_url") or "",
        "Query":       build_query(w["name"]),
    } for w in wines])
    st.dataframe(df_w, use_container_width=True, hide_index=True, height=450,
        column_config={
            "Leclerc": st.column_config.LinkColumn(display_text="🛒"),
            "Vivino":  st.column_config.LinkColumn(display_text="🍷"),
            "Prix (€)":st.column_config.NumberColumn(format="%.2f"),
            "Note":    st.column_config.NumberColumn(format="%.1f"),
            "Ratio":   st.column_config.NumberColumn(format="%.3f"),
        })

    st.divider()
    st.markdown("#### 🗂️ Cache Vivino")
    vc_now = load_vivino_cache()
    n_ok  = sum(1 for v in vc_now.values() if v.get("rating"))
    n_av  = sum(1 for v in vc_now.values() if (v.get("ratings_count") or 0) > 0)
    n_url2 = sum(1 for v in vc_now.values() if v.get("vivino_url"))
    st.caption(f"{len(vc_now)} entrées · {n_ok} notes · {n_av} nb avis · {n_url2} URLs")
    df_c = pd.DataFrame([{
        "Query":    k,
        "Note":     v.get("rating") or "",
        "Nb avis":  v.get("ratings_count") or "",
        "Vivino":   v.get("vivino_url") or "",
        "Millésime":v.get("vivino_year") or "",
        "Màj":      fmt_age(v.get("cached_at", 0)),
    } for k, v in vc_now.items()])
    st.dataframe(df_c, use_container_width=True, hide_index=True, height=400,
        column_config={
            "Vivino": st.column_config.LinkColumn(display_text="🍷"),
            "Note":   st.column_config.NumberColumn(format="%.1f"),
        })

# ── EXPORT ────────────────────────────────────────────────────────────────
with tab_export:
    def make_df(ws):
        return pd.DataFrame([{
            "Nom":              w["name"],
            "Millésime":        w.get("vintage") or "",
            "Prix (€)":         w.get("price") or 0,
            "EAN":              w.get("ean") or "",
            "Note Vivino":      w.get("rating") or "",
            "Nb avis":          w.get("ratings_count") or "",
            "Millésime Vivino": w.get("vivino_year") or "",
            "Millésime OK":     w.get("vintage_match") or "",
            "Ratio ★/€":        w.get("ratio") or "",
            "Disponible":       w.get("available", True),
            "URL Leclerc":      w.get("url") or "",
            "URL Vivino":       w.get("vivino_url") or "",
        } for w in ws])

    today = datetime.now().strftime("%Y%m%d")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Vins filtrés** ({len(filtered)} vins)")
        df_f = make_df(filtered)
        st.dataframe(df_f, use_container_width=True, hide_index=True, height=200)
        st.download_button("⬇️ CSV filtré",
            df_f.to_csv(index=False, sep=";").encode("utf-8-sig"),
            f"vins_{slug}_{today}.csv", "text/csv")
    with col2:
        st.markdown(f"**Tous les vins** ({len(wines)} vins)")
        df_a = make_df(wines)
        st.dataframe(df_a, use_container_width=True, hide_index=True, height=200)
        st.download_button("⬇️ CSV complet",
            df_a.to_csv(index=False, sep=";").encode("utf-8-sig"),
            f"vins_{slug}_complet_{today}.csv", "text/csv")

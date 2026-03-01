"""
Cave Leclerc Blagnac × Vivino
Scrape Leclerc → cache JSON  |  Vivino search + fiche JSON-LD → cache JSON
UI : Classement / Données / Export
"""

import re, json, time
import streamlit as st
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
STORE_CODE        = "1431"
MAX_PAGES         = 10
LECLERC_CACHE_TTL = 6 * 3600          # 6 h — le stock change
# Vivino : pas de TTL, refresh manuel uniquement

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

WINE_TYPES = {
    "🔴 Rouge":    "vins-rouges",
    "⚪ Blanc":    "vins-blancs",
    "🌸 Rosé":     "vins-roses",
    "🍾 Mousseux": "vins-mousseux-et-petillants",
}

st.set_page_config(page_title="Cave Leclerc Blagnac × Vivino", page_icon="🍷", layout="wide")

# ═══════════════════════════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=DM+Mono&family=DM+Sans:wght@300;400;500&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif}
.main-title{font-family:'Playfair Display',serif;font-size:2.2rem;font-weight:900;color:#1A0810}
.main-title span{color:#C9A84C}
.subtitle{color:#8B6B72;font-size:.82rem;letter-spacing:.08em;text-transform:uppercase}
.wine-card{
  background:white;border-radius:8px;padding:.9rem 1.3rem;margin-bottom:.5rem;
  border-left:4px solid #6B1A2A;box-shadow:0 2px 8px rgba(26,8,16,.07);
  display:flex;align-items:center;justify-content:space-between;gap:1rem}
.wine-card.top1{border-left-color:#C9A84C;background:#fffdf4}
.wine-card.top2{border-left-color:#9C9C9C}
.wine-card.top3{border-left-color:#CD7F32}
.wine-card.vintage-warn{border-right:3px solid #f59e0b}
.wine-card.unavailable{opacity:.45;filter:grayscale(60%)}
.wine-rank{font-family:'DM Mono',monospace;font-size:1.3rem;min-width:2.5rem;text-align:center}
.wine-info{flex:1;min-width:0}
.wine-name-text{font-weight:600;font-size:.9rem;color:#1A0810}
.wine-price{font-family:'DM Mono',monospace;font-size:1.05rem;color:#1A0810;min-width:68px;text-align:right}
.wine-rating{min-width:130px;text-align:center}
.stars{color:#C9A84C;font-size:.95rem;letter-spacing:1px}
.rating-num{font-family:'DM Mono';font-size:.82rem;color:#1A0810}
.reviews{font-size:.62rem;color:#8B6B72}
.no-rating{font-size:.72rem;color:#ccc;font-style:italic}
.ratio-wrap{min-width:130px}
.ratio-bar-bg{background:rgba(107,26,42,.1);border-radius:3px;height:6px;overflow:hidden;margin-top:4px}
.ratio-bar-fill{height:100%;background:linear-gradient(90deg,#6B1A2A,#C9A84C);border-radius:3px}
.ratio-num-text{font-family:'DM Mono';font-size:.78rem;color:#6B1A2A}
.badge{display:inline-block;padding:.15rem .5rem;border-radius:3px;font-size:.62rem;font-family:'DM Mono';margin-right:.2rem}
.badge-deal{background:rgba(201,168,76,.15);color:#8B6030;border:1px solid rgba(201,168,76,.4)}
.badge-top{background:rgba(107,26,42,.08);color:#6B1A2A;border:1px solid rgba(107,26,42,.2)}
.lnk{font-size:.68rem;text-decoration:none;border-radius:3px;padding:2px 7px;border:1px solid}
.lnk-lec{color:#2563eb;border-color:#2563eb}
.lnk-viv{color:#7B2D8B;border-color:#7B2D8B}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE  (lecture / écriture JSON)
# ═══════════════════════════════════════════════════════════════════════════════

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

def save_leclerc_cache(slug: str, wines: list[dict]) -> None:
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
    if ts == 0: return "importé manuellement"
    age = time.time() - ts
    if age < 60:    return "à l'instant"
    if age < 3600:  return f"il y a {int(age/60)} min"
    if age < 86400: return f"il y a {int(age/3600)} h"
    return f"il y a {int(age/86400)} j"


# ═══════════════════════════════════════════════════════════════════════════════
# LECLERC  (scraping HTML)
# ═══════════════════════════════════════════════════════════════════════════════

def leclerc_url(slug: str, page: int = 1) -> str:
    base = f"https://www.e.leclerc/cat/{slug}"
    return f"{base}?page={page}#oaf-sign-code={STORE_CODE}" if page > 1 \
           else f"{base}#oaf-sign-code={STORE_CODE}"

def _price(card) -> float:
    blk = card.find(class_=lambda c: c and "block-price-and-availability" in c.split())
    if blk:
        m = re.search(r"(\d+)€,(\d{2})", blk.get_text())
        if m: return float(f"{m.group(1)}.{m.group(2)}")
    ue = card.find_all(class_=lambda c: c and "price-unit"  in c.split())
    ce = card.find_all(class_=lambda c: c and "price-cents" in c.split())
    if ue and ce:
        try: return float(f"{ue[0].get_text(strip=True)}.{ce[0].get_text(strip=True).lstrip(',').strip()}")
        except ValueError: pass
    return 0.0

def parse_cards(html: str) -> list[dict]:
    wines = []
    for card in BeautifulSoup(html, "html.parser").find_all("app-product-card"):
        lbl  = card.find(class_="product-label")
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
            image = (img.get("src") or img.get("data-src") or
                     img.get("data-srcset","").split()[0] or "")
        ym = re.search(r"\b(19[5-9]\d|20[0-3]\d)\b", name)
        wines.append({"name": name, "price": _price(card), "url": url,
                      "ean": ean, "image": image,
                      "vintage": int(ym.group(1)) if ym else None})
    return wines

def get_nb_pages(html: str) -> int:
    nums = [int(m.group(1))
            for a in BeautifulSoup(html, "html.parser").find_all("a", href=True)
            if (m := re.search(r"[?&]page=(\d+)", a["href"]))]
    return max(nums) if nums else 1


# ═══════════════════════════════════════════════════════════════════════════════
# VIVINO  (query builder + parsing)
# ═══════════════════════════════════════════════════════════════════════════════

def build_query(wine_name: str) -> str:
    """Nom Leclerc → query Vivino optimisée."""
    nom = re.split(r",\s*|\s+-\s+", wine_name)[0].strip()
    nom = re.sub(r"^(Magnum|Demi-bouteille)\s+", "", nom, flags=re.I).strip()
    nom = re.sub(r"\b(19|20)\d{2}\b", "", nom).strip().strip("-").strip()
    if re.match(r"^[A-Z][A-Z\s'\-]+$", nom):
        nom = nom.title()
    # Tronquer sous-cuvées à partir du 3e mot
    cut = {"Cuvée", "Cuvee", "Vieilles", "Vieille", "Grande"}
    words = nom.split()
    for i, w in enumerate(words[2:], 2):
        if w in cut:
            nom = " ".join(words[:i]); break
    m = re.search(r"-\s*([\w\s\-]+?)\s*(?:AOP|IGP|AOC|Vin de France)", wine_name, re.I)
    app = m.group(1).strip() if m else ""
    parts = [nom]
    if app and app.lower() not in nom.lower():
        parts.append(app)
    return " ".join(parts)


def parse_wine_page_jsonld(html: str) -> dict:
    """
    Extrait note + nb avis depuis la FICHE d'un vin Vivino.
    Source 1 : JSON-LD aggregateRating (toujours présent, données exactes).
    Source 2 : JSON inline __NEXT_DATA__ / ratings_average.
    Source 3 : classes React hashées (fallback).
    """
    rating, count = None, 0
    soup = BeautifulSoup(html, "html.parser")

    # ── JSON-LD ───────────────────────────────────────────────────────────────
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

    # ── JSON inline ───────────────────────────────────────────────────────────
    if not rating:
        m = re.search(r'"ratings_average"\s*:\s*([\d.]+)', html)
        if m:
            v = round(float(m.group(1)), 1)
            if 2.5 <= v <= 5.0: rating = v
    if not count:
        m = re.search(r'"ratings_count"\s*:\s*(\d+)', html)
        if m: count = int(m.group(1))

    # ── Classes React hashées ─────────────────────────────────────────────────
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


def get_vivino_url_from_search(html: str) -> str | None:
    """Retourne l'URL de la 1ère fiche vin depuis la page de recherche."""
    for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        href = a["href"]
        if re.search(r"/wines/[\w\-]+", href) and "search" not in href:
            return href if href.startswith("http") else f"https://www.vivino.com{href}"
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# SELENIUM
# ═══════════════════════════════════════════════════════════════════════════════

def make_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import os
    opts = Options()
    for arg in ["--headless","--no-sandbox","--disable-dev-shm-usage",
                "--disable-gpu","--window-size=1280,900",
                "--disable-blink-features=AutomationControlled"]:
        opts.add_argument(arg)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    for b in ["/usr/bin/chromium","/usr/bin/chromium-browser",
              "/usr/bin/google-chrome","/usr/bin/google-chrome-stable"]:
        if os.path.exists(b): opts.binary_location = b; break
    for d in ["/usr/bin/chromedriver","/usr/lib/chromium/chromedriver",
              "/usr/lib/chromium-browser/chromedriver"]:
        if os.path.exists(d): return webdriver.Chrome(service=Service(d), options=opts)
    return webdriver.Chrome(options=opts)


def scrape_leclerc_all(slug: str, log=None) -> list[dict]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    driver = make_driver()
    wines, seen = [], set()
    try:
        driver.get(leclerc_url(slug, 1))
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


def fetch_vivino_data(driver, wine_name: str, vintage: int | None) -> dict:
    """
    Search page → URL fiche → fiche JSON-LD.
    2 navigations par vin = données complètes et fiables.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    EMPTY = {"rating": None, "ratings_count": 0,
             "vivino_url": "", "vivino_year": None, "vintage_match": None}

    query = build_query(wine_name)
    # ── 1. Page de recherche → URL du vin ─────────────────────────────────────
    try:
        driver.get(f"https://www.vivino.com/search/wines"
                   f"?q={requests.utils.quote(query)}&language=fr")
        try: WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "[class*='wineCard'],[class*='averageValue']")))
        except Exception: pass
        time.sleep(0.8)
        wine_url = get_vivino_url_from_search(driver.page_source)
    except Exception:
        return EMPTY

    if not wine_url:
        return EMPTY

    # ── 2. Fiche vin → JSON-LD ────────────────────────────────────────────────
    try:
        driver.get(wine_url)
        try: WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "script[type='application/ld+json']")))
        except Exception: pass
        time.sleep(0.8)
        d = parse_wine_page_jsonld(driver.page_source)
    except Exception:
        return EMPTY

    if not d.get("rating"):
        return EMPTY

    # Millésime depuis l'URL courante (Vivino y met ?year=XXXX)
    vy = None
    m  = re.search(r"year[=:](\d{4})", driver.current_url)
    if m: vy = int(m.group(1))

    vmatch = None
    if vintage and vy: vmatch = (vintage == vy)
    elif not vintage:  vmatch = True

    return {
        "rating":        d["rating"],
        "ratings_count": d["ratings_count"],
        "vivino_url":    wine_url,
        "vivino_year":   vy,
        "vintage_match": vmatch,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════════

def run(slug: str, force_leclerc=False, force_vivino=False, log=None) -> list[dict]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    vc  = load_vivino_cache()
    lc  = load_leclerc_cache(slug)
    now = time.time()

    # ── LECLERC ───────────────────────────────────────────────────────────────
    if lc and not force_leclerc:
        if log: log(f"📦 Cache Leclerc ({fmt_age(lc['cached_at'])}) — vérif. stock…")
        all_wines    = lc["wines"]
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
        for w in all_wines:
            w["available"] = w.get("ean", "") in current_eans
        nok = sum(1 for w in all_wines if w.get("available"))
        if log: log(f"✅ {nok} dispo, {len(all_wines)-nok} indispo à Blagnac")
    else:
        if log: log("🚀 Scrape Leclerc complet…")
        all_wines = scrape_leclerc_all(slug, log=log)
        for w in all_wines: w["available"] = True
        save_leclerc_cache(slug, all_wines)
        if log: log(f"💾 Cache Leclerc sauvegardé ({len(all_wines)} vins)")

    # ── VIVINO ────────────────────────────────────────────────────────────────
    # Vins à (re)scraper : pas en cache, ou URL manquante, ou refresh forcé
    todo = [w for w in all_wines
            if force_vivino
            or build_query(w["name"]) not in vc
            or not vc[build_query(w["name"])].get("vivino_url")]

    # Injecter le cache existant
    for w in all_wines:
        key = build_query(w["name"])
        cv  = vc.get(key, {})
        if cv.get("rating") is not None and not force_vivino:
            w.update({k: v for k, v in cv.items() if k != "cached_at"})
            w["ratio"] = round((cv["rating"] / w["price"]) * 10, 3) \
                         if w.get("price", 0) > 0 else 0
        else:
            w.setdefault("rating", None)
            w.setdefault("ratings_count", 0)
            w.setdefault("ratio", 0)
            w.setdefault("vivino_url", "")
            w.setdefault("vivino_year", None)
            w.setdefault("vintage_match", None)

    if todo:
        if log: log(f"🍷 Vivino : {len(todo)} vins → search + fiche (JSON-LD)…")
        driver2 = make_driver()
        found   = 0
        try:
            for i, wine in enumerate(todo):
                vd = fetch_vivino_data(driver2, wine["name"], wine.get("vintage"))
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
                if (i + 1) % 10 == 0 or i == len(todo) - 1:
                    if log: log(f"  🍷 {i+1}/{len(todo)} — {found} notes")
                time.sleep(0.4)
        finally:
            try: driver2.quit()
            except: pass
        save_vivino_cache(vc)
        if log: log(f"💾 Cache Vivino sauvegardé ({len(vc)} entrées)")
    else:
        if log: log("✅ Vivino : tout vient du cache (0 scraping)")

    return all_wines


# ═══════════════════════════════════════════════════════════════════════════════
# AFFICHAGE
# ═══════════════════════════════════════════════════════════════════════════════

def stars(r: float) -> str:
    return "".join("★" if r >= i else ("½" if r >= i-.5 else "☆") for i in range(1, 6))


def wine_card_html(wine: dict, rank: int, max_ratio: float) -> str:
    cls = {1:"top1",2:"top2",3:"top3"}.get(rank,"")
    if wine.get("vintage_match") is False: cls = (cls+" vintage-warn").strip()
    if not wine.get("available", True):    cls = (cls+" unavailable").strip()

    icon = {1:"🥇",2:"🥈",3:"🥉"}.get(rank, f"#{rank}")
    name_link = (
        f'<a href="{wine["url"]}" target="_blank" '
        f'style="color:#1A0810;text-decoration:none;font-weight:600">{wine["name"]}</a>'
        if wine.get("url") else f'<b>{wine["name"]}</b>'
    )
    yr = (f' <span style="color:#8B6B72;font-size:.7rem">{wine["vintage"]}</span>'
          if wine.get("vintage") else "")
    unavail = (' <span style="font-size:.65rem;color:#b0003a">⛔ indispo</span>'
               if not wine.get("available", True) else "")
    mil_warn = ""
    if wine.get("vivino_year") and wine.get("vintage") and wine["vivino_year"] != wine["vintage"]:
        mil_warn = (f'<div style="font-size:.68rem;color:#c17a00;margin-top:.1rem">'
                    f'⚠️ Vivino : {wine["vivino_year"]} vs Leclerc : {wine["vintage"]}</div>')
    links = []
    if wine.get("url"):
        links.append(f'<a href="{wine["url"]}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>')
    if wine.get("vivino_url"):
        links.append(f'<a href="{wine["vivino_url"]}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>')
    links_html = (f'<div style="display:flex;gap:.4rem;margin-top:.3rem">'
                  + "".join(links) + "</div>") if links else ""

    ratio  = wine.get("ratio") or 0
    rating = wine.get("rating")
    badges = ""
    if ratio > 0 and rank <= 5:   badges += '<span class="badge badge-deal">🔥 Top ratio</span>'
    if rating and rating >= 4.2:   badges += '<span class="badge badge-top">★ Top noté</span>'
    badges_html = f'<div style="margin-top:.2rem">{badges}</div>' if badges else ""

    if rating:
        cnt   = wine.get("ratings_count") or 0
        cnt_s = f"{cnt:,}".replace(",", "\u202f") if cnt else "—"
        rating_html = (f'<div class="wine-rating">'
                       f'<div class="stars">{stars(rating)}</div>'
                       f'<div class="rating-num">{rating:.1f} / 5</div>'
                       f'<div class="reviews">{cnt_s} avis</div></div>')
    else:
        rating_html = '<div class="wine-rating no-rating">non trouvé<br>sur Vivino</div>'

    pct = min(100, (ratio/max_ratio)*100) if max_ratio > 0 else 0
    ratio_html = (
        f'<div class="ratio-wrap"><div class="ratio-num-text">{ratio:.3f}</div>'
        f'<div class="ratio-bar-bg"><div class="ratio-bar-fill" style="width:{pct:.1f}%">'
        f'</div></div></div>'
    ) if ratio else '<div class="no-rating">—</div>'

    price_s = f'{wine["price"]:.2f}'.replace(".",",") + " €"
    return (f'<div class="wine-card {cls}">'
            f'<div class="wine-rank">{icon}</div>'
            f'<div class="wine-info">'
            f'<div class="wine-name-text">{name_link}{yr}{unavail}</div>'
            f'{mil_warn}{links_html}{badges_html}</div>'
            f'{rating_html}'
            f'<div class="wine-price">{price_s}</div>'
            f'{ratio_html}</div>')


# ═══════════════════════════════════════════════════════════════════════════════
# APP
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="main-title">Cave <span>Leclerc Blagnac</span> × Vivino</div>',
            unsafe_allow_html=True)
st.markdown('<div class="subtitle">Comparateur qualité / prix · magasin Blagnac</div>',
            unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# ── SIDEBAR ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🍾 Type de vin")
    wine_label = st.selectbox("Type", list(WINE_TYPES), label_visibility="collapsed")
    slug       = WINE_TYPES[wine_label]

    st.markdown("### 🔧 Filtres")
    search     = st.text_input("🔍 Recherche", placeholder="Bordeaux, Rhône…")
    price_max  = st.slider("💶 Prix max (€)", 0, 200, 200, step=5)
    rating_min = st.select_slider("⭐ Note Vivino min",
        options=[0.0,3.0,3.5,3.8,4.0,4.2,4.5], value=0.0,
        format_func=lambda x: "Toutes" if x==0 else f"≥ {x} ★")
    only_vintage = st.checkbox("✅ Millésime confirmé uniquement", False)
    only_dispo   = st.checkbox("🏪 Disponibles à Blagnac uniquement", True)
    sort_by = st.selectbox("↕ Trier par", [
        "Meilleur ratio ★/€","Meilleure note","Prix croissant","Prix décroissant"])

    st.divider()

    # Statut cache
    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache()
    n_note  = sum(1 for v in vc.values() if v.get("rating"))
    n_count = sum(1 for v in vc.values() if (v.get("ratings_count") or 0) > 0)
    n_url   = sum(1 for v in vc.values() if v.get("vivino_url"))

    st.caption(f"📦 **Leclerc** : {fmt_age(lc['cached_at']) if lc else 'pas de cache'}")
    st.caption(f"🍷 **Vivino** : {n_note} notes · {n_count} nb avis · {n_url} URLs")

    btn_stock  = st.button("🔄 Vérifier stock", use_container_width=True, type="primary",
                           help="Vérifie la disponibilité à Blagnac. Notes Vivino = cache.")
    btn_vivino = st.button("🍷 Rafraîchir Vivino", use_container_width=True,
                           help="Re-scrape Vivino : search + fiche JSON-LD (~2s/vin).")
    st.caption(f"📍 Leclerc Blagnac · magasin {STORE_CODE}")

# ── SESSION STATE ─────────────────────────────────────────────────────────────
for key in ("wines", "loaded_slug"):
    if key not in st.session_state:
        st.session_state[key] = None if key == "loaded_slug" else []

if slug != st.session_state.loaded_slug:
    st.session_state.wines = []

# ── CHARGEMENT ───────────────────────────────────────────────────────────────
if btn_stock or btn_vivino or not st.session_state.wines:
    st.session_state.wines = []
    with st.status("⏳ Chargement…", expanded=True) as status:
        logs, log_box = [], st.empty()
        def log(msg):
            logs.append(msg); log_box.markdown("\n\n".join(logs[-10:]))
        try:
            raw = run(slug, force_leclerc=btn_stock,
                      force_vivino=btn_vivino, log=log)
        except Exception as e:
            st.error(f"❌ Erreur Selenium : {e}\n\n"
                     "Vérifiez que `packages.txt` contient :\n```\nchromium\nchromium-driver\n```")
            st.stop()
        if not raw:
            st.error("Aucun produit récupéré."); st.stop()
        n_dispo  = sum(1 for w in raw if w.get("available", True))
        n_rated  = sum(1 for w in raw if w.get("rating"))
        n_counts = sum(1 for w in raw if (w.get("ratings_count") or 0) > 0)
        st.session_state.wines      = raw
        st.session_state.loaded_slug = slug
        status.update(
            label=f"✅ {n_dispo} vins · {n_rated} notes Vivino · {n_counts} nb avis",
            state="complete")

wines = st.session_state.wines
if not wines:
    st.info("👈 Cliquez sur **Vérifier stock** pour charger les vins.")
    st.stop()

# ── FILTRE ────────────────────────────────────────────────────────────────────
filtered = [w for w in wines
    if (w.get("price") or 0) <= price_max
    and (rating_min == 0 or (w.get("rating") and w["rating"] >= rating_min))
    and (not search or search.lower() in w["name"].lower())
    and (not only_vintage or w.get("vintage_match") is True)
    and (not only_dispo or w.get("available", True))]

sort_fns = {
    "Meilleur ratio ★/€": lambda x: -(x.get("ratio") or 0),
    "Meilleure note":      lambda x: -(x.get("rating") or 0),
    "Prix croissant":      lambda x:  x.get("price") or 999,
    "Prix décroissant":    lambda x: -(x.get("price") or 0),
}
filtered.sort(key=sort_fns[sort_by])

# ── ONGLETS ───────────────────────────────────────────────────────────────────
tab_rank, tab_data, tab_export = st.tabs(["🏅 Classement", "📊 Données", "📥 Export"])

# ── CLASSEMENT ────────────────────────────────────────────────────────────────
with tab_rank:
    c1, c2, c3, c4 = st.columns(4)
    prices = [w["price"] for w in filtered if w.get("price")]
    rated  = [w["rating"] for w in filtered if w.get("rating")]
    best   = max(filtered, key=lambda x: x.get("ratio") or 0, default=None)
    with c1: st.metric("🍷 Vins", len(filtered))
    with c2: st.metric("💶 Prix moy.", f"{sum(prices)/len(prices):.2f} €".replace(".",",") if prices else "—")
    with c3: st.metric("⭐ Note moy.", f"★ {sum(rated)/len(rated):.2f}" if rated else "—")
    with c4: st.metric("🏆 Top ratio", f"{best['ratio']:.3f}" if best and best.get("ratio") else "—")

    n_bad = sum(1 for w in filtered if w.get("vintage_match") is False)
    if n_bad:
        st.warning(f"⚠️ {n_bad} vins ont un millésime différent Leclerc / Vivino (bordure orange).")

    st.divider()
    if not filtered:
        st.info("Aucun vin ne correspond aux filtres.")
    else:
        max_ratio = max((w.get("ratio") or 0 for w in filtered), default=1)
        for i, w in enumerate(filtered):
            st.markdown(wine_card_html(w, i+1, max_ratio), unsafe_allow_html=True)

# ── DONNÉES ───────────────────────────────────────────────────────────────────
with tab_data:
    st.markdown("#### Tous les vins chargés")

    df_wines = pd.DataFrame([{
        "Nom":          w["name"],
        "Millésime":    w.get("vintage") or "",
        "Prix (€)":     w.get("price") or 0,
        "Note":         w.get("rating") or "",
        "Nb avis":      w.get("ratings_count") or "",
        "Ratio":        w.get("ratio") or "",
        "Dispo":        "✅" if w.get("available", True) else "⛔",
        "Mil. OK":      {True:"✅",False:"⚠️",None:"—"}.get(w.get("vintage_match"),"—"),
        "URL Leclerc":  w.get("url") or "",
        "URL Vivino":   w.get("vivino_url") or "",
        "Query Vivino": build_query(w["name"]),
    } for w in wines])

    st.dataframe(df_wines, use_container_width=True, hide_index=True, height=500,
        column_config={
            "URL Leclerc": st.column_config.LinkColumn("Leclerc", display_text="🛒"),
            "URL Vivino":  st.column_config.LinkColumn("Vivino",  display_text="🍷"),
            "Prix (€)":    st.column_config.NumberColumn(format="%.2f"),
            "Note":        st.column_config.NumberColumn(format="%.1f"),
            "Ratio":       st.column_config.NumberColumn(format="%.3f"),
        })

    st.divider()
    st.markdown("#### 🗂️ Cache Vivino")

    vc_now = load_vivino_cache()
    df_cache = pd.DataFrame([{
        "Query":       k,
        "Note":        v.get("rating") or "",
        "Nb avis":     v.get("ratings_count") or "",
        "URL":         v.get("vivino_url") or "",
        "Mil. Vivino": v.get("vivino_year") or "",
        "Màj":         fmt_age(v.get("cached_at", 0)),
    } for k, v in vc_now.items()])

    n_ok   = (df_cache["Note"] != "").sum()
    n_avis = (df_cache["Nb avis"] != "").sum()
    n_url2 = (df_cache["URL"] != "").sum()
    st.caption(f"{len(df_cache)} entrées · {n_ok} notes · {n_avis} nb avis · {n_url2} URLs")
    st.dataframe(df_cache, use_container_width=True, hide_index=True, height=400,
        column_config={
            "URL":   st.column_config.LinkColumn("Vivino", display_text="🍷"),
            "Note":  st.column_config.NumberColumn(format="%.1f"),
        })

# ── EXPORT ────────────────────────────────────────────────────────────────────
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
        st.dataframe(df_f, use_container_width=True, hide_index=True, height=250)
        st.download_button("⬇️ CSV filtré",
            df_f.to_csv(index=False, sep=";").encode("utf-8-sig"),
            f"vins_leclerc_{slug}_{today}.csv", "text/csv")

    with col2:
        st.markdown(f"**Tous les vins** ({len(wines)} vins)")
        df_a = make_df(wines)
        st.dataframe(df_a, use_container_width=True, hide_index=True, height=250)
        st.download_button("⬇️ CSV complet",
            df_a.to_csv(index=False, sep=";").encode("utf-8-sig"),
            f"vins_leclerc_{slug}_complet_{today}.csv", "text/csv")

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
import pandas as pd

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

def build_vivino_query(wine_name: str) -> str:
    """
    Construit la query web : "{nom propre} {appellation} vivino rating"
    Ex: "E.Guigal Côtes du Rhône vivino rating"
    Coupe sur la première virgule OU le premier " - " pour isoler le nom.
    """
    # Nom propre : avant la 1ère virgule OU avant le 1er " - "
    nom = re.split(r",\s*|\s+-\s+", wine_name)[0].strip()
    nom = re.sub(r"(19|20)\d{2}", "", nom).strip()

    # Appellation entre le 1er " - " et AOP/IGP/AOC/Vin de France
    app_m = re.search(r"-\s*([\w\s\-]+?)\s*(?:AOP|IGP|AOC|Vin de France)", wine_name, re.I)
    appellation = app_m.group(1).strip() if app_m else ""

    parts = [p for p in [nom, appellation] if p and p.lower() not in nom.lower() or p == nom]
    parts.append("vivino rating")
    return " ".join(p for p in parts if p)


def parse_vivino_rendered_html(html: str) -> tuple[float, int] | None:
    """
    Extrait (note, nb_avis) depuis le HTML rendu d'une page Vivino.
    Couvre : JSON-LD, data-attributes, classes CSS hashées, texte brut.
    """
    # 1. JSON-LD aggregateRating
    m = re.search(r'"ratingValue"\s*:\s*"?([\d.,]+)"?', html)
    if m:
        try:
            r = round(float(m.group(1).replace(",", ".")), 1)
            c_m = re.search(r'"(?:review|rating)Count"\s*:\s*"?(\d+)"?', html)
            if 2.5 <= r <= 5.0:
                return (r, int(c_m.group(1)) if c_m else 0)
        except ValueError:
            pass

    # 2. data-average attribute
    m = re.search(r'data-average="([\d.,]+)"', html)
    if m:
        try:
            r = round(float(m.group(1).replace(",", ".")), 1)
            c_m = re.search(r'data-ratings-count="(\d+)"', html)
            if 2.5 <= r <= 5.0:
                return (r, int(c_m.group(1)) if c_m else 0)
        except ValueError:
            pass

    # 3. Classes CSS hashées Vivino (vivinoRating_averageValue__xxxx)
    m = re.search(r'vivinoRating_averageValue[^>]*>([\d.,]+)<', html)
    if m:
        try:
            r = round(float(m.group(1).strip().replace(",", ".")), 1)
            c_m = re.search(r'vivinoRating_numRatings[^>]*>([\d\s\xa0]+)', html)
            count = int(re.sub(r"[^\d]", "", c_m.group(1)) or 0) if c_m else 0
            if 2.5 <= r <= 5.0:
                return (r, count)
        except ValueError:
            pass

    # 4. "3,9 / 5" ou "3.9/5" dans le texte
    m = re.search(r"(\d[.,]\d)\s*/\s*5", html)
    if m:
        try:
            r = round(float(m.group(1).replace(",", ".")), 1)
            c_m = re.search(r"(\d[\d\s\xa0]+)\s*(?:avis|notes?|ratings?)", html, re.I)
            count = int(re.sub(r"[^\d]", "", c_m.group(1))) if c_m else 0
            if 2.5 <= r <= 5.0:
                return (r, count)
        except ValueError:
            pass

    return None


def search_vivino_selenium(driver, wine_name: str, wine_vintage: int | None) -> dict | None:
    """
    Cherche un vin sur Vivino via Selenium (vrai navigateur = pas de blocage).
    Charge la page de recherche Vivino et parse le HTML rendu.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    query = build_vivino_query(wine_name)
    if not query:
        return None

    search_url = (
        "https://www.vivino.com/search/wines"
        f"?q={requests.utils.quote(query)}&language=fr"
    )

    try:
        driver.get(search_url)
        try:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    "[class*='wineName'], [class*='wine-card'], "
                    "[class*='averageValue'], .average__number"
                ))
            )
        except Exception:
            pass
        time.sleep(1.5)

        html = driver.page_source
        result = parse_vivino_rendered_html(html)
        if not result:
            return None

        rating, count = result

        # URL du premier résultat vin
        soup = BeautifulSoup(html, "html.parser")
        best_url = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"/wines/[\w-]+", href) and "search" not in href:
                best_url = href if href.startswith("http") else f"https://www.vivino.com{href}"
                break

        # Millésime Vivino (depuis l'URL ou le texte de page)
        vivino_year = None
        if best_url:
            ym = re.search(r"-(20[0-3]\d|19[5-9]\d)(?:-|$)", best_url)
            if ym:
                vivino_year = int(ym.group(1))
        if not vivino_year:
            ym2 = re.search(r"\b(20[0-3]\d|19[5-9]\d)\b", html[:3000])
            if ym2:
                vivino_year = int(ym2.group(1))

        vintage_match = None
        if wine_vintage and vivino_year:
            vintage_match = (wine_vintage == vivino_year)
        elif not wine_vintage:
            vintage_match = True

        return {
            "rating":          rating,
            "ratings_count":   count,
            "vivino_url":      best_url,
            "vivino_year":     vivino_year,
            "vintage_match":   vintage_match,
        }

    except Exception:
        return None



# ─────────────────────────────────────────────────────────────────────────────
# SELENIUM SCRAPER (Leclerc uniquement)
# ─────────────────────────────────────────────────────────────────────────────

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
            from selenium.webdriver.chrome.service import Service
            return webdriver.Chrome(service=Service(drv), options=opts)

    return webdriver.Chrome(options=opts)


def scrape_and_enrich(wine_type_slug: str, log=None) -> list[dict]:
    """
    Garde le driver Selenium ouvert pour :
    1. Scraper Leclerc (toutes les pages)
    2. Enrichir chaque vin avec les notes Vivino (pages de recherche Vivino)
    Tout dans le même navigateur — aucun blocage possible.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    if log: log("🚀 Démarrage de Chromium…")
    driver    = get_selenium_driver()
    all_wines = []
    seen_eans = set()

    try:
        # ── Étape 1 : Leclerc ───────────────────────────────────────────────
        if log: log("🌐 Chargement page 1 Leclerc…")
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

        # ── Étape 2 : Vivino (même driver) ──────────────────────────────────
        if log: log(f"🍷 Recherche notes Vivino ({len(all_wines)} vins)…")
        all_wines = enrich_with_vivino(all_wines, driver, log=log)

    finally:
        driver.quit()

    return all_wines


def enrich_with_vivino(wines: list[dict], driver, log=None) -> list[dict]:
    """
    Enrichit les vins avec Vivino via le driver Selenium existant.
    Charge chaque page de recherche Vivino — vrai navigateur, aucun blocage.
    """
    total = len(wines)
    found = 0
    EMPTY = {
        "rating": None, "ratings_count": 0, "ratio": 0,
        "vivino_url": "", "vivino_year": None, "vintage_match": None,
    }

    for i, wine in enumerate(wines):
        vd = search_vivino_selenium(driver, wine["name"], wine.get("vintage"))
        if vd and vd.get("rating"):
            wine.update(vd)
            wine["ratio"] = (
                round((vd["rating"] / wine["price"]) * 10, 3)
                if wine["price"] > 0 else 0
            )
            found += 1
        else:
            wine.update(EMPTY)

        if (i + 1) % 5 == 0 or i == total - 1:
            if log:
                log(f"  🍷 {i+1}/{total} — {found} notes trouvées")

        time.sleep(0.5)

    return wines




# ─────────────────────────────────────────────────────────────────────────────
# AFFICHAGE
# ─────────────────────────────────────────────────────────────────────────────

def build_stars(r: float) -> str:
    return "".join("★" if r >= i else ("½" if r >= i - .5 else "☆") for i in range(1, 6))


def wine_card_html(wine: dict, rank: int, max_ratio: float) -> str:
    card_cls  = {1:"top1", 2:"top2", 3:"top3"}.get(rank, "")
    # Ajouter indicateur millésime non confirmé
    vintage_warn = wine.get("vintage_match") is False
    if vintage_warn:
        card_cls = (card_cls + " vintage-warn").strip()

    rank_icon = {1:"🥇", 2:"🥈", 3:"🥉"}.get(rank, f"#{rank}")

    name_html = (
        f'<a href="{wine["url"]}" target="_blank" style="color:#1A0810;text-decoration:none">'
        f'{wine["name"]}</a>' if wine.get("url") else wine["name"]
    )

    # Sous-titre : Vivino name + alerte millésime
    subs = []
    if wine.get("vivino_year") and wine.get("vintage") and wine["vivino_year"] != wine["vintage"]:
        subs.append(
            f'<span class="badge badge-year">⚠️ Vivino trouve {wine["vivino_year"]} '
            f'(vous avez {wine["vintage"]})</span>'
        )
    elif wine.get("vivino_url"):
        pass  # pas d'alerte, millésime OK ou inconnu

    vivino_sub = "".join(subs)

    rating = wine.get("rating")
    if rating:
        cnt = f"{wine.get('ratings_count', 0):,}".replace(",", "\u202f")
        rating_html = (
            f'<div class="wine-rating">'
            f'<div class="stars">{build_stars(rating)}</div>'
            f'<div class="rating-num">{rating:.1f} / 5</div>'
            f'<div class="reviews">{cnt} avis</div>'
            f'</div>'
        )
    else:
        rating_html = '<div class="wine-rating no-rating">non trouvé sur Vivino</div>'

    ratio = wine.get("ratio") or 0
    pct   = min(100, (ratio / max_ratio) * 100) if max_ratio > 0 else 0
    ratio_html = (
        f'<div class="ratio-wrap">'
        f'<div class="ratio-num-text">{ratio:.3f}</div>'
        f'<div class="ratio-bar-bg">'
        f'<div class="ratio-bar-fill" style="width:{pct:.1f}%"></div>'
        f'</div></div>'
        if ratio else '<div class="no-rating">—</div>'
    )

    badges = ""
    if ratio > 0 and rank <= 5:  badges += '<span class="badge badge-deal">🔥 Top ratio</span>'
    if rating and rating >= 4.2: badges += '<span class="badge badge-top">★ Top noté</span>'
    if wine.get("vivino_url"):
        badges += (
            f'<a href="{wine["vivino_url"]}" target="_blank" '
            f'style="font-size:.68rem;color:#8B6B72;text-decoration:none;margin-left:.3rem">→ Vivino</a>'
        )

    price_fmt = f'{wine["price"]:.2f}'.replace(".", ",") + " €"
    vintage_tag = f' <span style="color:#8B6B72;font-size:.72rem">{wine["vintage"]}</span>' if wine.get("vintage") else ""

    return (
        f'<div class="wine-card {card_cls}">'
        f'<div class="wine-rank">{rank_icon}</div>'
        f'<div class="wine-info">'
        f'<div class="wine-name-text">{name_html}{vintage_tag}</div>'
        f'{vivino_sub}'
        f'<div style="margin-top:.3rem">{badges}</div>'
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
    sort_by = st.selectbox("↕ Trier par", [
        "Meilleur ratio ★/€", "Meilleure note", "Prix croissant", "Prix décroissant",
    ])
    st.divider()
    scrape_btn = st.button("🔄 Lancer / Rafraîchir", use_container_width=True, type="primary")
    st.caption(f"📍 Leclerc Blagnac · magasin {STORE_CODE}")

# ── Session state ──────────────────────────────────────────────────────────────
if "wines" not in st.session_state:
    st.session_state.wines = []
if "loaded_type" not in st.session_state:
    st.session_state.loaded_type = None

# Recharger si on change de type de vin
if wine_type_slug != st.session_state.loaded_type:
    st.session_state.wines = []

if scrape_btn or not st.session_state.wines:
    st.session_state.wines = []

    with st.status(f"🔍 Chargement {wine_type_label}…", expanded=True) as status:
        log_box = st.empty()
        logs    = []
        def log(msg):
            logs.append(msg)
            log_box.markdown("\n\n".join(logs[-6:]))

        # 1. Scraping Leclerc
        try:
            raw_wines = scrape_and_enrich(wine_type_slug, log=log)
        except Exception as e:
            st.error(
                f"❌ Erreur Selenium : {e}\n\n"
                "**Vérifiez que `packages.txt` contient :**\n```\nchromium\nchromium-driver\n```"
            )
            st.stop()

        if not raw_wines:
            st.error("Aucun produit récupéré — réessayez dans quelques instants.")
            st.stop()

        log(f"✅ {len(raw_wines)} vins trouvés")

        n_rated = sum(1 for w in raw_wines if w.get("rating"))
        n_vintage_ok  = sum(1 for w in raw_wines if w.get("vintage_match") is True)
        n_vintage_bad = sum(1 for w in raw_wines if w.get("vintage_match") is False)

        log(f"✅ {n_rated}/{len(raw_wines)} notes Vivino")
        if n_vintage_bad:
            log(f"⚠️ {n_vintage_bad} millésimes non confirmés")

        st.session_state.wines       = raw_wines
        st.session_state.loaded_type = wine_type_slug
        status.update(
            label=f"✅ {len(raw_wines)} vins analysés — {n_rated} notes Vivino",
            state="complete",
        )


# ── Affichage ──────────────────────────────────────────────────────────────────
wines = st.session_state.wines

if wines:
    filtered = [
        w for w in wines
        if w["price"] <= price_max
        and (rating_min == 0 or (w.get("rating") and w["rating"] >= rating_min))
        and (not search
             or search.lower() in w["name"].lower())
        and (not only_vintage_ok or w.get("vintage_match") is True)
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

"""
Cave Leclerc Blagnac — Comparateur Vivino
Testé et validé : parser HTML, nettoyage noms, ratio, pagination.
Utilise Selenium + Chromium système (packages.txt sur Streamlit Cloud).
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

STORE_CODE  = "1431"
LECLERC_BASE = "https://www.e.leclerc/cat/vins-rouges"
MAX_PAGES    = 10

HEADERS_VIVINO = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.vivino.com/",
}

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
.wine-rank { font-family:'DM Mono',monospace; font-size:1.3rem; min-width:2.5rem; text-align:center; }
.wine-info { flex:1; }
.wine-name-text { font-weight:600; font-size:.9rem; color:#1A0810; }
.wine-vivino-name { font-size:.72rem; color:#8B6B72; font-style:italic; }
.wine-price { font-family:'DM Mono',monospace; font-size:1.05rem; color:#1A0810; min-width:68px; text-align:right; }
.wine-rating { min-width:110px; text-align:center; }
.stars { color:#C9A84C; font-size:.95rem; letter-spacing:1px; }
.rating-num { font-family:'DM Mono'; font-size:.82rem; color:#1A0810; }
.reviews { font-size:.62rem; color:#8B6B72; }
.no-rating { font-size:.72rem; color:#ccc; font-style:italic; }
.ratio-wrap { min-width:130px; }
.ratio-bar-bg { background:rgba(107,26,42,.1); border-radius:3px; height:6px; overflow:hidden; margin-top:4px; }
.ratio-bar-fill { height:100%; background:linear-gradient(90deg,#6B1A2A,#C9A84C); border-radius:3px; }
.ratio-num-text { font-family:'DM Mono'; font-size:.78rem; color:#6B1A2A; }
.badge { display:inline-block; padding:.15rem .5rem; border-radius:3px; font-size:.62rem; font-family:'DM Mono'; }
.badge-deal { background:rgba(201,168,76,.15); color:#8B6030; border:1px solid rgba(201,168,76,.4); }
.badge-top  { background:rgba(107,26,42,.08);  color:#6B1A2A; border:1px solid rgba(107,26,42,.2); }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PARSEUR HTML  (testé sur 96 produits réels Blagnac)
# ─────────────────────────────────────────────────────────────────────────────

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
        # Prix : price-unit = entier, price-cents = centimes
        price = 0.0
        unit_els  = card.find_all(class_=lambda c: c and "price-unit"  in c.split())
        cents_els = card.find_all(class_=lambda c: c and "price-cents" in c.split())
        if unit_els and cents_els:
            u = unit_els[0].get_text(strip=True)
            c = cents_els[0].get_text(strip=True).replace(",","").replace(".","").strip()
            try:
                price = float(f"{u}.{c}")
            except ValueError:
                price = 0.0
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
        img = card.find("img")
        image = ""
        if img:
            image = (img.get("src") or img.get("data-src") or
                     (img.get("data-srcset","").split()[0] if img.get("data-srcset") else "") or "")
        wines.append({"name": name, "price": price, "url": url, "ean": ean, "image": image})
    return wines


def get_nb_pages(html: str) -> int:
    """Détecte le nb de pages depuis les liens de pagination dans le HTML rendu."""
    soup = BeautifulSoup(html, "html.parser")
    page_nums = []
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            page_nums.append(int(m.group(1)))
    # max page lié = dernière page (ex: lien "page=2" → 2 pages)
    return max(page_nums) if page_nums else 1


# ─────────────────────────────────────────────────────────────────────────────
# SELENIUM SCRAPER
# Pagination : navigation directe par URL (?page=N#oaf-sign-code=1431)
# car le bouton "next" Angular n'a pas de href cliquable
# ─────────────────────────────────────────────────────────────────────────────

def get_selenium_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options

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

    # Binaire Chromium — ordre de priorité pour Streamlit Cloud (Ubuntu)
    for binary in [
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
    ]:
        if __import__("os").path.exists(binary):
            opts.binary_location = binary
            break

    # Chromedriver
    for drv in [
        "/usr/bin/chromedriver",
        "/usr/lib/chromium/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
    ]:
        if __import__("os").path.exists(drv):
            return webdriver.Chrome(
                service=Service(drv), options=opts
            )

    # Fallback : Selenium le trouve dans le PATH
    return webdriver.Chrome(options=opts)


def scrape_with_selenium(log=None) -> list[dict]:
    from selenium.webdriver.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    if log: log("🚀 Démarrage de Chromium…")
    driver    = get_selenium_driver()
    all_wines = []
    seen_eans = set()

    try:
        # ── Page 1 ──────────────────────────────────────────────────────────
        url_p1 = f"{LECLERC_BASE}#oaf-sign-code={STORE_CODE}"
        if log: log(f"🌐 Chargement page 1…")
        driver.get(url_p1)

        # Attendre que Angular rende les cartes produit
        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card"))
            )
        except Exception:
            pass
        time.sleep(2)  # laisser le filtre magasin s'appliquer

        html    = driver.page_source
        wines_p1 = parse_cards_from_html(html)
        nb_pages = min(get_nb_pages(html), MAX_PAGES)

        for w in wines_p1:
            if w["ean"] not in seen_eans:
                seen_eans.add(w["ean"])
                all_wines.append(w)

        if log: log(f"✅ Page 1 : {len(wines_p1)} vins — {nb_pages} page(s) au total")

        # ── Pages suivantes ──────────────────────────────────────────────────
        for p in range(2, nb_pages + 1):
            # URL directe : plus fiable que cliquer sur le bouton "next" Angular
            url_p = f"{LECLERC_BASE}?page={p}#oaf-sign-code={STORE_CODE}"
            if log: log(f"🌐 Chargement page {p}/{nb_pages}…")
            driver.get(url_p)
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
                if log: log(f"⚠️ Page {p} vide ou doublons — arrêt")
                break
            for w in new:
                seen_eans.add(w["ean"])
            all_wines.extend(new)
            if log: log(f"✅ Page {p} : {len(new)} nouveaux vins (total {len(all_wines)})")

    finally:
        driver.quit()

    return all_wines


# ─────────────────────────────────────────────────────────────────────────────
# VIVINO  (nettoyage testé sur tous les formats Leclerc)
# ─────────────────────────────────────────────────────────────────────────────

def clean_name_for_vivino(wine_name: str) -> str:
    """
    Nettoie le nom Leclerc pour la recherche Vivino.
    Ex: "Gérard Bertrand Heresie, 2022 - Corbières AOP - Rouge - 75 cl"
     → "Gérard Bertrand Heresie - Corbières"
    """
    clean = wine_name
    # 1. Supprimer tout à partir de "- AOP/IGP/AOC/Vin de France/Rouge/Blanc/Rosé"
    clean = re.sub(
        r"\s*-\s*(AOP|IGP|AOC|Vin de France|Rouge|Blanc|Rosé|Moelleux).*",
        "", clean, flags=re.I
    )
    # 2. Supprimer l'année (avec ou sans virgule)
    clean = re.sub(r",?\s*\b(19|20)\d{2}\b", "", clean)
    # 3. Supprimer les mentions AOP/IGP/AOC résiduelles
    clean = re.sub(r"\s*(AOP|IGP|AOC)\b", "", clean, flags=re.I)
    # 4. Nettoyer ponctuation finale
    clean = re.sub(r"[\s,\-]+$", "", clean).strip()
    # 5. Max 6 mots
    return " ".join(clean.split()[:6])


def search_vivino(wine_name: str) -> dict | None:
    query = clean_name_for_vivino(wine_name)
    if not query:
        return None
    try:
        resp = requests.get(
            "https://www.vivino.com/api/explore/explore",
            params={"language":"fr","wine_type_ids[]":1,
                    "q":query,"order_by":"match","per_page":3},
            headers=HEADERS_VIVINO, timeout=8,
        )
        if resp.status_code != 200:
            return None
        records = resp.json().get("explore_vintage",{}).get("records",[])
        if not records:
            return None
        best    = records[0]
        vintage = best.get("vintage", {})
        wine    = vintage.get("wine", {})
        stats   = vintage.get("statistics", wine.get("statistics", {}))
        rating  = float(stats.get("ratings_average", 0) or 0)
        count   = int(stats.get("ratings_count", 0) or 0)
        if not rating:
            return None
        return {
            "vivino_name":   wine.get("name",""),
            "vivino_year":   vintage.get("year",""),
            "rating":        round(rating, 2),
            "ratings_count": count,
            "vivino_url":    f"https://www.vivino.com{wine.get('seo_name','')}",
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# AFFICHAGE
# ─────────────────────────────────────────────────────────────────────────────

def build_stars(r: float) -> str:
    return "".join("★" if r>=i else ("½" if r>=i-.5 else "☆") for i in range(1,6))


def wine_card_html(wine: dict, rank: int, max_ratio: float) -> str:
    card_cls  = {1:"top1", 2:"top2", 3:"top3"}.get(rank, "")
    rank_icon = {1:"🥇",   2:"🥈",   3:"🥉"  }.get(rank, f"#{rank}")

    name_html = (
        f'<a href="{wine["url"]}" target="_blank" style="color:#1A0810;text-decoration:none">'
        f'{wine["name"]}</a>' if wine.get("url") else wine["name"]
    )
    vivino_sub = ""
    if wine.get("vivino_name") and wine["vivino_name"] != wine["name"]:
        vivino_sub = (f'<div class="wine-vivino-name">'
                      f'Vivino : {wine["vivino_name"]} {wine.get("vivino_year","")}'
                      f'</div>')

    rating = wine.get("rating")
    if rating:
        cnt = f"{wine.get('ratings_count',0):,}".replace(",","\u202f")
        rating_html = (
            f'<div class="wine-rating">'
            f'<div class="stars">{build_stars(rating)}</div>'
            f'<div class="rating-num">{rating:.2f} / 5</div>'
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
    if ratio > 0 and rank <= 5:  badges += '<span class="badge badge-deal">🔥 Top ratio</span> '
    if rating and rating >= 4.2: badges += '<span class="badge badge-top">★ Top noté</span> '
    if wine.get("vivino_url"):
        badges += (f'<a href="{wine["vivino_url"]}" target="_blank" '
                   f'style="font-size:.68rem;color:#8B6B72;text-decoration:none">→ Vivino</a>')

    price_fmt = f'{wine["price"]:.2f}'.replace(".",",") + " €"

    return (
        f'<div class="wine-card {card_cls}">'
        f'<div class="wine-rank">{rank_icon}</div>'
        f'<div class="wine-info">'
        f'<div class="wine-name-text">{name_html}</div>'
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
    '<div class="subtitle">Comparateur qualité / prix — Vins rouges disponibles en magasin</div>',
    unsafe_allow_html=True,
)
st.markdown("<br>", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 🔧 Filtres")
    search     = st.text_input("🔍 Recherche", placeholder="Bordeaux, Rhône, Corbières…")
    price_max  = st.slider("💶 Prix max (€)", 0, 200, 200, step=5)
    rating_min = st.select_slider(
        "⭐ Note Vivino min",
        options=[0.0, 3.0, 3.5, 3.8, 4.0, 4.2, 4.5], value=0.0,
        format_func=lambda x: "Toutes" if x == 0 else f"≥ {x} ★",
    )
    sort_by = st.selectbox("↕ Trier par", [
        "Meilleur ratio ★/€", "Meilleure note", "Prix croissant", "Prix décroissant",
    ])
    st.divider()
    scrape_btn = st.button("🔄 Lancer / Rafraîchir", use_container_width=True, type="primary")
    st.caption(f"📍 Leclerc Blagnac · magasin {STORE_CODE}")

if "wines" not in st.session_state:
    st.session_state.wines = []

if scrape_btn or not st.session_state.wines:
    st.session_state.wines = []

    with st.status("🔍 Chargement en cours…", expanded=True) as status:
        log_box = st.empty()
        logs    = []
        def log(msg):
            logs.append(msg)
            log_box.markdown("\n\n".join(logs[-6:]))

        # 1. Scraping Selenium
        try:
            raw_wines = scrape_with_selenium(log=log)
        except Exception as e:
            st.error(
                f"❌ Erreur Selenium : {e}\n\n"
                "**Vérifiez que `packages.txt` contient :**\n```\nchromium\nchromium-driver\n```"
            )
            st.stop()

        if not raw_wines:
            st.error("Aucun produit récupéré — réessayez dans quelques instants.")
            st.stop()

        log(f"✅ {len(raw_wines)} vins Blagnac récupérés")

        # 2. Enrichissement Vivino
        log("🍷 Recherche des notes Vivino…")
        enriched = []
        prog = st.progress(0)

        for i, wine in enumerate(raw_wines):
            vd = search_vivino(wine["name"])
            if vd:
                wine.update(vd)
                wine["ratio"] = (
                    round((vd["rating"] / wine["price"]) * 10, 3)
                    if wine["price"] > 0 else 0
                )
            else:
                wine.update({
                    "rating": None, "ratings_count": 0,
                    "ratio": 0, "vivino_name": "", "vivino_url": "",
                })
            enriched.append(wine)
            prog.progress((i + 1) / len(raw_wines))
            time.sleep(0.25)   # respecter le rate-limit Vivino

        st.session_state.wines = enriched
        status.update(label=f"✅ {len(enriched)} vins analysés !", state="complete")


# ── Affichage ─────────────────────────────────────────────────────────────────
wines = st.session_state.wines

if wines:
    filtered = [
        w for w in wines
        if w["price"] <= price_max
        and (rating_min == 0 or (w.get("rating") and w["rating"] >= rating_min))
        and (
            not search
            or search.lower() in w["name"].lower()
            or search.lower() in (w.get("vivino_name") or "").lower()
        )
    ]

    sort_key = {
        "Meilleur ratio ★/€": lambda x: -(x.get("ratio") or 0),
        "Meilleure note":      lambda x: -(x.get("rating") or 0),
        "Prix croissant":      lambda x:  x["price"],
        "Prix décroissant":    lambda x: -x["price"],
    }
    filtered.sort(key=sort_key[sort_by])

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("🍷 Vins affichés", len(filtered))
    with c2:
        avg_p = sum(w["price"] for w in filtered if w["price"]) / max(len(filtered), 1)
        st.metric("💶 Prix moyen", f"{avg_p:.2f} €".replace(".", ","))
    with c3:
        rated = [w for w in filtered if w.get("rating")]
        avg_r = sum(w["rating"] for w in rated) / max(len(rated), 1) if rated else 0
        st.metric("⭐ Note moy. Vivino", f"★ {avg_r:.2f}" if rated else "—")
    with c4:
        best = max(filtered, key=lambda x: x.get("ratio") or 0, default=None)
        st.metric("🏆 Meilleur ratio", f"{best['ratio']:.2f}" if best and best.get("ratio") else "—")

    with st.expander("📥 Exporter en CSV"):
        df = pd.DataFrame([{
            "Nom":         w["name"],
            "Prix (€)":    w["price"],
            "EAN":         w.get("ean", ""),
            "Note Vivino": w.get("rating", ""),
            "Nb avis":     w.get("ratings_count", ""),
            "Ratio ★/€":  w.get("ratio", ""),
            "URL Leclerc": w.get("url", ""),
            "URL Vivino":  w.get("vivino_url", ""),
        } for w in filtered])
        st.download_button(
            "⬇️ Télécharger CSV",
            df.to_csv(index=False, sep=";").encode("utf-8-sig"),
            "vins_leclerc_blagnac.csv", "text/csv",
        )

    st.divider()
    st.markdown("### 🏅 Classement qualité / prix")
    max_ratio = max((w.get("ratio") or 0 for w in filtered), default=1)
    for i, wine in enumerate(filtered):
        st.markdown(wine_card_html(wine, i + 1, max_ratio), unsafe_allow_html=True)

else:
    st.info("👈 Cliquez sur **Lancer / Rafraîchir** dans le panneau gauche.")

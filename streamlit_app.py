"""
Cave Leclerc Blagnac — Comparateur Vivino
Scrape les vins via requests+BeautifulSoup (page SSR Angular),
puis enrichit avec les notes Vivino.
"""

import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG PAGE
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Cave Leclerc Blagnac × Vivino",
    page_icon="🍷",
    layout="wide",
)

STORE_CODE  = "1431"   # Leclerc Blagnac
LECLERC_URL = f"https://www.e.leclerc/cat/vins-rouges?oaf-sign-code={STORE_CODE}"

HEADERS_LECLERC = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.e.leclerc/",
}

HEADERS_VIVINO = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
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

.main-title {
    font-family: 'Playfair Display', serif;
    font-size: 2.2rem; font-weight: 900; color: #1A0810;
}
.main-title span { color: #C9A84C; }
.subtitle {
    color: #8B6B72; font-size: 0.82rem;
    letter-spacing: 0.08em; text-transform: uppercase;
}
.wine-card {
    background: white; border-radius: 8px;
    padding: 0.9rem 1.3rem; margin-bottom: 0.5rem;
    border-left: 4px solid #6B1A2A;
    box-shadow: 0 2px 8px rgba(26,8,16,.07);
    display: flex; align-items: center;
    justify-content: space-between; gap: 1rem;
}
.wine-card.top1 { border-left-color: #C9A84C; background: #fffdf4; }
.wine-card.top2 { border-left-color: #9C9C9C; }
.wine-card.top3 { border-left-color: #CD7F32; }
.wine-rank {
    font-family: 'DM Mono', monospace;
    font-size: 1.3rem; min-width: 2.5rem; text-align: center;
}
.wine-info   { flex: 1; }
.wine-name-text { font-weight: 600; font-size: 0.9rem; color: #1A0810; }
.wine-vivino-name { font-size: 0.72rem; color: #8B6B72; font-style: italic; }
.wine-price {
    font-family: 'DM Mono', monospace;
    font-size: 1.05rem; color: #1A0810;
    min-width: 65px; text-align: right;
}
.wine-rating { min-width: 110px; text-align: center; }
.stars { color: #C9A84C; font-size: 0.95rem; letter-spacing: 1px; }
.rating-num { font-family: 'DM Mono'; font-size: 0.82rem; color: #1A0810; }
.reviews { font-size: 0.62rem; color: #8B6B72; }
.no-rating { font-size: 0.72rem; color: #ccc; font-style: italic; }
.ratio-wrap { min-width: 130px; }
.ratio-bar-bg {
    background: rgba(107,26,42,.1); border-radius: 3px;
    height: 6px; overflow: hidden; margin-top: 4px;
}
.ratio-bar-fill {
    height: 100%;
    background: linear-gradient(90deg, #6B1A2A, #C9A84C);
    border-radius: 3px;
}
.ratio-num-text { font-family: 'DM Mono'; font-size: 0.78rem; color: #6B1A2A; }
.badge {
    display: inline-block; padding: 0.15rem 0.5rem;
    border-radius: 3px; font-size: 0.62rem; font-family: 'DM Mono';
}
.badge-deal { background: rgba(201,168,76,.15); color: #8B6030; border: 1px solid rgba(201,168,76,.4); }
.badge-top  { background: rgba(107,26,42,.08);  color: #6B1A2A; border: 1px solid rgba(107,26,42,.2); }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER LECLERC — requests + BeautifulSoup
# La page e.leclerc est rendue côté serveur (Angular Universal/SSR) :
# tous les produits sont dans le HTML initial, sans JS nécessaire.
# ─────────────────────────────────────────────────────────────────────────────

def scrape_leclerc(url: str = LECLERC_URL) -> list[dict]:
    try:
        resp = requests.get(url, headers=HEADERS_LECLERC, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Impossible de charger la page Leclerc : {e}")

    soup  = BeautifulSoup(resp.text, "html.parser")
    cards = soup.find_all("app-product-card")

    if not cards:
        raise RuntimeError(
            "Aucun produit trouvé dans la page. "
            "Leclerc a peut-être modifié sa structure HTML."
        )

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
            c = cents_els[0].get_text(strip=True).replace(",", "").replace(".", "").strip()
            try:
                price = float(f"{u}.{c}")
            except ValueError:
                price = 0.0

        # URL
        link = card.find("a", href=True)
        if link:
            href = link["href"]
            url_wine = href if href.startswith("http") else f"https://www.e.leclerc{href}"
        else:
            url_wine = ""

        # Image
        img   = card.find("img")
        image = ""
        if img:
            image = (
                img.get("src")
                or img.get("data-src")
                or (img.get("data-srcset", "").split()[0])
                or ""
            )

        wines.append({"name": name, "price": price, "url": url_wine, "image": image})

    return wines


# ─────────────────────────────────────────────────────────────────────────────
# VIVINO
# ─────────────────────────────────────────────────────────────────────────────

def search_vivino(wine_name: str) -> dict | None:
    # Nettoyer : retirer "75 cl", AOP, millésime final…
    clean = re.sub(r"\s*-\s*(Rouge|Blanc|Rosé|Moelleux)\s*-\s*\d+\s*cl.*", "", wine_name, flags=re.I)
    clean = re.sub(r"\s*-\s*(AOP|IGP|AOC|Vin de France).*",               "", clean,      flags=re.I)
    clean = re.sub(r"\s+\d{4}\s*$",                                         "", clean).strip()
    query = " ".join(clean.split()[:6])

    try:
        resp = requests.get(
            "https://www.vivino.com/api/explore/explore",
            params={
                "language": "fr",
                "wine_type_ids[]": 1,
                "q": query,
                "order_by": "match",
                "per_page": 3,
            },
            headers=HEADERS_VIVINO,
            timeout=8,
        )
        if resp.status_code != 200:
            return None

        records = resp.json().get("explore_vintage", {}).get("records", [])
        if not records:
            return None

        best    = records[0]
        vintage = best.get("vintage", {})
        wine    = vintage.get("wine", {})
        stats   = vintage.get("statistics", wine.get("statistics", {}))
        rating  = stats.get("ratings_average", 0) or 0
        count   = stats.get("ratings_count",   0) or 0

        if not rating:
            return None

        return {
            "vivino_name":   wine.get("name", ""),
            "vivino_year":   vintage.get("year", ""),
            "rating":        round(float(rating), 2),
            "ratings_count": int(count),
            "vivino_url":    f"https://www.vivino.com{wine.get('seo_name', '')}",
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# AFFICHAGE
# ─────────────────────────────────────────────────────────────────────────────

def build_stars(r: float) -> str:
    s = ""
    for i in range(1, 6):
        if r >= i:         s += "★"
        elif r >= i - .5: s += "½"
        else:              s += "☆"
    return s


def wine_card_html(wine: dict, rank: int, max_ratio: float) -> str:
    card_cls  = {1: "top1", 2: "top2", 3: "top3"}.get(rank, "")
    rank_icon = {1: "🥇",   2: "🥈",   3: "🥉"  }.get(rank, f"#{rank}")

    name_html = (
        f'<a href="{wine["url"]}" target="_blank" style="color:#1A0810;text-decoration:none">'
        f'{wine["name"]}</a>'
        if wine.get("url") else wine["name"]
    )
    vivino_sub = ""
    if wine.get("vivino_name") and wine["vivino_name"] != wine["name"]:
        vivino_sub = (
            f'<div class="wine-vivino-name">'
            f'Vivino : {wine["vivino_name"]} {wine.get("vivino_year","")}'
            f'</div>'
        )

    rating = wine.get("rating")
    if rating:
        cnt = f"{wine.get('ratings_count', 0):,}".replace(",", "\u202f")
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
        f'<div class="ratio-bar-bg"><div class="ratio-bar-fill" style="width:{pct:.1f}%"></div></div>'
        f'</div>'
        if ratio else '<div class="no-rating">—</div>'
    )

    badges = ""
    if ratio > 0 and rank <= 5:   badges += '<span class="badge badge-deal">🔥 Top ratio</span> '
    if rating and rating >= 4.2:  badges += '<span class="badge badge-top">★ Top noté</span> '
    if wine.get("vivino_url"):
        badges += (
            f'<a href="{wine["vivino_url"]}" target="_blank" '
            f'style="font-size:.68rem;color:#8B6B72;text-decoration:none">→ Vivino</a>'
        )

    price_fmt = f'{wine["price"]:.2f}'.replace(".", ",") + " €"

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

st.markdown('<div class="main-title">Cave <span>Leclerc Blagnac</span> × Vivino</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Comparateur qualité / prix — Vins rouges disponibles en magasin</div>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### 🔧 Filtres")
    search     = st.text_input("🔍 Recherche", placeholder="Bordeaux, Rhône, Corbières…")
    price_max  = st.slider("💶 Prix max (€)", 0, 200, 200, step=5)
    rating_min = st.select_slider(
        "⭐ Note Vivino min",
        options=[0.0, 3.0, 3.5, 3.8, 4.0, 4.2, 4.5],
        value=0.0,
        format_func=lambda x: "Toutes" if x == 0 else f"≥ {x} ★",
    )
    sort_by = st.selectbox(
        "↕ Trier par",
        ["Meilleur ratio ★/€", "Meilleure note", "Prix croissant", "Prix décroissant"],
    )
    st.divider()
    scrape_btn = st.button("🔄 Lancer / Rafraîchir", use_container_width=True, type="primary")
    st.caption(f"Source : Leclerc Blagnac (magasin {STORE_CODE})")

# Session
if "wines" not in st.session_state:
    st.session_state.wines = []

if scrape_btn or not st.session_state.wines:
    st.session_state.wines = []

    with st.status("🔍 Chargement des vins…", expanded=True) as status:
        # 1. Scraping Leclerc
        try:
            st.write("📡 Téléchargement de la page Leclerc Blagnac…")
            raw_wines = scrape_leclerc()
            st.write(f"✅ {len(raw_wines)} vins trouvés en stock à Blagnac")
        except RuntimeError as e:
            st.error(str(e))
            st.stop()

        # 2. Vivino
        st.write("🍷 Recherche des notes Vivino…")
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
            time.sleep(0.25)

        st.session_state.wines = enriched
        status.update(label=f"✅ {len(enriched)} vins analysés", state="complete")

# Affichage
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

    # Métriques
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("🍷 Vins affichés", len(filtered))
    with c2:
        avg_p = sum(w["price"] for w in filtered if w["price"]) / max(len(filtered), 1)
        st.metric("💶 Prix moyen", f"{avg_p:.2f} €".replace(".", ","))
    with c3:
        rated = [w for w in filtered if w.get("rating")]
        avg_r = sum(w["rating"] for w in rated) / max(len(rated), 1)
        st.metric("⭐ Note moy. Vivino", f"★ {avg_r:.2f}" if rated else "—")
    with c4:
        best = max(filtered, key=lambda x: x.get("ratio") or 0, default=None)
        st.metric("🏆 Meilleur ratio", f"{best['ratio']:.2f}" if best and best.get("ratio") else "—")

    # Export CSV
    with st.expander("📥 Exporter en CSV"):
        df = pd.DataFrame([{
            "Nom":         w["name"],
            "Prix (€)":    w["price"],
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

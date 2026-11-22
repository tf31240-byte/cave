"""Leclerc : lecture du catalogue (pages produits)."""
import time
from bs4 import BeautifulSoup
# AUTO-IMPORTS
from .vivino import _CARD_VINTAGE_RE, _EAN_OFFER_RE, _EAN_URL_RE, _PAGE_NUM_RE, _PRICE_CSS1_RE, _PRICE_CSS2_RE, _PRICE_FALLBACK_RE, extract_grapes_from_name, extract_volume_cl
from .config import LECLERC_PAGE_SIZE, MAX_PAGES, STORE_CODE
from .browser import make_driver
from .storage import update_price_history
# END-AUTO

# ═══════════════════════════════════════════════════════════════════════════
# SCRAPING LECLERC
# ═══════════════════════════════════════════════════════════════════════════

def leclerc_url(slug: str, page: int = 1) -> str:
    return f"https://www.e.leclerc/cat/{slug}?pageSize={LECLERC_PAGE_SIZE}&page={page}#oaf-sign-code={STORE_CODE}"

def _parse_price(card) -> float:
    """
    Extrait le prix d'une carte produit Leclerc.
    Plusieurs patterns en cascade pour résister aux changements de HTML du site.
    """
    # Pattern 1 : bloc dédié prix + dispo
    blk = card.find(class_=lambda c: c and "block-price-and-availability" in c.split())
    if blk:
        m = _PRICE_CSS1_RE.search(blk.get_text())
        if m: return float(f"{m.group(1)}.{m.group(2)}")

    # Pattern 2 : classes price-unit + price-cents (ancienne structure)
    ue = card.find_all(class_=lambda c: c and "price-unit"  in c.split())
    ce = card.find_all(class_=lambda c: c and "price-cents" in c.split())
    if ue and ce:
        try:
            return float(f"{ue[0].get_text(strip=True)}.{ce[0].get_text(strip=True).lstrip(',').strip()}")
        except ValueError: pass

    # Pattern 3 : microdata itemprop="price"
    mp = card.find(itemprop="price")
    if mp:
        val = mp.get("content") or mp.get_text(strip=True)
        try:
            return float(str(val).replace(",", ".").replace("€", "").strip())
        except ValueError: pass

    # Pattern 4 : tout élément dont la classe contient "price" ou "prix"
    for el in card.find_all(class_=lambda c: c and any(
            p in " ".join(c.split()).lower() for p in ("price", "prix", "amount"))):
        txt = el.get_text(strip=True)
        m = _PRICE_CSS2_RE.search(txt)
        if m:
            try:
                val = float(f"{m.group(1)}.{m.group(2)}")
                if 1.0 <= val <= 999.0:   # sanity check : un vin coûte entre 1€ et 999€
                    return val
            except ValueError: pass

    # Pattern 5 (fallback ultime) : regex XX,XX € n'importe où dans le HTML de la carte
    # Robuste à tout changement CSS — on cherche le premier montant plausible
    for m in _PRICE_FALLBACK_RE.finditer(card.get_text()):
        try:
            val = float(f"{m.group(1)}.{m.group(2)}")
            if 1.0 <= val <= 999.0:
                return val
        except ValueError: pass

    return 0.0

def parse_page(html: str) -> tuple[list, int]:
    """
    Parse une page Leclerc en une seule passe BeautifulSoup.
    Retourne (wines: list[dict], nb_pages: int).
    Évite de parser le même HTML deux fois (parse_cards puis get_nb_pages).
    """
    soup  = BeautifulSoup(html, "html.parser")
    wines = []
    for card in soup.find_all("app-product-card"):
        lbl  = card.find(class_="product-label")
        name = lbl.get_text(strip=True) if lbl else ""
        if not name: continue
        lnk  = card.find("a", href=True)
        href = lnk["href"] if lnk else ""
        url  = href if href.startswith("http") else f"https://www.e.leclerc{href}"
        em   = _EAN_OFFER_RE.search(card.decode_contents())
        ean  = em.group(1) if em else ""
        if not ean:
            m2 = _EAN_URL_RE.search(url)
            ean = m2.group(1) if m2 else ""
        img   = card.find("img")
        image = ""
        if img:
            image = img.get("src") or img.get("data-src") or \
                    img.get("data-srcset", "").split()[0] or ""
        ym = _CARD_VINTAGE_RE.search(name)
        wines.append({"name": name, "price": _parse_price(card),
                      "url": url, "ean": ean, "image": image,
                      "vintage":     int(ym.group(1)) if ym else None,
                      "grapes_hint": extract_grapes_from_name(name),
                      "volume_cl":   extract_volume_cl(name)})
    nums = [int(m.group(1))
            for a in soup.find_all("a", href=True)
            if (m := _PAGE_NUM_RE.search(a["href"]))]
    return wines, (max(nums) if nums else 1)


def parse_cards(html: str) -> list:
    """Extrait uniquement les cartes produit (sans pagination). Voir parse_page() pour l'usage combiné."""
    return parse_page(html)[0]


def get_nb_pages(html: str) -> int:
    """Nombre de pages de résultats. Voir parse_page() pour l'usage combiné."""
    return parse_page(html)[1]



def _set_store_cookie(driver) -> None:
    """
    Injecte le cookie de sélection de magasin avant de scraper.
    Sans ce cookie, e.leclerc affiche les prix à 0 car le JS ne sait
    pas quel magasin charger (le fragment #oaf-sign-code= n'est pas fiable).
    """
    try:
        # Naviguer sur la page d'accueil pour établir le domaine
        driver.get("https://www.e.leclerc/")  # domaine cookie — catégorie indépendante
        # Injecter le cookie AVANT de recharger la vraie page
        driver.add_cookie({
            "name":   "oafSignCode",
            "value":  STORE_CODE,
            "domain": ".e.leclerc",
            "path":   "/",
        })
        driver.add_cookie({
            "name":   "oaf-sign-code",
            "value":  STORE_CODE,
            "domain": ".e.leclerc",
            "path":   "/",
        })
    except Exception:
        pass   # non-bloquant : l'app fonctionne même sans cookie



def _scrape_all_leclerc_pages(driver, slug: str, log=None) -> list:
    """
    Itère toutes les pages Leclerc et retourne la liste brute de toutes les cartes.
    Facteur commun de scrape_leclerc_full, check_availability, repair_zero_prices.

    Gestion du timing :
    - WebDriverWait jusqu'à ce que les cartes soient présentes (rapide si le site charge vite)
    - sleep court (0.8s) après wait réussi, sleep plus long (2.5s) si wait échoué
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    _CARD_CSS = "app-product-card"
    all_cards, seen_keys = [], set()
    nb_pages = 1
    for p in range(1, MAX_PAGES + 1):
        if log: log(f"  📄 Page {p}/{nb_pages}…")
        driver.get(leclerc_url(slug, p))
        waited = False
        try:
            WebDriverWait(driver, 18).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, _CARD_CSS)))
            waited = True
        except Exception:
            pass
        time.sleep(0.8 if waited else 2.5)   # court si déjà chargé, long sinon
        html = driver.page_source
        if p == 1:
            cards, nb_pages = parse_page(html)
            nb_pages = min(nb_pages, MAX_PAGES)
        else:
            cards = parse_cards(html)
        if not cards:
            if log: log(f"  ⚠️ Page {p} vide — arrêt")
            break
        new = []
        for c in cards:
            k = c.get("ean") or c.get("name")
            if k and k not in seen_keys:
                seen_keys.add(k)
                new.append(c)
        all_cards.extend(new)
        if log: log(f"  ✅ Page {p} : +{len(new)} cartes (total {len(all_cards)})")
        if p >= nb_pages:
            break
    return all_cards

def scrape_leclerc_full(slug: str, log=None) -> list:
    """Scrape complet Leclerc via _scrape_all_leclerc_pages."""
    driver = None
    try:
        driver = make_driver()
        _set_store_cookie(driver)
        if log: log(f"🌐 Scrape complet Leclerc ({slug})…")
        wines = _scrape_all_leclerc_pages(driver, slug, log)
    finally:
        if driver is not None:
            try: driver.quit()
            except Exception: pass
    if log: log(f"✅ {len(wines)} vins récupérés")
    update_price_history(wines)
    return wines


def check_availability(slug: str, cached_wines: list, log=None) -> list:
    """
    Vérifie stock + met à jour les prix depuis les données fraîches du site.
    Utilise _scrape_all_leclerc_pages pour itérer toutes les pages.
    """
    driver = None
    fresh_cards: list[dict] = []
    try:
        driver = make_driver()
        _set_store_cookie(driver)
        if log: log(f"🌐 Vérification stock + prix ({slug})…")
        fresh_cards = _scrape_all_leclerc_pages(driver, slug, log)
    except Exception as e:
        if log: log(f"⚠️ Vérif. stock échouée : {e}")
    finally:
        if driver is not None:
            try: driver.quit()
            except Exception: pass

    # Indexer par EAN et par nom pour mise à jour disponibilité + prix
    current_eans: set[str]        = {c["ean"] for c in fresh_cards if c.get("ean")}
    fresh_by_ean: dict[str, dict] = {c["ean"]: c for c in fresh_cards if c.get("ean")}
    fresh_by_name: dict[str, dict]= {c["name"]: c for c in fresh_cards}

    if not current_eans:
        if log: log("⚠️ Aucun EAN récupéré — site inaccessible ? Disponibilité non mise à jour.")
        return cached_wines

    result, n_price_updated = [], 0
    for w in cached_wines:
        w2 = dict(w)
        if w2.get("ean"):
            w2["available"] = w2["ean"] in current_eans
            fresh = fresh_by_ean.get(w2["ean"]) or fresh_by_name.get(w2["name"])
        else:
            fresh = fresh_by_name.get(w2["name"])
        if fresh and fresh.get("price") and fresh["price"] > 0 and fresh["price"] != w2.get("price"):
            w2["price"] = fresh["price"]
            n_price_updated += 1
        result.append(w2)

    update_price_history(result)
    nok = sum(1 for w in result if w.get("available"))
    if log: log(f"✅ {nok} dispo · {len(result)-nok} indispo · {n_price_updated} prix mis à jour")
    return result

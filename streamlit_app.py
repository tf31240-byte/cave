"""
Cave Leclerc Blagnac × Vivino - v3
Améliorations : score composite, bonnes affaires, régions, historique prix,
                filtre range prix, couverture Vivino, pertinence Vivino, _merge_vivino.
"""

import re, json, time, math, unicodedata, threading
import streamlit as st
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
STORE_CODE            = "1431"
MAX_PAGES             = 15
LECLERC_CACHE_TTL     = 12 * 3600
LECLERC_PAGE_SIZE     = 96
VIVINO_SIMILARITY_MIN = 0.28   # ⑦ seuil pertinence
VIVINO_CANDIDATES_MAX = 8
VIVINO_API_TIMEOUT = 8

VIVINO_API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.vivino.com/",
}

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
JOB_STATE_PATH = CACHE_DIR / "job_state.json"

WINE_TYPES = {
    "🔴 Rouge":    "vins-rouges",
    "⚪ Blanc":    "vins-blancs",
    "🌸 Rosé":     "vins-roses",
    "🍾 Mousseux": "vins-mousseux-et-petillants",
}

st.set_page_config(
    page_title="Cave Leclerc Blagnac × Vivino",
    page_icon="🍷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ═══════════════════════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=DM+Mono&family=DM+Sans:wght@300;400;500&display=swap');

html,body,[class*="css"]{font-family:'DM Sans',sans-serif}
.main-title{font-family:'Playfair Display',serif;font-size:clamp(1.4rem,4vw,2.2rem);
  font-weight:900;color:#1A0810;line-height:1.1}
.main-title span{color:#C9A84C}
.subtitle{color:#8B6B72;font-size:.82rem;letter-spacing:.08em;text-transform:uppercase}

.wine-card{
  background:white;border-radius:10px;padding:.8rem 1rem;margin-bottom:.5rem;
  border-left:4px solid #6B1A2A;box-shadow:0 2px 10px rgba(26,8,16,.08);
  display:grid;grid-template-columns:2.2rem 1fr auto auto auto;
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
.p-up  {color:#dc2626;font-size:.7rem;font-weight:700;margin-left:3px}
.p-down{color:#16a34a;font-size:.7rem;font-weight:700;margin-left:3px}
.p-eq  {color:#9ca3af;font-size:.7rem;margin-left:3px}

.score-wrap{min-width:100px}
.score-num{font-family:'DM Mono';font-size:.75rem;color:#6B1A2A;font-weight:600}
.score-lbl{font-size:.55rem;color:#8B6B72;letter-spacing:.04em}
.score-bar{background:rgba(107,26,42,.1);border-radius:3px;height:5px;
  overflow:hidden;margin-top:3px}
.score-fill{height:100%;background:linear-gradient(90deg,#6B1A2A,#C9A84C);border-radius:3px}

.badge{display:inline-block;padding:.1rem .4rem;border-radius:3px;
  font-size:.58rem;font-family:'DM Mono';margin-right:.15rem;margin-top:.2rem}
.b-deal{background:rgba(201,168,76,.15);color:#8B6030;border:1px solid rgba(201,168,76,.4)}
.b-top {background:rgba(107,26,42,.08);color:#6B1A2A;border:1px solid rgba(107,26,42,.2)}
.b-reg {background:rgba(37,99,235,.06);color:#1d4ed8;border:1px solid rgba(37,99,235,.2)}

@media (max-width:640px){
  .wine-card{grid-template-columns:1.8rem 1fr;grid-template-rows:auto auto auto;gap:.3rem}
  .wine-rating{grid-column:1/3;display:flex;align-items:center;
    gap:.6rem;justify-content:flex-start;min-width:0}
  .stars{display:inline}
  .wine-price{grid-column:1/3;text-align:left;font-size:.95rem}
  .score-wrap{display:none}
  .wine-name{white-space:normal}
}
.deal-card{background:linear-gradient(135deg,#fffdf4,#fff8e7);border-radius:12px;
  padding:1rem;margin-bottom:.6rem;border:1.5px solid #C9A84C;
  box-shadow:0 2px 12px rgba(201,168,76,.15)}
.deal-score{font-family:'DM Mono';font-size:1.4rem;font-weight:900;color:#6B1A2A;line-height:1}
.deal-label{font-size:.6rem;color:#8B6B72;text-transform:uppercase;letter-spacing:.06em}
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
    p, tmp = _lec_path(slug), _lec_path(slug).with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps({"cached_at": time.time(), "slug": slug, "wines": wines},
                       ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)
    except Exception: tmp.unlink(missing_ok=True); raise


def _normalize_vivino_entry(entry: dict) -> dict:
    """Normalise un enregistrement cache Vivino (compat anciennes versions)."""
    if not isinstance(entry, dict):
        return {
            "rating": None,
            "ratings_count": 0,
            "vivino_url": "",
            "vivino_year": None,
            "vintage_match": None,
            "match_confidence": None,
            "manual_override": False,
            "suppressed": False,
            "locked": False,
            "cached_at": 0,
        }

    out = dict(entry)
    out.setdefault("rating", None)
    out.setdefault("ratings_count", 0)
    out.setdefault("vivino_url", "")
    out.setdefault("vivino_year", None)
    out.setdefault("vintage_match", None)
    out.setdefault("match_confidence", None)
    out.setdefault("manual_override", False)
    out.setdefault("suppressed", False)
    out.setdefault("locked", False)
    out.setdefault("cached_at", 0)
    return out


def load_vivino_cache() -> dict:
    p = _viv_path()
    if p.exists():
        try:
            raw = json.loads(p.read_text("utf-8"))
            if not isinstance(raw, dict):
                return {}
            return {k: _normalize_vivino_entry(v) for k, v in raw.items()}
        except Exception:
            pass
    return {}

def save_vivino_cache(cache: dict) -> None:
    p, tmp = _viv_path(), _viv_path().with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)
    except Exception: tmp.unlink(missing_ok=True); raise


# ═══════════════════════════════════════════════════════════════════════════
# CHECKPOINT
# ═══════════════════════════════════════════════════════════════════════════

def _ckpt_path(slug: str) -> Path: return CACHE_DIR / f"vivino_ckpt_{slug}.json"

def ckpt_load(slug: str) -> dict | None:
    p = _ckpt_path(slug)
    if not p.exists(): return None
    try:
        d = json.loads(p.read_text("utf-8"))
        if d.get("finished"): p.unlink(missing_ok=True); return None
        if time.time() - d.get("started_at", 0) > 86400: p.unlink(missing_ok=True); return None
        return d
    except Exception: return None

def ckpt_create(slug: str, total: int) -> None:
    p, tmp = _ckpt_path(slug), _ckpt_path(slug).with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps({"slug": slug, "started_at": time.time(),
            "total": total, "done_eans": [], "finished": False},
            ensure_ascii=False), "utf-8")
        tmp.replace(p)
    except Exception: tmp.unlink(missing_ok=True); raise

def ckpt_tick(slug: str, ean: str) -> None:
    p = _ckpt_path(slug)
    try:
        d = json.loads(p.read_text("utf-8"))
        d["done_eans"].append(ean)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, ensure_ascii=False), "utf-8")
        tmp.replace(p)
    except Exception: pass

def ckpt_finish(slug: str) -> None:
    _ckpt_path(slug).unlink(missing_ok=True)
    _ckpt_path(slug).with_suffix(".tmp").unlink(missing_ok=True)


_job_lock = threading.Lock()
_job_thread = None


def load_job_state() -> dict:
    if JOB_STATE_PATH.exists():
        try:
            return json.loads(JOB_STATE_PATH.read_text("utf-8"))
        except Exception:
            return {}
    return {}


def save_job_state(state: dict) -> None:
    tmp = JOB_STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(JOB_STATE_PATH)


def _set_job_state(**kwargs) -> None:
    with _job_lock:
        state = load_job_state()
        state.update(kwargs)
        state["updated_at"] = time.time()
        save_job_state(state)


def _background_job(slug: str, mode: str) -> None:
    _set_job_state(status="running", slug=slug, mode=mode, message="Démarrage…", error="")

    def _log(msg: str):
        _set_job_state(message=msg)

    try:
        if mode == "refresh_all":
            raw = run_refresh_vivino(slug, resume=False, log=_log)
        elif mode == "fill_missing":
            raw = run_fill_missing_vivino(slug, log=_log)
        elif mode == "resume":
            raw = run_refresh_vivino(slug, resume=True, log=_log)
        else:
            raise ValueError(f"Mode inconnu: {mode}")

        n_rated = sum(1 for w in raw if w.get("rating"))
        _set_job_state(status="done", message=f"✅ Terminé · {n_rated} vins notés", finished_at=time.time())
    except Exception as e:
        _set_job_state(status="error", error=str(e), message=f"❌ {e}", finished_at=time.time())


def start_background_job(slug: str, mode: str) -> bool:
    global _job_thread
    with _job_lock:
        current = load_job_state()
        if current.get("status") == "running":
            return False
        save_job_state({
            "status": "queued",
            "slug": slug,
            "mode": mode,
            "started_at": time.time(),
            "message": "Mise en file…",
            "error": "",
            "updated_at": time.time(),
        })

    _job_thread = threading.Thread(target=_background_job, args=(slug, mode), daemon=True)
    _job_thread.start()
    return True


# ═══════════════════════════════════════════════════════════════════════════
# HISTORIQUE DES PRIX  ⑨
# ═══════════════════════════════════════════════════════════════════════════

def _price_hist_path() -> Path: return CACHE_DIR / "price_history.json"

def load_price_history() -> dict:
    p = _price_hist_path()
    if p.exists():
        try: return json.loads(p.read_text("utf-8"))
        except Exception: pass
    return {}

def save_price_history(hist: dict) -> None:
    p, tmp = _price_hist_path(), _price_hist_path().with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(hist, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)
    except Exception: tmp.unlink(missing_ok=True)

def update_price_history(wines: list) -> None:
    """Enregistre le prix du jour pour chaque vin (EAN). Max 10 relevés."""
    hist  = load_price_history()
    today = datetime.now().strftime("%Y-%m-%d")
    for w in wines:
        ean = w.get("ean")
        if not ean or not w.get("price"): continue
        entry = hist.setdefault(ean, {"name": w["name"], "history": []})
        entry["name"] = w["name"]
        if not entry["history"] or entry["history"][-1]["date"] != today:
            entry["history"].append({"date": today, "price": w["price"]})
            entry["history"] = entry["history"][-10:]
    save_price_history(hist)

def price_trend(ean: str, current_price: float, ph: dict) -> str:
    """'↑' '↓' '=' ou '' selon l'évolution vs le relevé précédent."""
    if not ean: return ""
    h = ph.get(ean, {}).get("history", [])
    if len(h) < 2: return ""
    prev = h[-2]["price"]
    if current_price > prev + 0.05: return "↑"
    if current_price < prev - 0.05: return "↓"
    return "="


# ═══════════════════════════════════════════════════════════════════════════
# SCORE COMPOSITE  ①
# ═══════════════════════════════════════════════════════════════════════════

def compute_score(rating, ratings_count, price) -> float:
    """
    Score composite qualité/prix.
    confidence = min(1.0, sqrt(nb_avis) / 100)
      10 000 avis → confiance 1.0   |   100 avis → 0.10   |   25 avis → 0.05
    score = note × confiance / prix × 10
    """
    if not rating or not price or price <= 0: return 0.0
    confidence = min(1.0, math.sqrt(ratings_count or 0) / 100)
    return round(rating * confidence / price * 10, 2)


# ═══════════════════════════════════════════════════════════════════════════
# RÉGIONS / APPELLATIONS  ③
# ═══════════════════════════════════════════════════════════════════════════

_REGIONS = [
    "Saint-Émilion Grand Cru","Saint-Émilion","Pomerol","Fronsac",
    "Pauillac","Saint-Estèphe","Margaux","Saint-Julien","Listrac","Moulis",
    "Haut-Médoc","Médoc","Pessac-Léognan","Graves","Entre-Deux-Mers",
    "Bordeaux Supérieur","Bordeaux",
    "Gevrey-Chambertin","Nuits-Saint-Georges","Pommard","Volnay","Beaune",
    "Aloxe-Corton","Meursault","Puligny-Montrachet","Chablis",
    "Mâcon","Pouilly-Fuissé","Bourgogne",
    "Châteauneuf-du-Pape","Gigondas","Vacqueyras","Rasteau",
    "Crozes-Hermitage","Hermitage","Cornas","Saint-Joseph",
    "Côtes du Rhône Villages","Côtes du Rhône",
    "Bandol","Côtes de Provence","Provence",
    "Pic Saint-Loup","Terrasses du Larzac",
    "Faugères","Saint-Chinian","Minervois","Corbières","Fitou","La Clape","Languedoc",
    "Côtes du Roussillon Villages","Côtes du Roussillon","Roussillon",
    "Cahors","Madiran","Bergerac","Pécharmant","Fronton","Gaillac","Marcillac","Irouléguy",
    "Saumur-Champigny","Saumur","Bourgueil","Saint-Nicolas-de-Bourgueil","Chinon",
    "Anjou","Muscadet","Sancerre","Pouilly-Fumé","Loire",
    "Fleurie","Moulin-à-Vent","Morgon","Brouilly","Beaujolais Villages","Beaujolais",
    "Alsace","Côtes de Gascogne","Pays d'Oc","Vin de France",
]

def extract_region(wine_name: str) -> str:
    def _n(s):
        return unicodedata.normalize("NFD", s).encode("ascii","ignore").decode().lower()
    m = re.search(r"-\s*([\w\s\-\']+?)\s*(?:AOP|IGP|AOC|AOP-AOC)\b", wine_name, re.I)
    if m:
        raw = m.group(1).strip()
        for r in _REGIONS:
            if _n(raw) == _n(r): return r
        for r in _REGIONS:
            if _n(raw) in _n(r) or _n(r) in _n(raw): return r
        if len(raw) > 2: return raw.title()
    name_n = _n(wine_name)
    for r in _REGIONS:
        if _n(r) in name_n: return r
    return ""


# ═══════════════════════════════════════════════════════════════════════════
# _MERGE_VIVINO — source de vérité unique  ④
# ═══════════════════════════════════════════════════════════════════════════


def vivino_cache_type(entry: dict) -> str:
    if entry.get("suppressed"):
        return "masqué"
    if entry.get("manual_override"):
        return "manuel"
    return "auto"

def _merge_vivino(wines: list, vc: dict, ph: dict | None = None) -> list:
    """Injecte données Vivino + calcule score/région/tendance prix."""
    if ph is None: ph = {}
    for w in wines:
        key = build_query(w["name"])
        cv  = vc.get(key, {})
        w.setdefault("available", True)
        if cv.get("suppressed"):
            w["rating"] = None
            w["ratings_count"] = 0
            w["vivino_url"] = ""
            w["vivino_year"] = None
            w["vintage_match"] = None
            w["match_confidence"] = None
        elif cv.get("rating") is not None or cv.get("vivino_url"):
            w["rating"] = cv.get("rating")
            w["ratings_count"] = cv.get("ratings_count", 0)
            w["vivino_url"] = cv.get("vivino_url", "")
            w["vivino_year"] = cv.get("vivino_year")
            w["vintage_match"] = cv.get("vintage_match")
            w["match_confidence"] = cv.get("match_confidence")
        w.setdefault("rating", None);      w.setdefault("ratings_count", 0)
        w.setdefault("vivino_url", "");    w.setdefault("vivino_year", None)
        w.setdefault("vintage_match", None)
        w.setdefault("match_confidence", None)
        w["score"]       = compute_score(w.get("rating"), w.get("ratings_count"), w.get("price"))
        w["region"]      = extract_region(w["name"])
        w["price_trend"] = price_trend(w.get("ean",""), w.get("price") or 0, ph)
    return wines


# ═══════════════════════════════════════════════════════════════════════════
# UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════

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
    return f"https://www.e.leclerc/cat/{slug}?pageSize={LECLERC_PAGE_SIZE}&page={page}#oaf-sign-code={STORE_CODE}"

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
        img   = card.find("img")
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
# VIVINO — query + parsing + pertinence  ⑦
# ═══════════════════════════════════════════════════════════════════════════

def build_query(wine_name: str) -> str:
    nom = re.split(r",\s*|\s+-\s+", wine_name)[0].strip()
    nom = re.sub(r"^(Magnum|Demi-bouteille)\s+", "", nom, flags=re.I).strip()
    nom = re.sub(r"\b(19|20)\d{2}\b", "", nom).strip().strip("-").strip()
    if re.match(r"^[A-Z][A-Z\s\'\-]+$", nom): nom = nom.title()
    cut   = {"Cuvée","Cuvee","Vieilles","Vieille","Grande"}
    words = nom.split()
    for i, w in enumerate(words[2:], 2):
        if w in cut: nom = " ".join(words[:i]); break
    m   = re.search(r"-\s*([\w\s\-]+?)\s*(?:AOP|IGP|AOC|Vin de France)", wine_name, re.I)
    app = m.group(1).strip() if m else ""
    parts = [nom]
    if app and app.lower() not in nom.lower(): parts.append(app)
    result = " ".join(parts).strip()
    return result if result else wine_name[:40].strip()

def _norm_words(s: str) -> set:
    STOP = {"de","du","des","le","la","les","et","au","aux","en","par","sur",
            "un","une","the","of","and","for","vin","wines","wine"}
    ascii_s = unicodedata.normalize("NFD", s).encode("ascii","ignore").decode()
    return {w.lower() for w in re.findall(r"[a-z]{3,}", ascii_s.lower())
            if w.lower() not in STOP}

def _name_similarity(name1: str, name2: str) -> float:
    w1, w2 = _norm_words(name1), _norm_words(name2)
    if not w1 or not w2: return 0.0
    return len(w1 & w2) / min(len(w1), len(w2))

def _extract_year(text: str) -> int | None:
    m = re.search(r"\b(19|20)\d{2}\b", text or "")
    return int(m.group(0)) if m else None


def vivino_candidates_from_search(html: str, max_candidates: int = VIVINO_CANDIDATES_MAX) -> list[dict]:
    """Retourne plusieurs candidats Vivino avec URL+titre depuis la page de recherche."""
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not re.search(r"/w/\d+", href) or "search" in href:
            continue
        url = href if href.startswith("http") else f"https://www.vivino.com{href}"
        if url in seen:
            continue
        seen.add(url)
        out.append({
            "url": url,
            "title": a.get_text(separator=" ", strip=True),
            "year": _extract_year(a.get_text(separator=" ", strip=True)),
        })
        if len(out) >= max_candidates:
            break
    return out


def choose_best_vivino_candidate(query: str, vintage, candidates: list[dict]) -> tuple[dict | None, float]:
    """Choisit le meilleur candidat en combinant similarité nom + cohérence millésime."""
    best, best_score = None, -1.0
    for c in candidates:
        score = _name_similarity(query, c.get("title", ""))
        c_year = c.get("year")
        if vintage and c_year:
            if c_year == vintage:
                score += 0.20
            elif abs(c_year - vintage) == 1:
                score += 0.08
            else:
                score -= 0.12
        elif vintage and not c_year:
            score -= 0.03

        if score > best_score:
            best, best_score = c, score

    if not best or best_score < VIVINO_SIMILARITY_MIN:
        return None, best_score
    return best, best_score


def fetch_vivino_via_api(query: str, vintage) -> dict | None:
    """Fallback API Vivino quand le HTML scraping échoue."""
    try:
        resp = requests.get(
            "https://www.vivino.com/api/explore/explore",
            params={
                "language": "fr",
                "country_codes[]": "fr",
                "price_range_max": 300,
                "price_range_min": 0,
                "wine_type_ids[]": 1,
                "q": query,
                "order_by": "match",
            },
            headers=VIVINO_API_HEADERS,
            timeout=VIVINO_API_TIMEOUT,
        )
        if resp.status_code != 200:
            return None

        records = (resp.json().get("explore_vintage", {}) or {}).get("records", [])
        candidates = []
        for r in records[:VIVINO_CANDIDATES_MAX]:
            vintage_obj = r.get("vintage", {}) or {}
            wine_obj = vintage_obj.get("wine", {}) or {}
            title = f"{wine_obj.get('name','')} {vintage_obj.get('name','')}".strip()
            candidates.append({
                "title": title,
                "year": vintage_obj.get("year"),
                "record": r,
            })

        best, confidence = choose_best_vivino_candidate(query, vintage, candidates)
        if not best:
            return None

        picked = best.get("record", {})
        vintage_obj = picked.get("vintage", {}) or {}
        wine_obj = vintage_obj.get("wine", {}) or {}
        stats = vintage_obj.get("statistics", wine_obj.get("statistics", {})) or {}
        vy = vintage_obj.get("year")

        vmatch = None
        if vintage and vy:
            vmatch = (vintage == vy)
        elif not vintage:
            vmatch = True

        return {
            "rating": stats.get("ratings_average"),
            "ratings_count": int(stats.get("ratings_count") or 0),
            "vivino_url": f"https://www.vivino.com{wine_obj.get('seo_name','')}",
            "vivino_year": vy,
            "vintage_match": vmatch,
            "match_confidence": round(confidence, 3),
        }
    except Exception:
        return None

def parse_wine_jsonld(html: str) -> dict:
    rating, count = None, 0
    soup = BeautifulSoup(html, "html.parser")
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
                    v = round(float(str(rv).replace(",",".")), 1)
                    if 2.5 <= v <= 5.0: rating = v
                if rc:
                    count = int(re.sub(r"[^\d]", "", str(rc)) or 0)
                if rating: break
        except Exception: pass
        if rating: break
    if not rating:
        m = re.search(r'"ratings_average"\s*:\s*([\d.]+)', html)
        if m:
            v = round(float(m.group(1)), 1)
            if 2.5 <= v <= 5.0: rating = v
    if not count:
        m = re.search(r'"ratings_count"\s*:\s*(\d+)', html)
        if m: count = int(m.group(1))
    if not rating:
        for el in soup.find_all(class_=lambda c: c and "averageValue" in c):
            try:
                v = round(float(el.get_text(strip=True).replace(",",".")), 1)
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
    for arg in ["--headless","--no-sandbox","--disable-dev-shm-usage",
                "--disable-gpu","--window-size=1280,900",
                "--disable-blink-features=AutomationControlled"]:
        opts.add_argument(arg)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
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


def scrape_leclerc_full(slug: str, log=None) -> list:
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
    update_price_history(wines)   # ⑨
    return wines


def check_availability(slug: str, cached_wines: list, log=None) -> list:
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
    if not current_eans:
        if log: log("⚠️ Aucun EAN récupéré — site Leclerc inaccessible ? Disponibilité non mise à jour.")
        return cached_wines
    for w in cached_wines:
        w["available"] = w.get("ean","") in current_eans
    update_price_history(cached_wines)   # ⑨
    nok = sum(1 for w in cached_wines if w.get("available"))
    if log: log(f"✅ {nok} dispo, {len(cached_wines)-nok} indispo à Blagnac")
    return cached_wines


def fetch_vivino(driver, wine_name: str, vintage) -> dict:
    """⑦ 2 navigations avec choix du meilleur candidat (nom + millésime)."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    EMPTY = {"rating": None, "ratings_count": 0,
             "vivino_url": "", "vivino_year": None, "vintage_match": None,
             "match_confidence": 0.0}
    query = build_query(wine_name)

    api_data = fetch_vivino_via_api(query, vintage)
    if api_data and (api_data.get("rating") or api_data.get("vivino_url")):
        return api_data

    try:
        driver.get(f"https://www.vivino.com/search/wines"
                   f"?q={requests.utils.quote(query)}&language=fr")
        try: WebDriverWait(driver, 9).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "[class*='wineCard'],[class*='wine-card'],[class*='averageValue'],[href*='/w/']")))
        except Exception: pass
        time.sleep(1)
        candidates = vivino_candidates_from_search(driver.page_source)
        best, confidence = choose_best_vivino_candidate(query, vintage, candidates)
    except Exception:
        return EMPTY

    if not best:
        fallback = fetch_vivino_via_api(query, vintage)
        return fallback if fallback else EMPTY

    wine_url = best.get("url", "")
    if not wine_url:
        return EMPTY

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

    vy = None
    m = re.search(r"[?&]year=(\d{4})", driver.current_url)
    if m:
        vy = int(m.group(1))
    elif best.get("year"):
        vy = best.get("year")

    vmatch = None
    if vintage and vy:
        vmatch = (vintage == vy)
    elif not vintage:
        vmatch = True

    if not d.get("rating"):
        return {"rating": None, "ratings_count": 0,
                "vivino_url": wine_url, "vivino_year": vy, "vintage_match": vmatch,
                "match_confidence": round(confidence, 3)}
    return {"rating": d["rating"], "ratings_count": d["ratings_count"],
            "vivino_url": wine_url, "vivino_year": vy, "vintage_match": vmatch,
            "match_confidence": round(confidence, 3)}


# ═══════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════

def load_wines_from_cache(slug: str) -> list:
    lc = load_leclerc_cache(slug)
    if not lc: return []
    return _merge_vivino(lc["wines"], load_vivino_cache(), load_price_history())


def run_check_stock(slug: str, log=None) -> list:
    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache()
    if lc:
        if log: log(f"📦 Cache Leclerc ({fmt_age(lc['cached_at'])}) — vérif. stock…")
        wines = check_availability(slug, lc["wines"], log=log)
        save_leclerc_cache(slug, wines)
    else:
        if log: log("🚀 Pas de cache — scrape Leclerc complet…")
        wines = scrape_leclerc_full(slug, log=log)
        for w in wines: w["available"] = True
        save_leclerc_cache(slug, wines)
        if log: log(f"💾 Cache Leclerc sauvegardé ({len(wines)} vins)")
    return _merge_vivino(wines, vc, load_price_history())


def _scrape_vivino_list(slug, wines, todo, vc, log):
    """Boucle de scraping Vivino mutualisée pour run_refresh et run_fill."""
    now = time.time()
    found = 0
    done_count = len(wines) - len(todo)
    interrupted = False
    driver = make_driver()
    try:
        for wine in todo:
            ean = wine.get("ean") or build_query(wine["name"])
            key = build_query(wine["name"])
            if vc.get(key, {}).get("locked"):
                done_count += 1
                if log: log(f"  🔒 [{done_count}/{len(wines)}] {wine['name'][:38]} (correction manuelle conservée)")
                continue
            vd  = fetch_vivino(driver, wine["name"], wine.get("vintage"))
            vc[key] = {
                **vd,
                "cached_at": now,
                "locked": False,
                "manual_override": False,
                "suppressed": False,
            }
            save_vivino_cache(vc)
            ckpt_tick(slug, ean)
            done_count += 1
            if vd.get("rating"):
                found += 1
                cnt_s = f"{vd['ratings_count']:,}".replace(",", "\u202f") \
                        if vd["ratings_count"] else "—"
                if log: log(f"  ✅ [{done_count}/{len(wines)}] {wine['name'][:38]}\n"
                            f"     ★ {vd['rating']} · {cnt_s} avis")
            else:
                if log and done_count % 5 == 0:
                    log(f"  🍷 [{done_count}/{len(wines)}] — {found} notes trouvées")
            time.sleep(0.3)
    except Exception as e:
        interrupted = True
        if log: log(f"⚠️ Interrompu à [{done_count}/{len(wines)}] : {e}")
    finally:
        try: driver.quit()
        except: pass
        save_vivino_cache(vc)
        remaining = len(wines) - done_count
        if interrupted or remaining > 0:
            if log: log(f"\n⚠️ {done_count}/{len(wines)} traités · {remaining} restants\n"
                        f"💡 Cliquez **▶️ Reprendre** pour continuer")
        else:
            ckpt_finish(slug)
            if log: log(f"✅ Terminé — {found} notes · {len(vc)} entrées cache")


def run_refresh_vivino(slug: str, resume: bool = False, log=None) -> list:
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("🚀 Pas de cache Leclerc — scrape complet…")
        wines = scrape_leclerc_full(slug, log=log)
        for w in wines: w["available"] = True
        save_leclerc_cache(slug, wines)
    else:
        wines = lc["wines"]
        for w in wines: w.setdefault("available", True)
    vc   = load_vivino_cache()
    ckpt = ckpt_load(slug) if resume else None
    if ckpt:
        done_eans = set(ckpt["done_eans"])
        n_done    = len(done_eans)
        if log: log(f"🔁 Reprise : {n_done}/{len(wines)} ({int(100*n_done/max(len(wines),1))}%) déjà traités")
    else:
        done_eans = set()
        ckpt_create(slug, len(wines))
    todo = [w for w in wines if (w.get("ean") or build_query(w["name"])) not in done_eans]
    if not todo:
        if log: log("✅ Tous les vins sont déjà dans le cache !")
        ckpt_finish(slug)
    else:
        n_skip = len(wines) - len(todo)
        if log: log(f"🍷 {len(todo)} vins à scraper" + (f" ({n_skip} ignorés)" if n_skip else "") + "…")
        _scrape_vivino_list(slug, wines, todo, vc, log)
    return _merge_vivino(wines, vc, load_price_history())


def run_fill_missing_vivino(slug: str, log=None) -> list:
    """② Scrape uniquement les vins sans note ET sans URL Vivino."""
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("❌ Pas de cache Leclerc. Lancez d'abord 🔄 Vérifier disponibilité.")
        return []
    vc    = load_vivino_cache()
    wines = lc["wines"]
    for w in wines: w.setdefault("available", True)
    missing = [w for w in wines
               if not vc.get(build_query(w["name"]), {}).get("locked")
               and not vc.get(build_query(w["name"]), {}).get("rating")
               and not vc.get(build_query(w["name"]), {}).get("vivino_url")]
    if not missing:
        if log: log("✅ Tous les vins ont déjà une note ou un lien Vivino !")
        return _merge_vivino(wines, vc, load_price_history())
    if log: log(f"🔍 {len(missing)}/{len(wines)} vins sans données Vivino…")
    ckpt_create(slug, len(missing))
    _scrape_vivino_list(slug, missing, missing, vc, log)
    return _merge_vivino(wines, vc, load_price_history())


# ═══════════════════════════════════════════════════════════════════════════
# RENDU HTML
# ═══════════════════════════════════════════════════════════════════════════

def stars(r: float) -> str:
    return "".join("★" if r >= i else ("½" if r >= i-.5 else "☆") for i in range(1, 6))

def fmt_count(n: int) -> str:
    if not n: return "—"
    return f"{n:,}".replace(",", "\u202f")


def wine_card_html(wine: dict, rank: int, max_score: float) -> str:
    cls = {1:"top1",2:"top2",3:"top3"}.get(rank, "")
    if wine.get("vintage_match") is False: cls = (cls + " vintage-warn").strip()
    if not wine.get("available", True):    cls = (cls + " unavailable").strip()
    icon = {1:"🥇",2:"🥈",3:"🥉"}.get(rank, f"<span style='font-size:.75rem'>#{rank}</span>")

    name = wine["name"]
    name_html = (f'<a href="{wine["url"]}" target="_blank" '
                 f'style="color:#1A0810;text-decoration:none">{name}</a>'
                 ) if wine.get("url") else name
    yr = (f' <span style="color:#8B6B72;font-size:.68rem;font-weight:400">'
          f'{wine["vintage"]}</span>') if wine.get("vintage") else ""
    unavail = (' <span style="font-size:.62rem;color:#dc2626">⛔ indispo</span>'
               if not wine.get("available", True) else "")
    mil = ""
    if wine.get("vivino_year") and wine.get("vintage") and wine["vivino_year"] != wine["vintage"]:
        mil = (f'<div class="wine-sub" style="color:#c17a00">'
               f'⚠️ Vivino={wine["vivino_year"]} / Leclerc={wine["vintage"]}</div>')
    links = []
    if wine.get("url"):
        links.append(f'<a href="{wine["url"]}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>')
    if wine.get("vivino_url"):
        links.append(f'<a href="{wine["vivino_url"]}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>')
    links_html = (f'<div class="wine-links">' + "".join(links) + "</div>") if links else ""

    score  = wine.get("score") or 0
    rating = wine.get("rating")
    region = wine.get("region", "")
    badges = ""
    if score > 0 and rank <= 5:  badges += '<span class="badge b-deal">🔥 Top score</span>'
    if rating and rating >= 4.2:  badges += '<span class="badge b-top">★ Top noté</span>'
    if region:                    badges += f'<span class="badge b-reg">{region}</span>'

    if rating:
        cnt = wine.get("ratings_count") or 0
        rating_col = (f'<div class="wine-rating">'
                      f'<span class="stars">{stars(rating)}</span>'
                      f'<span class="r-num">{rating:.1f}</span>'
                      f'<span class="r-cnt">{fmt_count(cnt)} avis</span>'
                      f'</div>')
    else:
        rating_col = '<div class="no-rat">—<br>Vivino</div>'

    # ⑨ tendance prix
    trend = wine.get("price_trend", "")
    trend_html = {"↑":'<span class="p-up">↑</span>',
                  "↓":'<span class="p-down">↓</span>',
                  "=":'<span class="p-eq">=</span>'}.get(trend, "")
    price_s = f'{wine.get("price") or 0:.2f} €'.replace(".", ",")

    # ① score composite
    pct = min(100, (score / max_score) * 100) if max_score > 0 else 0
    score_col = (
        f'<div class="score-wrap">'
        f'<div class="score-num">{score:.2f}</div>'
        f'<div class="score-lbl">score</div>'
        f'<div class="score-bar"><div class="score-fill" style="width:{pct:.1f}%"></div></div>'
        f'</div>'
    ) if score else '<div class="score-wrap" style="color:#ccc;font-size:.72rem">—</div>'

    return (f'<div class="wine-card {cls}">'
            f'<div class="wine-rank">{icon}</div>'
            f'<div class="wine-info">'
            f'<div class="wine-name">{name_html}{yr}{unavail}</div>'
            f'{mil}{links_html}<div>{badges}</div>'
            f'</div>'
            f'{rating_col}'
            f'<div class="wine-price">{price_s}{trend_html}</div>'
            f'{score_col}'
            f'</div>')


def _make_logger(max_lines: int = 10):
    import streamlit as _st
    logs, box = [], _st.empty()
    def _log(msg: str):
        logs.append(msg)
        box.markdown("\n\n".join(logs[-max_lines:]))
    return _log, box


# ═══════════════════════════════════════════════════════════════════════════
# APP STREAMLIT
# ═══════════════════════════════════════════════════════════════════════════

st.markdown('<div class="main-title">Cave <span>Leclerc Blagnac</span> × Vivino</div>',
            unsafe_allow_html=True)
st.markdown('<div class="subtitle">Comparateur qualité / prix · Blagnac</div>',
            unsafe_allow_html=True)

for k, v in [("wines",[]),("loaded_slug",None),("data_ready",False)]:
    if k not in st.session_state: st.session_state[k] = v

# ── SIDEBAR ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🍾 Type de vin")
    wine_label = st.selectbox("Type", list(WINE_TYPES), label_visibility="collapsed")
    slug       = WINE_TYPES[wine_label]

    st.divider()
    st.markdown("### 🔄 Mise à jour")

    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache()

    # ⑥ Stats couverture
    if lc:
        n_total   = len(lc["wines"])
        n_rated   = sum(1 for w in lc["wines"]
                        if vc.get(build_query(w["name"]), {}).get("rating"))
        n_missing = n_total - n_rated
        cov_pct   = int(100 * n_rated / max(n_total, 1))
        st.caption(f"📦 **Leclerc** : {fmt_age(lc['cached_at'])} · {n_total} vins")
        st.caption(f"🍷 **Vivino** : {n_rated}/{n_total} ({cov_pct}%)"
                   + (f" · ⚠️ {n_missing} manquants" if n_missing else " · ✅ complet"))
    else:
        n_missing = 0
        st.caption("📦 **Leclerc** : ❌ pas de cache")
        st.caption("🍷 **Vivino** : —")

    st.info("💡 **Les données sont en cache sur le serveur.**\n\n"
            "Revenez plus tard : les données se chargent instantanément.", icon=None)

    btn_stock  = st.button("🔄 Vérifier disponibilité", use_container_width=True, type="primary",
                           help="Vérifie les vins en rayon. Vivino depuis le cache.")
    btn_vivino = st.button("🍷 Rafraîchir toutes les notes (arrière-plan)", use_container_width=True,
                           help="Lance le scraping Vivino en tâche de fond pour continuer à utiliser l'app.")
    btn_fill = False
    if n_missing > 0 and lc:
        btn_fill = st.button(f"🔎 Compléter les manquants ({n_missing}) (arrière-plan)",
                             use_container_width=True,
                             help=f"Scrape uniquement les {n_missing} vins sans données Vivino en tâche de fond.")

    ckpt = ckpt_load(slug)
    btn_resume = False
    if ckpt:
        n_done = len(ckpt["done_eans"])
        pct    = int(100 * n_done / max(ckpt["total"], 1))
        st.warning(f"⚠️ **Scraping interrompu**\n\n"
                   f"{n_done}/{ckpt['total']} vins traités ({pct}%)\n\n"
                   f"Lancé {fmt_age(ckpt['started_at'])}", icon=None)
        col_r, col_x = st.columns(2)
        with col_r: btn_resume = st.button("▶️ Reprendre", use_container_width=True, type="primary")
        with col_x:
            if st.button("✖ Annuler", use_container_width=True):
                ckpt_finish(slug); st.rerun()

    job = load_job_state()
    auto_live = st.checkbox("🟢 Suivi temps réel (auto 5s)", value=True,
                            help="Pendant un scraping en arrière-plan, met à jour l'interface automatiquement.")
    if job.get("status") == "running" and job.get("slug") == slug:
        st.info(
            f"⏳ Job en cours ({job.get('mode')})\n\n"
            f"{job.get('message','')}\n\n"
            f"Màj: {fmt_age(job.get('updated_at',0))}",
            icon=None,
        )
    elif job.get("status") == "done" and job.get("slug") == slug:
        st.success(job.get("message", "✅ Job terminé"), icon=None)
    elif job.get("status") == "error" and job.get("slug") == slug:
        st.error(f"Job en erreur: {job.get('error','inconnue')}", icon=None)

    st.caption(f"📍 Leclerc Blagnac · magasin {STORE_CODE}")
    st.divider()
    st.markdown("### 🔧 Filtres")
    search = st.text_input("🔍 Recherche", placeholder="Bordeaux, Guigal…")

    # ⑤ Range slider prix
    price_range = st.slider("💶 Prix (€)", 0, 200, (0, 200), step=5)

    rating_min = st.select_slider("⭐ Note min",
        options=[0.0,3.0,3.5,3.8,4.0,4.2,4.5], value=0.0,
        format_func=lambda x: "Toutes" if x==0 else f"≥ {x} ★")

    # ③ Filtre région
    all_regions_cache = sorted({extract_region(w["name"]) for w in lc["wines"]
                                 if extract_region(w["name"])}) if lc else []
    regions_filter = st.multiselect("🗺️ Région", all_regions_cache, placeholder="Toutes les régions")

    only_vintage = st.checkbox("✅ Millésime confirmé", False)
    only_dispo   = st.checkbox("🏪 Dispos à Blagnac", True)

# ── CHARGEMENT / SCRAPING ─────────────────────────────────────────────────
if slug != st.session_state.loaded_slug:
    st.session_state.wines = []; st.session_state.data_ready = False

if not st.session_state.data_ready and not btn_stock and not btn_vivino \
        and not btn_fill and not btn_resume:
    cached = load_wines_from_cache(slug)
    if cached:
        st.session_state.wines = cached; st.session_state.loaded_slug = slug
        st.session_state.data_ready = True

if btn_stock:
    st.session_state.wines = []; st.session_state.data_ready = False
    with st.status("🔄 Vérification du stock…", expanded=True) as status:
        log, _ = _make_logger(10)
        try:
            raw = run_check_stock(slug, log=log)
        except Exception as e:
            st.error(f"❌ Erreur Selenium : {e}\n\nVérifiez `packages.txt` :\n```\nchromium\nchromium-driver\n```")
            st.stop()
        n_dispo = sum(1 for w in raw if w.get("available",True))
        n_rated = sum(1 for w in raw if w.get("rating"))
        st.session_state.wines = raw; st.session_state.loaded_slug = slug
        st.session_state.data_ready = True
        status.update(label=f"✅ {n_dispo} vins dispo · {n_rated} notes Vivino", state="complete")

if btn_vivino:
    ckpt_finish(slug)
    if start_background_job(slug, "refresh_all"):
        st.success("Scraping Vivino lancé en arrière-plan.")
    else:
        st.warning("Un job est déjà en cours.")

if btn_fill:
    if start_background_job(slug, "fill_missing"):
        st.success("Complétion des manquants lancée en arrière-plan.")
    else:
        st.warning("Un job est déjà en cours.")

if btn_resume:
    if start_background_job(slug, "resume"):
        st.success("Reprise du scraping lancée en arrière-plan.")
    else:
        st.warning("Un job est déjà en cours.")

job_state = load_job_state()
if job_state.get("status") in {"running", "queued"} and job_state.get("slug") == slug:
    latest = load_wines_from_cache(slug)
    if latest:
        st.session_state.wines = latest
        st.session_state.loaded_slug = slug
        st.session_state.data_ready = True
    if auto_live:
        time.sleep(5)
        st.rerun()

if (job_state.get("status") == "done") and job_state.get("slug") == slug:
    latest = load_wines_from_cache(slug)
    if latest:
        st.session_state.wines = latest
        st.session_state.loaded_slug = slug
        st.session_state.data_ready = True

wines = st.session_state.wines
if not wines:
    st.markdown("<br>", unsafe_allow_html=True)
    st.info("👈 Ouvrez le menu et cliquez sur **Vérifier disponibilité** pour charger les vins.")
    st.stop()

# ── FILTRE ────────────────────────────────────────────────────────────────
filtered = [w for w in wines
    if price_range[0] <= (w.get("price") or 0) <= price_range[1]
    and (rating_min == 0 or (w.get("rating") and w["rating"] >= rating_min))
    and (not search or search.lower() in w["name"].lower())
    and (not only_vintage or w.get("vintage_match") is True)
    and (not only_dispo or w.get("available", True))
    and (not regions_filter or w.get("region","") in regions_filter)]

# ── TRI ───────────────────────────────────────────────────────────────────
SORTS = {
    "🏆 Score":   lambda x: -(x.get("score") or 0),
    "⭐ Note":    lambda x: -(x.get("rating") or 0),
    "💶 Prix ↑": lambda x:  x.get("price") or 999,
    "💶 Prix ↓": lambda x: -(x.get("price") or 0),
}
sort_cols = st.columns(len(SORTS))
if "sort_key" not in st.session_state: st.session_state.sort_key = "🏆 Score"
for col, (label, _) in zip(sort_cols, SORTS.items()):
    with col:
        active = st.session_state.sort_key == label
        if st.button(label, key=f"sort_{label}",
                     type="primary" if active else "secondary",
                     use_container_width=True):
            st.session_state.sort_key = label
filtered.sort(key=SORTS.get(st.session_state.sort_key, SORTS["🏆 Score"]))

# ── ONGLETS ───────────────────────────────────────────────────────────────
tab_rank, tab_deals, tab_data, tab_export = st.tabs(
    ["🏅 Classement", "💡 Bonnes Affaires", "📊 Données & Cache", "📥 Export"])

# ── CLASSEMENT ────────────────────────────────────────────────────────────
with tab_rank:
    c1,c2,c3,c4,c5 = st.columns(5)
    prices = [w["price"] for w in filtered if w.get("price")]
    rated  = [w["rating"] for w in filtered if w.get("rating")]
    best   = max(filtered, key=lambda x: x.get("score") or 0, default=None)
    n_rated_fil = sum(1 for w in filtered if w.get("rating"))
    with c1: st.metric("🍷 Vins", f"{len(filtered)}" + (f"/{len(wines)}" if len(filtered)!=len(wines) else ""))
    with c2: st.metric("💶 Prix moy.", f"{sum(prices)/len(prices):.2f} €".replace(".",",") if prices else "—")
    with c3: st.metric("⭐ Note moy.", f"★ {sum(rated)/len(rated):.2f}" if rated else "—")
    with c4: st.metric("🏆 Meilleur score", f"{best['score']:.2f}" if best and best.get("score") else "—")
    with c5: st.metric("📊 Couverts Vivino", f"{n_rated_fil}/{len(filtered)}" if filtered else "—",
                       delta=None if n_rated_fil==len(filtered) else f"{len(filtered)-n_rated_fil} sans note")

    n_bad = sum(1 for w in filtered if w.get("vintage_match") is False)
    if n_bad: st.warning(f"⚠️ {n_bad} vins avec millésime différent Leclerc / Vivino (bordure orange).")
    st.divider()
    if not filtered:
        st.info("Aucun vin ne correspond aux filtres.")
    else:
        max_score = max((w.get("score") or 0 for w in filtered), default=1)
        for i, w in enumerate(filtered):
            st.markdown(wine_card_html(w, i+1, max_score), unsafe_allow_html=True)

# ── BONNES AFFAIRES  ⑧ ──────────────────────────────────────────────────
with tab_deals:
    st.markdown("#### 💡 Bonnes Affaires")
    st.caption("Critères : note ≥ 4.0 · prix ≤ 15 € · ≥ 500 avis · disponible")

    deals = sorted(
        [w for w in wines if (w.get("rating") or 0) >= 4.0
                          and (w.get("price") or 999) <= 15
                          and (w.get("ratings_count") or 0) >= 500
                          and w.get("available", True)],
        key=lambda x: -(x.get("score") or 0))

    if not deals:
        deals_soft = sorted(
            [w for w in wines if (w.get("rating") or 0) >= 3.8
                              and (w.get("price") or 999) <= 20
                              and (w.get("ratings_count") or 0) >= 100
                              and w.get("available", True)],
            key=lambda x: -(x.get("score") or 0))
        if deals_soft:
            st.info("Aucun vin ne remplit les critères stricts. Résultats assouplis : ≥ 3.8★ · ≤ 20€ · ≥ 100 avis.")
            deals = deals_soft[:20]
        else:
            st.info("Aucune bonne affaire identifiée. Lancez **🔎 Compléter les manquants** pour enrichir les données.")
    else:
        st.success(f"🎉 {len(deals)} bonne(s) affaire(s) !")

    for w in deals[:30]:
        score = w.get("score") or 0
        trend = w.get("price_trend","")
        trend_h = {"↑":'<span class="p-up">↑</span>',"↓":'<span class="p-down">↓</span>',
                   "=":'<span class="p-eq">=</span>'}.get(trend,"")
        url_lec = f'<a href="{w["url"]}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>' if w.get("url") else ""
        url_viv = f'<a href="{w["vivino_url"]}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>' if w.get("vivino_url") else ""
        region  = w.get("region","")
        st.markdown(f"""
<div class="deal-card">
  <div style="display:flex;align-items:center;gap:1rem;flex-wrap:wrap">
    <div style="text-align:center;min-width:52px">
      <div class="deal-score">{score:.2f}</div>
      <div class="deal-label">score</div>
    </div>
    <div style="flex:1;min-width:0">
      <div style="font-weight:700;font-size:.95rem;color:#1A0810;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{w["name"]}</div>
      <div style="font-size:.75rem;color:#8B6B72;margin-top:.15rem">
        {'🗺️ '+region+' · ' if region else ''}★ {w.get("rating",0):.1f}
        · {fmt_count(w.get("ratings_count",0))} avis
        · <strong>{(w.get("price") or 0):.2f} €</strong>{trend_h}
      </div>
      <div class="wine-links" style="margin-top:.3rem">{url_lec}{url_viv}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

# ── DONNÉES ───────────────────────────────────────────────────────────────
with tab_data:
    st.markdown("#### Tous les vins chargés")
    df_w = pd.DataFrame([{
        "Nom":         w["name"],
        "Région":      w.get("region",""),
        "Millésime":   w.get("vintage") or "",
        "Prix (€)":    w.get("price") or 0,
        "Tendance":    w.get("price_trend",""),
        "Note":        w.get("rating") or "",
        "Nb avis":     w.get("ratings_count") or "",
        "Score":       w.get("score") or "",
        "Dispo":       "✅" if w.get("available",True) else "⛔",
        "Mil. OK":     {True:"✅",False:"⚠️",None:"—"}.get(w.get("vintage_match"),"—"),
        "Leclerc":     w.get("url") or "",
        "Vivino":      w.get("vivino_url") or "",
        "Query":       build_query(w["name"]),
    } for w in wines])
    st.dataframe(df_w, use_container_width=True, hide_index=True, height=450,
        column_config={
            "Leclerc":  st.column_config.LinkColumn(display_text="🛒"),
            "Vivino":   st.column_config.LinkColumn(display_text="🍷"),
            "Prix (€)": st.column_config.NumberColumn(format="%.2f"),
            "Note":     st.column_config.NumberColumn(format="%.1f"),
            "Score":    st.column_config.NumberColumn(format="%.2f"),
        })

    st.divider()
    st.markdown("#### 🗂️ Cache Vivino")
    vc_now = load_vivino_cache()
    n_ok   = sum(1 for v in vc_now.values() if v.get("rating"))
    n_av   = sum(1 for v in vc_now.values() if (v.get("ratings_count") or 0)>0)
    n_url2 = sum(1 for v in vc_now.values() if v.get("vivino_url"))
    st.caption(f"{len(vc_now)} entrées · {n_ok} notes · {n_av} nb avis · {n_url2} URLs")
    df_c = pd.DataFrame([{
        "Query":     k,
        "Note":      v.get("rating") or "",
        "Nb avis":   v.get("ratings_count") or "",
        "Vivino":    v.get("vivino_url") or "",
        "Millésime": v.get("vivino_year") or "",
        "Confiance": v.get("match_confidence") or "",
        "Type":      vivino_cache_type(v),
        "🔒":        "🔒" if v.get("locked") else "",
        "Màj":       fmt_age(v.get("cached_at",0)),
    } for k, v in vc_now.items()])
    st.dataframe(df_c, use_container_width=True, hide_index=True, height=400,
        column_config={"Vivino":st.column_config.LinkColumn(display_text="🍷"),
                       "Note":  st.column_config.NumberColumn(format="%.1f")})

    st.divider()
    st.markdown("#### 🛠️ Correction manuelle Vivino")
    names = sorted({w["name"] for w in wines})
    if names:
        selected_name = st.selectbox("Vin à corriger", names, key="manual_vivino_name")
        manual_key = build_query(selected_name)
        current = vc_now.get(manual_key, {})

        c1, c2, c3 = st.columns(3)
        with c1:
            rating_input = st.text_input("Note (0-5)", value="" if current.get("rating") is None else str(current.get("rating")), key="manual_rating")
        with c2:
            ratings_count_input = st.number_input("Nb avis", min_value=0, step=1, value=int(current.get("ratings_count") or 0), key="manual_count")
        with c3:
            year_input = st.text_input("Millésime Vivino", value="" if current.get("vivino_year") is None else str(current.get("vivino_year")), key="manual_year")

        url_input = st.text_input("URL Vivino", value=current.get("vivino_url", ""), key="manual_url")
        col_a, col_b, col_c = st.columns(3)

        if col_a.button("💾 Enregistrer correction", use_container_width=True):
            try:
                rating_val = None if not rating_input.strip() else float(str(rating_input).replace(",", "."))
                if rating_val is not None and not (0 <= rating_val <= 5):
                    raise ValueError("La note doit être entre 0 et 5")
                year_val = None if not year_input.strip() else int(year_input)
                url_val = (url_input or "").strip()
                if rating_val is None and not url_val:
                    raise ValueError("Renseignez au moins une note ou une URL Vivino (sinon utilisez 'Supprimer info Vivino').")
                vc_now[manual_key] = {
                    "rating": rating_val,
                    "ratings_count": int(ratings_count_input or 0),
                    "vivino_url": url_val,
                    "vivino_year": year_val,
                    "vintage_match": None,
                    "manual_override": True,
                    "suppressed": False,
                    "locked": True,
                    "cached_at": time.time(),
                }
                save_vivino_cache(vc_now)
                st.success("Correction enregistrée (entrée verrouillée).")
                st.rerun()
            except Exception as e:
                st.error(f"Valeur invalide: {e}")

        if col_b.button("🧹 Supprimer info Vivino", use_container_width=True):
            vc_now[manual_key] = {
                "rating": None,
                "ratings_count": 0,
                "vivino_url": "",
                "vivino_year": None,
                "vintage_match": None,
                "manual_override": True,
                "suppressed": True,
                "locked": True,
                "cached_at": time.time(),
            }
            save_vivino_cache(vc_now)
            st.success("Info Vivino supprimée et verrouillée (ne sera plus auto-remplie).")
            st.rerun()

        if col_c.button("🔓 Déverrouiller", use_container_width=True):
            if manual_key in vc_now:
                vc_now[manual_key]["locked"] = False
                vc_now[manual_key]["suppressed"] = False
                vc_now[manual_key]["manual_override"] = False
                save_vivino_cache(vc_now)
            st.success("Entrée déverrouillée. Les prochains refresh Vivino pourront la recalculer.")
            st.rerun()

    ph = load_price_history()
    if ph:
        st.divider()
        st.markdown("#### 📈 Historique des prix  ⑨")
        rows = []
        for ean, entry in ph.items():
            for rec in entry.get("history",[]):
                rows.append({"EAN":ean,"Nom":entry.get("name","")[:40],
                             "Date":rec["date"],"Prix (€)":rec["price"]})
        if rows:
            df_ph = pd.DataFrame(rows).sort_values(["Nom","Date"])
            st.dataframe(df_ph, use_container_width=True, hide_index=True, height=300,
                column_config={"Prix (€)":st.column_config.NumberColumn(format="%.2f")})

# ── EXPORT ────────────────────────────────────────────────────────────────
with tab_export:
    def make_df(ws):
        return pd.DataFrame([{
            "Nom":              w["name"],
            "Région":           w.get("region",""),
            "Millésime":        w.get("vintage") or "",
            "Prix (€)":         w.get("price") or 0,
            "Tendance prix":    w.get("price_trend",""),
            "EAN":              w.get("ean") or "",
            "Note Vivino":      w.get("rating") or "",
            "Nb avis":          w.get("ratings_count") or "",
            "Score":            w.get("score") or "",
            "Millésime Vivino": w.get("vivino_year") or "",
            "Millésime OK":     w.get("vintage_match") or "",
            "Disponible":       w.get("available",True),
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

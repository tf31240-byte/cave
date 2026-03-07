"""
Cave Leclerc Blagnac × Vivino - v5

NOUVEAUX AXES v5 :

  1. I/O DISQUE RÉDUIT DE ~95% (job state + checkpoint)
     • _set_job_state() : buffer en mémoire, flush ≤1×/s au lieu de
       N×/s (une écriture par log, soit 200-300 écritures/run).
       Seuls status=done/error forcent un flush immédiat.
     • ckpt_tick() : même approche, flush toutes les 3s au lieu de
       1 écriture par vin (gain de 30-100× sur 100 vins).
     • _read_json_cached() : cache en mémoire de process (TTL 2s) pour
       load_vivino_cache / load_leclerc_cache / load_job_state — élimine
       les relectures disque répétées à chaque render Streamlit.

  2. FORMULE DE SCORE CORRIGÉE : log(1+prix) au lieu de prix linéaire
     Avant : un 3.5★ à 4€ (score 4.38) écrasait un 4.6★ à 25€ (score 1.84).
     Après : 3.5★ à 4€ → 4.03 | 4.6★ à 25€ → 5.65.
     log(1+prix) compresse l'axe prix de façon perceptuellement cohérente.

  3. MULTI-QUERY FALLBACK VIVINO (API + Selenium)
     Quand la query principale renvoie zéro candidat valide, on tente
     automatiquement 4 niveaux de repli dans l'ordre :
       ① sans appellation  ② ASCII normalisé (accents supprimés)
       ③ 3 premiers mots   ④ 2 premiers mots (domaine seul)
     Cela améliore la couverture des vins avec des noms atypiques ou
     des orthographes non standard sur Vivino.

CORRECTIFS PRÉCÉDENTS (v3-v4) : voir historique git.
"""

import re, json, time, math, unicodedata, threading, html as _html
import concurrent.futures
from functools import lru_cache
import streamlit as st
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
VIVINO_SIMILARITY_MIN  = 0.28
VIVINO_CANDIDATES_MAX  = 8
VIVINO_API_TIMEOUT     = 8
VIVINO_CACHE_TTL_DAYS  = 30    # Entrées Vivino auto-marquées stale après N jours
CARDS_PER_PAGE         = 24    # Nb de cartes affichées par page dans le classement

VIVINO_API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.vivino.com/",
}

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
JOB_STATE_PATH    = CACHE_DIR / "job_state.json"
JOB_LOG_PATH      = CACHE_DIR / "job_log.txt"
REJECTION_LOG_PATH = CACHE_DIR / "vivino_rejections.json"  # Apprentissage rejets Vivino

WINE_TYPES = {
    "🔴 Rouge":    "vins-rouges",
    "⚪ Blanc":    "vins-blancs",
    "🌸 Rosé":     "vins-roses",
    "🍾 Mousseux": "vins-mousseux-et-petillants",
}

# mapping slug Leclerc → wine_type_id Vivino (1=rouge, 2=blanc, 3=mousseux, 4=rosé)
# FIX : vins-roses était 7 (dessert) au lieu de 4 (rosé)
VIVINO_TYPE_IDS: dict[str, int] = {
    "vins-rouges":                    1,
    "vins-blancs":                    2,
    "vins-roses":                     4,   # FIX bug : 7=dessert, 4=rosé
    "vins-mousseux-et-petillants":    3,
}

def _make_session() -> requests.Session:
    """Session HTTP avec retry automatique et connection pooling."""
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.4,
                  status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=16))
    s.headers.update(VIVINO_API_HEADERS)
    return s

_SESSION = _make_session()

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
  background:white;border-radius:12px;padding:.85rem 1rem;margin-bottom:.45rem;
  border-left:4px solid #6B1A2A;box-shadow:0 2px 10px rgba(26,8,16,.07);
  display:grid;grid-template-columns:2.2rem 1fr auto auto auto;
  align-items:center;gap:.6rem;
  transition:box-shadow .15s ease, transform .15s ease}
.wine-card:hover{box-shadow:0 6px 20px rgba(26,8,16,.13);transform:translateY(-1px)}
.wine-card.top1{border-left-color:#C9A84C;background:linear-gradient(100deg,#fffdf4,#fff)}
.wine-card.top2{border-left-color:#9C9C9C;background:linear-gradient(100deg,#f9f9f9,#fff)}
.wine-card.top3{border-left-color:#CD7F32;background:linear-gradient(100deg,#fdf8f3,#fff)}
.wine-card.vintage-warn{border-right:3px solid #f59e0b}
.wine-card.unavailable{opacity:.38;filter:grayscale(80%)}
.wine-card.stale{border-left-style:dashed}

.wine-rank{font-family:'DM Mono',monospace;font-size:1.2rem;text-align:center}
.wine-info{min-width:0}
.wine-name{font-weight:700;font-size:.9rem;color:#1A0810;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wine-sub{font-size:.7rem;color:#8B6B72;margin-top:.1rem}
.wine-links{display:flex;gap:.35rem;margin-top:.3rem;flex-wrap:wrap;align-items:center}
.lnk{font-size:.65rem;text-decoration:none;border-radius:4px;
  padding:2px 8px;border:1px solid;white-space:nowrap;font-family:'DM Mono';
  transition:background .12s}
.lnk-lec{color:#2563eb;border-color:#2563eb}
.lnk-lec:hover{background:rgba(37,99,235,.08)}
.lnk-viv{color:#7B2D8B;border-color:#7B2D8B}
.lnk-viv:hover{background:rgba(123,45,139,.08)}

.wine-rating{text-align:center;min-width:90px}
.stars{color:#C9A84C;font-size:.9rem;letter-spacing:1px;display:block}
.r-num{font-family:'DM Mono';font-size:.85rem;font-weight:700;color:#1A0810}
.r-cnt{font-size:.6rem;color:#8B6B72}
.no-rat{font-size:.68rem;color:#ccc;font-style:italic;text-align:center;min-width:80px}

.conf-bar{height:3px;border-radius:2px;margin-top:3px;background:#e5e7eb;overflow:hidden}
.conf-fill{height:100%;border-radius:2px}

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
.b-top  {background:rgba(107,26,42,.08);color:#6B1A2A;border:1px solid rgba(107,26,42,.2)}
.b-reg  {background:rgba(37,99,235,.06);color:#1d4ed8;border:1px solid rgba(37,99,235,.2)}
.b-stale{background:rgba(245,158,11,.08);color:#92400e;border:1px solid rgba(245,158,11,.3)}
.b-nat  {background:rgba(22,163,74,.07);color:#15803d;border:1px solid rgba(22,163,74,.25)}
.b-grape{background:rgba(124,58,237,.06);color:#5b21b6;border:1px solid rgba(124,58,237,.2)}
.b-style{background:rgba(8,145,178,.06);color:#0e7490;border:1px solid rgba(8,145,178,.2)}
.b-vol  {background:rgba(180,83,9,.07);color:#92400e;border:1px solid rgba(180,83,9,.2)}

@media (max-width:640px){
  .wine-card{grid-template-columns:1.8rem 1fr;grid-template-rows:auto auto auto;gap:.3rem}
  .wine-rating{grid-column:1/3;display:flex;align-items:center;
    gap:.6rem;justify-content:flex-start;min-width:0}
  .stars{display:inline}
  .wine-price{grid-column:1/3;text-align:left;font-size:.95rem}
  .score-wrap{display:none}
  .wine-name{white-space:normal}
}

/* Deals podium */
.deal-card{background:white;border-radius:12px;padding:1rem 1.1rem;
  margin-bottom:.5rem;border-left:4px solid #C9A84C;
  box-shadow:0 2px 12px rgba(201,168,76,.12);
  display:flex;align-items:center;gap:1rem;
  transition:box-shadow .15s,transform .15s}
.deal-card:hover{box-shadow:0 6px 20px rgba(201,168,76,.22);transform:translateY(-1px)}
.deal-card.d-top{background:linear-gradient(100deg,#fffdf4,#fff);border-left-width:5px}
.deal-score{font-family:'DM Mono';font-size:1.35rem;font-weight:900;
  color:#6B1A2A;line-height:1;text-align:center;min-width:52px}
.deal-label{font-size:.58rem;color:#8B6B72;text-transform:uppercase;letter-spacing:.05em}
.deal-body{flex:1;min-width:0}
.deal-name{font-weight:700;font-size:.92rem;color:#1A0810;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.deal-meta{font-size:.73rem;color:#8B6B72;margin-top:.2rem}
.deal-price{font-family:'DM Mono';font-size:1.05rem;font-weight:700;
  color:#1A0810;white-space:nowrap;text-align:right}

/* Bouton 🚫 — petit rond flottant directement sur la carte */
.bad-viv-wrap button,
div[data-testid="column"] button[kind="secondary"][title*="bad_viv"],
div[data-testid="column"] button[data-testid="baseButton-secondary"]{
  width:1.45rem !important;height:1.45rem !important;
  min-height:0 !important;padding:0 !important;
  border-radius:50% !important;font-size:.68rem !important;
  background:rgba(220,38,38,.06) !important;
  border:1px solid rgba(220,38,38,.2) !important;
  color:#dc2626 !important;line-height:1 !important;
  opacity:.3;transition:opacity .15s,transform .15s
}
div[data-testid="column"] button[data-testid="baseButton-secondary"]:hover{
  opacity:1 !important;transform:scale(1.2)
}

/* Pagination */
.page-info{font-size:.75rem;color:#8B6B72;font-family:'DM Mono';
  text-align:center;padding:.4rem}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════════════════════════

def _lec_path(slug): return CACHE_DIR / f"leclerc_{slug}.json"
def _viv_path():     return CACHE_DIR / "vivino.json"

def load_leclerc_cache(slug: str) -> dict | None:
    p = _lec_path(slug)
    d = _read_json_cached(p, ttl=30.0)   # TTL long : le cache Leclerc évolue peu
    if not isinstance(d, dict): return None
    if time.time() - d.get("cached_at", 0) < LECLERC_CACHE_TTL:
        return d
    return None

def save_leclerc_cache(slug: str, wines: list) -> None:
    p, tmp = _lec_path(slug), _lec_path(slug).with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps({"cached_at": time.time(), "slug": slug, "wines": wines},
                       ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)
        _invalidate_mem_cache(p)
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
    # Fix 6 : coerce les types pour survivre aux caches JSON corrompus ou anciens
    # rating : str '3.5' → float, float 3.5 → float, None → None
    if out["rating"] is not None:
        try:
            out["rating"] = round(float(str(out["rating"]).replace(",", ".")), 2)
        except (ValueError, TypeError):
            out["rating"] = None
    # ratings_count : float 1500.0 → int, str '1500' → int
    try:
        out["ratings_count"] = int(out["ratings_count"] or 0)
    except (ValueError, TypeError):
        out["ratings_count"] = 0
    return out


def load_vivino_cache() -> dict:
    p = _viv_path()
    raw = _read_json_cached(p, ttl=_MEM_CACHE_TTL)
    if not isinstance(raw, dict):
        return {}
    ttl_secs = VIVINO_CACHE_TTL_DAYS * 86400
    now = time.time()
    result = {}
    for k, v in raw.items():
        entry = _normalize_vivino_entry(v)
        # Marquer les entrées non-verrouillées dépassant le TTL comme stales
        age = now - (entry.get("cached_at") or 0)
        entry["_stale"] = (
            not entry.get("locked")
            and entry.get("rating") is not None
            and age > ttl_secs
        )
        result[k] = entry
    return result

def save_vivino_cache(cache: dict) -> None:
    p, tmp = _viv_path(), _viv_path().with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)
        _invalidate_mem_cache(p)
    except Exception: tmp.unlink(missing_ok=True); raise


# ═══════════════════════════════════════════════════════════════════════════
# REJETS VIVINO — Apprentissage des erreurs de correspondance
# ═══════════════════════════════════════════════════════════════════════════

# Raisons de rejet — utilisées dans l'UI et pour adapter la stratégie de recherche
REJECTION_REASONS = {
    "wrong_wine":     "🍷 Mauvais vin (autre château/domaine)",
    "wrong_vintage":  "📅 Mauvais millésime",
    "wrong_producer": "🏭 Mauvais producteur (même appellation)",
    "other":          "❓ Autre",
}

def load_vivino_rejections() -> dict:
    """Charge le log des rejets. Structure : {query → {rejected_urls: [...], history: [...]}}"""
    if not REJECTION_LOG_PATH.exists():
        return {}
    try:
        raw = json.loads(REJECTION_LOG_PATH.read_text("utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}

def save_vivino_rejection(wine_name: str, query: str, rejected_url: str,
                          rejected_title: str, reason: str) -> None:
    """Enregistre un rejet et met à jour l'index des URLs rejetées pour ce vin."""
    data = load_vivino_rejections()
    entry = data.setdefault(query, {"rejected_urls": [], "history": []})
    # Ajouter l'URL à la liste noire si pas déjà présente
    if rejected_url and rejected_url not in entry["rejected_urls"]:
        entry["rejected_urls"].append(rejected_url)
    # Historique complet
    entry["history"].append({
        "wine_name":      wine_name,
        "rejected_url":   rejected_url,
        "rejected_title": rejected_title,
        "reason":         reason,
        "ts":             time.time(),
    })
    # Analyser les patterns de rejets pour améliorer la stratégie
    reasons = [h["reason"] for h in entry["history"]]
    entry["dominant_reason"] = max(set(reasons), key=reasons.count)
    # Si la majorité des rejets = mauvais vin → baisser le seuil de confiance requis
    # ou au contraire marquer le vin comme "difficile à trouver"
    entry["hard_to_match"] = (
        reasons.count("wrong_wine") >= 2
        or len(entry["rejected_urls"]) >= 3
    )
    try:
        tmp = REJECTION_LOG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(REJECTION_LOG_PATH)
    except Exception:
        pass

def get_rejected_urls(query: str, rejections: dict) -> set:
    """Retourne l'ensemble des URLs Vivino rejetées pour ce vin."""
    return set(rejections.get(query, {}).get("rejected_urls", []))

def is_hard_to_match(query: str, rejections: dict) -> bool:
    """True si ce vin a été signalé trop souvent comme mal matchés → skip Vivino."""
    return rejections.get(query, {}).get("hard_to_match", False)


# ═══════════════════════════════════════════════════════════════════════════
# CHECKPOINT
# ═══════════════════════════════════════════════════════════════════════════

def _ckpt_path(slug: str) -> Path: return CACHE_DIR / f"vivino_ckpt_{slug}.json"

# Buffer pour ckpt_tick : accumule les EANs, flush toutes les 3s
_ckpt_pending:    dict  = {}   # slug -> [ean, ...]
_ckpt_last_flush: dict  = {}   # slug -> timestamp
_CKPT_FLUSH_INTERVAL    = 3.0  # secondes

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
    # ⑥ CORRIGÉ : nettoyage explicite de l'ancien checkpoint avant création
    ckpt_finish(slug)
    p, tmp = _ckpt_path(slug), _ckpt_path(slug).with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps({"slug": slug, "started_at": time.time(),
            "total": total, "done_eans": [], "finished": False},
            ensure_ascii=False), "utf-8")
        tmp.replace(p)
    except Exception: tmp.unlink(missing_ok=True); raise

def ckpt_tick(slug: str, ean: str) -> None:
    """
    Accumule les EANs en mémoire et ne flush le checkpoint sur disque
    que toutes les 3 secondes. Évite N écritures pour N vins scrapés.
    """
    global _ckpt_pending, _ckpt_last_flush
    _ckpt_pending.setdefault(slug, []).append(ean)
    now = time.time()
    if now - _ckpt_last_flush.get(slug, 0) >= _CKPT_FLUSH_INTERVAL:
        _flush_ckpt(slug)

def _flush_ckpt(slug: str) -> None:
    """Écrit en une seule passe tous les EANs accumulés depuis le dernier flush."""
    global _ckpt_pending, _ckpt_last_flush
    pending = _ckpt_pending.get(slug, [])
    if not pending:
        return
    p = _ckpt_path(slug)
    if not p.exists():
        # Fichier absent (ex: ckpt_finish appelé avant) → on vide juste le buffer
        _ckpt_pending[slug] = []
        _ckpt_last_flush[slug] = time.time()
        return
    try:
        d = json.loads(p.read_text("utf-8"))
        d["done_eans"].extend(pending)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, ensure_ascii=False), "utf-8")
        tmp.replace(p)
        # Fix 3 : on ne vide pending QUE si l'écriture a réussi
        _ckpt_pending[slug] = []
        _ckpt_last_flush[slug] = time.time()
    except Exception:
        # Échec d'écriture → on conserve pending pour réessayer au prochain tick
        pass

def ckpt_finish(slug: str) -> None:
    # Flush les EANs en attente avant de marquer terminé
    _flush_ckpt(slug)
    _ckpt_path(slug).unlink(missing_ok=True)
    _ckpt_path(slug).with_suffix(".tmp").unlink(missing_ok=True)


_job_lock = threading.Lock()
_job_thread = None

# ── Cache JSON en mémoire (évite les relectures disque à chaque render) ────
# Structure : {path_str: (timestamp, data)}
_mem_cache: dict = {}
_MEM_CACHE_TTL = 2.0  # secondes avant re-lecture disque

def _read_json_cached(path: Path, ttl: float = _MEM_CACHE_TTL):
    """Lit un fichier JSON avec cache en mémoire de process (TTL = 2s)."""
    key = str(path)
    now = time.time()
    if key in _mem_cache:
        ts, data = _mem_cache[key]
        if now - ts < ttl:
            return data
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text("utf-8"))
        _mem_cache[key] = (now, data)
        return data
    except Exception:
        return None

def _invalidate_mem_cache(path: Path) -> None:
    """Invalide l'entrée mémoire après une écriture disque."""
    _mem_cache.pop(str(path), None)

# ── Buffer job state : n'écrit sur disque qu'1×/seconde max ───────────────
_job_buf: dict         = {}
_job_buf_last_flush: float = 0.0
_JOB_FLUSH_INTERVAL    = 1.0  # s

def load_job_state() -> dict:
    data = _read_json_cached(JOB_STATE_PATH, ttl=_MEM_CACHE_TTL)
    return data if isinstance(data, dict) else {}


def save_job_state(state: dict) -> None:
    tmp = JOB_STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(JOB_STATE_PATH)
    _invalidate_mem_cache(JOB_STATE_PATH)


def _set_job_state(**kwargs) -> None:
    """
    Mise à jour du job state avec flush différé (≤1 écriture disque/s).
    Seuls status=done/error forcent un flush immédiat pour ne pas perdre
    le résultat final si le process se termine juste après.
    """
    global _job_buf, _job_buf_last_flush
    with _job_lock:
        _job_buf.update(kwargs)
        _job_buf["updated_at"] = time.time()
        force = kwargs.get("status") in {"done", "error"}
        now   = time.time()
        if force or now - _job_buf_last_flush >= _JOB_FLUSH_INTERVAL:
            state = load_job_state()
            state.update(_job_buf)
            save_job_state(state)
            _job_buf_last_flush = now


def _background_job(slug: str, mode: str) -> None:
    global _job_buf, _job_buf_last_flush
    # Fix 1 : vider le buffer du job précédent pour ne pas contaminer
    with _job_lock:
        _job_buf.clear()
        _job_buf_last_flush = 0.0
    # Effacer le log précédent au démarrage d'un nouveau job
    try:
        JOB_LOG_PATH.write_text("", "utf-8")
    except Exception:
        pass
    _set_job_state(status="running", slug=slug, mode=mode, message="Démarrage…", error="")

    def _log(msg: str):
        _set_job_state(message=msg)
        # Appender chaque ligne dans le fichier log → console temps réel
        try:
            ts = time.strftime("%H:%M:%S")
            with JOB_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(f"[{ts}] {msg}\n")
        except Exception:
            pass

    try:
        if mode == "refresh_all":
            raw = run_refresh_vivino(slug, resume=False, log=_log)
        elif mode == "fill_missing":
            raw = run_fill_missing_vivino(slug, log=_log)
        elif mode == "resume":
            raw = run_refresh_vivino(slug, resume=True, log=_log)
        elif mode == "refresh_stale":
            raw = run_refresh_stale_vivino(slug, log=_log)
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
# HISTORIQUE DES PRIX
# ═══════════════════════════════════════════════════════════════════════════

def _price_hist_path() -> Path: return CACHE_DIR / "price_history.json"

def load_price_history() -> dict:
    p = _price_hist_path()
    data = _read_json_cached(p, ttl=60.0)   # TTL 60s — l'historique change peu souvent
    return data if isinstance(data, dict) else {}

def save_price_history(hist: dict) -> None:
    p, tmp = _price_hist_path(), _price_hist_path().with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(hist, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)
        _invalidate_mem_cache(p)            # cohérence du cache mémoire
    except Exception: tmp.unlink(missing_ok=True)

def update_price_history(wines: list) -> None:
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
    if not ean: return ""
    h = ph.get(ean, {}).get("history", [])
    if len(h) < 2: return ""
    prev = h[-2]["price"]
    if current_price > prev + 0.05: return "↑"
    if current_price < prev - 0.05: return "↓"
    return "="


# ═══════════════════════════════════════════════════════════════════════════
# SCORE COMPOSITE
# ═══════════════════════════════════════════════════════════════════════════

def compute_score(rating, ratings_count, price, vintage_match=None) -> float:
    """
    Score qualité/prix composite.

    Formule : note × confiance / log(1 + prix) × 4

    Pourquoi log(prix) plutôt que prix linéaire ?
    Avec l'ancienne formule (÷ prix), un Beaujolais 3.5★ à 4€ (score ~4.4)
    écrasait systématiquement un Pomerol 4.6★ à 25€ (score ~0.9).
    log(1+prix) compresse l'axe prix : la différence entre 4€ et 8€ pèse
    autant que la différence entre 25€ et 50€ — ce qui correspond mieux à
    la perception réelle de la valeur pour un acheteur.

    Bonus millésime (+5%) : si Vivino confirme que le millésime Leclerc
    correspond exactement à celui noté sur Vivino, le score est légèrement
    boosté pour favoriser les vins dont la note est certifiée sur le bon
    millésime (plutôt qu'une note de millésime approché).

    ⑧ Fallback confiance ×0.5 si ratings_count=0 (inchangé depuis v4).
    """
    if rating is None or not price or price <= 0:
        return 0.0
    cnt = ratings_count or 0
    confidence = min(1.0, math.sqrt(cnt) / 100) if cnt > 0 else 0.5
    base = rating * confidence / math.log1p(price) * 4
    if vintage_match is True:
        base *= 1.05   # +5% si millésime confirmé
    return round(base, 2)


# ═══════════════════════════════════════════════════════════════════════════
# RÉGIONS / APPELLATIONS
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

def _norm_ascii(s: str) -> str:
    """Normalise une chaîne en ASCII lowercase (accents supprimés)."""
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower()


def _fuzzy_match(query: str, target: str) -> bool:
    """
    Recherche floue : retourne True si la query correspond au target.
    Strategies (dans l'ordre) :
      1. Sous-chaîne exacte (après normalisation ASCII)
      2. Tous les mots de la query présents dans le target (ordre libre)
      3. Bigrammes : ≥ 60% des bigrammes de la query trouvés dans target
    Exemples : "bordx" ne matche pas, "bord" matche "Bordeaux",
               "saint em" matche "Saint-Émilion".
    """
    q = _norm_ascii(query.strip())
    t = _norm_ascii(target)
    if not q:
        return True
    # 1. Sous-chaîne directe
    if q in t:
        return True
    # 2. Tous les mots présents (tolère ordre différent et tirets)
    t_words = re.sub(r"[^a-z0-9 ]", " ", t)
    words = q.split()
    if len(words) > 1 and all(w in t_words for w in words):
        return True
    # 3. Bigrammes (tolérance aux fautes de frappe courtes)
    if len(q) >= 5:
        def bigrams(s): return {s[i:i+2] for i in range(len(s) - 1)}
        bq, bt = bigrams(re.sub(r"[^a-z]", "", q)), bigrams(re.sub(r"[^a-z]", "", t))
        if bq and len(bq & bt) / len(bq) >= 0.70:
            return True
    return False

# Précompilation des régions normalisées — calculé 1× au démarrage, pas à chaque appel
_REGIONS_NORM: list[tuple[str, str]] = [(r, _norm_ascii(r)) for r in _REGIONS]

def extract_region(wine_name: str) -> str:
    m = re.search(r"-\s*([\w\s\-\']+?)\s*(?:AOP|IGP|AOC|AOP-AOC)\b", wine_name, re.I)
    if m:
        raw = m.group(1).strip()
        raw_n = _norm_ascii(raw)
        for r, rn in _REGIONS_NORM:
            if raw_n == rn: return r
        for r, rn in _REGIONS_NORM:
            if raw_n in rn or rn in raw_n: return r
        if len(raw) > 2: return raw.title()
    name_n = _norm_ascii(wine_name)
    for r, rn in _REGIONS_NORM:
        if rn in name_n: return r
    return ""


# ═══════════════════════════════════════════════════════════════════════════
# _MERGE_VIVINO
# ═══════════════════════════════════════════════════════════════════════════

def vivino_cache_type(entry: dict) -> str:
    if entry.get("suppressed"):
        return "masqué"
    if entry.get("manual_override"):
        return "manuel"
    return "auto"

def _merge_vivino(wines: list, vc: dict, ph: dict | None = None) -> list:
    """
    Injecte données Vivino + calcule score/région/tendance prix.
    Retourne une NOUVELLE liste de copies de dicts pour éviter de muter
    les objets stockés dans st.session_state entre les reruns Streamlit.
    Nouvelles données propagées : winery, vivino_region, grapes, style_name,
    is_natural, acidity, tannin, sweetness, body, ratings_count_all
    """
    if ph is None: ph = {}
    # Champs Vivino enrichis à propager depuis le cache
    _VIVINO_FIELDS = (
        "rating", "ratings_count", "ratings_count_all",
        "vivino_url", "vivino_year", "vintage_match", "match_confidence",
        "vivino_name", "winery", "vivino_region", "vivino_region_seo",
        "country", "grapes", "style_name", "is_natural",
        "acidity", "tannin", "sweetness", "body",
    )
    result = []
    for w in wines:
        w = dict(w)
        key = build_query(w["name"])
        cv  = vc.get(key, {})
        w.setdefault("available", True)
        if cv.get("suppressed"):
            for f in _VIVINO_FIELDS:
                w[f] = [] if f == "grapes" else (0 if f in ("ratings_count","ratings_count_all") else None)
            w["vivino_url"] = ""
        elif cv.get("rating") is not None or cv.get("vivino_url"):
            for f in _VIVINO_FIELDS:
                if f in cv:
                    w[f] = cv[f]
        # Defaults pour les champs manquants
        for f in _VIVINO_FIELDS:
            if f not in w:
                w[f] = [] if f == "grapes" else (0 if f in ("ratings_count","ratings_count_all") else None)
        w.setdefault("vivino_url", "")
        w["score"]       = compute_score(w.get("rating"), w.get("ratings_count"),
                                          w.get("price"), w.get("vintage_match"))
        # Région : préférer la région Vivino (plus précise) si disponible
        w["region"]      = w.get("vivino_region") or extract_region(w["name"])
        w["price_trend"] = price_trend(w.get("ean",""), w.get("price") or 0, ph) if w.get("price") else ""
        # Propager grapes_hint si Vivino n'en a pas
        if not w.get("grapes") and w.get("grapes_hint"):
            w["grapes"] = [g.title() for g in w["grapes_hint"]]
        result.append(w)
    return result


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
        em   = re.search(r"offer_m-(\d{13})-\d+", card.decode_contents())
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
                      "vintage":     int(ym.group(1)) if ym else None,
                      "grapes_hint": extract_grapes_from_name(name),
                      "volume_cl":   extract_volume_cl(name)})
    return wines

def get_nb_pages(html: str) -> int:
    nums = [int(m.group(1))
            for a in BeautifulSoup(html, "html.parser").find_all("a", href=True)
            if (m := re.search(r"[?&]page=(\d+)", a["href"]))]
    return max(nums) if nums else 1


# ═══════════════════════════════════════════════════════════════════════════
# VIVINO — query + parsing + pertinence
# ═══════════════════════════════════════════════════════════════════════════

# ── Amélioration 3 : dictionnaire d'alias ──────────────────────────────────
# Noms Leclerc → nom Vivino canonique.
# Utilisé par build_query si le nom normalisé correspond à un alias connu.
# Format : {fragment normalisé ASCII → query Vivino correcte}
_WINE_ALIASES: dict[str, str] = {
    # Seconds vins et châteaux abrégés
    "hauts de smith":            "Les Hauts de Smith",
    "reserve de leoville barton":"La Réserve de Léoville Barton",
    "carruades de lafite":       "Carruades de Lafite",
    "chapelle de la mission":    "La Chapelle de la Mission",
    "second vin lynch bages":    "Echo de Lynch Bages",
    "pavillon rouge":            "Pavillon Rouge du Château Margaux",
    "benjamin de beauregard":    "Benjamin de Beauregard",
    # Producteurs avec alias courants
    "jaboulet aine":             "Paul Jaboulet Aîné",
    "delas freres":              "Delas Frères",
    "e guigal":                  "E. Guigal",
    "m chapoutier":              "M. Chapoutier",
    "baron philippe":            "Baron Philippe de Rothschild",
    # Domaines avec casse ou accent atypiques
    "clos des papes":            "Clos des Papes",
    "domaine leflaive":          "Domaine Leflaive",
    "domaine leroy":             "Domaine Leroy",
    "denis mortet":              "Denis Mortet",
    "comte armand":              "Comte Armand",
}

# Mots à supprimer de la query car présents dans les noms Leclerc
# mais absents des titres Vivino → diluent le score Jaccard
_NOISE_WORDS = re.compile(
    r"\b(rouge|blanc|rosé|rose|sec|demi-sec|moelleux|brut|nature|"
    r"aop|igp|aoc|appellation|vin de france|vin de pays|"
    r"grand vin|selection|elevé en fûts de chêne|futs de chene|"
    r"mis en bouteille|bouteille|magnum|demi-bouteille)\b",
    re.I | re.U,
)

# Appellations génériques qui polluent la recherche Vivino
# (trop communes — présentes dans des centaines de vins)
# Utilisé aussi par choose_best pour différencier query mono-mot appellation
# (seuil 0.70) vs nom propre court comme Yquem ou Opus (seuil normal 0.28)
_GENERIC_APPELLATIONS = {
    "bordeaux", "bourgogne", "languedoc", "provence",
    "roussillon", "alsace", "loire", "rhone", "sud ouest",
    "pays doc", "vin de france",
    # Appellations mono-mot qui matchent trop facilement
    "pomerol", "margaux", "medoc", "graves", "sancerre",
    "chablis", "beaujolais", "muscadet", "cahors", "madiran",
}

# ── Amélioration 5 : cépages connus ────────────────────────────────────────
# Utilisé pour extraire les cépages du nom Leclerc ET pour booster la
# similarité si les cépages Vivino correspondent à ceux du nom du vin.
_GRAPES = {
    # Rouges
    "cabernet sauvignon", "cabernet franc", "merlot", "malbec", "petit verdot",
    "grenache", "syrah", "shiraz", "mourvèdre", "mourvedre", "carignan", "cinsault",
    "pinot noir", "gamay", "poulsard", "trousseau", "mondeuse",
    "tannat", "fer servadou", "côt", "cot", "duras", "negrette",
    "tempranillo", "garnacha", "bobal",
    # Blancs
    "chardonnay", "sauvignon blanc", "sauvignon", "sémillon", "semillon",
    "chenin blanc", "chenin", "viognier", "marsanne", "roussanne", "grenache blanc",
    "muscadet", "melon de bourgogne", "muscadet",
    "riesling", "gewurztraminer", "gewürztraminer", "pinot gris", "pinot blanc",
    "aligoté", "aligote", "vermentino",
    "mauzac", "ondenc", "len de lel",
    # Rosé
    "cinsaut",
}
# Regex compilée pour extraction rapide depuis un nom de vin
_GRAPE_RE = re.compile(
    r"\b(" + "|".join(re.escape(g) for g in sorted(_GRAPES, key=len, reverse=True)) + r")\b",
    re.I | re.U,
)

def extract_grapes_from_name(wine_name: str) -> list[str]:
    """
    Extrait les cépages présents dans le nom Leclerc.
    Exemples :
      "Côtes du Rhône Grenache Syrah" → ["Grenache", "Syrah"]
      "Château Margaux 2019"          → []
    Retourne des noms en Title Case. Les doublons (mourvèdre/mourvedre)
    sont dédupliqués par normalisation ASCII.
    """
    found, seen_norm = [], set()
    for m in _GRAPE_RE.finditer(wine_name):
        raw  = m.group(0)
        norm = _norm_ascii(raw)
        if norm not in seen_norm:
            seen_norm.add(norm)
            found.append(raw.title())
    return found


# ── Volume depuis nom ───────────────────────────────────────────────────────
_VOLUME_MAP = {
    # Formats courants
    "demi-bouteille": 37.5,
    "half bottle":    37.5,
    "magnum":         150,
    # Grands formats
    "jeroboam":       300,    # 4 bouteilles
    "rehoboam":       450,    # 6 bouteilles (Champagne)
    "imperiale":      600,    # 8 bouteilles (Bordeaux) = Mathusalem Champagne
    "mathusalem":     600,    # 8 bouteilles
    "salmanazar":     900,    # 12 bouteilles
    "balthazar":     1200,    # 16 bouteilles
    "nabuchodonosor": 1500,   # 20 bouteilles
    "nebuchadnezzar": 1500,   # graphie alternative
    "melchior":      1800,    # 24 bouteilles
    "solomon":       2000,
    "sovereign":     2500,
    "primat":        2700,
    "melchizedek":   3000,
}
# Tri par longueur décroissante pour que "demi-bouteille" soit tenté avant "bouteille"
_VOLUME_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in sorted(_VOLUME_MAP, key=len, reverse=True)) + r")\b",
    re.I | re.U,
)
# Nettoyage final des prépositions/articles orphelins en FIN de query
# (résidus après suppression des mots-bruit, ex: "Guigal Côtes du Rhône et")
_TAIL_JUNK_RE = re.compile(
    r"\s+\b(au|aux|du|de|des|en|par|sur|un|une|le|la|les|et|ou|son|ses|leur|leurs)\b\s*$",
    re.I | re.U,
)

def _clean_tail(s: str) -> str:
    """Supprime les prépositions/articles/conjonctions orphelines en fin de chaîne (itératif)."""
    prev = None
    while s != prev:
        prev = s
        s = _TAIL_JUNK_RE.sub("", s).strip()
    return s


# Précalculé au niveau module — utilisé dans build_query (évite recompilation à chaque appel)
_VOLUME_PREFIX_RE = re.compile(
    r"^(" + "|".join(re.escape(k) for k in sorted(_VOLUME_MAP, key=len, reverse=True)) + r")\s+",
    re.I,
)

def extract_volume_cl(wine_name: str) -> float:
    """
    Extrait le volume en cl depuis le nom Leclerc (défaut 75cl).
    Couvre : mots-clés (Magnum, Demi-bouteille, Jeroboam…).
    Les formats numériques (75cl, 1.5L) n'apparaissent pas dans les
    labels produit Leclerc scrapés — inutile de les traiter.
    """
    m = _VOLUME_RE.search(wine_name)
    return _VOLUME_MAP.get(m.group(1).lower(), 75.0) if m else 75.0

# ── Type Vivino → slug Leclerc ─────────────────────────────────────────────
# Permet de détecter si Vivino retourne un vin d'une couleur différente
_VIVINO_TYPE_TO_SLUG: dict[int, str] = {
    1: "vins-rouges", 2: "vins-blancs", 3: "vins-mousseux-et-petillants",  # FIX : était "vins-mousseux"
    4: "vins-roses", 7: "vins-de-dessert", 24: "vins-fortifies",
}


@lru_cache(maxsize=2048)
def build_query(wine_name: str) -> str:
    """
    Construit la query optimale pour la recherche Vivino.

    Améliorations v6 :
    1. Suppression AOP/IGP/AOC en fin de chaîne (pas seulement après tiret)
    2. Suppression des mots de couleur et termes génériques (rouge, blanc, sec…)
    3. Appellations génériques supprimées si elles n'apportent pas de signal
    4. Résolution d'alias : noms Leclerc abrégés → nom Vivino canonique
    """
    # Étape 1 : tronquer au premier séparateur fort (virgule ou " - ")
    nom = re.split(r",\s*|\s+-\s+", wine_name)[0].strip()

    # Étape 2 : supprimer le format en tête (Magnum, Demi-bouteille, Jeroboam…)
    nom_stripped = _VOLUME_PREFIX_RE.sub("", nom).strip()
    # Guard : ne pas vider le nom si le format était le seul mot
    if nom_stripped:
        nom = nom_stripped

    # Étape 3 : supprimer l'année
    nom = re.sub(r"\b(19|20)\d{2}\b", "", nom).strip().strip("-").strip()

    # Étape 4 : supprimer AOP/IGP/AOC/Vin de France où qu'ils soient
    nom = re.sub(r"\s*\b(AOP|IGP|AOC|A\.O\.P|A\.O\.C|I\.G\.P)\b\.?", "", nom, re.I).strip()

    # Étape 5 : normalisation casse (tout-majuscule → Title Case)
    if re.match(r"^[A-Z][A-Z\s\'\-]+$", nom):
        nom = nom.title()

    # Étape 6 : supprimer les mots parasites (couleur, termes de vinif…)
    nom_clean = _NOISE_WORDS.sub(" ", nom).strip()
    nom_clean = re.sub(r"\s{2,}", " ", nom_clean).strip()
    # N'appliquer que si le résultat n'est pas trop court
    if len(nom_clean.split()) >= 2:
        nom = nom_clean

    # Étape 6b : supprimer les prépositions/conjonctions orphelines en fin
    nom = _clean_tail(nom)

    # Étape 7 : couper avant les mots-coupure (Cuvée, Vieilles Vignes…)
    _CUT = {"Cuvée", "Cuvee", "Vieilles", "Vieille", "Grande", "Vignes"}
    words = nom.split()
    for i, w in enumerate(words[2:], 2):
        if w in _CUT:
            nom = " ".join(words[:i])
            break

    # Étape 8 : supprimer les appellations génériques en fin de query
    # (seulement si elles ne sont pas le cœur du nom)
    words = nom.split()
    if len(words) > 2:
        tail = _norm_ascii(words[-1])
        if tail in _GENERIC_APPELLATIONS:
            nom = " ".join(words[:-1]).strip()

    # Étape 9 : résolution d'alias
    nom_key = _norm_ascii(nom)
    for alias_frag, canonical in _WINE_ALIASES.items():
        if alias_frag in nom_key:
            return canonical

    result = nom.strip()
    return result if len(result) > 2 else wine_name[:40].strip()


def _norm_words(s: str) -> set:
    STOP = {"de","du","des","le","la","les","et","au","aux","en","par","sur",
            "un","une","the","of","and","for","vin","wines","wine",
            "rouge","blanc","rose","rosé","sec","brut"}  # couleurs ajoutées au stop
    return {w for w in re.findall(r"[a-z]{3,}", _norm_ascii(s)) if w not in STOP}


def _name_similarity(name1: str, name2: str) -> float:
    """
    Score de similarité combiné :
    - Jaccard sur mots significatifs (70%)
    - Bigrammes caractères (30%)
    - Bonus producteur : mot le plus long de name1 exact dans name2 (+0.10)
    - Pénalité mots exclusifs Vivino : titre Vivino contient des mots absents de la
      query → signal fort de 2nd vin ou vin différent (-0.08 par mot exclusif, max -0.20)
    - Pénalité 1er mot absent : si le 1er mot significatif de la query (≥5 car.) est
      absent du titre Vivino → probablement pas le bon producteur (-0.15)
    """
    w1, w2 = _norm_words(name1), _norm_words(name2)
    if not w1 or not w2: return 0.0
    jaccard = len(w1 & w2) / len(w1 | w2)

    def bigrams(s):
        a = re.sub(r"[^a-z]", "", _norm_ascii(s))
        return {a[i:i+2] for i in range(len(a)-1)} if len(a) > 1 else set()
    bg1, bg2  = bigrams(name1), bigrams(name2)
    bg_score  = len(bg1 & bg2) / len(bg1 | bg2) if (bg1 | bg2) else 0.0

    # Bonus producteur : mot le plus long de name1 présent exactement dans name2
    key_word = max(w1, key=len, default="")
    producer_bonus = 0.10 if key_word and len(key_word) >= 5 and key_word in w2 else 0.0

    # Pénalité mots exclusifs Vivino (absents de la query)
    # Signale un 2nd vin, un sous-domaine ou une cuvée différente
    # ex: "Château Margaux de Brane" → 'brane' exclusif → -0.08
    exclusive_vivino = w2 - w1
    # Ne pénaliser que les mots longs (≥5 car.) — les courts sont souvent des stopwords
    # non filtrés ou des mots sans signal
    extra_penalty = min(0.20, len({w for w in exclusive_vivino if len(w) >= 5}) * 0.08)

    # Pénalité 1er mot absent : le 1er mot significatif de la query (hors "chateau")
    # doit être présent dans le titre Vivino
    first_word_penalty = 0.0
    w1_ordered = [w for w in re.findall(r"[a-z]{3,}", _norm_ascii(name1))
                  if w not in {"chateau","domaine","maison","cave","les","de","du"}]
    if w1_ordered:
        first_sig = w1_ordered[0]
        if len(first_sig) >= 4 and first_sig not in w2:
            first_word_penalty = 0.15

    base = jaccard * 0.7 + bg_score * 0.3
    return round(min(1.0, max(0.0, base + producer_bonus - extra_penalty - first_word_penalty)), 4)


def _safe_year(val) -> int | None:
    """Caste l'année Vivino en int de façon défensive (str '2019' ou int 2019 → int)."""
    if val is None: return None
    try:
        y = int(val)
        return y if 1900 <= y <= 2100 else None
    except (ValueError, TypeError):
        return None

def _extract_year(text: str) -> int | None:
    m = re.search(r"\b(19|20)\d{2}\b", text or "")
    return int(m.group(0)) if m else None


def vivino_candidates_from_search(html: str, max_candidates: int = VIVINO_CANDIDATES_MAX) -> list[dict]:
    """
    Retourne plusieurs candidats Vivino depuis la page de recherche.
    
    ① CORRIGÉ : l'ancienne regex r"/w/[0-9]+" ne matchait que les URLs numériques
    (ex: /w/12345). Les URLs Vivino modernes sont slug-based :
      /w/chateau-latour-rouge-2019  ou  /wines/chateau-margaux
    Nouvelle regex : r"/w(?:ines)?/[^/?&#\x20]+" pour couvrir les deux formats.
    """
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # ① CORRIGÉ : regex étendue pour les URLs slug-based
        if not re.search(r"/w(?:ines)?/[^/?&#\s]+", href) or "search" in href:
            continue
        url = href if href.startswith("http") else f"https://www.vivino.com{href}"
        if url in seen:
            continue
        seen.add(url)
        title = a.get_text(separator=" ", strip=True)   # calculé 1× seulement
        out.append({
            "url":   url,
            "title": title,
            "year":  _extract_year(title),              # réutilise title
        })
        if len(out) >= max_candidates:
            break
    return out


def choose_best_vivino_candidate(
    query: str,
    vintage,
    candidates: list[dict],
    region: str = "",
    rejected_urls: set | None = None,
    grapes_hint: list | None = None,
    slug: str = "vins-rouges",
) -> tuple[dict | None, float]:
    """
    Choisit le meilleur candidat Vivino parmi les résultats.

    Signaux de scoring :
    - Similarité nom (Jaccard + bigrammes + bonus producteur)
    - Boost appellation (+0.30) si région Leclerc dans titre Vivino
    - Millésime exact (+0.20), ±1 an (+0.08), différent (-0.12)
    - Boost cépage (+0.08/cépage, max +0.20) si cépages Leclerc = cépages Vivino
    - Pénalité type incohérent (-0.40) si rouge/blanc/rosé ne correspond pas
    - Boost région Vivino exacte (+0.15) si wine.region.name match
    - Filtre URLs rejetées par l'utilisateur
    """
    best, best_score = None, -1.0
    region_norm  = _norm_ascii(region) if region else ""
    _rejected    = rejected_urls or set()
    _grapes_hint = {_norm_ascii(g) for g in (grapes_hint or [])}

    for c in candidates:
        # Ignorer les candidats dont l'URL a déjà été rejetée
        c_url = c.get("url") or (
            "https://www.vivino.com/wines/" +
            (((c.get("record") or {}).get("vintage") or {}).get("wine") or {}).get("seo_name", "")
            if c.get("record") else ""
        )
        if c_url and c_url in _rejected:
            continue

        score = _name_similarity(query, c.get("title", ""))

        # ── Boost appellation ──────────────────────────────────────────────
        if region_norm and region_norm in _norm_ascii(c.get("title", "")):
            score += 0.30

        # ── Millésime ─────────────────────────────────────────────────────
        c_year = c.get("year")
        if vintage and c_year:
            if c_year == vintage:            score += 0.20
            elif abs(c_year - vintage) == 1: score += 0.08
            else:                            score -= 0.12
        elif vintage and not c_year:
            score -= 0.03

        # ── Boost cépages (nouveau) ────────────────────────────────────────
        # Si le nom Leclerc contient des cépages, vérifier s'ils matchent
        # les cépages retournés par l'API Vivino pour ce candidat
        if _grapes_hint and c.get("record"):
            wine_obj   = (c["record"].get("vintage") or {}).get("wine") or {}
            style      = wine_obj.get("style") or {}
            viv_grapes = {_norm_ascii(g.get("name","")) for g in (style.get("grapes") or [])}
            if viv_grapes:
                common = _grapes_hint & viv_grapes
                grape_boost = min(0.20, len(common) * 0.08)
                score += grape_boost
                # Pénalité si aucun cépage en commun alors qu'on en attendait
                if not common and len(_grapes_hint) >= 2:
                    score -= 0.08

        # ── Pénalité type incohérent (nouveau) ────────────────────────────
        # Si Vivino retourne un vin d'une couleur différente du slug Leclerc
        if c.get("record"):
            wine_obj   = (c["record"].get("vintage") or {}).get("wine") or {}
            viv_type   = wine_obj.get("type_id")
            if viv_type:
                expected_slug = _VIVINO_TYPE_TO_SLUG.get(viv_type, "")
                if expected_slug and expected_slug != slug:
                    score -= 0.75  # pénalité forte : rouge vs blanc/rosé
                    # FIX : -0.40 puis -0.60 insuffisants car les couleurs (blanc/rosé)
                    # sont dans STOP → titre Vivino perd son signal → score de base = 1.0
                    # -0.75 garantit le rejet (1.0 - 0.75 = 0.25 < seuil 0.28)

        # ── Boost région Vivino exacte (nouveau) ──────────────────────────
        # wine.region.name est plus précis que notre extract_region
        if region_norm and c.get("record"):
            wine_obj   = (c["record"].get("vintage") or {}).get("wine") or {}
            viv_region = _norm_ascii((wine_obj.get("region") or {}).get("name") or "")
            if viv_region and (viv_region in region_norm or region_norm in viv_region):
                score += 0.15

        if score > best_score:
            best, best_score = c, score

    if not best or best_score < VIVINO_SIMILARITY_MIN:
        return None, best_score
    # Seuil dynamique : query mono-mot = appellation générique → exige 0.70
    # Un nom comme "Pomerol" ou "Bordeaux" seul matcherait n'importe quel vin.
    # En revanche un nom propre court ("Yquem", "Opus", "Gevrey") est très spécifique
    # → seuil normal 0.28.
    # Distinction : le mot unique est-il dans _GENERIC_APPELLATIONS ?
    query_sig_words = _norm_words(query)
    if len(query_sig_words) <= 1:
        single_word = _norm_ascii(query).strip()
        if single_word in _GENERIC_APPELLATIONS and best_score < 0.70:
            return None, best_score
    return best, best_score


def _fallback_queries(wine_name: str, vintage,
                      rejections: dict | None = None) -> list[str]:
    """
    Génère une cascade de requêtes Vivino du plus spécifique au plus général.

    Niveaux :
      1. Sans appellation  — partie avant le premier " - "
      2. ASCII normalisé   — sans accents
      3. 3 premiers mots
      4. 2 premiers mots
      5. (si mauvais millésime dominant) — query sans vintage pour trouver le bon
      6. (si mauvais producteur dominant) — query avec appellation seule
    """
    q0 = build_query(wine_name)
    base_raw  = re.split(r",\s*|\s+-\s+", wine_name)[0].strip()
    q_no_app  = build_query(base_raw) if base_raw != wine_name else q0
    q_ascii   = _norm_ascii(q0).strip()
    words     = q0.split()
    q3 = " ".join(words[:3]) if len(words) > 3 else None
    q2 = " ".join(words[:2]) if len(words) > 2 else None

    seen, result = {q0}, []
    for q in filter(None, [q_no_app, q_ascii, q3, q2]):
        if q and q not in seen and len(q) > 2:
            seen.add(q)
            result.append(q)

    # Amélioration 4 : adapter la cascade selon la raison dominante des rejets
    if rejections:
        entry = rejections.get(q0, {})
        dominant = entry.get("dominant_reason", "")
        if dominant == "wrong_vintage" and vintage:
            # build_query supprime déjà l'année → q_no_vintage = q0.
            # La vraie correction : retenter la même query mais sans filtrage
            # par millésime. On utilise la query ascii normalisée comme signal
            # pour que choose_best ne pénalise pas le mauvais millésime.
            # En pratique : ajouter q_ascii en priorité haute s'il n'y est pas.
            if q_ascii not in seen and len(q_ascii) > 2:
                result.insert(0, q_ascii)  # priorité haute, sans info millésime
        elif dominant == "wrong_producer":
            # Tenter avec seulement l'appellation/région extraite
            region = extract_region(wine_name)
            if region and region not in seen:
                result.append(region)

    return result


def fetch_vivino_via_api(query: str, vintage, slug: str = "vins-rouges",
                         _tried: set | None = None,
                         rejected_urls: set | None = None,
                         grapes_hint: list | None = None) -> dict | None:
    """
    Appel API Vivino avec cascade de requêtes de repli.
    - grapes_hint : cépages extraits du nom Leclerc → boostent le scoring
    - winery.name : utilisé comme fallback query si aucun candidat trouvé
    - Extraction enrichie : winery, region, grapes, taste, type_id
    """
    if _tried is None:
        _tried = {query}
    wine_type_id = VIVINO_TYPE_IDS.get(slug, 1)
    region = extract_region(query)
    _rejected    = rejected_urls or set()
    _grapes_hint = grapes_hint or []
    try:
        resp = _SESSION.get(
            "https://www.vivino.com/api/explore/explore",
            params={
                "language": "fr",
                "country_codes[]": "fr",
                "price_range_max": 300,
                "price_range_min": 0,
                "wine_type_ids[]": wine_type_id,
                "q": query,
                "order_by": "match",
            },
            timeout=VIVINO_API_TIMEOUT,
        )
        if resp.status_code != 200:
            return None

        records = (resp.json().get("explore_vintage", {}) or {}).get("records", [])
        candidates = []
        for r in records[:VIVINO_CANDIDATES_MAX]:
            vintage_obj = r.get("vintage", {}) or {}
            wine_obj    = vintage_obj.get("wine", {}) or {}
            title = f"{wine_obj.get('name','')} {vintage_obj.get('name','')}".strip()
            seo = (wine_obj.get("seo_name") or "").strip().lstrip("/")
            if seo and not seo.startswith(("w/", "wines/")):
                seo = f"wines/{seo}"
            c_url = f"https://www.vivino.com/{seo}" if seo else ""
            candidates.append({"title": title, "year": _safe_year(vintage_obj.get("year")),
                                "record": r, "url": c_url})

        best, confidence = choose_best_vivino_candidate(
            query, vintage, candidates, region=region,
            rejected_urls=_rejected, grapes_hint=_grapes_hint, slug=slug)

        # Aucun candidat → cascade de repli
        if not best:
            _rejs_for_fallback = load_vivino_rejections() if not _rejected else None
            fallbacks = _fallback_queries(query, vintage, rejections=_rejs_for_fallback)

            # Fallback winery : si le 1er candidat a un winery connu, le tenter
            if records:
                w0 = ((records[0].get("vintage") or {}).get("wine") or {})
                winery_name = (w0.get("winery") or {}).get("name", "")
                if winery_name and _norm_ascii(winery_name) not in _norm_ascii(query):
                    if winery_name not in _tried:
                        fallbacks = [winery_name] + fallbacks

            for fallback_q in fallbacks:
                if fallback_q not in _tried:
                    _tried.add(fallback_q)
                    result = fetch_vivino_via_api(fallback_q, vintage, slug=slug,
                                                  _tried=_tried, rejected_urls=_rejected,
                                                  grapes_hint=_grapes_hint)
                    if result:
                        return result
            return None

        picked      = best.get("record", {})
        vintage_obj = picked.get("vintage", {}) or {}
        wine_obj    = vintage_obj.get("wine", {}) or {}
        stats       = vintage_obj.get("statistics", wine_obj.get("statistics", {})) or {}
        vy = _safe_year(vintage_obj.get("year"))

        seo_name = (wine_obj.get("seo_name") or "").strip().lstrip("/")
        if seo_name and not seo_name.startswith(("w/", "wines/")):
            seo_name = f"wines/{seo_name}"
        vivino_url = f"https://www.vivino.com/{seo_name}" if seo_name else ""

        vmatch = None
        if vintage and vy:    vmatch = (vintage == vy)
        elif not vintage:     vmatch = True

        # ── Extraction enrichie des données Vivino ─────────────────────────
        style   = wine_obj.get("style") or {}
        region_obj = wine_obj.get("region") or {}
        winery_obj = wine_obj.get("winery") or {}
        taste   = wine_obj.get("taste") or {}
        structure = (taste.get("structure") or {})

        grapes_vivino = [g.get("name", "") for g in (style.get("grapes") or []) if g.get("name")]

        return {
            "rating":             stats.get("ratings_average"),
            "ratings_count":      int(stats.get("ratings_count") or 0),
            "ratings_count_all":  int((wine_obj.get("statistics") or {}).get("ratings_count") or 0),
            "vivino_url":         vivino_url,
            "vivino_year":        vy,
            "vintage_match":      vmatch,
            "match_confidence":   round(confidence, 3),
            # Nouvelles données
            "vivino_name":        wine_obj.get("name", ""),
            "winery":             winery_obj.get("name", ""),
            "vivino_region":      region_obj.get("name", ""),
            "vivino_region_seo":  region_obj.get("seo_name", ""),
            "country":            (region_obj.get("country") or {}).get("code", ""),
            "grapes":             grapes_vivino,
            "style_name":         style.get("regional_name") or style.get("seo_name", ""),
            "is_natural":         bool(wine_obj.get("is_natural")),
            "acidity":            structure.get("acidity"),
            "tannin":             structure.get("tannin"),
            "sweetness":          structure.get("sweetness"),
            "body":               structure.get("intensity"),
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
    """
    ③ CORRIGÉ (indirectement) : les appelants initialisent désormais
    driver = None avant d'appeler make_driver(), ce qui évite le NameError
    dans les blocs finally si make_driver() lève une exception.
    """
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

    wines, seen = [], set()
    # ③ CORRIGÉ : initialisation à None pour éviter NameError dans finally
    driver = None
    try:
        driver = make_driver()
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
            # Bug 1 fix : ean='' n'est pas un identifiant unique — on déduplique
            # sur (ean or name) pour éviter que tous les vins sans EAN soient écrasés
            key = w["ean"] or w["name"]
            if key not in seen:
                seen.add(key)
                wines.append(w)
        if log: log(f"✅ Page 1 : {len(wines)} vins — {nb} page(s)")
        for p in range(2, nb + 1):
            if log: log(f"🌐 Page {p}/{nb}…")
            driver.get(leclerc_url(slug, p))
            try: WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card")))
            except Exception: pass
            time.sleep(2)
            new = [w for w in parse_cards(driver.page_source)
                   if (w["ean"] or w["name"]) not in seen]
            if not new: break
            for w in new: seen.add(w["ean"] or w["name"])
            wines.extend(new)
            if log: log(f"✅ Page {p} : +{len(new)} (total {len(wines)})")
    finally:
        # ③ CORRIGÉ : vérification driver is not None avant quit()
        if driver is not None:
            try: driver.quit()
            except Exception: pass
    update_price_history(wines)
    return wines


def check_availability(slug: str, cached_wines: list, log=None) -> list:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    current_eans = set()
    driver = None
    nb_pages = 1
    try:
        driver = make_driver()
        for p in range(1, MAX_PAGES + 1):
            driver.get(leclerc_url(slug, p))
            try: WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-product-card")))
            except Exception: pass
            time.sleep(1.5)
            page_src = driver.page_source
            page_w   = parse_cards(page_src)
            if not page_w: break
            # Bug 2 fix : on n'ajoute pas les EAN vides — '' in current_eans
            # marquerait TOUS les vins sans EAN comme disponibles à tort
            current_eans.update(w["ean"] for w in page_w if w.get("ean"))
            # Fix 11b : lit le nb de pages réel depuis la page 1
            if p == 1:
                nb_pages = min(get_nb_pages(page_src), MAX_PAGES)
            if p >= nb_pages: break
    except Exception as e:
        if log: log(f"⚠️ Vérif. stock échouée : {e}")
    finally:
        if driver is not None:
            try: driver.quit()
            except Exception: pass

    if not current_eans:
        if log: log("⚠️ Aucun EAN récupéré — site Leclerc inaccessible ? Disponibilité non mise à jour.")
        return cached_wines

    # Bug 2 fix : les vins sans EAN conservent leur statut précédent
    # (on ne peut pas savoir s'ils sont en rayon sans EAN pour les identifier)
    result = []
    for w in cached_wines:
        w2 = dict(w)
        if w2.get("ean"):
            w2["available"] = w2["ean"] in current_eans
        # else: w2["available"] inchangé — statut précédent conservé
        result.append(w2)
    update_price_history(result)
    nok = sum(1 for w in result if w.get("available"))
    if log: log(f"✅ {nok} dispo, {len(result)-nok} indispo à Blagnac")
    return result


def fetch_vivino(driver, wine_name: str, vintage, slug: str = "vins-rouges", region: str = "") -> dict:
    """
    2 navigations avec choix du meilleur candidat (nom + millésime + région).

    ⑩ CORRIGÉ : le double appel API (ligne 865 + ligne 885 si Selenium échoue)
    est éliminé : l'API n'est appelée qu'une fois en entrée. Si elle retourne
    un résultat suffisant (note OU URL avec confiance ≥ 0.35), on s'arrête.
    En l'absence de résultat, Selenium est utilisé, sans second appel API.

    Ajout : millésime inclus dans la query Selenium pour une meilleure précision.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    EMPTY = {"rating": None, "ratings_count": 0,
             "vivino_url": "", "vivino_year": None, "vintage_match": None,
             "match_confidence": 0.0}
    query = build_query(wine_name)

    # ⑩ Appel API unique — on accepte si confiance ≥ 0.35 ou note présente
    _gh = extract_grapes_from_name(wine_name)
    api_data = fetch_vivino_via_api(query, vintage, slug=slug, grapes_hint=_gh)
    if api_data:
        conf = api_data.get("match_confidence") or 0
        if api_data.get("rating") or (api_data.get("vivino_url") and conf >= 0.35):
            return api_data

    # Selenium : query enrichie avec le millésime pour plus de précision
    # Si zéro candidat, on essaie les requêtes de repli (_fallback_queries)
    sel_query = f"{query} {vintage}" if vintage else query
    best, confidence = None, 0.0
    tried_sel = {sel_query}

    def _selenium_search(q):
        """Effectue une recherche Selenium et retourne (best, confidence)."""
        try:
            driver.get(f"https://www.vivino.com/search/wines"
                       f"?q={requests.utils.quote(q)}&language=fr")
            try: WebDriverWait(driver, 9).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR,
                     "[class*='wineCard'],[class*='wine-card'],[class*='averageValue'],[href*='/w/']")))
            except Exception: pass
            time.sleep(1)
            cands = vivino_candidates_from_search(driver.page_source)
            return choose_best_vivino_candidate(query, vintage, cands, region=region,
                                                grapes_hint=_gh, slug=slug)
        except Exception:
            return None, 0.0

    try:
        best, confidence = _selenium_search(sel_query)

        # Cascade de repli si aucun candidat trouvé
        if not best:
            for fallback_q in _fallback_queries(wine_name, vintage):
                fsel = f"{fallback_q} {vintage}" if vintage else fallback_q
                if fsel not in tried_sel:
                    tried_sel.add(fsel)
                    best, confidence = _selenium_search(fsel)
                    if best:
                        break
    except Exception:
        return EMPTY

    if not best:
        return EMPTY

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
        vy = _safe_year(m.group(1))
    elif best.get("year"):
        vy = _safe_year(best.get("year"))

    vmatch = None
    if vintage and vy:
        vmatch = (vintage == vy)
    elif not vintage:
        vmatch = True

    # Champs enrichis vides — Selenium ne donne pas accès à l'API JSON Vivino
    # qui contient winery/grapes/region/style. Ils resteront vides dans le cache
    # et seront remplis lors du prochain scrape API ou d'un refresh.
    _enriched_empty = {
        "ratings_count_all": 0, "vivino_name": "", "winery": "",
        "vivino_region": "", "vivino_region_seo": "", "country": "",
        "grapes": [], "style_name": "", "is_natural": False,
        "acidity": None, "tannin": None, "sweetness": None, "body": None,
    }

    if not d.get("rating"):
        return {**_enriched_empty, "rating": None, "ratings_count": 0,
                "vivino_url": wine_url, "vivino_year": vy, "vintage_match": vmatch,
                "match_confidence": round(confidence, 3)}
    return {**_enriched_empty, "rating": d["rating"], "ratings_count": d["ratings_count"],
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


def _api_lookup_wine(wine: dict, slug: str,
                     rejections: dict | None = None) -> tuple[str, str, dict]:
    """Appel API Vivino pour un vin (exécuté en parallèle)."""
    key    = build_query(wine["name"])
    region = extract_region(wine["name"])
    _rej   = rejections or {}
    if is_hard_to_match(key, _rej):
        return key, region, None
    rejected = get_rejected_urls(key, _rej)
    result = fetch_vivino_via_api(key, wine.get("vintage"), slug=slug,
                                  rejected_urls=rejected,
                                  grapes_hint=wine.get("grapes_hint") or [])
    return key, region, result


def _scrape_vivino_list(slug, wines, todo, vc, log):
    """
    Boucle de scraping Vivino avec stratégie deux phases :

    Phase 1 (rapide) — API parallèle × 8 workers :
      Tous les vins sans verrou sont interrogés via l'API Vivino simultanément.
      Les vins avec confiance ≥ VIVINO_SIMILARITY_MIN et une note sont acceptés.

    Phase 2 (lente) — Selenium séquentiel :
      Seulement les vins pour lesquels l'API n'a rien retourné ou dont la
      confiance est trop faible. Une seule instance Chrome est démarrée.

    Vitesse : 5-10× plus rapide qu'un scraping purement séquentiel.
    """
    found = 0
    done_count = len(wines) - len(todo)
    interrupted = False
    driver = None

    # ── Séparer vins verrouillés / à traiter (build_query 1× par vin) ───────
    locked, to_process = [], []
    for _w in todo:
        (locked if vc.get(build_query(_w["name"]), {}).get("locked") else to_process).append(_w)

    for w in locked:
        ean = w.get("ean") or build_query(w["name"])
        ckpt_tick(slug, ean)   # Fix 2 : enregistré dans done_eans → sauté proprement en cas de reprise
        done_count += 1
        if log: log(f"  🔒 [{done_count}/{len(wines)}] {w['name'][:38]} (correction manuelle conservée)")

    if not to_process:
        ckpt_finish(slug)
        if log: log(f"✅ Terminé — {found} notes · {len(vc)} entrées cache")
        return

    # Charger les rejets 1× pour toute la session de scraping
    _rejections = load_vivino_rejections()
    n_skip_hard = sum(1 for w in to_process if is_hard_to_match(build_query(w["name"]), _rejections))
    if n_skip_hard and log:
        log(f"  ⚠️ {n_skip_hard} vins skippés (trop de rejets précédents)")

    # ── PHASE 1 : API parallèle ────────────────────────────────────────────
    if log: log(f"⚡ Phase 1 : appels API parallèles pour {len(to_process)} vins…")
    api_results: dict[str, tuple[str, dict | None]] = {}   # key → (region, result)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_api_lookup_wine, w, slug, _rejections): w for w in to_process}
        for fut in concurrent.futures.as_completed(futures):
            try:
                key, region, res = fut.result()
                api_results[key] = (region, res)
            except Exception:
                w = futures[fut]
                api_results[build_query(w["name"])] = ("", None)

    # Accepter les résultats API de bonne qualité
    need_selenium: list[tuple[dict, str]] = []   # (wine, region)
    for w in to_process:
        key          = build_query(w["name"])
        region, res  = api_results.get(key, ("", None))
        ean          = w.get("ean") or key
        conf         = (res or {}).get("match_confidence") or 0
        if res and (res.get("rating") or (res.get("vivino_url") and conf >= VIVINO_SIMILARITY_MIN)):
            # Fix 6 : cached_at au moment réel de la récupération, pas au début du scrape
            vc[key] = {**res, "cached_at": time.time(), "locked": False,
                       "manual_override": False, "suppressed": False}
            ckpt_tick(slug, ean)
            done_count += 1
            found += bool(res.get("rating"))
            # Sauvegarde incrémentale tous les 10 résultats → le polling temps réel
            # voit les nouvelles notes au fur et à mesure (pas seulement à la fin)
            if done_count % 10 == 0:
                save_vivino_cache(vc)
                if log: log(f"  ⚡ [{done_count}/{len(wines)}] {found} notes…")
        else:
            need_selenium.append((w, region))

    save_vivino_cache(vc)
    phase1_found = found
    if log: log(f"  ✅ Phase 1 : {phase1_found} notes, {len(need_selenium)} vins restants pour Selenium")

    # ── PHASE 2 : Selenium pour les cas difficiles ─────────────────────────
    if not need_selenium:
        ckpt_finish(slug)
        if log: log(f"✅ Terminé — {found} notes · {len(vc)} entrées cache")
        return

    if log: log(f"🌐 Phase 2 : Selenium pour {len(need_selenium)} vins…")
    try:
        driver = make_driver()
        for wine, region in need_selenium:    # Fix 13 : region déjà calculée en Phase 1
            key = build_query(wine["name"])
            ean = wine.get("ean") or key
            vd  = fetch_vivino(driver, wine["name"], wine.get("vintage"),
                               slug=slug, region=region)
            # Fix 6 : timestamp réel, pas now figé au début
            vc[key] = {**vd, "cached_at": time.time(), "locked": False,
                       "manual_override": False, "suppressed": False}
            ckpt_tick(slug, ean)
            done_count += 1
            if vd.get("rating"):
                found += 1
                cnt_s = f"{vd['ratings_count']:,}".replace(",", "\u202f") \
                        if vd["ratings_count"] else "—"
                if log: log(f"  ✅ [{done_count}/{len(wines)}] {wine['name'][:38]}\n"
                            f"     ★ {vd['rating']} · {cnt_s} avis")
                # Sauvegarde immédiate après chaque note trouvée → visible au prochain rerun
                save_vivino_cache(vc)
            else:
                if log and done_count % 5 == 0:
                    log(f"  🍷 [{done_count}/{len(wines)}] — {found} notes trouvées")
            time.sleep(0.3)
    except Exception as e:
        interrupted = True
        if log: log(f"⚠️ Interrompu à [{done_count}/{len(wines)}] : {e}")
    finally:
        if driver is not None:
            try: driver.quit()
            except Exception: pass
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
        wines = [dict(w) for w in lc["wines"]]  # copie — ne mute pas lc["wines"]
        for w in wines: w.setdefault("available", True)
    vc   = load_vivino_cache()
    ckpt = ckpt_load(slug) if resume else None
    if ckpt:
        done_eans = set(ckpt["done_eans"])
        n_done    = len(done_eans)
        if log: log(f"🔁 Reprise : {n_done}/{len(wines)} ({int(100*n_done/max(len(wines),1))}%) déjà traités")
    else:
        done_eans = set()
        # ⑥ CORRIGÉ : ckpt_create appelle désormais ckpt_finish() en interne
        # pour nettoyer tout checkpoint stale avant d'en créer un nouveau
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
    """Scrape uniquement les vins sans note ET sans URL Vivino."""
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("❌ Pas de cache Leclerc. Lancez d'abord 🔄 Vérifier disponibilité.")
        return []
    vc    = load_vivino_cache()
    wines = [dict(w) for w in lc["wines"]]  # copie — ne mute pas lc["wines"]
    for w in wines: w.setdefault("available", True)
    missing = []
    for w in wines:
        key   = build_query(w["name"])
        entry = vc.get(key, {})
        if not entry.get("locked") and not entry.get("rating") and not entry.get("vivino_url"):
            missing.append(w)
    if not missing:
        if log: log("✅ Tous les vins ont déjà une note ou un lien Vivino !")
        return _merge_vivino(wines, vc, load_price_history())
    if log: log(f"🔍 {len(missing)}/{len(wines)} vins sans données Vivino…")
    ckpt_create(slug, len(missing))
    _scrape_vivino_list(slug, missing, missing, vc, log)
    return _merge_vivino(wines, vc, load_price_history())


def run_refresh_stale_vivino(slug: str, log=None) -> list:
    """Rafraîchit uniquement les entrées Vivino marquées obsolètes (> TTL jours)."""
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("❌ Pas de cache Leclerc. Lancez d'abord 🔄 Vérifier disponibilité.")
        return []
    vc    = load_vivino_cache()
    wines = [dict(w) for w in lc["wines"]]
    for w in wines: w.setdefault("available", True)
    stale = [w for w in wines
             if vc.get(build_query(w["name"]), {}).get("_stale")]
    if not stale:
        if log: log(f"✅ Aucune entrée obsolète (TTL = {VIVINO_CACHE_TTL_DAYS} jours) !")
        return _merge_vivino(wines, vc, load_price_history())
    if log: log(f"⏳ {len(stale)} entrées obsolètes à rafraîchir…")
    ckpt_create(slug, len(stale))
    _scrape_vivino_list(slug, stale, stale, vc, log)
    return _merge_vivino(wines, vc, load_price_history())


# ═══════════════════════════════════════════════════════════════════════════
# RENDU HTML
# ═══════════════════════════════════════════════════════════════════════════

def stars(r: float) -> str:
    r = max(0.0, min(5.0, float(r or 0)))
    return "".join("★" if r >= i else ("½" if r >= i - .5 else "☆") for i in range(1, 6))

def fmt_count(n) -> str:
    """Formate un nombre d'avis. Coerce silencieusement float → int, str → int."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "—"
    if not n:
        return "—"
    return f"{n:,}".replace(",", "\u202f")


def wine_card_html(wine: dict, rank: int, max_score: float) -> str:
    cls = {1:"top1",2:"top2",3:"top3"}.get(rank, "")
    if wine.get("vintage_match") is False: cls = (cls + " vintage-warn").strip()
    if not wine.get("available", True):    cls = (cls + " unavailable").strip()
    if wine.get("_stale"):                 cls = (cls + " stale").strip()
    icon = {1:"🥇",2:"🥈",3:"🥉"}.get(rank, f"<span style='font-size:.75rem'>#{rank}</span>")

    # XSS : toutes les valeurs utilisateur échappées avant injection HTML
    name     = _html.escape(wine["name"])
    safe_url = _html.escape(wine.get("url") or "")
    safe_viv = _html.escape(wine.get("vivino_url") or "")
    safe_reg = _html.escape(wine.get("region") or "")

    name_html = (f'<a href="{safe_url}" target="_blank" '
                 f'style="color:#1A0810;text-decoration:none">{name}</a>'
                 ) if safe_url else name
    yr = (f' <span style="color:#8B6B72;font-size:.68rem;font-weight:400">'
          f'{wine["vintage"]}</span>') if wine.get("vintage") else ""
    unavail = (' <span style="font-size:.62rem;color:#dc2626">⛔ indispo</span>'
               if not wine.get("available", True) else "")
    mil = ""
    if wine.get("vivino_year") and wine.get("vintage") and wine["vivino_year"] != wine["vintage"]:
        mil = (f'<div class="wine-sub" style="color:#c17a00">'
               f'⚠️ Vivino={wine["vivino_year"]} / Leclerc={wine["vintage"]}</div>')

    # Barre de confiance Vivino (couleur selon niveau)
    conf = wine.get("match_confidence") or 0
    conf_color = "#16a34a" if conf >= 0.7 else ("#f59e0b" if conf >= 0.4 else "#e5e7eb")
    conf_html = (
        f'<div class="conf-bar" title="Confiance Vivino : {conf:.0%}">'
        f'<div class="conf-fill" style="width:{conf*100:.0f}%;background:{conf_color}"></div>'
        f'</div>'
    ) if conf else ""

    links = []
    if safe_url:
        links.append(f'<a href="{safe_url}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>')
    if safe_viv:
        links.append(f'<a href="{safe_viv}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>')
    links_html = (f'<div class="wine-links">' + "".join(links) + conf_html + "</div>") if links or conf_html else ""

    score  = wine.get("score") or 0
    rating = wine.get("rating")
    badges = ""
    if rating and rating >= 4.3:             badges += '<span class="badge b-top">★ Top noté</span>'
    if safe_reg:                             badges += f'<span class="badge b-reg">{safe_reg}</span>'
    if wine.get("_stale"):                   badges += f'<span class="badge b-stale">⏳ obsolète</span>'
    if wine.get("is_natural"):               badges += '<span class="badge b-nat">🌿 Naturel</span>'
    # Cépages — max 3, depuis Vivino ou hint Leclerc
    grapes = wine.get("grapes") or []
    if grapes:
        gstr = " · ".join(_html.escape(g) for g in grapes[:3])
        badges += f'<span class="badge b-grape">🍇 {gstr}</span>'
    # Style régional Vivino (ex: "Médoc Rouge", "Bourgogne Blanc")
    if wine.get("style_name"):
        badges += f'<span class="badge b-style">{_html.escape(wine["style_name"])}</span>'
    # Volume si différent de 75cl (Magnum, etc.)
    if wine.get("volume_cl") and wine["volume_cl"] != 75:
        badges += f'<span class="badge b-vol">{wine["volume_cl"]:.0f}cl</span>'

    if rating:
        cnt = wine.get("ratings_count") or 0
        rating_col = (f'<div class="wine-rating">'
                      f'<span class="stars">{stars(rating)}</span>'
                      f'<span class="r-num">{rating:.1f}</span>'
                      f'<span class="r-cnt">{fmt_count(cnt)} avis</span>'
                      f'</div>')
    else:
        rating_col = '<div class="no-rat">—<br>pas de note</div>'

    trend = wine.get("price_trend", "")
    trend_html = {"↑":'<span class="p-up">↑</span>',
                  "↓":'<span class="p-down">↓</span>',
                  "=":'<span class="p-eq">=</span>'}.get(trend, "")
    price_s = f'{wine.get("price") or 0:.2f} €'.replace(".", ",")
    # Prix en baisse : couleur verte pour attirer l'attention
    price_style = 'color:#16a34a' if trend == "↓" else ''
    price_col = (f'<div class="wine-price" style="{price_style}">'
                 f'{price_s}{trend_html}</div>')

    pct = min(100, (score / max_score) * 100) if max_score > 0 else 0
    score_col = (
        f'<div class="score-wrap">'
        f'<div class="score-num">{score:.2f}</div>'
        f'<div class="score-lbl">score Q/P</div>'
        f'<div class="score-bar"><div class="score-fill" style="width:{pct:.1f}%"></div></div>'
        f'</div>'
    ) if score else '<div class="score-wrap" style="color:#ccc;font-size:.72rem;text-align:center">—</div>'

    return (f'<div class="wine-card {cls}">'
            f'<div class="wine-rank">{icon}</div>'
            f'<div class="wine-info">'
            f'<div class="wine-name">{name_html}{yr}{unavail}</div>'
            f'{mil}{links_html}<div>{badges}</div>'
            f'</div>'
            f'{rating_col}'
            f'{price_col}'
            f'{score_col}'
            f'</div>')


def _make_wines_df(ws: list) -> "pd.DataFrame":
    """
    Fix I : fonction top-level (plus de redéfinition à chaque render dans tab_export).
    Colonnes communes pour tab_data ET tab_export — une seule source de vérité.
    """
    return pd.DataFrame([{
        "Nom":              w["name"],
        "Région":           w.get("region", ""),
        "Millésime":        w.get("vintage") or "",
        "Prix (€)":         w.get("price") or 0,
        "Volume (cl)":      w.get("volume_cl") or 75,
        "Tendance":         w.get("price_trend", ""),
        "EAN":              w.get("ean") or "",
        "Note":             w.get("rating") or "",
        "Nb avis":          w.get("ratings_count") or "",
        "Nb avis (total)":  w.get("ratings_count_all") or "",
        "Score":            w.get("score") or "",
        "Cépages":          ", ".join(w.get("grapes") or []),
        "Style":            w.get("style_name") or "",
        "Domaine":          w.get("winery") or "",
        "Naturel":          "🌿" if w.get("is_natural") else "",
        "Tanin":            w.get("tannin") or "",
        "Acidité":          w.get("acidity") or "",
        "Sucrosité":        w.get("sweetness") or "",
        "Corps":            w.get("body") or "",
        "Mil. Vivino":      w.get("vivino_year") or "",
        "Mil. OK":          {True: "✅", False: "⚠️", None: "—"}.get(w.get("vintage_match"), "—"),
        "Dispo":            "✅" if w.get("available", True) else "⛔",
        "Leclerc":          w.get("url") or "",
        "Vivino":           w.get("vivino_url") or "",
        "Query":            build_query(w["name"]),
    } for w in ws])

_DF_COL_CONFIG = {
    "Leclerc":  st.column_config.LinkColumn(display_text="🛒"),
    "Vivino":   st.column_config.LinkColumn(display_text="🍷"),
    "Prix (€)": st.column_config.NumberColumn(format="%.2f"),
    "Note":     st.column_config.NumberColumn(format="%.1f"),
    "Score":    st.column_config.NumberColumn(format="%.2f"),
}

def _make_logger(max_lines: int = 10):
    logs, box = [], st.empty()
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

for k, v in [("wines",[]),("loaded_slug",None),("data_ready",False),
             ("last_live_refresh", 0.0)]:
    if k not in st.session_state: st.session_state[k] = v

# ── REFRESH ANTICIPÉ ──────────────────────────────────────────────────────
# Charger les données fraîches AVANT tout rendu (sidebar + onglets).
# Sans ça, st.rerun() redémarre le script mais wines est relu depuis
# session_state qui peut être périmé si _update_wines_from_cache() est
# appelé APRÈS le rendu des onglets (trop tard pour ce cycle).
_early_job = load_job_state()
if _early_job.get("status") in {"running", "queued"}:
    _early_slug = _early_job.get("slug")
    _early_lc   = load_leclerc_cache(_early_slug) if _early_slug else None
    if _early_lc:
        _fresh = _merge_vivino(
            _early_lc["wines"], load_vivino_cache(), load_price_history()
        )
        if _fresh:
            st.session_state.wines       = _fresh
            st.session_state.loaded_slug = _early_slug
            st.session_state.data_ready  = True

# ── AUTO-RERUN ────────────────────────────────────────────────────────────
# Déclencher le rerun ici (avant tout rendu) garantit que les onglets
# reçoivent les données fraîches chargées ci-dessus, et non les données
# du cycle précédent.
if _early_job.get("status") in {"running", "queued"}:
    _auto_live_on = st.session_state.get("auto_live", True)
    if _auto_live_on:
        _now_top = time.time()
        _elapsed = _now_top - st.session_state.get("last_live_refresh", 0.0)
        if _elapsed >= 2.0:
            st.session_state["last_live_refresh"] = _now_top
            st.rerun()

# ── SIDEBAR ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🍾 Type de vin")
    wine_label = st.selectbox("Type", list(WINE_TYPES), label_visibility="collapsed")
    slug       = WINE_TYPES[wine_label]

    st.divider()
    st.markdown("### 🔄 Mise à jour")

    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache()

    if lc:
        n_total = len(lc["wines"])
        # Fix 16 : réutilise wines de session_state (déjà mergées avec Vivino)
        # plutôt que de recalculer build_query sur lc["wines"] brutes
        _session_wines = st.session_state.get("wines") or []
        if _session_wines and st.session_state.get("loaded_slug") == slug:
            n_rated   = sum(1 for w in _session_wines if w.get("rating"))
            n_missing = n_total - n_rated
        else:
            # Fallback : calcul depuis le cache brut si pas encore chargé en session
            n_rated   = sum(1 for w in lc["wines"]
                            if vc.get(build_query(w["name"]), {}).get("rating"))
            n_missing = n_total - n_rated
        cov_pct = int(100 * n_rated / max(n_total, 1))
        st.caption(f"📦 **Leclerc** : {fmt_age(lc['cached_at'])} · {n_total} vins")
        n_stale = sum(1 for v in vc.values() if v.get("_stale"))
        stale_txt = f" · ⏳ {n_stale} obsolètes (>{VIVINO_CACHE_TTL_DAYS}j)" if n_stale else ""
        st.caption(f"🍷 **Vivino** : {n_rated}/{n_total} ({cov_pct}%)"
                   + (f" · ⚠️ {n_missing} manquants" if n_missing else " · ✅ complet")
                   + stale_txt)
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
    n_stale_total = sum(1 for v in vc.values() if v.get("_stale"))
    btn_stale = False
    if n_stale_total > 0 and lc:
        btn_stale = st.button(f"⏳ Rafraîchir obsolètes ({n_stale_total}) (arrière-plan)",
                              use_container_width=True,
                              help=f"{n_stale_total} notes Vivino datent de plus de {VIVINO_CACHE_TTL_DAYS} jours.")

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

    # ④ CORRIGÉ : auto_live sans time.sleep() dans le thread principal.
    # L'ancien code faisait time.sleep(5) + st.rerun() à chaque cycle,
    # bloquant le serveur Streamlit pour tous les utilisateurs pendant 5s.
    # Nouvelle approche : st.rerun() immédiat avec un délai minimum géré via
    # session_state (last_live_refresh) pour éviter les boucles infinies.
    auto_live = st.checkbox("🟢 Suivi temps réel (auto ~2s)", value=True,
                            key="auto_live_chk",
                            help="Met à jour l'ensemble de l'interface toutes les 2s pendant un scraping.")
    # Propager la préférence pour le trigger en haut de script
    st.session_state["auto_live"] = auto_live

    if job.get("status") == "running" and job.get("slug") == slug:
        msg = job.get("message", "")
        age = fmt_age(job.get("updated_at", 0))
        # Extraire le ratio [X/Y] du message pour afficher une barre de progression
        import re as _re
        m_prog = _re.search(r"\[(\d+)/(\d+)\]", msg)
        if m_prog:
            done_n, total_n = int(m_prog.group(1)), int(m_prog.group(2))
            pct = done_n / max(total_n, 1)
            st.progress(pct, text=f"⏳ {done_n}/{total_n} vins · {age}")
        else:
            st.info(f"⏳ Job en cours ({job.get('mode')})\n\n{msg}\n\nMàj: {age}", icon=None)
    elif job.get("status") == "done" and job.get("slug") == slug:
        st.success(job.get("message", "✅ Job terminé"), icon=None)
    elif job.get("status") == "error" and job.get("slug") == slug:
        st.error(f"Job en erreur: {job.get('error','inconnue')}", icon=None)

    st.caption(f"📍 Leclerc Blagnac · magasin {STORE_CODE}")
    st.divider()
    st.markdown("### 🔧 Filtres")

    # Fix G : la recherche couvre aussi la région
    search = st.text_input("🔍 Recherche (nom ou région)", placeholder="Bordeaux, Guigal, Pomerol…")

    # Max prix dynamique depuis les données réelles (plus de 200€ fixe)
    _all_wines  = st.session_state.get("wines") or []
    _price_max  = max((w.get("price") or 0 for w in _all_wines), default=200)
    _price_ceil = max(200, int(math.ceil(_price_max / 10) * 10))

    # Fix 8 : préserver la sélection utilisateur entre reruns (auto_live, job polling)
    # On stocke la dernière valeur choisie et on la reclamp si _price_ceil change.
    _prev_ceil = st.session_state.get("_price_ceil_prev", _price_ceil)
    _prev_val  = st.session_state.get("price_range_val", (0, _price_ceil))
    if _price_ceil != _prev_ceil:
        # Le max a changé (nouvelles données) : reclamp la borne haute
        _prev_val = (max(0, _prev_val[0]), min(_prev_val[1], _price_ceil))
        st.session_state["_price_ceil_prev"] = _price_ceil
    price_range = st.slider(
        "💶 Prix (€)", 0, _price_ceil, _prev_val, step=5,
        key=f"price_range_{slug}",
    )
    st.session_state["price_range_val"] = price_range

    rating_min = st.select_slider("⭐ Note min",
        options=[0.0, 3.0, 3.5, 3.8, 4.0, 4.2, 4.5], value=0.0,
        format_func=lambda x: "Toutes" if x == 0 else f"≥ {x} ★")

    # Fix H : régions depuis wines enrichis (.region déjà calculé) si dispo
    _region_source = _all_wines or (lc["wines"] if lc else [])
    all_regions_cache = sorted({
        _r for w in _region_source
        if (_r := w.get("region") or extract_region(w["name"]))
    })
    regions_filter = st.multiselect("🗺️ Région", all_regions_cache, placeholder="Toutes les régions")

    only_vintage = st.checkbox("✅ Millésime confirmé", False)
    only_dispo   = st.checkbox("🏪 Dispos à Blagnac", True)



# ── CHARGEMENT / SCRAPING ─────────────────────────────────────────────────
def _update_wines_from_cache():
    """Helper : charge les dernières données depuis le cache et met à jour la session."""
    latest = load_wines_from_cache(slug)
    if latest:
        st.session_state.wines     = latest
        st.session_state.loaded_slug = slug
        st.session_state.data_ready  = True
    return bool(latest)

if slug != st.session_state.loaded_slug:
    st.session_state.wines = []; st.session_state.data_ready = False

if not st.session_state.data_ready and not btn_stock and not btn_vivino \
        and not btn_fill and not btn_resume and not btn_stale:
    _update_wines_from_cache()

if btn_stock:
    st.session_state.wines = []; st.session_state.data_ready = False
    with st.status("🔄 Vérification du stock…", expanded=True) as status:
        log, _ = _make_logger(10)
        try:
            raw = run_check_stock(slug, log=log)
        except Exception as e:
            st.error(f"❌ Erreur Selenium : {e}\n\nVérifiez `packages.txt` :\n```\nchromium\nchromium-driver\n```")
            st.stop()
        n_dispo = sum(1 for w in raw if w.get("available", True))
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

if btn_stale:
    if start_background_job(slug, "refresh_stale"):
        st.success(f"Rafraîchissement de {n_stale_total} entrées obsolètes lancé en arrière-plan.")
    else:
        st.warning("Un job est déjà en cours.")

if btn_resume:
    if start_background_job(slug, "resume"):
        st.success("Reprise du scraping lancée en arrière-plan.")
    else:
        st.warning("Un job est déjà en cours.")

# Le polling (auto-rerun + _update_wines_from_cache) est maintenant en tête
# de script, avant tout rendu, pour que les onglets voient les données fraîches.
# On recharge seulement si le slug du job ne correspond pas au slug actif.
if job.get("status") in {"running", "queued"} and job.get("slug") != slug:
    _update_wines_from_cache()   # slug différent — recharger quand même

if job.get("status") == "done" and job.get("slug") == slug:
    _update_wines_from_cache()

wines = st.session_state.wines
if not wines:
    st.markdown("<br>", unsafe_allow_html=True)
    st.info("👈 Ouvrez le menu et cliquez sur **Vérifier disponibilité** pour charger les vins.")
    st.stop()

# ── FILTRE ────────────────────────────────────────────────────────────────
filtered = [w for w in wines
    if price_range[0] <= (w.get("price") or 0) <= price_range[1]
    and (rating_min == 0 or (w.get("rating") and w["rating"] >= rating_min))
    and (not search or _fuzzy_match(search, w["name"])
         or _fuzzy_match(search, w.get("region") or ""))
    and (not only_vintage or w.get("vintage_match") is True)
    and (not only_dispo or w.get("available", True))
    and (not regions_filter or w.get("region","") in regions_filter)]

# ── TRI ───────────────────────────────────────────────────────────────────
SORTS = {
    # Fix 9 : clé de tri secondaire pour éviter que les vins sans note/score
    # remontent en tête (ex: tri "Note" avec vins non notés = 0 → dessous)
    "🏆 Score":   lambda x: (-(x.get("score") or 0),   -(x.get("rating") or 0)),
    "⭐ Note":    lambda x: (-(x.get("rating") or 0),   -(x.get("score") or 0)),
    "💶 Prix ↑": lambda x: ( (x.get("price") or 9999), -(x.get("score") or 0)),
    "💶 Prix ↓": lambda x: (-(x.get("price") or 0),    -(x.get("score") or 0)),
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
tab_rank, tab_deals, tab_stats, tab_data, tab_export, tab_rej = st.tabs(
    ["🏅 Classement", "💡 Bonnes Affaires", "📊 Stats", "🗂️ Données & Cache", "📥 Export", "🚫 Rejets Vivino"])

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
    n_drop = sum(1 for w in filtered if w.get("price_trend") == "↓" and w.get("available", True))
    if n_drop:
        drop_wines = [w for w in filtered if w.get("price_trend") == "↓" and w.get("available", True)]
        drop_names = ", ".join(f"**{w['name'][:35]}** ({w.get('price',0):.2f} €)" for w in drop_wines[:3])
        st.success(f"📉 **{n_drop} vin(s) en baisse de prix** depuis le dernier relevé : {drop_names}"
                   + (" …" if n_drop > 3 else ""), icon=None)
    if n_bad: st.warning(f"⚠️ {n_bad} vins avec millésime différent Leclerc / Vivino (bordure orange).")
    st.divider()
    if not filtered:
        st.info("Aucun vin ne correspond aux filtres.")
    else:
        max_score = max((w.get("score") or 0 for w in filtered), default=1)

        # ── Pagination ────────────────────────────────────────────────────
        page_key = f"rank_page_{slug}"
        if page_key not in st.session_state: st.session_state[page_key] = 0
        # Reset page si les filtres changent (nombre de résultats différent)
        size_key = f"rank_size_{slug}"
        if st.session_state.get(size_key) != len(filtered):
            st.session_state[page_key] = 0
            st.session_state[size_key] = len(filtered)

        page      = st.session_state[page_key]
        n_total   = len(filtered)
        n_pages   = max(1, (n_total + CARDS_PER_PAGE - 1) // CARDS_PER_PAGE)
        start     = page * CARDS_PER_PAGE
        end       = min(start + CARDS_PER_PAGE, n_total)
        page_wines = filtered[start:end]

        for i, w in enumerate(page_wines):
            _uid = f"{slug}_{w.get('ean') or i}_{page}"
            _has_viv = bool(w.get("vivino_url"))
            _reject_key = f"reject_mode_{_uid}"

            if _has_viv:
                _c_card, _c_btn = st.columns([1, 0.08])
                with _c_card:
                    st.markdown(wine_card_html(w, start + i + 1, max_score),
                                unsafe_allow_html=True)
                with _c_btn:
                    st.markdown(
                        f'<style>'
                        f'div[data-testid="column"]:has(button[title*="{_uid}"])'
                        f'{{margin-left:-4.5rem;margin-top:.55rem;z-index:10;position:relative}}'
                        f'</style>',
                        unsafe_allow_html=True)
                    if st.button("🚫", key=f"bad_viv_{_uid}",
                                 help=f"Lien Vivino incorrect ({_uid})"):
                        st.session_state[_reject_key] = True
                        st.rerun()

                # Formulaire de raison — s'affiche sous la carte si bouton cliqué
                if st.session_state.get(_reject_key):
                    with st.container():
                        st.markdown(
                            f'<div style="background:#fff5f5;border:1px solid #fca5a5;'
                            f'border-radius:8px;padding:.6rem .8rem;margin-bottom:.45rem;'
                            f'font-size:.8rem;color:#7f1d1d">'
                            f'<strong>🚫 Pourquoi ce lien Vivino est incorrect ?</strong><br>'
                            f'<em>{w.get("vivino_url","")[:60]}…</em></div>',
                            unsafe_allow_html=True)
                        _r_cols = st.columns([3, 1, 1])
                        with _r_cols[0]:
                            _reason = st.selectbox(
                                "Raison",
                                list(REJECTION_REASONS.keys()),
                                format_func=lambda k: REJECTION_REASONS[k],
                                key=f"reason_{_uid}",
                                label_visibility="collapsed")
                        with _r_cols[1]:
                            if st.button("✅ Confirmer", key=f"confirm_rej_{_uid}",
                                         use_container_width=True, type="primary"):
                                _vc_live = load_vivino_cache()
                                _q = build_query(w["name"])
                                # Récupérer le titre Vivino depuis le cache avant suppression
                                _old = _vc_live.get(_q, {})
                                _old_title = _old.get("vivino_url", "")
                                # Enregistrer le rejet avec raison
                                save_vivino_rejection(
                                    wine_name=w["name"],
                                    query=_q,
                                    rejected_url=w.get("vivino_url", ""),
                                    rejected_title=_old_title,
                                    reason=_reason,
                                )
                                # Marquer supprimé dans le cache Vivino
                                _vc_live[_q] = {
                                    "rating": None, "ratings_count": 0,
                                    "vivino_url": "", "vivino_year": None,
                                    "vintage_match": None, "match_confidence": None,
                                    "manual_override": True, "suppressed": True,
                                    "locked": True, "cached_at": time.time(),
                                }
                                save_vivino_cache(_vc_live)
                                st.session_state.pop(_reject_key, None)
                                _rlab = REJECTION_REASONS[_reason]
                                st.toast(f"✅ Rejet enregistré · {_rlab}", icon="🚫")
                                st.rerun()
                        with _r_cols[2]:
                            if st.button("Annuler", key=f"cancel_rej_{_uid}",
                                         use_container_width=True):
                                st.session_state.pop(_reject_key, None)
                                st.rerun()
            else:
                st.markdown(wine_card_html(w, start + i + 1, max_score),
                            unsafe_allow_html=True)

        # Contrôles de pagination
        if n_pages > 1:
            st.markdown(f'<div class="page-info">Page {page+1}/{n_pages} · {n_total} vins</div>',
                        unsafe_allow_html=True)
            nav_cols = st.columns([1, 2, 1])
            with nav_cols[0]:
                if page > 0 and st.button("← Préc.", key=f"prev_{slug}", use_container_width=True):
                    st.session_state[page_key] -= 1
                    st.rerun()
            with nav_cols[1]:
                # Saut direct de page
                jump = st.selectbox("Aller à", range(1, n_pages+1), index=page,
                                    format_func=lambda x: f"Page {x}", key=f"jump_{slug}",
                                    label_visibility="collapsed")
                if jump - 1 != page:
                    st.session_state[page_key] = jump - 1
                    st.rerun()
            with nav_cols[2]:
                if page < n_pages - 1 and st.button("Suiv. →", key=f"next_{slug}", use_container_width=True):
                    st.session_state[page_key] += 1
                    st.rerun()

# ── BONNES AFFAIRES ───────────────────────────────────────────────────────
with tab_deals:
    st.markdown("#### 💡 Bonnes Affaires")
    st.caption("Critères : note ≥ 4.0 · prix ≤ 15 € · ≥ 500 avis · disponible")

    # Fix 4: utilise filtered (respecte région/dispo/prix/recherche de la sidebar)
    deals = sorted(
        [w for w in filtered if (w.get("rating") or 0) >= 4.0
                             and (w.get("price") or 999) <= 15
                             and (w.get("ratings_count") or 0) >= 500
                             and w.get("available", True)],
        key=lambda x: -(x.get("score") or 0))

    if not deals:
        deals_soft = sorted(
            [w for w in filtered if (w.get("rating") or 0) >= 3.8
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

    for idx_d, w in enumerate(deals[:30]):
        score   = w.get("score") or 0
        trend   = w.get("price_trend", "")
        trend_h = {"↑": '<span class="p-up">↑</span>',
                   "↓": '<span class="p-down">↓</span>',
                   "=": '<span class="p-eq">=</span>'}.get(trend, "")
        safe_name = _html.escape(w["name"])
        safe_url  = _html.escape(w.get("url") or "")
        safe_viv  = _html.escape(w.get("vivino_url") or "")
        safe_reg  = _html.escape(w.get("region") or "")
        url_lec = f'<a href="{safe_url}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>' if safe_url else ""
        url_viv = f'<a href="{safe_viv}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>' if safe_viv else ""
        rank_icon = {0:"🥇",1:"🥈",2:"🥉"}.get(idx_d, f"#{idx_d+1}")
        top_cls = "d-top" if idx_d < 3 else ""
        reg_txt = f"🗺️ {safe_reg} · " if safe_reg else ""
        st.markdown(f"""
<div class="deal-card {top_cls}">
  <div style="text-align:center;min-width:56px">
    <div style="font-size:1.4rem;line-height:1">{rank_icon}</div>
    <div class="deal-score">{score:.2f}</div>
    <div class="deal-label">score</div>
  </div>
  <div class="deal-body">
    <div class="deal-name">{safe_name}</div>
    <div class="deal-meta">{reg_txt}★ {w.get("rating",0):.1f}
      · {fmt_count(w.get("ratings_count",0))} avis</div>
    <div class="wine-links" style="margin-top:.3rem">{url_lec}{url_viv}</div>
  </div>
  <div class="deal-price"><strong>{(w.get("price") or 0):.2f} €</strong>{trend_h}</div>
</div>""", unsafe_allow_html=True)

# ── STATISTIQUES ─────────────────────────────────────────────────────────
with tab_stats:
    import altair as alt

    if not filtered:
        st.info("Aucun vin à analyser avec les filtres actuels.")
    else:
        st.markdown("#### 📊 Statistiques — vins filtrés")
        s1, s2, s3, s4 = st.columns(4)
        rated_w  = [w for w in filtered if w.get("rating")]
        priced_w = [w for w in filtered if w.get("price")]
        s1.metric("Vins analysés", len(filtered))
        s2.metric("Notés Vivino",  f"{len(rated_w)}/{len(filtered)}")
        s3.metric("Prix médian",   f"{sorted(w['price'] for w in priced_w)[len(priced_w)//2]:.2f} €" if priced_w else "—")
        s4.metric("Note médiane",  f"{sorted(w['rating'] for w in rated_w)[len(rated_w)//2]:.1f} ★" if rated_w else "—")
        st.divider()

        df_s = pd.DataFrame([{
            "Nom":      w["name"],
            "Région":   w.get("region") or "Inconnue",
            "Note":     w.get("rating"),
            "Prix":     w.get("price") or 0,
            "Score":    w.get("score") or 0,
            "Dispo":    w.get("available", True),
            "Tendance": w.get("price_trend", ""),
        } for w in filtered])

        col_a, col_b = st.columns(2)

        # Distribution des notes
        with col_a:
            st.markdown("**Distribution des notes Vivino**")
            df_rat = df_s.dropna(subset=["Note"])
            if not df_rat.empty:
                cut_result = pd.cut(df_rat["Note"],
                                   bins=[2.5, 3.0, 3.3, 3.6, 3.9, 4.2, 4.5, 5.1],
                                   labels=["2.5-3.0","3.0-3.3","3.3-3.6","3.6-3.9","3.9-4.2","4.2-4.5","4.5+"])
                counts = cut_result.value_counts().sort_index()
                hist_data = pd.DataFrame({"Note": counts.index.astype(str), "Nb": counts.values})
                chart_rat = (alt.Chart(hist_data)
                    .mark_bar(color="#C9A84C", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                    .encode(
                        x=alt.X("Note:O", title=None, sort=None),
                        y=alt.Y("Nb:Q", title="Nb vins"),
                        tooltip=["Note:O", "Nb:Q"]
                    ).properties(height=180)
                    .configure_view(strokeWidth=0)
                    .configure_axis(grid=False, labelFont="DM Mono"))
                st.altair_chart(chart_rat, use_container_width=True)
            else:
                st.caption("Aucune note disponible.")

        # Distribution des prix
        with col_b:
            st.markdown("**Distribution des prix**")
            df_pr = df_s[df_s["Prix"] > 0]
            if not df_pr.empty:
                price_max = df_pr["Prix"].max()
                step = 5 if price_max <= 50 else (10 if price_max <= 100 else 20)
                bins = list(range(0, int(price_max) + step + 1, step))
                labels = [f"{bins[i]}-{bins[i+1]}" for i in range(len(bins)-1)]
                cut_p = pd.cut(df_pr["Prix"], bins=bins, labels=labels, right=False)
                counts_p = cut_p.value_counts().sort_index()
                hist_p = pd.DataFrame({"Tranche": counts_p.index.astype(str), "Nb": counts_p.values})
                chart_pr = (alt.Chart(hist_p)
                    .mark_bar(color="#6B1A2A", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                    .encode(
                        x=alt.X("Tranche:O", title=None, sort=None),
                        y=alt.Y("Nb:Q", title="Nb vins"),
                        tooltip=["Tranche:O", "Nb:Q"]
                    ).properties(height=180)
                    .configure_view(strokeWidth=0)
                    .configure_axis(grid=False, labelFont="DM Mono"))
                st.altair_chart(chart_pr, use_container_width=True)

        st.divider()
        col_c, col_d = st.columns(2)

        # Top régions
        with col_c:
            st.markdown("**Vins par région**")
            reg_counts = df_s["Région"].value_counts().head(10)
            top_reg = pd.DataFrame({"Région": reg_counts.index, "Nb": reg_counts.values})
            chart_reg = (alt.Chart(top_reg)
                .mark_bar(color="#2563eb", cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    y=alt.Y("Région:O", sort="-x", title=None),
                    x=alt.X("Nb:Q", title="Nb vins"),
                    tooltip=["Région:O", "Nb:Q"]
                ).properties(height=220)
                .configure_view(strokeWidth=0)
                .configure_axis(grid=False, labelFont="DM Sans", labelFontSize=11))
            st.altair_chart(chart_reg, use_container_width=True)

        # Scatter Note vs Prix (coloré par score)
        with col_d:
            st.markdown("**Note vs Prix**")
            df_sc = df_s.dropna(subset=["Note"]).copy()
            df_sc = df_sc[df_sc["Prix"] > 0]
            if not df_sc.empty:
                chart_sc = (alt.Chart(df_sc)
                    .mark_circle(opacity=0.7, size=55)
                    .encode(
                        x=alt.X("Prix:Q", title="Prix (€)", scale=alt.Scale(zero=False)),
                        y=alt.Y("Note:Q", title="Note Vivino",
                                scale=alt.Scale(domain=[2.5, 5.0])),
                        color=alt.Color("Score:Q",
                            scale=alt.Scale(scheme="goldred", reverse=False),
                            legend=alt.Legend(title="Score")),
                        tooltip=[
                            alt.Tooltip("Nom:N"),
                            alt.Tooltip("Note:Q", format=".1f"),
                            alt.Tooltip("Prix:Q", format=".2f", title="Prix (€)"),
                            alt.Tooltip("Score:Q", format=".2f"),
                            alt.Tooltip("Région:N"),
                        ]
                    ).properties(height=220)
                    .configure_view(strokeWidth=0)
                    .configure_axis(grid=True, gridColor="#f0f0f0", labelFont="DM Mono"))
                st.altair_chart(chart_sc, use_container_width=True)
            else:
                st.caption("Données insuffisantes.")

        # Tableau tendances prix
        n_up   = (df_s["Tendance"] == "↑").sum()
        n_down = (df_s["Tendance"] == "↓").sum()
        n_eq   = (df_s["Tendance"] == "=").sum()
        if n_up + n_down + n_eq > 0:
            st.divider()
            st.markdown("**Tendances de prix**")
            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("📉 En baisse", n_down, delta=None)
            tc2.metric("📈 En hausse", n_up, delta=None)
            tc3.metric("➡️ Stables", n_eq, delta=None)


# ── DONNÉES ───────────────────────────────────────────────────────────────
with tab_data:
    st.markdown("#### Tous les vins chargés")
    # Fix 5 : calculé une fois ici, réutilisé dans tab_export
    df_wines = _make_wines_df(wines)
    st.dataframe(df_wines, use_container_width=True, hide_index=True, height=450,
                 column_config=_DF_COL_CONFIG)

    st.divider()
    st.markdown("#### 🗂️ Cache Vivino")
    # Fix L : vc déjà chargé en sidebar (cache mémoire TTL 2s) — pas de 2e lecture disque
    vc_now = vc
    n_ok   = sum(1 for v in vc_now.values() if v.get("rating"))
    n_av   = sum(1 for v in vc_now.values() if (v.get("ratings_count") or 0) > 0)
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
                entry = dict(vc_now[manual_key])   # copie — évite mutation in-place
                entry["locked"]          = False
                entry["suppressed"]      = False
                entry["manual_override"] = False
                vc_now[manual_key] = entry
                save_vivino_cache(vc_now)
            st.success("Entrée déverrouillée. Les prochains refresh Vivino pourront la recalculer.")
            st.rerun()

    ph = load_price_history()
    if ph:
        st.divider()
        st.markdown("#### 📈 Historique des prix")
        # Construire le DataFrame complet
        rows = []
        for ean, entry in ph.items():
            for rec in entry.get("history", []):
                rows.append({
                    "EAN":    ean,
                    "Nom":    entry.get("name", ean)[:50],
                    "Date":   rec["date"],
                    "Prix":   rec["price"],
                })
        if rows:
            import altair as alt
            df_ph = pd.DataFrame(rows)
            df_ph["Date"] = pd.to_datetime(df_ph["Date"])

            # Sélecteur du vin à afficher
            wine_names = sorted(df_ph["Nom"].unique())
            sel_name = st.selectbox("Vin", wine_names, key="ph_select",
                                    label_visibility="collapsed")
            df_sel = df_ph[df_ph["Nom"] == sel_name].sort_values("Date")

            if len(df_sel) >= 2:
                chart = (
                    alt.Chart(df_sel)
                    .mark_line(point=alt.OverlayMarkDef(filled=True, size=60),
                               strokeWidth=2, color="#6B1A2A")
                    .encode(
                        x=alt.X("Date:T", title=None, axis=alt.Axis(format="%d/%m/%y")),
                        y=alt.Y("Prix:Q", title="Prix (€)",
                                scale=alt.Scale(zero=False),
                                axis=alt.Axis(format=".2f")),
                        tooltip=[
                            alt.Tooltip("Date:T", format="%d/%m/%Y"),
                            alt.Tooltip("Prix:Q", format=".2f", title="Prix (€)"),
                        ]
                    )
                    .properties(height=180)
                    .configure_view(strokeWidth=0)
                    .configure_axis(grid=True, gridColor="#f0f0f0",
                                    labelFont="DM Mono", titleFont="DM Sans")
                )
                st.altair_chart(chart, use_container_width=True)
                # Mini tableau résumé
                first_p, last_p = df_sel["Prix"].iloc[0], df_sel["Prix"].iloc[-1]
                delta = last_p - first_p
                delta_pct = delta / first_p * 100 if first_p else 0
                c1, c2, c3 = st.columns(3)
                c1.metric("Premier prix", f"{first_p:.2f} €")
                c2.metric("Dernier prix", f"{last_p:.2f} €",
                          delta=f"{delta:+.2f} € ({delta_pct:+.1f}%)")
                c3.metric("Nb relevés", len(df_sel))
            else:
                st.caption(f"Un seul relevé pour ce vin — revenez après une prochaine vérification stock.")
                st.dataframe(df_sel[["Date","Prix"]].rename(columns={"Prix":"Prix (€)"}),
                             use_container_width=True, hide_index=True)

# ── EXPORT ────────────────────────────────────────────────────────────────
with tab_export:
    today = datetime.now().strftime("%Y%m%d")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Vins filtrés** ({len(filtered)} vins)")
        df_f = _make_wines_df(filtered)
        st.dataframe(df_f, use_container_width=True, hide_index=True, height=200,
                     column_config=_DF_COL_CONFIG)
        st.download_button("⬇️ CSV filtré",
            df_f.drop(columns=["Query"], errors="ignore").to_csv(index=False, sep=";").encode("utf-8-sig"),
            f"vins_{slug}_{today}.csv", "text/csv")
    with col2:
        st.markdown(f"**Tous les vins** ({len(wines)} vins)")
        # Réutilise df_wines déjà calculé dans tab_data
        st.dataframe(df_wines, use_container_width=True, hide_index=True, height=200,
                     column_config=_DF_COL_CONFIG)
        st.download_button("⬇️ CSV complet",
            df_wines.drop(columns=["Query"], errors="ignore").to_csv(index=False, sep=";").encode("utf-8-sig"),
            f"vins_{slug}_complet_{today}.csv", "text/csv")

# ── CONSOLE ───────────────────────────────────────────────────────────────
st.divider()
_has_log = JOB_LOG_PATH.exists() and JOB_LOG_PATH.stat().st_size > 0
_console_visible = st.session_state.get("console_open", False)

_con_cols = st.columns([1, 6, 1])
with _con_cols[0]:
    _btn_lbl = "▼ Console" if not _console_visible else "▲ Console"
    if st.button(_btn_lbl, key="toggle_console",
                 type="primary" if job.get("status") == "running" else "secondary",
                 help="Affiche l'historique complet des logs du dernier scraping"):
        st.session_state["console_open"] = not _console_visible
        st.rerun()
with _con_cols[1]:
    if job.get("status") == "running":
        st.caption(f"🟢 Scraping en cours · {job.get('message','')[:80]}")
    elif _has_log:
        st.caption("🖥️ Logs du dernier job disponibles")

if _console_visible:
    _con_inner = st.container()
    with _con_inner:
        try:
            _log_txt   = JOB_LOG_PATH.read_text("utf-8") if _has_log else "(aucun log)"
            _log_lines = _log_txt.strip().splitlines()[-300:]
            st.code("\n".join(_log_lines), language=None)
        except Exception:
            st.caption("(log inaccessible)")
        _cc1, _cc2 = st.columns([1, 5])
        with _cc1:
            if st.button("🗑️ Effacer", key="clear_console", use_container_width=True):
                try: JOB_LOG_PATH.write_text("", "utf-8")
                except Exception: pass
                st.session_state["console_open"] = False
                st.rerun()

# ── REJETS VIVINO ─────────────────────────────────────────────────────────
with tab_rej:
    _rejs = load_vivino_rejections()
    if not _rejs:
        st.info("Aucun rejet enregistré. Cliquez sur 🚫 sur une carte pour signaler un lien Vivino incorrect.")
    else:
        # Stats globales
        total_rej = sum(len(v.get("history",[])) for v in _rejs.values())
        hard = sum(1 for v in _rejs.values() if v.get("hard_to_match"))
        r1, r2, r3 = st.columns(3)
        r1.metric("🚫 Total rejets", total_rej)
        r2.metric("🍷 Vins concernés", len(_rejs))
        r3.metric("⚠️ Difficiles à matcher", hard,
                  help="Vins avec ≥3 rejets ou ≥2 rejets 'mauvais vin' — Vivino skippé au prochain scan")
        st.divider()

        # Tableau des rejets
        rows_rej = []
        for q, entry in sorted(_rejs.items()):
            for h in entry.get("history", []):
                rows_rej.append({
                    "Vin (query)":   q,
                    "Nom Leclerc":   h.get("wine_name","")[:45],
                    "URL rejetée":   h.get("rejected_url",""),
                    "Raison":        REJECTION_REASONS.get(h.get("reason",""), h.get("reason","")),
                    "Date":          time.strftime("%d/%m/%y %H:%M", time.localtime(h.get("ts",0))),
                    "Hard":          "⚠️" if entry.get("hard_to_match") else "",
                })
        df_rej = pd.DataFrame(rows_rej)
        st.dataframe(df_rej, use_container_width=True, hide_index=True, height=350,
                     column_config={"URL rejetée": st.column_config.LinkColumn(display_text="🔗")})
        st.divider()

        # Actions
        ra1, ra2 = st.columns([1, 3])
        with ra1:
            if st.button("🗑️ Effacer tous les rejets", use_container_width=True):
                try: REJECTION_LOG_PATH.unlink(missing_ok=True)
                except Exception: pass
                st.toast("Rejets effacés.", icon="🗑️")
                st.rerun()
        with ra2:
            st.caption("Les rejets sont utilisés automatiquement lors du prochain scraping Vivino "
                       "pour éviter de reproposer les mêmes correspondances incorrectes.")

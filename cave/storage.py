"""Persistance : caches disque, Gist GitHub, checkpoint, rejets, historique des prix."""
import streamlit as st
import json
import time
import threading
from pathlib import Path
from datetime import datetime
import requests
# AUTO-IMPORTS
from .config import CACHE_DIR, REJECTION_LOG_PATH, VIVINO_CACHE_TTL_DAYS
# END-AUTO

# ═══════════════════════════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# PERSISTANCE GIST — sauvegarde sur GitHub Gist (Streamlit Cloud safe)
# ═══════════════════════════════════════════════════════════════════════════
#
# POURQUOI : Streamlit Cloud a un filesystem éphémère.
#   Le dossier .cache/ est effacé à chaque mise en veille (~5 min) ou
#   redéploiement → toutes les données Vivino (semaines de scraping) perdues.
#
# SOLUTION : GitHub Gist comme backend de persistance transparent.
#   • Au démarrage : si .cache/ vide → restore depuis le Gist
#   • À chaque save_*  : écriture locale + push Gist en arrière-plan
#   • Le reste du code ne change pas
#
# CONFIGURATION → ajouter dans .streamlit/secrets.toml :
#   [gist]
#   github_token = "ghp_xxxxxxxxxxxx"   # token GitHub, scope "gist" suffit
#   gist_id      = ""                   # laisser vide → créé automatiquement
#
# Le Gist est créé en mode privé (public=false).
# ─────────────────────────────────────────────────────────────────────────

_GIST_API  = "https://api.github.com/gists"
try:
    _GIST_CONF = dict(st.secrets.get("gist", {}))
except Exception:   # pas de secrets.toml (installation locale) → Gist facultatif désactivé
    _GIST_CONF = {}
_GIST_TOKEN   = _GIST_CONF.get("github_token", "")
_GIST_ID_CFG  = _GIST_CONF.get("gist_id", "")

# Fichiers à persister dans le Gist (filenames exacts du CACHE_DIR)
_GIST_FILES = {
    # Leclerc par slug
    "leclerc_vins-rouges.json",
    "leclerc_vins-blancs.json",
    "leclerc_vins-roses.json",
    "leclerc_vins-mousseux-et-petillants.json",
    # Vivino par slug (un fichier par type de vin)
    "vivino_vins-rouges.json",
    "vivino_vins-blancs.json",
    "vivino_vins-roses.json",
    "vivino_vins-mousseux-et-petillants.json",
    # Partagés
    "price_history.json",
    "vivino_rejections.json",
}

def _gist_headers() -> dict:
    return {
        "Authorization": f"token {_GIST_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _gist_is_configured() -> bool:
    """Renvoie True si le token GitHub est présent (écriture)."""
    return bool(_GIST_TOKEN)

def _gist_can_restore() -> bool:
    """Renvoie True si un Gist ID est disponible (lecture publique sans token)."""
    return bool(_GIST_ID_CFG or st.session_state.get("gist_id") or st.session_state.get("gist_restore_url"))

def _gist_restore_id() -> str:
    """Retourne l'ID Gist pour la restauration (extrait de l'URL si besoin)."""
    import re
    # Priorité : session gist_id > secrets > URL manuelle
    for source in [st.session_state.get("gist_id"), _GIST_ID_CFG,
                   st.session_state.get("gist_restore_url","")]:
        if not source: continue
        # Extraire l'ID depuis une URL Gist (github.com/user/ID ou gist.githubusercontent.com/user/ID/raw/...)
        m = re.search(r"github(?:usercontent)?\.com/[^/]+/([a-f0-9]{20,})", str(source))
        if m: return m.group(1)
        # ID brut (32 hex chars)
        if re.fullmatch(r"[a-f0-9]{20,40}", str(source).strip()):
            return str(source).strip()
    return ""


def _gist_id() -> str:
    """
    Retourne l'ID du Gist.
    Priorité : session_state > secrets.toml > création automatique.
    La création n'a lieu qu'une seule fois ; l'ID est ensuite affiché
    pour être copié dans secrets.toml.
    """
    if "gist_id" in st.session_state and st.session_state["gist_id"]:
        return st.session_state["gist_id"]
    if _GIST_ID_CFG:
        st.session_state["gist_id"] = _GIST_ID_CFG
        return _GIST_ID_CFG
    # Créer un nouveau Gist privé
    try:
        resp = requests.post(
            _GIST_API,
            headers=_gist_headers(),
            json={
                "description": "ScoreMaster — cache Vivino+Leclerc (persistance auto)",
                "public": False,
                "files": {"README.md": {"content": (
                    "# ScoreMaster Cache\n"
                    "Persistance automatique — ne pas modifier manuellement.\n"
                )}}
            },
            timeout=15,
        )
        resp.raise_for_status()
        gid = resp.json()["id"]
        st.session_state["gist_id"] = gid
        return gid
    except Exception:
        return ""


def gist_push(filename: str, content: str) -> bool:
    """
    Pousse un fichier vers le Gist (bloquant).
    Retourne True si succès, False sinon (non bloquant pour l'app).
    """
    if not _gist_is_configured():
        return False
    gid = _gist_id()
    if not gid:
        return False
    try:
        resp = requests.patch(
            f"{_GIST_API}/{gid}",
            headers=_gist_headers(),
            json={"files": {filename: {"content": content}}},
            timeout=20,
        )
        return resp.status_code in (200, 201)
    except Exception:
        return False


def gist_pull_all() -> dict:
    """
    Récupère tous les fichiers du Gist.
    Retourne {filename: content_str} ou {} si erreur ou non configuré.
    """
    if not _gist_is_configured():
        return {}
    gid = _gist_id()
    if not gid:
        return {}
    try:
        resp = requests.get(
            f"{_GIST_API}/{gid}",
            headers=_gist_headers(),
            timeout=20,
        )
        resp.raise_for_status()
        return {
            fname: fdata.get("content", "")
            for fname, fdata in resp.json().get("files", {}).items()
            if fname in _GIST_FILES and fdata.get("content")
        }
    except Exception:
        return {}


_gist_push_last: dict[str, float] = {}   # filename → last push timestamp
_gist_push_lock = threading.Lock()       # protection accès concurrent
_GIST_PUSH_MIN_INTERVAL = 60.0           # 1 push/min max par fichier (throttle scraping)

def _gist_push_async(filename: str, content: str, force: bool = False) -> None:
    """Push non-bloquant throttlé : max 1 push par fichier toutes les 60 s
    (_GIST_PUSH_MIN_INTERVAL).
    Évite la saturation de threads lors des sauvegardes incrémentielles du scraping.
    force=True : bypasse le throttle (fin de scraping, correction manuelle).
    daemon=False : le thread survit un peu plus longtemps si Streamlit recycle."""
    if not _gist_is_configured():
        return
    now = time.time()
    with _gist_push_lock:
        if not force and now - _gist_push_last.get(filename, 0) < _GIST_PUSH_MIN_INTERVAL:
            return   # trop tôt — skip ce push
        _gist_push_last[filename] = now
    threading.Thread(
        target=gist_push, args=(filename, content), daemon=False
    ).start()


def restore_from_gist() -> int:
    """
    Restaure les fichiers depuis le Gist vers CACHE_DIR.
    Fonctionne sans token pour les Gists publics (lecture seule via API publique).
    Retourne le nombre de fichiers restaurés (0 si impossible).
    Gère la migration automatique de l'ancien vivino.json monolithique.
    """
    gid = _gist_restore_id()
    if not gid:
        return 0
    # Headers optionnels : avec token si disponible, sans sinon (Gist public)
    headers = _gist_headers() if _gist_is_configured() else {"Accept": "application/vnd.github.v3+json"}
    try:
        resp = requests.get(f"{_GIST_API}/{gid}", headers=headers, timeout=20)
        resp.raise_for_status()
        gist_data = resp.json()
    except Exception:
        return 0
    restored = 0
    for fname, fdata in gist_data.get("files", {}).items():
        # ── Récupérer le contenu complet (gérer la troncature GitHub) ────────
        # L'API tronque les fichiers > 1MB : truncated=True + raw_url pour le complet.
        file_content = fdata.get("content", "")
        if fdata.get("truncated") or not file_content:
            raw_url = fdata.get("raw_url", "")
            if raw_url:
                try:
                    r2 = requests.get(raw_url, headers=headers, timeout=30)
                    r2.raise_for_status()
                    file_content = r2.text
                except Exception:
                    pass
        if not file_content:
            continue

        # Migration : vivino.json (ancien monolithique) → vivino_vins-rouges.json
        if fname == "vivino.json":
            target = CACHE_DIR / "vivino_vins-rouges.json"
            if not target.exists():
                try:
                    json.loads(file_content)
                    target.write_text(file_content, "utf-8")
                    _invalidate_mem_cache(target)
                    restored += 1
                except Exception:
                    pass
            continue
        # Ignorer les fichiers inconnus (sécurité)
        if fname not in _GIST_FILES:
            continue
        target = CACHE_DIR / fname
        try:
            parsed = json.loads(file_content)
            # Ne pas écraser avec un fichier vide depuis le Gist
            is_vivino  = fname.startswith("vivino_")
            is_leclerc = fname.startswith("leclerc_")
            if is_vivino and (not isinstance(parsed, dict) or len(parsed) == 0):
                continue
            if is_leclerc and (not isinstance(parsed, dict) or len(parsed.get("wines",[])) == 0):
                continue
            target.write_text(file_content, "utf-8")
            _invalidate_mem_cache(target)
            restored += 1
        except Exception:
            pass   # JSON corrompu dans le Gist → ignorer
    return restored


def _lec_path(slug): return CACHE_DIR / f"leclerc_{slug}.json"
def _viv_path(slug="vins-rouges"): return CACHE_DIR / f"vivino_{slug}.json"

def load_leclerc_cache(slug: str) -> dict | None:
    """Retourne le cache Leclerc sans expiration — conservé jusqu'au prochain scrape manuel."""
    p = _lec_path(slug)
    d = _read_json_cached(p, ttl=30.0)
    if not isinstance(d, dict): return None
    if "wines" not in d: return None
    return d

def save_leclerc_cache(slug: str, wines: list) -> None:
    p, tmp = _lec_path(slug), _lec_path(slug).with_suffix(".tmp")
    bak = _lec_path(slug).with_suffix(".bak")

    # Guard anti-régression : refus d'écraser avec liste vide ou réduite de > 50%
    if p.exists():
        try:
            _ex = json.loads(p.read_text("utf-8"))
            n_ex = len(_ex.get("wines", [])) if isinstance(_ex, dict) else 0
            if n_ex > 0 and len(wines) == 0:
                import logging
                logging.warning(f"[ScoreMaster] save_leclerc_cache({slug}): refus d'écraser "
                                f"avec liste vide ({n_ex} vins existants).")
                return
            if n_ex >= 5 and len(wines) < n_ex // 2:
                import logging
                logging.warning(f"[ScoreMaster] save_leclerc_cache({slug}): refus d'écraser "
                                f"({n_ex} vins) avec {len(wines)} vins (< 50%).")
                return
        except Exception:
            pass

    try:
        data_str = json.dumps({"cached_at": time.time(), "slug": slug, "wines": wines},
                              ensure_ascii=False, indent=2)
        if p.exists():
            try:
                import shutil; shutil.copy2(p, bak)
            except Exception:
                pass
        tmp.write_text(data_str, "utf-8")
        tmp.replace(p)
        _invalidate_mem_cache(p)
        _gist_push_async(p.name, data_str)  # persistance cloud
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


def load_vivino_cache(slug: str = "vins-rouges") -> dict:
    """Charge le cache Vivino avec protection contre la corruption.

    Si le fichier local est absent ou vide, tente de restaurer depuis :
    1. Le backup .bak local
    2. Le Gist GitHub (si configuré)
    """
    p   = _viv_path(slug)
    bak = p.with_suffix(".bak")

    raw = _read_json_cached(p, ttl=_MEM_CACHE_TTL)

    # ── Récupération si fichier local absent ou corrompu ─────────────────
    if not isinstance(raw, dict) or len(raw) == 0:
        # Tentative 1 : backup .bak local
        if bak.exists():
            try:
                bak_raw = json.loads(bak.read_text("utf-8"))
                if isinstance(bak_raw, dict) and len(bak_raw) > 0:
                    import logging
                    logging.warning(f"[ScoreMaster] load_vivino_cache({slug}): "
                                    f"fichier principal vide/absent, restauration depuis .bak "
                                    f"({len(bak_raw)} entrées)")
                    import shutil
                    shutil.copy2(bak, p)  # copie (conserve .bak) plutôt que déplacer
                    _invalidate_mem_cache(p)
                    raw = bak_raw
            except Exception:
                pass
        # Tentative 2 : Gist
        if not isinstance(raw, dict) or len(raw) == 0:
            if _gist_is_configured():
                try:
                    gid = _gist_id()
                    resp = requests.get(f"{_GIST_API}/{gid}",
                                        headers=_gist_headers(), timeout=15)
                    resp.raise_for_status()
                    fname = f"vivino_{slug}.json"
                    fdata = resp.json().get("files", {}).get(fname, {})
                    content = fdata.get("content", "")
                    if content:
                        gist_raw = json.loads(content)
                        if isinstance(gist_raw, dict) and len(gist_raw) > 0:
                            import logging
                            logging.warning(f"[ScoreMaster] load_vivino_cache({slug}): "
                                            f"restauration depuis Gist ({len(gist_raw)} entrées)")
                            p.write_text(content, "utf-8")
                            _invalidate_mem_cache(p)
                            raw = gist_raw
                except Exception:
                    pass

    if not isinstance(raw, dict):
        return {}

    ttl_secs = VIVINO_CACHE_TTL_DAYS * 86400
    now = time.time()
    result = {}
    for k, v in raw.items():
        entry = _normalize_vivino_entry(v)
        age = now - (entry.get("cached_at") or 0)
        entry["_stale"] = (
            not entry.get("locked")
            and entry.get("rating") is not None
            and age > ttl_secs
        )
        result[k] = entry
    return result

def save_vivino_cache(cache: dict, slug: str = "vins-rouges",
                      _force_gist: bool = False) -> None:
    """Sauvegarde le cache Vivino de façon sécurisée.

    Protections :
    - Écriture atomique via .tmp → rename
    - Backup .bak du fichier existant avant écrasement
    - Guard anti-régression : refuse d'écraser si len(cache) < 50% du fichier existant
      (protège contre l'écrasement accidentel avec un dict vide ou partiel)
    - _force_gist=True : bypass le throttle 30s pour forcer le push final
    """
    p   = _viv_path(slug)
    tmp = p.with_suffix(".tmp")
    bak = p.with_suffix(".bak")

    # ── Guard anti-régression ─────────────────────────────────────────────
    if p.exists():
        try:
            _existing = json.loads(p.read_text("utf-8"))
            n_existing = len(_existing) if isinstance(_existing, dict) else 0
        except Exception:
            n_existing = 0  # fichier illisible → on laisse l'écrasement

        # Refus absolu : ne jamais écraser avec un dict vide
        if n_existing > 0 and len(cache) == 0:
            import logging
            logging.warning(f"[ScoreMaster] save_vivino_cache({slug}): refus d'écraser "
                            f"{p.name} ({n_existing} entrées) avec un dict vide.")
            return

        # Refus si régression > 50% (sauf si le cache existant est lui-même très petit)
        if n_existing >= 3 and len(cache) < n_existing // 2:
            import logging
            logging.warning(
                f"[ScoreMaster] save_vivino_cache({slug}): refus d'écraser "
                f"{p.name} ({n_existing} entrées) avec {len(cache)} entrées "
                f"(< 50% — probable corruption). Sauvegarde annulée."
            )
            return

    try:
        data_str = json.dumps(cache, ensure_ascii=False, indent=2)
        # Backup atomique du fichier existant
        if p.exists():
            try:
                import shutil
                shutil.copy2(p, bak)
            except Exception:
                pass
        tmp.write_text(data_str, "utf-8")
        tmp.replace(p)
        _invalidate_mem_cache(p)
        if _force_gist:
            # Push immédiat bypass throttle (fin de scraping)
            threading.Thread(
                target=gist_push, args=(p.name, data_str), daemon=True
            ).start()
        else:
            _gist_push_async(p.name, data_str)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


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
        data_str = json.dumps(data, ensure_ascii=False, indent=2)
        tmp.write_text(data_str, "utf-8")
        tmp.replace(REJECTION_LOG_PATH)
        _invalidate_mem_cache(REJECTION_LOG_PATH)
        _gist_push_async(REJECTION_LOG_PATH.name, data_str)  # persistance cloud
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
        data_str = json.dumps(hist, ensure_ascii=False, indent=2)
        tmp.write_text(data_str, "utf-8")
        tmp.replace(p)
        _invalidate_mem_cache(p)            # cohérence du cache mémoire
        _gist_push_async(p.name, data_str)  # persistance cloud
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
    prev = h[-2].get("price")
    if not prev: return ""
    if current_price > prev + 0.05: return "↑"
    if current_price < prev - 0.05: return "↓"
    return "="


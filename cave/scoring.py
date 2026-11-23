"""Score qualité/prix et fusion des caches Leclerc + Vivino."""
import time
import math
# AUTO-IMPORTS
from .vivino import build_query
from .regions import extract_region
from .storage import price_trend
# END-AUTO

# ═══════════════════════════════════════════════════════════════════════════
# SCORE COMPOSITE
# ═══════════════════════════════════════════════════════════════════════════

_CONF_UNKNOWN_COUNT = 0.2   # confiance attribuée à une note dont le nombre d'avis est inconnu

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

    Nombre d'avis inconnu (0) : confiance fixée à _CONF_UNKNOWN_COUNT (0.2, soit
    l'équivalent d'environ 400 avis). Avant, la valeur était 0.5 (≈ 2 500 avis) : un vin
    dont on ne connaissait pas le nombre d'avis passait devant un vin à 1 000 avis
    confirmés. Il reste désormais derrière les vins bien documentés.
    """
    if rating is None or not price or price <= 0:
        return 0.0
    cnt = ratings_count or 0
    confidence = min(1.0, math.sqrt(cnt) / 100) if cnt > 0 else _CONF_UNKNOWN_COUNT
    base = rating * confidence / math.log1p(price) * 4
    if vintage_match is True:
        base *= 1.05   # +5% si millésime confirmé
    return round(base, 2)


# ═══════════════════════════════════════════════════════════════════════════
# RÉGIONS / APPELLATIONS
# ═══════════════════════════════════════════════════════════════════════════

def vivino_cache_type(entry: dict) -> str:
    if entry.get("suppressed"):
        return "masqué"
    if entry.get("manual_override"):
        return "manuel"
    return "auto"

# Champs Vivino enrichis — définis au niveau module (évite recréation tuple à chaque appel _merge_vivino)
_VIVINO_FIELDS: tuple = (
    "rating", "ratings_count", "ratings_count_all",
    "vivino_url", "vivino_year", "vintage_match", "match_confidence",
    "vivino_name", "winery", "vivino_region", "vivino_region_seo",
    "country", "grapes", "style_name", "is_natural",
    "acidity", "tannin", "sweetness", "body",
)
# Champs numériques qui valent 0 (pas None) par défaut dans les resets suppressed/defaults
_VIVINO_COUNT_FIELDS = frozenset({"ratings_count", "ratings_count_all"})

def _merge_vivino(wines: list, vc: dict, ph: dict | None = None) -> list:
    """
    Injecte données Vivino + calcule score/région/tendance prix.
    Retourne une NOUVELLE liste de copies de dicts pour éviter de muter
    les objets stockés dans st.session_state entre les reruns Streamlit.
    Nouvelles données propagées : winery, vivino_region, grapes, style_name,
    is_natural, acidity, tannin, sweetness, body, ratings_count_all
    """
    if ph is None: ph = {}
    # Pré-calculer build_query 1× par vin (évite N appels répétés lors des reruns)
    _bq_cache: dict[str, str] = {}
    def _bq(name):
        if name not in _bq_cache:
            _bq_cache[name] = build_query(name)
        return _bq_cache[name]

    result = []
    for w in wines:
        w = dict(w)
        key = _bq(w["name"])
        cv  = vc.get(key, {})
        w.setdefault("available", True)
        if cv.get("suppressed"):
            for f in _VIVINO_FIELDS:
                w[f] = [] if f == "grapes" else (0 if f in _VIVINO_COUNT_FIELDS else None)
            w["vivino_url"] = ""
        elif cv.get("rating") is not None or cv.get("vivino_url"):
            for f in _VIVINO_FIELDS:
                if f in cv:
                    w[f] = cv[f]
            if cv.get("cached_at"):   # propagé pour le badge "Nouveau"
                w["cached_at"] = cv["cached_at"]
        # Defaults pour les champs manquants
        for f in _VIVINO_FIELDS:
            if f not in w:
                w[f] = [] if f == "grapes" else (0 if f in _VIVINO_COUNT_FIELDS else None)
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


def fmt_count(n) -> str:
    """Formate un nombre d'avis. Coerce silencieusement float → int, str → int."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "—"
    if not n:
        return "—"
    return f"{n:,}".replace(",", "\u202f")


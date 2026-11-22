"""Constantes et réglages."""
import os
from pathlib import Path
# AUTO-IMPORTS
# END-AUTO

STORE_CODE            = "1431"
MAX_PAGES             = 15
_VIVINO_API_WORKERS    = 4     # conservé pour compatibilité (Phase 1 désactivée)
_VIVINO_SEL_WORKERS    = 3     # drivers Selenium parallèles pour Vivino
LECLERC_CACHE_TTL     = 12 * 3600  # conservé pour compatibilité — plus utilisé (cache permanent)
LECLERC_PAGE_SIZE     = 96
VIVINO_SIMILARITY_MIN  = 0.45   # relevé : 0.28 acceptait les faux-positifs API non triés
VIVINO_CANDIDATES_MAX  = 8
VIVINO_GOOD_ENOUGH      = 0.75   # au-dessus : correspondance sûre, on arrête de chercher
VIVINO_MAX_SEARCHES    = 3      # recherches Vivino max par vin (requête principale + 2 variantes)
VIVINO_LOW_CONFIDENCE  = 0.60   # en dessous : correspondance à vérifier (signalée dans l'interface)
VIVINO_API_TIMEOUT     = 12   # augmenté : 8s trop court sur réseau lent
VIVINO_CACHE_TTL_DAYS  = 30    # Entrées Vivino auto-marquées stale après N jours
CARDS_PER_PAGE         = 24    # Nb de cartes affichées par page dans le classement

VIVINO_API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.vivino.com/",
}

CACHE_DIR = Path(os.environ.get("CAVE_CACHE_DIR") or Path(__file__).resolve().parent.parent / ".cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

JOB_STATE_PATH    = CACHE_DIR / "job_state.json"
JOB_LOG_PATH      = CACHE_DIR / "job_log.txt"
REJECTION_LOG_PATH = CACHE_DIR / "vivino_rejections.json"  # Apprentissage rejets Vivino

WINE_TYPES = {
    "🔴 Rouge":    "vins-rouges",
    "⚪ Blanc":    "vins-blancs",
    "🌸 Rosé":     "vins-roses",
    "🍾 Mousseux": "vins-mousseux-et-petillants",
}

# Clés de tri — définies une fois au niveau module pour éviter la recréation
# de lambdas à chaque rerun Streamlit.
SORTS: dict[str, object] = {
    "Meilleur rapport qualité/prix":  lambda x: (-(x.get("score") or 0),   -(x.get("rating") or 0)),
    "Meilleure note":  lambda x: (-(x.get("rating") or 0),   -(x.get("score") or 0)),
    "Prix croissant":  lambda x: ( (x.get("price") or 9999), -(x.get("score") or 0)),
    "Prix décroissant":  lambda x: (-(x.get("price") or 0),    -(x.get("score") or 0)),
}
_SORT_HELP: dict[str, str] = {
    "Meilleur rapport qualité/prix":  "Note × popularité ÷ prix : les meilleures affaires d'abord.",
    "Meilleure note":  "Note Vivino décroissante.",
    "Prix croissant":  "Du moins cher au plus cher.",
    "Prix décroissant":  "Du plus cher au moins cher.",
}

# mapping slug Leclerc → wine_type_id Vivino (1=rouge, 2=blanc, 3=mousseux, 4=rosé)
# FIX : vins-roses était 7 (dessert) au lieu de 4 (rosé)
VIVINO_TYPE_IDS: dict[str, int] = {
    "vins-rouges":                    1,
    "vins-blancs":                    2,
    "vins-roses":                     4,   # FIX bug : 7=dessert, 4=rosé
    "vins-mousseux-et-petillants":    3,
}

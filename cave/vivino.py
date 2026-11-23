"""Vivino : session HTTP, requêtes, candidats, choix du meilleur, lecture des pages."""
import re
import json
import time
import threading
from functools import lru_cache
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
# AUTO-IMPORTS
from .regions import _norm_ascii, extract_region
from .config import VIVINO_API_HEADERS, VIVINO_API_TIMEOUT, VIVINO_CANDIDATES_MAX, VIVINO_GOOD_ENOUGH, VIVINO_MAX_SEARCHES, VIVINO_SIMILARITY_MIN, VIVINO_TYPE_IDS
from .storage import load_vivino_rejections
# END-AUTO

def _make_session() -> requests.Session:
    """Session HTTP avec retry automatique et connection pooling."""
    s = requests.Session()
    # 429 exclu de status_forcelist : le backoff global _vivino_429_until
    # gère la congestion. Laisser Retry retenter le 429 en parallèle × 8
    # threads aggraverait le throttling (24 requêtes supplémentaires).
    retry = Retry(total=3, backoff_factor=0.5,
                  status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=16))
    s.headers.update(VIVINO_API_HEADERS)
    return s

_SESSION = _make_session()

# ── Backoff 429/403 Vivino (partagé entre tous les threads Phase 1) ─────────
_vivino_429_until: float = 0.0
_vivino_429_lock  = threading.Lock()
_vivino_403_count: int   = 0    # nombre de 403 depuis le début de cette session API
_vivino_403_lock  = threading.Lock()
_VIVINO_403_ABORT = 10          # abandonner Phase 1 si ≥ N 403 consécutifs

def _vivino_inc_403() -> int:
    """Incrémente le compteur 403. Retourne la nouvelle valeur."""
    global _vivino_403_count
    with _vivino_403_lock:
        _vivino_403_count += 1
        return _vivino_403_count

def _vivino_reset_403() -> None:
    global _vivino_403_count
    with _vivino_403_lock:
        _vivino_403_count = 0

def _vivino_is_blocked() -> bool:
    """True si l'IP est probablement bloquée (trop de 403)."""
    with _vivino_403_lock:
        return _vivino_403_count >= _VIVINO_403_ABORT

def _vivino_wait_if_throttled() -> None:
    """Attend si Vivino a retourné 429 récemment."""
    global _vivino_429_until
    with _vivino_429_lock:
        wait = _vivino_429_until - time.time()
    if wait > 0:
        time.sleep(max(0.0, wait))

def _vivino_set_backoff(retry_after_hdr: str = "") -> float:
    """Enregistre un backoff exponentiel après un 429. Retourne le délai en s."""
    global _vivino_429_until
    try:
        delay = float(retry_after_hdr) if retry_after_hdr else 0.0
    except ValueError:
        delay = 0.0
    with _vivino_429_lock:
        current_wait = max(0.0, _vivino_429_until - time.time())
        if delay <= 0:
            delay = max(2.0, min(current_wait * 2 + 2.0, 60.0))
        _vivino_429_until = time.time() + delay
    return delay

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
    # Effervescents — query mono-mot trop générique
    "champagne", "prosecco", "cava",
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


# Regex compilées pour build_query — évite la recompilation à chaque appel
_YEAR_RE      = re.compile(r"\b(19|20)\d{2}\b")
_AOC_RE       = re.compile(r"\s*\b(AOP|IGP|AOC|A\.O\.P|A\.O\.C|I\.G\.P)\b\.?", re.I)
_ALLCAPS_RE   = re.compile(r"^[A-Z][A-Z\s\'\-]+$")
_MULTISPC_RE  = re.compile(r"\s{2,}")
_NONALPHA_RE  = re.compile(r"[^a-z]")   # utilisé dans _name_similarity bigrams

# Ensembles de mots-coupure pour build_query étape 7 — évite la recréation dict à chaque appel
_BQ_CUT             = frozenset({"Cuvée", "Cuvee", "Vieilles", "Vieille", "Vignes"})
_BQ_CUT_AFTER_GRANDE = frozenset({"Cuvée", "Cuvee", "Réserve", "Reserve", "Tradition"})

# Précalculé au niveau module — utilisé dans build_query (évite recompilation à chaque appel)
_VOLUME_PREFIX_RE = re.compile(
    r"^(" + "|".join(re.escape(k) for k in sorted(_VOLUME_MAP, key=len, reverse=True)) + r")\s+",
    re.I,
)

# ── Regex compilées (hot paths) ────────────────────────────────────────────
# parse_page : appelées sur chaque carte/ancre de chaque page Leclerc
_CARD_VINTAGE_RE = re.compile(r"\b(19[5-9]\d|20[0-3]\d)\b")
_EAN_OFFER_RE    = re.compile(r"offer_m-(\d{13})-\d+")
_EAN_URL_RE      = re.compile(r"-(\d{13})$")
_PAGE_NUM_RE     = re.compile(r"[?&]page=(\d+)")
# _parse_price fallback ultime (pattern 5)
_PRICE_FALLBACK_RE = re.compile(r"(?<![0-9])(\d{1,3})[,.](\d{2})\s*€")
# _parse_price patterns 1 & 2 — present dans le sous-bloc CSS price
_PRICE_CSS1_RE    = re.compile(r"(\d+)[€\s]*[,.](\d{2})")
_PRICE_CSS2_RE    = re.compile(r"(\d+)[,.](\d{2})")
# fetch_vivino_selenium : année dans l'URL courante
_VIVINO_YEAR_URL_RE = re.compile(r"[?&]year=(\d{4})")
# _norm_words : appelée dans _name_similarity, _fallback_queries, etc.
_WORDS3_RE = re.compile(r"[a-z]{3,}")
# extract_region word cleaning
_NON_ALPHANUM_RE = re.compile(r"[^a-z0-9 ]")
# extract_region : regex AOP/IGP après tiret
_REGION_AOP_RE   = re.compile(r"-\s*([\w\s\-\']+?)\s*(?:AOP|IGP|AOC|AOP-AOC)\b", re.I)
# vivino_candidates_from_search : filtre hrefs résultats de recherche
_VIVINO_WINE_HREF_RE = re.compile(r"/w(?:ines)?/[^/?&#\s]+")
# fetch_vivino : extraction JSON fallback depuis HTML brut
_VIVINO_AVG_RE    = re.compile(r'"ratings_average"\s*:\s*([\d.]+)')
_VIVINO_CNT_RE    = re.compile(r'"ratings_count"\s*:\s*(\d+)')
# Nettoyage de caractères non-chiffres
_NON_DIGIT_RE     = re.compile(r"[^\d]")
# Barre de progression dans la console job : [N/Total]
_PROGRESS_RE      = re.compile(r"\[(\d+)/(\d+)\]")
# _extract_year : utilisée dans vivino_candidates_from_search (1× par titre candidat)
# → réutilise _YEAR_RE déjà compilé (r"\b(19|20)\d{2}\b") ✓


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
    nom = _YEAR_RE.sub("", nom).strip().strip("-").strip()

    # Étape 4 : supprimer AOP/IGP/AOC/Vin de France où qu'ils soient
    nom = _AOC_RE.sub("", nom).strip()

    # Étape 5 : normalisation casse (tout-majuscule → Title Case)
    if _ALLCAPS_RE.match(nom):
        nom = nom.title()

    # Étape 6 : supprimer les mots parasites (couleur, termes de vinif…)
    nom_clean = _NOISE_WORDS.sub(" ", nom).strip()
    nom_clean = _MULTISPC_RE.sub(" ", nom_clean).strip()
    # N'appliquer que si le résultat n'est pas trop court
    if len(nom_clean.split()) >= 2:
        nom = nom_clean

    # Étape 6b : supprimer les prépositions/conjonctions orphelines en fin
    nom = _clean_tail(nom)

    # Étape 7 : couper avant les mots-coupure (Cuvée, Vieilles Vignes…)
    # "Grande" ne coupe que si suivi d'un autre descripteur de cuvée (Cuvée, Réserve…)
    # afin de ne pas tronquer des noms propres comme "Domaine de la Grande Bellane".
    words = nom.split()
    for i, w in enumerate(words[2:], 2):
        if w in _BQ_CUT:
            nom = " ".join(words[:i])
            break
        if w == "Grande" and i + 1 < len(words) and words[i + 1] in _BQ_CUT_AFTER_GRANDE:
            nom = " ".join(words[:i])
            break

    # Étape 8 : supprimer les appellations génériques en fin de query
    # (seulement si elles ne sont pas le cœur du nom)
    # Garde : ne pas couper si l'appellation fait partie d'un nom composé
    # (« Côtes du Rhône », « Côtes de Bordeaux ») — sinon il resterait « Côtes du ».
    words = nom.split()
    if len(words) > 2:
        tail = _norm_ascii(words[-1])
        if tail in _GENERIC_APPELLATIONS:
            candidate = " ".join(words[:-1]).strip()
            if _clean_tail(candidate) == candidate:   # pas de préposition orpheline
                nom = candidate

    # Étape 9 : résolution d'alias
    nom_key = _norm_ascii(nom)
    for alias_frag, canonical in _WINE_ALIASES.items():
        if alias_frag in nom_key:
            return canonical

    result = nom.strip()
    return result if len(result) > 2 else wine_name[:40].strip()


# _norm_words stop-words — défini une fois au niveau module (évite recréation set à chaque appel)
_STOP_WORDS = frozenset({
    "de","du","des","le","la","les","et","au","aux","en","par","sur",
    "un","une","the","of","and","for","vin","wines","wine",
    "rouge","blanc","rose","rosé","sec","brut",
})
# Préfixes producteur exclus du calcul de pénalité premier-mot
_NAME_PREFIX_STOP = frozenset({"chateau","domaine","maison","cave","les","de","du"})

_ABBREV_SUBS = (
    (re.compile(r"\bste?[.\-\s]+(?=[a-z])"), lambda m: "sainte " if m.group(0).lower().startswith("ste") else "saint "),
    (re.compile(r"\bch(?:at)?\.\s*"), lambda m: "chateau "),
    (re.compile(r"\bdom\.\s*"), lambda m: "domaine "),
)

def _expand_abbrev(ascii_text: str) -> str:
    """St-Émilion → saint emilion ; Ch. Margaux → chateau margaux ; Dom. X → domaine x."""
    for rx, fn in _ABBREV_SUBS:
        ascii_text = rx.sub(fn, ascii_text)
    return ascii_text

def _norm_words(s: str) -> set:
    return {w for w in _WORDS3_RE.findall(_expand_abbrev(_norm_ascii(s))) if w not in _STOP_WORDS}


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
        a = _NONALPHA_RE.sub("", _norm_ascii(s))
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
    w1_ordered = [w for w in _WORDS3_RE.findall(_norm_ascii(name1))
                  if w not in _NAME_PREFIX_STOP]
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
    m = _YEAR_RE.search(text or "")
    return int(m.group(0)) if m else None


_CARD_NOISE_RE = re.compile(
    r"\b\d[.,]\d\b|\b\d[\d\s.,\u202f]*\s*(?:notes?|avis|ratings?|reviews?)\b|"
    r"\b\d+[.,]?\d*\s*[€$]|[€$]\s*\d+[.,]?\d*|\b(?:notes?|avis|ratings?|reviews?|prix|price)\b",
    re.I)

def _clean_card_title(text: str) -> str:
    """Retire du texte d'une carte Vivino les notes, nombres d'avis et prix qui faussent la similarité."""
    return re.sub(r"\s+", " ", _CARD_NOISE_RE.sub(" ", text or "")).strip()

def _year_from_href(href: str) -> int | None:
    m = _VIVINO_YEAR_URL_RE.search(href or "")
    return _safe_year(m.group(1)) if m else None

def _slug_title(href: str) -> str:
    """Segment lisible de l'URL avant /w/<id> : /FR/fr/chateau-gazin-pomerol/w/123 → 'chateau gazin pomerol'."""
    path = re.split(r"[?#]", href or "", maxsplit=1)[0]
    m = re.search(r"/([^/]+)/w(?:ines)?/\d+", path)
    return m.group(1).replace("-", " ") if m else ""


def vivino_candidates_from_search(html: str, max_candidates: int = VIVINO_CANDIDATES_MAX) -> list[dict]:
    """
    Retourne plusieurs candidats depuis la page de recherche Vivino.
    Extrait aussi rating/count depuis les JSON embarqués → évite la 2e navigation.
    """
    # ── Extraire ratings depuis les JSON embarqués ──────────────────────────
    # La page React encode les données de chaque carte dans des blocs JSON.
    # On parse tous les blocs pour construire seo_name→(rating, count, nb_all).
    _ratings_by_seo: dict[str, tuple] = {}
    for m in re.finditer(r'"seo_name"\s*:\s*"([^"]+)"[^}]{0,600}?"ratings_average"\s*:\s*([\d.]+)[^}]{0,200}?"ratings_count"\s*:\s*(\d+)', html):
        seo, avg, cnt = m.group(1), float(m.group(2)), int(m.group(3))
        if seo not in _ratings_by_seo:
            _ratings_by_seo[seo] = (avg, cnt)
    # Fallback: parser les ratings_count_all (niveau wine, pas vintage)
    _all_by_seo: dict[str, int] = {}
    for m in re.finditer(r'"seo_name"\s*:\s*"([^"]+)"[^}]{0,800}?"ratings_count"\s*:\s*(\d+)', html):
        seo = m.group(1)
        if seo not in _all_by_seo:
            _all_by_seo[seo] = int(m.group(2))

    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not _VIVINO_WINE_HREF_RE.search(href) or "search" in href:
            continue
        url = href if href.startswith("http") else f"https://www.vivino.com{href}"
        if url in seen:
            continue
        seen.add(url)
        title = _clean_card_title(a.get_text(separator=" ", strip=True))
        # Extraire le seo_name depuis l'URL pour le lookup rating
        _seo_m = re.search(r"/w(?:ines)?/([^/?&#]+)", href)
        _seo = _seo_m.group(1) if _seo_m else ""
        _rating, _count = _ratings_by_seo.get(_seo, (None, 0))
        out.append({
            "url":    url,
            "title":  title,
            "year":   _year_from_href(href) or _extract_year(title),
            "slug_title": _slug_title(href),   # « chateau gazin pomerol » : propre, sans bruit de carte
            "rating": _rating,          # None si absent de la page
            "ratings_count": _count,
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

    # Pré-calculer les représentations de la query 1× (réutilisées sur chaque candidat)
    _q_words  = _norm_words(query)
    _q_ascii  = _NONALPHA_RE.sub("", _norm_ascii(query))
    _q_bg     = {_q_ascii[i:i+2] for i in range(len(_q_ascii)-1)} if len(_q_ascii) > 1 else set()
    _q_key    = max(_q_words, key=len, default="")
    _q_ordered= [w for w in _WORDS3_RE.findall(_norm_ascii(query)) if w not in _NAME_PREFIX_STOP]

    for c in candidates:
        # Ignorer les candidats dont l'URL a déjà été rejetée
        c_url = c.get("url") or (
            "https://www.vivino.com/wines/" +
            (((c.get("record") or {}).get("vintage") or {}).get("wine") or {}).get("seo_name", "")
            if c.get("record") else ""
        )
        if c_url and c_url in _rejected:
            continue

        # ── Similarité nom (utilise les pré-calculs query) ─────────────────
        c_title = c.get("title", "")
        # Le titre de carte est bruité ; le slug d'URL est propre. On garde le plus proche.
        _slug_t = c.get("slug_title", "")
        if _slug_t and len(_norm_words(_slug_t) & _q_words) > len(_norm_words(c_title) & _q_words):
            c_title = _slug_t
        c_words = _norm_words(c_title)
        if not c_words:
            continue
        inter = len(_q_words & c_words)
        union = len(_q_words | c_words)
        jaccard = inter / union if union else 0.0
        c_ascii = _NONALPHA_RE.sub("", _norm_ascii(c_title))
        c_bg    = {c_ascii[i:i+2] for i in range(len(c_ascii)-1)} if len(c_ascii) > 1 else set()
        bg_union = _q_bg | c_bg
        bg_score = len(_q_bg & c_bg) / len(bg_union) if bg_union else 0.0
        producer_bonus = 0.10 if _q_key and len(_q_key) >= 5 and _q_key in c_words else 0.0
        exclusive_vivino = c_words - _q_words
        extra_penalty = min(0.20, len({w for w in exclusive_vivino if len(w) >= 5}) * 0.08)
        first_word_penalty = 0.0
        if _q_ordered:
            first_sig = _q_ordered[0]
            if len(first_sig) >= 4 and first_sig not in c_words:
                first_word_penalty = 0.15
        score = round(min(1.0, max(0.0,
            jaccard * 0.7 + bg_score * 0.3 + producer_bonus - extra_penalty - first_word_penalty
        )), 4)

        # ── Boost appellation ──────────────────────────────────────────────
        if region_norm and region_norm in _norm_ascii(c_title):
            score += 0.30

        # ── Millésime ─────────────────────────────────────────────────────
        c_year = c.get("year")
        if vintage and c_year:
            if c_year == vintage:            score += 0.20
            elif abs(c_year - vintage) == 1: score += 0.08
            else:                            score -= 0.12
        elif vintage and not c_year:
            score -= 0.03

        # ── Boost cépages ──────────────────────────────────────────────────
        if _grapes_hint and c.get("record"):
            wine_obj   = (c["record"].get("vintage") or {}).get("wine") or {}
            style      = wine_obj.get("style") or {}
            viv_grapes = {_norm_ascii(g.get("name","")) for g in (style.get("grapes") or [])}
            if viv_grapes:
                common = _grapes_hint & viv_grapes
                score += min(0.20, len(common) * 0.08)
                if not common and len(_grapes_hint) >= 2:
                    score -= 0.08

        # ── Pénalité type incohérent ────────────────────────────────────────
        if c.get("record"):
            wine_obj = (c["record"].get("vintage") or {}).get("wine") or {}
            viv_type = wine_obj.get("type_id")
            if viv_type:
                expected_slug = _VIVINO_TYPE_TO_SLUG.get(viv_type, "")
                if expected_slug and expected_slug != slug:
                    score -= 0.75

        # ── Boost région Vivino exacte ──────────────────────────────────────
        if region_norm and c.get("record"):
            wine_obj   = (c["record"].get("vintage") or {}).get("wine") or {}
            viv_region = _norm_ascii((wine_obj.get("region") or {}).get("name") or "")
            if viv_region and (viv_region in region_norm or region_norm in viv_region):
                score += 0.15

        if score > best_score:
            best, best_score = c, score

    if not best or best_score < VIVINO_SIMILARITY_MIN:
        return None, best_score
    # Seuil dynamique : query mono-mot appellation générique → exige 0.70
    if len(_q_words) <= 1:
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
        _vivino_wait_if_throttled()   # attendre si 429 récent
        resp = _SESSION.get(
            "https://www.vivino.com/api/explore/explore",
            params={
                "language": "fr",
                "country_codes[]": "fr",
                "price_range_max": 300,
                "price_range_min": 0,
                "wine_type_ids[]": wine_type_id,
                "q": query,
                "order_by": "ratings_count",   # tri par popularité ≈ pertinence (seule valeur valide)
            },
            timeout=VIVINO_API_TIMEOUT,
        )
        if resp.status_code == 429:
            delay = _vivino_set_backoff(resp.headers.get("Retry-After", ""))
            time.sleep(delay)
            return None
        if resp.status_code == 403:
            # IP bloquée — incrémenter compteur global, ne pas faire de fallbacks
            n403 = _vivino_inc_403()
            if n403 == 1:   # loguer une seule fois
                import logging as _log
                _log.warning(f"[Vivino API] HTTP 403 pour {query!r} — IP bloquée ({n403})")
            return None
        if resp.status_code != 200:
            import logging as _log
            ct = resp.headers.get("Content-Type", "?")
            _log.warning(
                f"[Vivino API] HTTP {resp.status_code} pour {query!r} "
                f"| Content-Type: {ct} "
                f"| Body: {resp.text[:120]!r}"
            )
            return None

        # Vérifier que la réponse est bien du JSON (anti-scraping renvoie parfois du HTML)
        ct = resp.headers.get("Content-Type", "")
        if "json" not in ct:
            import logging as _log
            _log.warning(
                f"[Vivino API] Réponse non-JSON pour {query!r} "
                f"| Content-Type: {ct} | Body: {resp.text[:120]!r}"
            )
            return None

        data = resp.json()
        records = (data.get("explore_vintage", {}) or {}).get("records", [])
        # Si la clé explore_vintage est absente, loguer la structure pour déboguer
        if "explore_vintage" not in data:
            import logging as _log
            _log.warning(
                f"[Vivino API] Clé explore_vintage absente pour {query!r} "
                f"| Clés top-level: {list(data.keys())[:8]}"
            )
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
                    count = int(_NON_DIGIT_RE.sub("", str(rc)) or 0)
                if rating: break
        except Exception: pass
        if rating: break
    if not rating:
        m = _VIVINO_AVG_RE.search(html)
        if m:
            v = round(float(m.group(1)), 1)
            if 2.5 <= v <= 5.0: rating = v
    if not count:
        m = _VIVINO_CNT_RE.search(html)
        if m: count = int(m.group(1))
    if not rating:
        for el in soup.find_all(class_=lambda c: c and "averageValue" in c):
            try:
                v = round(float(el.get_text(strip=True).replace(",",".")), 1)
                if 2.5 <= v <= 5.0: rating = v; break
            except ValueError: pass
    if not count:
        for el in soup.find_all(class_=lambda c: c and "numRatings" in c):
            d = _NON_DIGIT_RE.sub("", el.get_text())
            if d: count = int(d); break
    return {"rating": rating, "ratings_count": count}


def _fetch_wine_page(driver, url: str) -> dict:
    """Ouvre une page vin Vivino et lit note + nb d'avis (JSON-LD). {} en cas d'échec."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        driver.get(url)
        waited = False
        try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "script[type='application/ld+json']")))
            waited = True
        except Exception:
            pass
        time.sleep(0.15 if waited else 1.2)
        d = parse_wine_jsonld(driver.page_source) or {}
        d["_final_url"] = driver.current_url
        return d
    except Exception:
        return {}


def fetch_vivino(driver, wine_name: str, vintage, slug: str = "vins-rouges", region: str = "",
                 known_url: str = "") -> dict:
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
    _gh = extract_grapes_from_name(wine_name)

    # Selenium : query enrichie avec le millésime pour plus de précision
    # Si zéro candidat, on essaie les requêtes de repli (_fallback_queries)
    sel_query = f"{query} {vintage}" if vintage else query
    best, confidence = None, 0.0
    tried_sel = {sel_query}

    # Raccourci : URL Vivino déjà connue (ex. « URL trouvée » sans note) → on saute
    # la recherche et on lit directement la page du vin (1 navigation au lieu de 2).
    if known_url:
        d = _fetch_wine_page(driver, known_url)
        if d.get("rating"):
            vy = None
            m = _VIVINO_YEAR_URL_RE.search(d.get("_final_url") or known_url)
            if m: vy = _safe_year(m.group(1))
            vmatch = (vintage == vy) if (vintage and vy) else (True if not vintage else None)
            return {"ratings_count_all": 0, "vivino_name": "", "winery": "",
                    "vivino_region": "", "vivino_region_seo": "", "country": "",
                    "grapes": [], "style_name": "", "is_natural": False,
                    "acidity": None, "tannin": None, "sweetness": None, "body": None,
                    "rating": d["rating"], "ratings_count": d["ratings_count"],
                    "vivino_url": known_url, "vivino_year": vy, "vintage_match": vmatch,
                    "match_confidence": 0.9}
        # sinon : retomber sur la recherche classique

    def _selenium_search(q):
        """Effectue une recherche Selenium et retourne (best, confidence)."""
        try:
            driver.get(f"https://www.vivino.com/search/wines"
                       f"?q={requests.utils.quote(q)}&language=fr")
            _sel_waited = False
            try:
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR,
                         "[class*='wineCard'],[class*='wine-card'],[class*='averageValue'],[href*='/w/']")))
                _sel_waited = True
            except Exception: pass
            time.sleep(0.15 if _sel_waited else 1.2)
            cands = vivino_candidates_from_search(driver.page_source)
            return choose_best_vivino_candidate(query, vintage, cands, region=region,
                                                grapes_hint=_gh, slug=slug)
        except Exception:
            return None, 0.0

    try:
        best, confidence = _selenium_search(sel_query)
        n_search = 1

        # Variantes de requête : si aucun candidat OU correspondance incertaine,
        # on essaie d'autres formulations et on garde la MEILLEURE (pas la première).
        # Toujours notée contre la requête complète → une variante courte ne peut pas
        # « gagner » en trouvant un vin plus générique.
        if (not best) or confidence < VIVINO_GOOD_ENOUGH:
            for fallback_q in _fallback_queries(wine_name, vintage):
                if n_search >= VIVINO_MAX_SEARCHES:
                    break
                fsel = f"{fallback_q} {vintage}" if vintage else fallback_q
                if fsel in tried_sel:
                    continue
                tried_sel.add(fsel)
                n_search += 1
                b2, c2 = _selenium_search(fsel)
                if b2 is not None and (best is None or c2 > confidence):
                    best, confidence = b2, c2
                if best and confidence >= VIVINO_GOOD_ENOUGH:
                    break
    except Exception:
        return EMPTY

    if not best:
        return EMPTY

    wine_url = best.get("url", "")
    if not wine_url:
        return EMPTY

    _enriched_empty = {
        "ratings_count_all": 0, "vivino_name": "", "winery": "",
        "vivino_region": "", "vivino_region_seo": "", "country": "",
        "grapes": [], "style_name": "", "is_natural": False,
        "acidity": None, "tannin": None, "sweetness": None, "body": None,
    }

    vy = _safe_year(best.get("year")) if best.get("year") else None
    vmatch = None
    if vintage and vy:   vmatch = (vintage == vy)
    elif not vintage:    vmatch = True

    # ── Optimisation : rating déjà disponible depuis la page de recherche ──
    # Si vivino_candidates_from_search a pu extraire le rating directement,
    # on évite la 2e navigation (wine page) : gain ~1.5s par vin.
    if best.get("rating"):
        return {**_enriched_empty,
                "rating":           best["rating"],
                "ratings_count":    best.get("ratings_count", 0),
                "vivino_url":       wine_url,
                "vivino_year":      vy,
                "vintage_match":    vmatch,
                "match_confidence": round(confidence, 3)}

    # ── Fallback : naviguer sur la page du vin pour extraire le rating ──────
    try:
        driver.get(wine_url)
        _wine_waited = False
        try:
            WebDriverWait(driver, 9).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "script[type='application/ld+json']")))
            _wine_waited = True
        except Exception: pass
        time.sleep(0.15 if _wine_waited else 1.2)
        d = parse_wine_jsonld(driver.page_source)
        # Affiner le millésime depuis l'URL de la page wine (plus fiable que le titre)
        m = _VIVINO_YEAR_URL_RE.search(driver.current_url)
        if m:
            vy = _safe_year(m.group(1))
            if vintage and vy: vmatch = (vintage == vy)
    except Exception:
        return EMPTY

    if not d.get("rating"):
        return {**_enriched_empty, "rating": None, "ratings_count": 0,
                "vivino_url": wine_url, "vivino_year": vy, "vintage_match": vmatch,
                "match_confidence": round(confidence, 3)}
    return {**_enriched_empty, "rating": d["rating"], "ratings_count": d["ratings_count"],
            "vivino_url": wine_url, "vivino_year": vy, "vintage_match": vmatch,
            "match_confidence": round(confidence, 3)}


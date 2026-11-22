"""Régions, appellations et comparaison floue de noms."""
import re
import unicodedata
# AUTO-IMPORTS
# END-AUTO

_NONALPHA_RE  = re.compile(r"[^a-z]")   # utilisé dans _name_similarity bigrams
_NON_ALPHANUM_RE = re.compile(r"[^a-z0-9 ]")
_REGION_AOP_RE   = re.compile(r"-\s*([\w\s\-\']+?)\s*(?:AOP|IGP|AOC|AOP-AOC)\b", re.I)

_REGIONS = [
    "Saint-Émilion Grand Cru","Saint-Émilion","Pomerol","Fronsac",
    "Pauillac","Saint-Estèphe","Margaux","Saint-Julien","Listrac","Moulis",
    "Haut-Médoc","Médoc","Pessac-Léognan","Graves","Entre-Deux-Mers",
    "Bordeaux Supérieur","Bordeaux",
    "Gevrey-Chambertin","Nuits-Saint-Georges","Pommard","Volnay","Beaune",
    "Aloxe-Corton","Meursault","Puligny-Montrachet","Chassagne-Montrachet",
    "Saint-Aubin","Chablis Premier Cru","Chablis Grand Cru","Chablis",
    "Mâcon-Villages","Mâcon","Pouilly-Fuissé","Saint-Véran","Viré-Clessé","Bourgogne",
    "Châteauneuf-du-Pape","Gigondas","Vacqueyras","Rasteau","Condrieu","Viognier",
    "Crozes-Hermitage","Hermitage","Cornas","Saint-Joseph",
    "Côtes du Rhône Villages","Côtes du Rhône",
    "Bandol","Côtes de Provence","Provence",
    "Pic Saint-Loup","Terrasses du Larzac",
    "Faugères","Saint-Chinian","Minervois","Corbières","Fitou","La Clape","Languedoc",
    "Côtes du Roussillon Villages","Côtes du Roussillon","Roussillon",
    "Cahors","Madiran","Bergerac","Pécharmant","Fronton","Gaillac","Marcillac","Irouléguy",
    "Saumur-Champigny","Saumur","Bourgueil","Saint-Nicolas-de-Bourgueil","Chinon",
    "Vouvray","Montlouis-sur-Loire","Anjou","Muscadet Sèvre et Maine","Muscadet",
    "Sancerre","Pouilly-Fumé","Quincy","Reuilly","Menetou-Salon","Loire",
    "Fleurie","Moulin-à-Vent","Morgon","Brouilly","Beaujolais Villages","Beaujolais",
    # Alsace
    "Alsace Grand Cru","Alsace Riesling","Alsace Gewurztraminer",
    "Alsace Pinot Gris","Alsace Pinot Blanc","Alsace",
    # Champagne & Mousseux
    "Champagne","Crémant d'Alsace","Crémant de Bourgogne","Crémant de Loire",
    "Crémant du Jura","Blanquette de Limoux","Clairette de Die","Prosecco","Cava",
    # Divers
    "Côtes de Gascogne","Jurançon","Pacherenc du Vic-Bilh",
    "Pays d'Oc","Vin de France",
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
    t_words = _NON_ALPHANUM_RE.sub(" ", t)
    words = q.split()
    if len(words) > 1 and all(w in t_words for w in words):
        return True
    # 3. Bigrammes (tolérance aux fautes de frappe courtes)
    if len(q) >= 5:
        def bigrams(s): return {s[i:i+2] for i in range(len(s) - 1)}
        bq, bt = bigrams(_NONALPHA_RE.sub("", q)), bigrams(_NONALPHA_RE.sub("", t))
        if bq and len(bq & bt) / len(bq) >= 0.70:
            return True
    return False

# Précompilation des régions normalisées — calculé 1× au démarrage, pas à chaque appel
_REGIONS_NORM: list[tuple[str, str]] = [(r, _norm_ascii(r)) for r in _REGIONS]

def extract_region(wine_name: str) -> str:
    m = _REGION_AOP_RE.search(wine_name)
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



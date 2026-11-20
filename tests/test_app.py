"""Tests de l'app Cave Leclerc × Vivino — rapides, sans réseau ni navigateur.

    pip install -r requirements-dev.txt
    pytest tests -q
"""
import pytest


# ═══════════════════════════════════════════════════════════════════════════
# Score qualité / prix
# ═══════════════════════════════════════════════════════════════════════════

def test_score_zero_when_data_missing(ns):
    cs = ns["compute_score"]
    assert cs(None, 5000, 10) == 0.0
    assert cs(4.0, 5000, 0) == 0.0
    assert cs(4.0, 5000, -3) == 0.0
    assert cs(4.0, 5000, None) == 0.0


def test_score_rises_with_rating_and_falls_with_price(ns):
    cs = ns["compute_score"]
    assert cs(4.4, 5000, 10) > cs(3.8, 5000, 10)
    assert cs(4.0, 5000, 8) > cs(4.0, 5000, 20)


def test_score_rises_with_review_count_until_cap(ns):
    cs = ns["compute_score"]
    scores = [cs(4.0, n, 10) for n in (1, 25, 100, 1000, 5000, 10000)]
    assert scores == sorted(scores)
    assert cs(4.0, 10000, 10) == cs(4.0, 500000, 10)   # confiance plafonnée à 1.0


def test_score_vintage_bonus(ns):
    cs = ns["compute_score"]
    assert cs(4.0, 5000, 10, True) == pytest.approx(cs(4.0, 5000, 10) * 1.05, abs=0.01)
    assert cs(4.0, 5000, 10, False) == cs(4.0, 5000, 10, None)


def test_score_unknown_count_does_not_beat_known_reviews(ns):
    """Régression : un nombre d'avis inconnu (0) valait confiance 0.5 (≈ 2 500 avis) et
    doublait un vin à 1 000 avis confirmés."""
    cs = ns["compute_score"]
    assert cs(4.0, 0, 10) <= cs(4.0, 1000, 10)
    assert cs(4.0, 0, 10) == cs(4.0, None, 10)          # 0 et None = inconnu
    assert cs(4.0, 0, 10) > cs(4.0, 1, 10)              # mais mieux qu'un seul avis


# ═══════════════════════════════════════════════════════════════════════════
# Historique de prix
# ═══════════════════════════════════════════════════════════════════════════

def test_price_trend(ns):
    pt = ns["price_trend"]
    # history[-1] = relevé du jour ; la tendance compare au relevé précédent (history[-2] = 10 €)
    ph = {"E1": {"history": [{"date": "d1", "price": 10.0}, {"date": "d2", "price": 12.0}]}}
    assert pt("E1", 12.0, ph) == "↑"
    assert pt("E1", 10.0, ph) == "="
    assert pt("E1", 10.04, ph) == "="      # tolérance de 5 centimes
    assert pt("E1", 9.0, ph) == "↓"
    assert pt("", 10.0, ph) == ""
    assert pt("inconnu", 10.0, ph) == ""
    assert pt("E1", 10.0, {"E1": {"history": [{"date": "d1", "price": 10.0}]}}) == ""   # 1 seul relevé


def test_update_price_history_is_idempotent_per_day(ns):
    wines = [{"name": "Vin A", "price": 9.9, "ean": "1234567890123"}]
    ns["update_price_history"](wines)
    ns["update_price_history"](wines)
    hist = ns["load_price_history"]()
    assert len(hist["1234567890123"]["history"]) == 1


# ═══════════════════════════════════════════════════════════════════════════
# Régions, requêtes Vivino, recherche floue
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("name, region", [
    ("Château Margaux 2015 - Margaux AOP", "Margaux"),
    ("Château Gazin Pomerol", "Pomerol"),
    ("Magnum E.Guigal Côtes du Rhône 2019", "Côtes du Rhône"),
    ("Mouton Cadet Bordeaux", "Bordeaux"),
    ("La Vieille Ferme", ""),
])
def test_extract_region(ns, name, region):
    assert ns["extract_region"](name) == region


@pytest.mark.parametrize("name, expected", [
    ("Château Margaux 2015 - Margaux AOP", "Château Margaux"),
    ("Demi-bouteille Château Tour Prignac Médoc", "Château Tour Prignac"),
])
def test_build_query_strips_noise(ns, name, expected):
    assert ns["build_query"](name) == expected


def test_build_query_never_empty_and_stable(ns):
    bq = ns["build_query"]
    for name in ["Château X", "2020", "Magnum", "Bleu de Mer Rouge", "SANGOUARD-CHÉNÉ Mâcon-Igé"]:
        q = bq(name)
        assert q.strip(), name
        assert bq(name) == q   # déterministe (clé de cache Vivino)


@pytest.mark.parametrize("name, expected", [
    ("M. Chapoutier Côtes du Rhône", "M. Chapoutier Côtes du Rhône"),
    ("Chateau De Ruth Côtes du Rhône", "Chateau De Ruth Côtes du Rhône"),
    ("Château Latour Camblanes Cadillac Côtes de Bordeaux", "Château Latour Camblanes Cadillac Côtes de Bordeaux"),
    ("Berne Grande Récolte Côtes de Provence", "Berne Grande Récolte Côtes de Provence"),
])
def test_build_query_keeps_compound_appellations(ns, name, expected):
    """Régression : l'étape « appellation générique en fin de query » laissait « Côtes du »."""
    assert ns["build_query"](name) == expected


@pytest.mark.parametrize("name, expected", [
    ("Château Dutruch Grand Poujeaux Moulis Bordeaux", "Château Dutruch Grand Poujeaux Moulis"),
    ("Domaine Test Les Terrasses Languedoc", "Domaine Test Les Terrasses"),
])
def test_build_query_still_strips_plain_generic_tail(ns, name, expected):
    """La garde ne doit pas désactiver le cas normal : « Bordeaux »/« Languedoc » en fin de nom."""
    assert ns["build_query"](name) == expected


def test_build_query_never_ends_with_dangling_word(ns):
    bq = ns["build_query"]
    for name in ["Magnum E.Guigal Côtes du Rhône 2019", "Dourthe La Bordeaux",
                 "Le Fruit Défendu du Château Lamour Sans Sulfites Ajoutés Castillon Côtes de Bordeaux"]:
        assert not bq(name).lower().endswith((" du", " de", " des", " la", " le", " les", " aux", " au")), name


@pytest.mark.parametrize("query, target, expected", [
    ("bord", "Bordeaux Supérieur", True),
    ("saint em", "Saint-Émilion Grand Cru", True),
    ("chateau margaux", "Château Margaux 2015", True),
    ("", "n'importe quoi", True),
    ("zzzzzz", "Bordeaux", False),
])
def test_fuzzy_match(ns, query, target, expected):
    assert ns["_fuzzy_match"](query, target) is expected


# ═══════════════════════════════════════════════════════════════════════════
# Cache : fusion Leclerc + Vivino
# ═══════════════════════════════════════════════════════════════════════════

def test_merge_vivino_does_not_mutate_input_and_computes_score(ns):
    wines = [{"name": "Château Gazin Pomerol", "price": 20.0, "ean": "1"}]
    vc = {ns["build_query"]("Château Gazin Pomerol"): {
        "rating": 4.3, "ratings_count": 8000, "vivino_url": "https://www.vivino.com/w/1"}}
    out = ns["_merge_vivino"](wines, vc, {})
    assert "rating" not in wines[0]                 # entrée intacte
    assert out[0]["rating"] == 4.3 and out[0]["score"] > 0
    assert out[0]["region"] == "Pomerol"


def test_merge_vivino_suppressed_entry_hides_rating(ns):
    name = "Château Gazin Pomerol"
    vc = {ns["build_query"](name): {"rating": 4.3, "ratings_count": 8000,
                                    "vivino_url": "https://x", "suppressed": True}}
    out = ns["_merge_vivino"]([{"name": name, "price": 20.0}], vc, {})
    assert out[0]["rating"] is None and out[0]["vivino_url"] == "" and out[0]["score"] == 0.0


# ═══════════════════════════════════════════════════════════════════════════
# UI complète (AppTest) : démarre sans secrets.toml, 5 onglets, filtres sans erreur
# ═══════════════════════════════════════════════════════════════════════════

def test_app_starts_without_secrets_and_renders_tabs(app_test):
    assert not app_test.exception, [e.value for e in app_test.exception]
    assert len(app_test.tabs) == 5


def test_sort_buttons_filters_and_wine_types(app_test):
    at = app_test
    for b in [b for b in at.button if b.key and b.key.startswith("sort_")]:
        b.click()
        at.run()
        assert not at.exception, (b.label, [e.value for e in at.exception])
    for c in at.sidebar.checkbox:
        c.set_value(not c.value)
        at.run()
        assert not at.exception, (c.label, [e.value for e in at.exception])
        c.set_value(not c.value)
        at.run()
    for t in at.sidebar.text_input:
        for value in ("bordeaux", "zzzzzz", ""):
            t.set_value(value)
            at.run()
            assert not at.exception, (value, [e.value for e in at.exception])
    box = at.sidebar.selectbox[0]
    for option in box.options:
        box.select(option)
        at.run()
        assert not at.exception, (option, [e.value for e in at.exception])

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
# UI complète (AppTest) : 3 écrans, filtres, tris, vues
# ═══════════════════════════════════════════════════════════════════════════

SCREENS = ["🏅 Classement", "🔄 Mise à jour", "⚙️ Réglages"]


def _assert_ok(at, ctx=""):
    assert not at.exception, (ctx, [e.value for e in at.exception])


def test_app_starts_without_secrets_on_ranking(app_test):
    _assert_ok(app_test)
    assert app_test.sidebar.radio[0].options == SCREENS
    assert app_test.sidebar.radio[0].value == SCREENS[0]


def test_every_screen_renders(app_test):
    at = app_test
    for screen in SCREENS:
        at.sidebar.radio[0].set_value(screen).run()
        _assert_ok(at, screen)


def test_ranking_sorts_views_and_filters(app_test):
    at = app_test
    sort_box = next(s for s in at.selectbox if s.key == "sort_key")
    for option in sort_box.options:
        sort_box.select(option).run()
        _assert_ok(at, option)
        sort_box = next(s for s in at.selectbox if s.key == "sort_key")
    for view in ("Bonnes affaires", "Statistiques", "Tableau & export", "Liste"):
        at.session_state["view"] = view
        at.run()
        _assert_ok(at, view)
    search = next(t for t in at.text_input if t.key and t.key.startswith("f_search_"))
    for value in ("bordeaux", "zzzzzz", ""):
        search.set_value(value).run()
        _assert_ok(at, value)
        search = next(t for t in at.text_input if t.key and t.key.startswith("f_search_"))
    for box in list(at.sidebar.selectbox):
        for option in box.options:
            box.select(option).run()
            _assert_ok(at, option)
            box = at.sidebar.selectbox[0]


def test_update_screen_shows_guided_steps_and_actions(app_test):
    at = app_test
    at.sidebar.radio[0].set_value(SCREENS[1]).run()
    _assert_ok(at)
    labels = [b.label for b in at.button]
    assert any("Mettre à jour le catalogue" in l for l in labels)
    assert any("Compléter" in l for l in labels)
    assert any("Tout rafraîchir" in l for l in labels)


def test_ranking_cards_have_visible_reject_control(app_test):
    labels = [p.proto.popover.label for p in app_test.get("popover")]
    assert labels and all("bon vin sur Vivino" in l for l in labels)


# ═══════════════════════════════════════════════════════════════════════════
# Scraping : rapidité (sans réseau ni navigateur)
# ═══════════════════════════════════════════════════════════════════════════

class _FakeDriver:
    current_url = "https://www.vivino.com/w/1?year=2019"
    page_source = ""

    def __init__(self):
        self.gets = []

    def get(self, url):
        self.gets.append(url)


@pytest.fixture()
def no_selenium_wait(monkeypatch):
    """WebDriverWait factice : échoue aussitôt (évite d'attendre 8 s sur un faux driver)."""
    import selenium.webdriver.support.ui as ui

    class _W:
        def __init__(self, *a, **k): pass
        def until(self, *a, **k): raise TimeoutError

    monkeypatch.setattr(ui, "WebDriverWait", _W)


def test_fetch_vivino_known_url_skips_search(ns, monkeypatch):
    """URL déjà connue (« URL trouvée » sans note) : une seule navigation, pas de recherche."""
    monkeypatch.setattr("cave.vivino._fetch_wine_page", lambda drv, url: {
        "rating": 4.1, "ratings_count": 321, "_final_url": url + "?year=2019"})
    drv = _FakeDriver()
    r = ns["fetch_vivino"](drv, "Château Test Bordeaux", 2019,
                           known_url="https://www.vivino.com/w/1")
    assert drv.gets == []                      # la recherche n'a pas été ouverte
    assert r["rating"] == 4.1 and r["ratings_count"] == 321
    assert r["vivino_url"] == "https://www.vivino.com/w/1"
    assert r["vivino_year"] == 2019 and r["vintage_match"] is True


def test_fetch_vivino_known_url_without_rating_falls_back_to_search(ns, monkeypatch, no_selenium_wait):
    monkeypatch.setattr("cave.vivino._fetch_wine_page", lambda drv, url: {})
    drv = _FakeDriver()
    r = ns["fetch_vivino"](drv, "Château Test Bordeaux", 2019,
                           known_url="https://www.vivino.com/w/1")
    assert any("/search/wines" in u for u in drv.gets)   # repli sur la recherche classique
    assert r["rating"] is None


def test_make_driver_loads_fast(ns, monkeypatch):
    """Chargement « eager » + images/polices bloquées."""
    import selenium.webdriver as wd
    captured = {}
    monkeypatch.setattr(wd, "Chrome", lambda *a, **k: captured.setdefault("opts", k["options"]))
    import cave.browser as browser
    browser.make_driver(fast=True)
    o = captured["opts"]
    assert o.page_load_strategy == "eager"
    prefs = o.experimental_options["prefs"]
    assert prefs["profile.managed_default_content_settings.images"] == 2


def test_make_driver_default_is_unchanged_for_leclerc(ns, monkeypatch):
    """Leclerc garde le chargement normal (images comprises)."""
    import selenium.webdriver as wd
    captured = {}
    monkeypatch.setattr(wd, "Chrome", lambda *a, **k: captured.setdefault("opts", k["options"]))
    import cave.browser as browser
    browser.make_driver()
    o = captured["opts"]
    assert o.page_load_strategy == "normal"
    assert "prefs" not in o.experimental_options


def test_refresh_skips_wines_with_fresh_rating(ns, monkeypatch):
    """Régression : _is_fresh ne ménageait jamais aucun vin (locked=False y était lu comme
    « verrouillé ») → un rafraîchissement refaisait tout."""
    seen = {}
    monkeypatch.setattr("cave.jobs._scrape_vivino_list",
                        lambda slug, wines, todo, vc, log: seen.setdefault("todo", todo))
    ns["run_refresh_vivino"]("vins-rouges")
    vc = ns["load_vivino_cache"]("vins-rouges")
    todo_names = {w["name"] for w in seen["todo"]}
    fresh = {k for k, e in vc.items() if e.get("rating") and not e.get("_stale")}
    lc = ns["load_leclerc_cache"]("vins-rouges")["wines"]
    for w in lc:
        key = ns["build_query"](w["name"])
        if key in fresh:
            assert w["name"] not in todo_names
        else:
            assert w["name"] in todo_names
    assert 0 < len(todo_names) < len(lc)


# ═══════════════════════════════════════════════════════════════════════════
# Détection Vivino (HTML fictif, sans réseau)
# ═══════════════════════════════════════════════════════════════════════════

def test_card_title_noise_is_stripped(ns):
    raw = "Château Gazin Pomerol 4.2 1 234 notes 25,00 €"
    assert ns["_clean_card_title"](raw) == "Château Gazin Pomerol"


def test_candidates_read_year_from_url_and_slug(ns):
    html = ('<a href="/FR/fr/chateau-gazin-pomerol/w/1234?year=2019">Château Gazin 4.2 900 notes</a>'
            '<a href="/FR/fr/chateau-gazin-rocquencourt-pessac/w/99">Rocquencourt</a>')
    cands = ns["vivino_candidates_from_search"](html)
    assert cands[0]["year"] == 2019
    assert cands[0]["slug_title"] == "chateau gazin pomerol"
    assert cands[1]["year"] is None


def test_choose_best_prefers_exact_wine_over_namesake(ns):
    html = ('<a href="/FR/fr/chateau-gazin-rocquencourt-pessac-leognan/w/99">Château Gazin Rocquencourt 4.0</a>'
            '<a href="/FR/fr/chateau-gazin-pomerol/w/1234?year=2019">Château Gazin 4.2 900 notes</a>')
    cands = ns["vivino_candidates_from_search"](html)
    best, score = ns["choose_best_vivino_candidate"]("Château Gazin Pomerol", 2019, cands, region="Pomerol")
    assert best and "/w/1234" in best["url"] and score >= 0.6


def test_choose_best_uses_clean_slug_when_card_text_is_poor(ns):
    """Le texte de la carte ne contient que le domaine ; le slug d'URL donne le reste."""
    html = '<a href="/FR/fr/domaine-test-les-terrasses-languedoc/w/55">Domaine Test</a>'
    cands = ns["vivino_candidates_from_search"](html)
    best, score = ns["choose_best_vivino_candidate"]("Domaine Test Les Terrasses", None, cands)
    assert best is not None


def test_name_abbreviations_are_expanded(ns):
    nw = ns["_norm_words"]
    assert {"chateau", "margaux"} <= nw("Ch. Margaux")
    assert "saint" in nw("St-Émilion Grand Cru")
    assert {"domaine", "leflaive"} <= nw("Dom. Leflaive")
    assert ns["_name_similarity"]("St Emilion Château Test", "Saint-Émilion Château Test") > 0.8


class _SearchDriver:
    """Faux driver : chaque requête de recherche renvoie la page prévue pour sa requête."""
    current_url = ""

    def __init__(self):
        self.gets = []
        self.page_source = ""

    def get(self, url):
        self.gets.append(url)


def _run_fetch(monkeypatch, ns, results_by_search):
    """results_by_search : liste de (candidat|None, score) renvoyés à chaque recherche successive."""
    import cave.vivino as viv
    import selenium.webdriver.support.ui as ui

    class _W:
        def __init__(self, *a, **k): pass
        def until(self, *a, **k): return True

    monkeypatch.setattr(ui, "WebDriverWait", _W)
    monkeypatch.setattr(viv.time, "sleep", lambda *_: None)
    monkeypatch.setattr(viv, "vivino_candidates_from_search", lambda html: [{"x": 1}])
    it = iter(results_by_search)
    monkeypatch.setattr(viv, "choose_best_vivino_candidate", lambda *a, **k: next(it, (None, 0.0)))
    monkeypatch.setattr(viv, "_fallback_queries", lambda name, vintage, rej=None: ["variante un", "variante deux", "variante trois"])
    drv = _SearchDriver()
    return viv.fetch_vivino(drv, "Château Test Bordeaux", 2019), drv


def _cand(n, rating=4.2):
    return {"url": f"https://www.vivino.com/w/{n}", "title": f"vin {n}", "year": 2019,
            "rating": rating, "ratings_count": 500}


def test_fetch_stops_after_one_search_when_match_is_sure(ns, monkeypatch):
    r, drv = _run_fetch(monkeypatch, ns, [(_cand(1), 0.9)])
    assert len(drv.gets) == 1 and r["vivino_url"].endswith("/w/1")


def test_fetch_keeps_best_of_several_searches(ns, monkeypatch):
    r, drv = _run_fetch(monkeypatch, ns, [(_cand(1), 0.50), (_cand(2), 0.82)])
    assert len(drv.gets) == 2 and r["vivino_url"].endswith("/w/2")


def test_fetch_keeps_first_when_variants_are_worse(ns, monkeypatch):
    r, drv = _run_fetch(monkeypatch, ns, [(_cand(1), 0.60), (_cand(2), 0.48), (_cand(3), 0.50)])
    assert r["vivino_url"].endswith("/w/1")
    assert len(drv.gets) == ns["VIVINO_MAX_SEARCHES"]


def test_fetch_recovers_when_first_search_finds_nothing(ns, monkeypatch):
    r, drv = _run_fetch(monkeypatch, ns, [(None, -1.0), (None, 0.2), (_cand(3), 0.55)])
    assert r["vivino_url"].endswith("/w/3")


def test_fetch_gives_up_cleanly_when_nothing_matches(ns, monkeypatch):
    r, drv = _run_fetch(monkeypatch, ns, [(None, -1.0)] * 5)
    assert r["rating"] is None and r["vivino_url"] == ""
    assert len(drv.gets) <= ns["VIVINO_MAX_SEARCHES"]


# ═══════════════════════════════════════════════════════════════════════════
# Reprise du scraping et compatibilité des caches
# ═══════════════════════════════════════════════════════════════════════════

def test_checkpoint_roundtrip_enables_resume(ns, tmp_path, monkeypatch):
    import cave.storage as stg
    monkeypatch.setattr(stg, "CACHE_DIR", tmp_path)
    ns_ckpt = stg
    ns_ckpt.ckpt_create("vins-test", 3)
    ns_ckpt.ckpt_tick("vins-test", "E1")
    ns_ckpt.ckpt_tick("vins-test", "E2")
    ns_ckpt._flush_ckpt("vins-test")
    ck = ns_ckpt.ckpt_load("vins-test")
    assert ck and set(ck["done_eans"]) == {"E1", "E2"} and ck["total"] == 3
    ns_ckpt.ckpt_finish("vins-test")
    assert ns_ckpt.ckpt_load("vins-test") is None


def test_existing_vivino_cache_format_still_loads_and_merges(ns):
    """Les données déjà acquises (ancien format de cache) restent lisibles par la nouvelle version."""
    vc = ns["load_vivino_cache"]("vins-rouges")
    assert vc and all("rating" in e for e in vc.values())
    wines = ns["load_wines_from_cache"]("vins-rouges")
    assert wines and any(w.get("rating") for w in wines) and all("score" in w for w in wines)

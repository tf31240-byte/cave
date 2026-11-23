"""Tests de l'app Cave Leclerc × Vivino — rapides, sans réseau ni navigateur.

    pip install -r requirements-dev.txt
    pytest tests -q
"""
import time

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


def _group(at, key):
    return next(b for b in at.get("button_group") if b.key == key)


def _go(at, screen):
    _group(at, "screen").set_value(screen).run()


def test_app_starts_without_secrets_on_ranking(app_test):
    _assert_ok(app_test)
    assert _group(app_test, "screen").value == SCREENS[0]


def test_every_screen_renders(app_test):
    at = app_test
    for screen in SCREENS:
        _go(at, screen)
        _assert_ok(at, screen)


def test_every_wine_type_renders(app_test):
    at = app_test
    for label in list(_group(at, "wine_type").options):
        _group(at, "wine_type").set_value(label).run()
        _assert_ok(at, label)


def test_ranking_sorts_views_and_filters(app_test):
    at = app_test
    sort_box = next(s for s in at.selectbox if s.key == "sort_key")
    for option in sort_box.options:
        sort_box.select(option).run()
        _assert_ok(at, option)
        sort_box = next(s for s in at.selectbox if s.key == "sort_key")
    for view in ("Affaires", "Stats", "Tableau", "Liste"):
        _group(at, "view").set_value(view).run()
        _assert_ok(at, view)
    search = next(t for t in at.text_input if t.key and t.key.startswith("f_search_"))
    for value in ("bordeaux", "zzzzzz", ""):
        search.set_value(value).run()
        _assert_ok(at, value)
        search = next(t for t in at.text_input if t.key and t.key.startswith("f_search_"))


def test_update_screen_shows_guided_steps_and_actions(app_test):
    at = app_test
    _go(at, SCREENS[1])
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


# ═══════════════════════════════════════════════════════════════════════════
# Journal d'activité et rapports
# ═══════════════════════════════════════════════════════════════════════════

def test_activity_journal_events_filters_and_reports(ns):
    ns["new_run"]("vivino", label="test")
    ns["add_event"]("vivino", "✅ bien", )
    ns["add_event"]("vivino", "⚠️ douteux")
    ns["add_event"]("catalogue", "❌ cassé")
    assert ns["level_of"]("🔍 vin introuvable sur Vivino") == "warn"
    run = ns["current_run"]()
    evs = ns["read_events"](run=run)
    assert [e["level"] for e in evs] == ["info", "ok", "warn", "error"]
    assert [e["msg"] for e in ns["read_events"](kinds=["catalogue"], run=run)] == ["❌ cassé"]
    assert len(ns["read_events"](levels=["warn"], run=run)) == 1
    assert len(ns["read_events"](contains="DOUTEUX", run=run)) == 1
    log = ns["make_log"]("vivino", mirror=lambda m: None)
    log("ligne 1\nligne 2")
    assert [e["msg"] for e in ns["read_events"](limit=2)] == ["ligne 1", "ligne 2"]
    ns["save_report"]("t", {"a": 1})
    assert ns["load_report"]("t")["a"] == 1 and ns["load_report"]("absent") == {}


def test_activity_journal_rotates(ns, monkeypatch):
    import cave.activity as act
    monkeypatch.setattr(act, "_MAX_BYTES", 2000)
    monkeypatch.setattr(act, "_KEEP_LINES", 5)
    for i in range(60):
        act.add_event("systeme", f"evt {i}")
    assert len(act.ACTIVITY_PATH.read_text("utf-8").splitlines()) < 40
    assert act.read_events(limit=1)[0]["msg"] == "evt 59"


def _card(name, ean, price):
    return {"name": name, "ean": ean, "price": price, "url": "u", "image": "", "vintage": None}


def _fake_leclerc(monkeypatch, cards):
    import cave.leclerc as lc

    class _D:
        def quit(self): pass
    monkeypatch.setattr(lc, "make_driver", lambda *a, **k: _D())
    monkeypatch.setattr(lc, "_set_store_cookie", lambda d: None)
    monkeypatch.setattr(lc, "_scrape_all_leclerc_pages", lambda d, slug, log: cards)


def test_check_availability_reports_new_removed_back_and_prices(ns, monkeypatch):
    cached = [dict(_card("Vin A", "1", 10.0), available=True),
              dict(_card("Vin B", "2", 8.0), available=True),
              dict(_card("Vin C", "3", 5.0), available=False)]
    _fake_leclerc(monkeypatch, [_card("Vin A", "1", 12.5), _card("Vin C", "3", 5.0), _card("Vin D", "4", 7.0)])
    lines, rep = [], {}
    out = ns["check_availability"]("vins-rouges", cached, log=lines.append, report=rep)
    by = {w["name"]: w for w in out}
    assert by["Vin B"]["available"] is False and by["Vin C"]["available"] is True
    assert by["Vin A"]["price"] == 12.5
    assert by["Vin D"]["available"] is True and by["Vin D"]["first_seen"] > 0
    assert [n["name"] for n in rep["new"]] == ["Vin D"]
    assert [n["name"] for n in rep["removed"]] == ["Vin B"]
    assert [n["name"] for n in rep["back"]] == ["Vin C"]
    assert rep["price_changes"][0]["old"] == 10.0 and rep["price_changes"][0]["new"] == 12.5
    text = "\n".join(lines)
    assert "➕ Nouveau" in text and "➖ Plus en rayon" in text and "🔄 De retour" in text and "Prix" in text


def test_check_availability_leaves_catalogue_untouched_when_site_blocked(ns, monkeypatch):
    cached = [dict(_card("Vin A", "1", 10.0), available=True)]
    _fake_leclerc(monkeypatch, [])
    lines, rep = [], {}
    out = ns["check_availability"]("vins-rouges", cached, log=lines.append, report=rep)
    assert out == cached and rep["ok"] is False
    assert any("⚠️" in l for l in lines)


def _scrape(ns, monkeypatch, fetch, driver_ok=True):
    import cave.jobs as jobs
    monkeypatch.setattr(jobs, "_VIVINO_SEL_WORKERS", 2)
    if driver_ok:
        class _D:
            def quit(self): pass
        monkeypatch.setattr(jobs, "make_driver", lambda *a, **k: _D())
    else:
        def _boom(*a, **k): raise RuntimeError("chromium introuvable")
        monkeypatch.setattr(jobs, "make_driver", _boom)
    monkeypatch.setattr(jobs, "fetch_vivino", fetch)
    wines = [{"name": f"Test Vin {c} Bordeaux", "price": 9.0, "ean": f"99{i}", "vintage": None}
             for i, c in enumerate("ABCD")]
    todo = [(w, "Bordeaux") if False else w for w in wines]
    lines = []
    jobs._scrape_vivino_list("vins-test", wines, wines, {}, lambda m, **k: lines.append(m))
    return lines, ns["load_report"]("vivino")


def test_vivino_scrape_logs_each_wine_and_writes_report(ns, monkeypatch):
    def fetch(drv, name, vintage, **k):
        if "Vin A" in name:
            return {"rating": 4.1, "ratings_count": 500, "vivino_url": "u", "vivino_year": 2019,
                    "vintage_match": True, "match_confidence": 0.9}
        if "Vin B" in name:
            return {"rating": 3.9, "ratings_count": 20, "vivino_url": "u", "vivino_year": None,
                    "vintage_match": None, "match_confidence": 0.4}
        if "Vin C" in name:
            raise ValueError("page cassée")
        return {"rating": None, "ratings_count": 0, "vivino_url": "", "vivino_year": None,
                "vintage_match": None, "match_confidence": 0.0}
    lines, rep = _scrape(ns, monkeypatch, fetch)
    text = "\n".join(lines)
    assert "✅" in text and "confiance 90%" in text
    assert "correspondance peu sûre" in text
    assert "page cassée" in text and "❌" in text
    assert "introuvable sur Vivino" in text and "📊 Bilan" in text
    st_ = rep["stats"]
    assert (st_["found"], st_["uncertain"], st_["errors"], st_["not_found"]) == (1, 1, 1, 1)
    assert {r["outcome"] for r in rep["rows"]} == {"found", "uncertain", "error", "not_found"}


def test_vivino_scrape_reports_browser_that_cannot_start(ns, monkeypatch):
    lines, rep = _scrape(ns, monkeypatch, lambda *a, **k: {}, driver_ok=False)
    text = "\n".join(lines)
    assert "impossible à démarrer" in text and "chromium introuvable" in text
    assert rep["stats"]["errors"] == 4          # rien n'est passé sous silence


def test_new_wines_get_a_badge(ns):
    w = {"name": "Vin Neuf", "price": 9.0, "rating": 4.0, "score": 5.0, "first_seen": time.time()}
    assert "Nouveau" in ns["wine_card_html"](w, 1, 5.0)
    assert "Nouveau" not in ns["wine_card_html"](dict(w, first_seen=time.time() - 40 * 86400), 1, 5.0)


def test_update_screen_shows_reports_and_journal(app_test, ns):
    ns["save_report"]("catalogue", {"ok": True, "total": 30, "available": 25, "secs": 4, "first_load": False,
                                     "new": [{"name": "Vin Neuf", "price": 7.5}], "removed": [{"name": "Vin Parti"}],
                                     "back": [], "price_changes": [{"name": "Vin X", "old": 5.0, "new": 6.0}]})
    ns["save_report"]("vivino", {"total": 2, "processed": 2, "secs": 9, "stats": {"found": 1, "uncertain": 1},
                                  "rows": [{"name": "Vin Douteux", "outcome": "uncertain", "rating": 3.9,
                                            "count": 12, "conf": 0.4, "secs": 2.0, "error": "", "url": "u"}]})
    ns["add_event"]("vivino", "✅ évènement visible")
    at = app_test
    _go(at, SCREENS[1])
    _assert_ok(at)
    html = " ".join(m.value for m in at.markdown)
    assert "Rapport du dernier catalogue" in html and "Rapport du dernier scraping Vivino" in html
    assert any("Vin Neuf" in m.value for m in at.markdown)
    assert any("Journal d'activité" in e.label for e in at.expander)


def test_background_job_runs_end_to_end_and_journals(ns, monkeypatch):
    """Régression : _set_job_state référençait un tampon inexistant → le job plantait au démarrage."""
    import cave.jobs as jobs
    monkeypatch.setattr(jobs, "run_fill_missing_vivino", lambda slug, log=None: log("✅ simulé") or [])
    jobs._background_job("vins-rouges", "fill_missing")
    st_ = jobs.load_job_state()
    assert st_["status"] == "done", st_
    msgs = [e["msg"] for e in ns["read_events"](kinds=["vivino"], run=ns["current_run"]())]
    assert any("Début" in m for m in msgs) and "✅ simulé" in msgs and any("Passage terminé" in m for m in msgs)

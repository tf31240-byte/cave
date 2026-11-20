"""Fixtures partagées : app copiée dans un dossier temporaire + données synthétiques.

Aucun accès réseau, aucun navigateur : on teste les fonctions pures et le rendu de l'UI
(streamlit.testing.v1.AppTest) sur un catalogue fictif. Le dossier .cache du dépôt n'est
jamais touché.
"""
import contextlib
import io
import json
import logging
import shutil
import time
from pathlib import Path

import pytest

APP_SRC = Path(__file__).resolve().parent.parent / "streamlit_app.py"


def _synthetic_cache(cache_dir: Path) -> None:
    """Écrit un petit catalogue Leclerc + cache Vivino fictifs (rouge uniquement)."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    names = [
        "Château Margaux 2015 - Margaux AOP", "Château Gazin Pomerol",
        "E.Guigal Côtes du Rhône 2019", "Mouton Cadet Bordeaux", "Domaine Test Alsace",
        "Château Cantelaudette Graves de vayres", "Gérard Bertrand Réserve Corbières",
        "Bouchard Aîné & Fils Bourgogne Pinot Noir", "La Vieille Ferme", "Cuvée Hortense Côtes de Bordeaux",
    ] * 3  # 30 vins → plusieurs pages de cartes
    prices = [3.9, 6.5, 8.9, 12.0, 15.5, 22.0, 35.0, 55.0, 9.9, 18.0]
    wines, vivino = [], {}
    for i, base in enumerate(names):
        name = f"{base} #{i}" if i >= 10 else base       # noms uniques
        ean = str(3000000000000 + i)
        wines.append({"name": name, "price": prices[i % len(prices)], "ean": ean,
                      "url": f"https://www.e.leclerc/fp/{ean}", "image": "",
                      "vintage": (2018 + i % 5) if i % 3 else None, "available": i % 7 != 0})
        if i % 4 != 3:   # 1 vin sur 4 sans note → bouton « Compléter les manquants »
            vivino[name] = {"rating": 3.4 + (i % 12) / 10, "ratings_count": [0, 40, 600, 9000][i % 4],
                            "vivino_url": f"https://www.vivino.com/w/{1000 + i}",
                            "vivino_year": None, "vintage_match": [None, True, False][i % 3],
                            "cached_at": time.time() - i * 3600}
    (cache_dir / "leclerc_vins-rouges.json").write_text(
        json.dumps({"cached_at": time.time(), "slug": "vins-rouges", "wines": wines}, ensure_ascii=False), "utf-8")
    (cache_dir / "vivino_vins-rouges.json").write_text(json.dumps(vivino, ensure_ascii=False), "utf-8")


@pytest.fixture(scope="session")
def app_dir(tmp_path_factory) -> Path:
    """Copie de l'app dans un dossier temporaire avec son .cache synthétique."""
    d = tmp_path_factory.mktemp("cave_app")
    shutil.copy(APP_SRC, d / "streamlit_app.py")
    _synthetic_cache(d / ".cache")
    return d


@pytest.fixture(scope="session")
def ns(app_dir):
    """Espace de noms de l'app après exécution « à nu » : donne accès aux fonctions pures.

    Le script s'arrête de lui-même quand il n'a rien à afficher (st.stop) ; tout ce qui est
    défini avant — c'est-à-dire toutes les fonctions testées ici — reste disponible.
    """
    logging.disable(logging.CRITICAL)
    path = str(app_dir / "streamlit_app.py")
    namespace = {"__name__": "streamlit_app", "__file__": path}
    with contextlib.redirect_stderr(io.StringIO()):
        try:
            exec(compile(Path(path).read_text("utf-8"), path, "exec"), namespace)
        except BaseException:
            pass
    assert "compute_score" in namespace, "l'app n'a pas pu être chargée"
    return namespace


@pytest.fixture()
def app_test(app_dir):
    """AppTest Streamlit déjà exécuté une fois."""
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(app_dir / "streamlit_app.py"), default_timeout=60)
    at.run()
    return at

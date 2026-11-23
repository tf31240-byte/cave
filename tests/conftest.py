"""Fixtures partagées : cache fictif dans un dossier temporaire (variable CAVE_CACHE_DIR).

Aucun accès réseau, aucun navigateur : on teste les fonctions pures et le rendu de l'UI
(streamlit.testing.v1.AppTest) sur un catalogue fictif. Le dossier .cache du dépôt n'est
jamais touché.
"""
import contextlib
import io
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
APP_SRC = ROOT / "streamlit_app.py"
sys.path.insert(0, str(ROOT))


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


# Le dossier de cache est fixé AVANT le premier import du paquet `cave`.
_CACHE = Path(tempfile.mkdtemp(prefix="cave_cache_"))
os.environ["CAVE_CACHE_DIR"] = str(_CACHE)
_synthetic_cache(_CACHE)


@pytest.fixture(scope="session")
def ns():
    """Espace de noms regroupant les fonctions de tous les modules de `cave` (lecture seule)."""
    logging.disable(logging.CRITICAL)
    import importlib
    merged = {}
    for name in ("config", "regions", "storage", "vivino", "browser", "leclerc", "scoring",
                 "activity", "jobs", "render"):
        merged.update(vars(importlib.import_module(f"cave.{name}")))
    assert "compute_score" in merged
    return merged


@pytest.fixture()
def app_test():
    """AppTest Streamlit déjà exécuté une fois (écran Classement)."""
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(APP_SRC), default_timeout=60)
    at.run()
    return at

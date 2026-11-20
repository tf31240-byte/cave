# 🍷 Cave Leclerc Blagnac × Vivino

Comparateur des vins en rayon chez **Leclerc Blagnac** (magasin 1431), enrichis avec les notes
et le nombre d'avis **Vivino**, classés par un **score qualité / prix**.

Application [Streamlit](https://streamlit.io) : le catalogue Leclerc est lu avec Selenium
(Chromium headless), les notes Vivino via leur API puis, en repli, par navigation.
Tout est mis en cache sur disque : l'app s'ouvre instantanément et un scraping interrompu peut
être repris.

---

## ⚡ Installation

```bash
pip install -r requirements.txt
# Chromium + chromedriver (Debian/Ubuntu) — déjà listés dans packages.txt pour Streamlit Cloud
sudo apt install chromium chromium-driver

streamlit run streamlit_app.py
```

Au premier lancement, ouvrir le menu latéral et cliquer sur **🔄 Mettre à jour le catalogue**,
puis **🍷 Rafraîchir les notes Vivino**. Les données sont ensuite conservées dans `.cache/`.

> Streamlit **≥ 1.50** est requis (paramètre `width="stretch"` des boutons, tableaux et graphiques).

---

## 🔐 Sauvegarde Gist (facultative)

Sur un hébergement dont le disque est éphémère (Streamlit Community Cloud), les caches peuvent
être sauvegardés dans un **Gist GitHub privé**. Sans configuration, l'app fonctionne normalement
et garde simplement ses données en local.

1. Copier `.streamlit/secrets.toml.example` en `.streamlit/secrets.toml` (ignoré par git).
2. Renseigner `github_token` (scope `gist`). Laisser `gist_id` vide au premier lancement, puis
   y recopier l'identifiant du Gist créé automatiquement.

Sur Streamlit Cloud, coller le même contenu dans *Settings → Secrets*.
**Ne jamais commiter `secrets.toml`.**

---

## 🧮 Score

`score = note × confiance(nb d'avis) / log(1 + prix) × 4`, avec un petit bonus si le millésime
Vivino correspond à celui de Leclerc. Une note dont le nombre d'avis est inconnu reçoit une
confiance de 0,2 (≈ 400 avis) : elle reste derrière les vins bien documentés. Le `log(1 + prix)` évite qu'un vin à 4 € écrase
systématiquement un excellent vin à 25 € (explication détaillée dans `compute_score`).

---

## 🧪 Tests

```bash
pip install -r requirements-dev.txt
pytest tests -q
```

Les tests n'utilisent ni réseau ni navigateur : fonctions pures (score, régions, requêtes Vivino,
fusion des caches) et rendu complet de l'interface (`streamlit.testing`) sur un catalogue
fictif. Ils tournent aussi sur GitHub Actions à chaque push.

---

## 🗂 Structure

```
├── streamlit_app.py            # Application (scraping, caches, score, interface)
├── requirements.txt            # Dépendances d'exécution
├── requirements-dev.txt        # + pytest
├── packages.txt                # Paquets système (Chromium) pour Streamlit Cloud
├── tests/                      # Suite pytest
├── .streamlit/secrets.toml.example
├── .github/workflows/tests.yml # CI
├── leclerc_vins-rouges.json    # Export de départ (ancien format de cache)
└── app.py, index.html          # Ancienne version Flask/Playwright (historique, non utilisée)
```

Le dossier `.cache/` (catalogues, notes Vivino, historique des prix, état du scraping) est généré
à l'exécution et ignoré par git.

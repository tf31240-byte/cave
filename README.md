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

## ⚡ Rapidité du scraping

- Chromium charge en mode `eager`, sans images ni polices (on ne lit que du texte / JSON-LD).
- Pauses fixes réduites : on n'attend plus qu'un élément précis (carte produit, JSON-LD).
- Vin dont l'URL Vivino est déjà connue mais sans note : lecture directe de la page du vin
  (1 navigation au lieu de 2).
- Un rafraîchissement ignore les vins déjà notés et non obsolètes (> 30 jours).
- Chaque navigateur est réutilisé pour tout son lot ; le journal affiche le temps par vin (`⏱️`).
- Le nombre de navigateurs parallèles (`_VIVINO_SEL_WORKERS`, 3 par défaut) peut être augmenté,
  mais Vivino répond par des 429/403 si on va trop vite.

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

## 🖥️ Interface

Trois écrans, choisis dans la barre latérale :

| Écran | Rôle |
|---|---|
| 🏅 **Classement** | Recherche, tri et filtres en haut ; vues *Liste*, *Bonnes affaires*, *Statistiques*, *Tableau & export*. Sous chaque vin, « 🚫 Ce n'est pas le bon vin sur Vivino ? » écarte un lien erroné. |
| 🔄 **Mise à jour** | Étape 1 : catalogue Leclerc. Étape 2 : notes Vivino (*Compléter les manquants*, *Rafraîchir les obsolètes*, *Tout refaire*). Un scraping interrompu se **reprend** là où il s'est arrêté ; journal en direct. |
| ⚙️ **Réglages** | Sauvegarde Gist, correction manuelle d'une note, liens rejetés, cache Vivino. |

Les données déjà acquises (`.cache/`) sont conservées et reprises telles quelles.

## 🔎 Détection des vins sur Vivino

- Titre de carte nettoyé (notes, nombre d'avis et prix retirés) et comparé aussi au *slug* de l'URL.
- Millésime lu dans l'URL (`?year=`), abréviations développées (`St-Émilion`, `Ch.`, `Dom.`).
- Jusqu'à 3 recherches par vin ; on garde la **meilleure** correspondance, pas la première.
- Correspondance peu sûre (< 0,60) : badge « 🔎 À vérifier » sur la carte.

---

## 🗂 Structure

```
├── streamlit_app.py     # Point d'entrée (met en page, charge le style, lance l'interface)
├── cave/
│   ├── config.py        # Constantes, tris, chemins (CAVE_CACHE_DIR pour changer .cache/)
│   ├── storage.py       # Caches disque, Gist, checkpoint de reprise, rejets, prix
│   ├── regions.py       # Régions, appellations, comparaison floue
│   ├── vivino.py        # Requêtes, candidats, choix du meilleur, lecture des pages
│   ├── browser.py       # Chromium headless (mode rapide pour Vivino)
│   ├── leclerc.py       # Lecture du catalogue Leclerc
│   ├── scoring.py       # Score qualité/prix, fusion Leclerc + Vivino
│   ├── jobs.py          # Tâches de fond, mises à jour, reprise
│   ├── render.py        # Cartes, tableaux, console
│   ├── style.py         # Feuille de style
│   └── screens.py       # Les 3 écrans
├── tests/               # Suite pytest (sans réseau)
├── requirements.txt · requirements-dev.txt · packages.txt
├── .streamlit/secrets.toml.example
└── app.py, index.html   # Ancienne version Flask/Playwright (historique, non utilisée)
```

Le dossier `.cache/` (catalogues, notes Vivino, historique des prix, état du scraping) est généré
à l'exécution et ignoré par git.

## Savoir ce qui se passe

L'écran **Mise à jour** montre tout ce que fait l'app :

- **En direct** pendant un scraping Vivino : compteurs (notés / à vérifier / introuvables / erreurs), temps restant, et journal détaillé (un vin = une ligne : note, nombre d'avis, confiance, millésime, durée).
- **Rapport du dernier catalogue** : vins ajoutés ➕, retirés ➖, revenus 🔄 et changements de prix 💶 (et un toast de bilan à la fin).
- **Rapport du dernier scraping Vivino** : tableau filtrable (à vérifier, introuvables, erreurs) avec lien Vivino.
- **Journal d'activité complet** : conservé entre les sessions (`.cache/activity.jsonl`), filtrable par type, niveau et texte, téléchargeable.
- Les erreurs (navigateur qui ne démarre pas, page illisible…) sont désormais écrites dans le journal au lieu d'être silencieuses.
- Les vins apparus depuis moins de 14 jours portent le badge « 🆕 Nouveau ».

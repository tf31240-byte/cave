# 🍷 Cave Leclerc Blagnac × Vivino

Comparateur de vins rouges disponibles chez **Leclerc Blagnac** enrichis avec les notes **Vivino**.  
Classe automatiquement les vins par **ratio qualité/prix** (note Vivino ÷ prix × 10).

---

## ⚡ Installation rapide

```bash
# 1. Installer les dépendances Python
pip install -r requirements.txt

# 2. Installer Chromium pour Playwright
playwright install chromium

# 3. Lancer le serveur
python app.py
```

Puis ouvrir **http://localhost:5000** dans votre navigateur.

---

## 📖 Comment ça marche

### 1. Scraping Leclerc (Playwright)
- Playwright lance un **navigateur Chromium headless** et navigue vers la page Leclerc
- L'URL `?oaf-sign-code=1431` filtre automatiquement sur le **magasin de Blagnac**
- Le navigateur exécute le JavaScript Angular et **intercepte les réponses API** du catalogue
- Si l'API n'est pas interceptée, fallback sur le **scraping DOM** des cartes produits

### 2. Enrichissement Vivino
- Pour chaque vin, une requête est faite à l'**API Vivino** (endpoint public)
- On récupère : note moyenne, nombre d'avis, URL Vivino
- Un délai de 300ms entre chaque requête pour respecter le rate limit

### 3. Calcul du ratio
```
ratio = (note_vivino / prix_en_euros) × 10
```
Plus le ratio est élevé, meilleur est le rapport qualité/prix.  
Exemple : un vin à 3.9★ pour 8€ a un ratio de 4.88, bien meilleur qu'un 4.2★ à 45€ (ratio 0.93).

---

## 🔧 Configuration

Dans `app.py`, vous pouvez modifier :
- `STORE_CODE = "1431"` — code du magasin Leclerc
- `max_pages` dans `scrape_leclerc_wines()` — nombre de pages à scraper

---

## 🗂 Structure

```
cave-leclerc/
├── app.py                 # Backend Flask + scraper + API Vivino
├── templates/
│   └── index.html         # Interface web
├── requirements.txt
└── README.md
```

---

## ⚠️ Notes importantes

- Le scraping peut échouer si Leclerc change sa structure HTML → ouvrir une issue
- En cas d'échec, l'app affiche des **données de démonstration** pour tester l'interface
- Les données sont **mises en cache** en mémoire jusqu'au clic sur "Rafraîchir"
- Vivino peut limiter les requêtes : en cas de 429, attendre quelques minutes

---

## 🆚 Fonctionnalités

| Feature | Statut |
|---------|--------|
| Scraping Leclerc Blagnac | ✅ |
| Notes Vivino automatiques | ✅ |
| Classement ratio ★/€ | ✅ |
| Filtres prix / note / recherche | ✅ |
| Tri multi-critères | ✅ |
| Cache en mémoire | ✅ |
| Vins blancs / rosés | 🔜 |
| Export CSV | 🔜 |
| Historique des prix | 🔜 |

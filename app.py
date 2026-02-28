<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Cave Leclerc Blagnac × Vivino</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=DM+Mono:wght@300;400&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet" />

  <style>
    :root {
      --burgundy: #6B1A2A;
      --deep:     #1A0810;
      --garnet:   #9B2C3E;
      --gold:     #C9A84C;
      --cream:    #F5EDD8;
      --pale:     #FAF5EC;
      --text:     #2A1018;
      --muted:    #8B6B72;
      --glass:    rgba(107, 26, 42, 0.08);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: 'DM Sans', sans-serif;
      background: var(--pale);
      color: var(--text);
      min-height: 100vh;
    }

    /* ── Header ─────────────────────────────────────────────── */
    header {
      background: var(--deep);
      padding: 2.5rem 3rem;
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 2rem;
      position: sticky;
      top: 0;
      z-index: 100;
      border-bottom: 1px solid rgba(201,168,76,.3);
    }

    .header-left h1 {
      font-family: 'Playfair Display', serif;
      font-size: 2rem;
      font-weight: 900;
      color: var(--cream);
      letter-spacing: -0.02em;
    }

    .header-left h1 span {
      color: var(--gold);
    }

    .header-left p {
      color: var(--muted);
      font-size: 0.8rem;
      font-weight: 300;
      margin-top: 0.25rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .header-right {
      display: flex;
      align-items: center;
      gap: 1rem;
    }

    .btn {
      padding: 0.6rem 1.4rem;
      border-radius: 3px;
      font-family: 'DM Mono', monospace;
      font-size: 0.75rem;
      cursor: pointer;
      transition: all .2s;
      border: none;
    }

    .btn-primary {
      background: var(--gold);
      color: var(--deep);
      font-weight: 400;
    }

    .btn-primary:hover { background: #d4b258; transform: translateY(-1px); }

    .btn-ghost {
      background: transparent;
      color: var(--cream);
      border: 1px solid rgba(201,168,76,.4);
    }

    .btn-ghost:hover { border-color: var(--gold); color: var(--gold); }

    /* ── Controls bar ───────────────────────────────────────── */
    .controls {
      background: white;
      padding: 1rem 3rem;
      display: flex;
      align-items: center;
      gap: 1.5rem;
      border-bottom: 1px solid rgba(107,26,42,.1);
      flex-wrap: wrap;
    }

    .control-group {
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .control-group label {
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--muted);
      font-weight: 500;
    }

    select, input[type="range"], input[type="text"] {
      border: 1px solid rgba(107,26,42,.15);
      border-radius: 3px;
      padding: 0.4rem 0.8rem;
      font-family: 'DM Sans', sans-serif;
      font-size: 0.8rem;
      color: var(--text);
      background: var(--pale);
      outline: none;
    }

    select:focus, input:focus { border-color: var(--garnet); }

    .search-input {
      width: 220px;
    }

    .stats-bar {
      margin-left: auto;
      display: flex;
      gap: 2rem;
    }

    .stat {
      text-align: right;
    }

    .stat-value {
      font-family: 'DM Mono', monospace;
      font-size: 1.2rem;
      font-weight: 400;
      color: var(--burgundy);
    }

    .stat-label {
      font-size: 0.65rem;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--muted);
    }

    /* ── Loading ────────────────────────────────────────────── */
    #loading {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 8rem 2rem;
      gap: 2rem;
    }

    .wine-loader {
      width: 60px;
      height: 80px;
      position: relative;
    }

    .bottle {
      width: 30px;
      height: 70px;
      background: var(--burgundy);
      border-radius: 4px 4px 8px 8px;
      margin: 0 auto;
      position: relative;
      overflow: hidden;
    }

    .bottle::before {
      content: '';
      position: absolute;
      top: -12px;
      left: 50%;
      transform: translateX(-50%);
      width: 10px;
      height: 14px;
      background: var(--burgundy);
      border-radius: 2px;
    }

    .wine-fill {
      position: absolute;
      bottom: 0;
      left: 0;
      right: 0;
      background: var(--garnet);
      animation: fill 2s ease-in-out infinite;
    }

    @keyframes fill {
      0%, 100% { height: 20%; }
      50% { height: 80%; }
    }

    .loading-text {
      font-family: 'DM Mono', monospace;
      font-size: 0.8rem;
      color: var(--muted);
      letter-spacing: 0.1em;
    }

    .loading-steps {
      list-style: none;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }

    .loading-steps li {
      font-size: 0.75rem;
      color: var(--muted);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .loading-steps li.active { color: var(--burgundy); }
    .loading-steps li.done { color: #4a7c59; }

    .step-icon { font-size: 1rem; }

    /* ── Table ──────────────────────────────────────────────── */
    .table-container {
      padding: 2rem 3rem;
    }

    .table-wrapper {
      background: white;
      border-radius: 6px;
      overflow: hidden;
      box-shadow: 0 2px 20px rgba(26,8,16,.06);
    }

    table {
      width: 100%;
      border-collapse: collapse;
    }

    thead {
      background: var(--deep);
    }

    th {
      padding: 1rem 1.2rem;
      text-align: left;
      font-family: 'DM Mono', monospace;
      font-size: 0.65rem;
      font-weight: 400;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--gold);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }

    th:hover { color: var(--cream); }
    th.sorted { color: white; }
    th.sorted::after { content: ' ↕'; }

    tbody tr {
      border-bottom: 1px solid rgba(107,26,42,.07);
      transition: background .15s;
    }

    tbody tr:hover { background: var(--glass); }

    tbody tr.top-3 { background: rgba(201,168,76,.05); }
    tbody tr.top-3:hover { background: rgba(201,168,76,.1); }

    td {
      padding: 0.85rem 1.2rem;
      vertical-align: middle;
    }

    /* Rank column */
    .rank {
      font-family: 'DM Mono', monospace;
      font-size: 0.9rem;
      color: var(--muted);
      text-align: center;
    }

    .rank.gold { color: var(--gold); font-weight: 400; }
    .rank.silver { color: #9C9C9C; }
    .rank.bronze { color: #CD7F32; }

    /* Wine name */
    .wine-name {
      font-weight: 500;
      font-size: 0.9rem;
      line-height: 1.3;
    }

    .wine-name a {
      color: var(--text);
      text-decoration: none;
    }

    .wine-name a:hover { color: var(--burgundy); }

    .wine-vivino-name {
      font-size: 0.72rem;
      color: var(--muted);
      margin-top: 0.2rem;
      font-style: italic;
    }

    /* Price */
    .price {
      font-family: 'DM Mono', monospace;
      font-size: 0.95rem;
      color: var(--text);
      white-space: nowrap;
    }

    /* Rating stars */
    .rating-cell {
      white-space: nowrap;
    }

    .stars {
      display: inline-flex;
      gap: 1px;
    }

    .star {
      font-size: 0.8rem;
    }

    .star.full  { color: var(--gold); }
    .star.half  { color: var(--gold); opacity: 0.6; }
    .star.empty { color: #ddd; }

    .rating-num {
      font-family: 'DM Mono', monospace;
      font-size: 0.85rem;
      font-weight: 400;
      color: var(--text);
      margin-left: 0.4rem;
    }

    .ratings-count {
      font-size: 0.68rem;
      color: var(--muted);
      display: block;
      margin-top: 0.1rem;
    }

    .no-rating {
      font-size: 0.72rem;
      color: #ccc;
      font-style: italic;
    }

    /* Ratio bar */
    .ratio-cell {
      min-width: 140px;
    }

    .ratio-bar-wrap {
      display: flex;
      align-items: center;
      gap: 0.6rem;
    }

    .ratio-bar {
      flex: 1;
      height: 6px;
      background: rgba(107,26,42,.1);
      border-radius: 3px;
      overflow: hidden;
    }

    .ratio-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--garnet), var(--gold));
      border-radius: 3px;
      transition: width .4s ease;
    }

    .ratio-num {
      font-family: 'DM Mono', monospace;
      font-size: 0.78rem;
      color: var(--burgundy);
      white-space: nowrap;
    }

    /* Badge */
    .badge {
      display: inline-block;
      padding: 0.2rem 0.55rem;
      border-radius: 2px;
      font-size: 0.62rem;
      font-family: 'DM Mono', monospace;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .badge-deal {
      background: rgba(201,168,76,.15);
      color: var(--gold);
      border: 1px solid rgba(201,168,76,.3);
    }

    .badge-toprated {
      background: rgba(107,26,42,.08);
      color: var(--burgundy);
      border: 1px solid rgba(107,26,42,.2);
    }

    /* Empty / error */
    #error-msg {
      display: none;
      text-align: center;
      padding: 4rem;
      color: var(--muted);
    }

    /* Responsive */
    @media (max-width: 900px) {
      header, .controls, .table-container { padding-left: 1.2rem; padding-right: 1.2rem; }
      .header-left h1 { font-size: 1.4rem; }
      .stats-bar { display: none; }
      th, td { padding: 0.7rem 0.8rem; }
    }

    /* Tooltip */
    [data-tip] {
      position: relative;
      cursor: help;
    }

    [data-tip]::after {
      content: attr(data-tip);
      position: absolute;
      bottom: 120%;
      left: 50%;
      transform: translateX(-50%);
      background: var(--deep);
      color: var(--cream);
      padding: 0.4rem 0.7rem;
      border-radius: 4px;
      font-size: 0.7rem;
      white-space: nowrap;
      opacity: 0;
      pointer-events: none;
      transition: opacity .2s;
    }

    [data-tip]:hover::after { opacity: 1; }

    /* Animate rows */
    @keyframes fadeInUp {
      from { opacity: 0; transform: translateY(10px); }
      to   { opacity: 1; transform: translateY(0); }
    }

    tbody tr { animation: fadeInUp .3s ease both; }

    /* Refresh spinner */
    .spin { animation: spin 1s linear infinite; display: inline-block; }
    @keyframes spin { to { transform: rotate(360deg); } }
  </style>
</head>
<body>

<header>
  <div class="header-left">
    <h1>Cave <span>Leclerc Blagnac</span> × Vivino</h1>
    <p>Comparateur qualité / prix — vins rouges</p>
  </div>
  <div class="header-right">
    <button class="btn btn-ghost" onclick="refreshData()">
      <span id="refresh-icon">↻</span> Rafraîchir
    </button>
  </div>
</header>

<div class="controls">
  <div class="control-group">
    <label>Recherche</label>
    <input type="text" class="search-input" id="search" placeholder="Ex: Bordeaux, Côtes du Rhône…" oninput="applyFilters()" />
  </div>

  <div class="control-group">
    <label>Trier par</label>
    <select id="sort-by" onchange="applyFilters()">
      <option value="ratio">Meilleur ratio ★/€</option>
      <option value="rating">Meilleure note</option>
      <option value="price_asc">Prix croissant</option>
      <option value="price_desc">Prix décroissant</option>
    </select>
  </div>

  <div class="control-group">
    <label>Prix max</label>
    <input type="range" id="price-max" min="0" max="200" value="200" oninput="updatePriceLabel(); applyFilters()" />
    <span id="price-label" style="font-family:'DM Mono';font-size:.8rem;color:var(--burgundy);min-width:50px">200 €</span>
  </div>

  <div class="control-group">
    <label>Note min</label>
    <select id="rating-min" onchange="applyFilters()">
      <option value="0">Toutes</option>
      <option value="3.5">≥ 3.5 ★</option>
      <option value="3.8">≥ 3.8 ★</option>
      <option value="4.0">≥ 4.0 ★</option>
      <option value="4.2">≥ 4.2 ★</option>
    </select>
  </div>

  <div class="stats-bar">
    <div class="stat">
      <div class="stat-value" id="stat-count">—</div>
      <div class="stat-label">Vins affichés</div>
    </div>
    <div class="stat">
      <div class="stat-value" id="stat-avg-price">—</div>
      <div class="stat-label">Prix moyen</div>
    </div>
    <div class="stat">
      <div class="stat-value" id="stat-avg-rating">—</div>
      <div class="stat-label">Note moy. Vivino</div>
    </div>
  </div>
</div>

<!-- Loading -->
<div id="loading">
  <div class="wine-loader">
    <div class="bottle"><div class="wine-fill"></div></div>
  </div>
  <div class="loading-text">Chargement en cours…</div>
  <ul class="loading-steps">
    <li id="step1" class="active"><span class="step-icon">🔍</span> Scraping Leclerc Blagnac…</li>
    <li id="step2"><span class="step-icon">🍷</span> Interrogation Vivino…</li>
    <li id="step3"><span class="step-icon">📊</span> Calcul des ratios qualité/prix…</li>
  </ul>
</div>

<!-- Error -->
<div id="error-msg">
  <h2>❌ Impossible de récupérer les données</h2>
  <p>Vérifiez que le serveur Flask tourne et que Playwright est installé.</p>
</div>

<!-- Table -->
<div class="table-container" id="table-section" style="display:none">
  <div class="table-wrapper">
    <table>
      <thead>
        <tr>
          <th style="width:50px; text-align:center">#</th>
          <th>Vin</th>
          <th data-col="price" onclick="sortTable('price_asc')">Prix</th>
          <th data-col="rating" onclick="sortTable('rating')">Note Vivino</th>
          <th data-col="ratio" onclick="sortTable('ratio')"
              data-tip="Note ÷ Prix × 10 — Plus c'est élevé, meilleur le rapport qualité/prix">
            Ratio ★/€
          </th>
          <th>Badges</th>
        </tr>
      </thead>
      <tbody id="wine-list"></tbody>
    </table>
  </div>
</div>


<script>
let allWines = [];
let maxRatio = 0;

// ── Load data ──────────────────────────────────────────────
async function loadWines() {
  document.getElementById('loading').style.display = 'flex';
  document.getElementById('table-section').style.display = 'none';
  document.getElementById('error-msg').style.display = 'none';

  // Animate loading steps
  animateSteps();

  try {
    const res = await fetch('/api/wines');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    allWines = await res.json();
    maxRatio = Math.max(...allWines.map(w => w.ratio || 0));

    document.getElementById('loading').style.display = 'none';
    document.getElementById('table-section').style.display = 'block';
    applyFilters();
  } catch (e) {
    console.error(e);
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error-msg').style.display = 'block';
  }
}

function animateSteps() {
  const steps = ['step1', 'step2', 'step3'];
  steps.forEach((s, i) => {
    document.getElementById(s).className = '';
    setTimeout(() => document.getElementById(s).className = 'active', i * 3000);
  });
}

async function refreshData() {
  const icon = document.getElementById('refresh-icon');
  icon.className = 'spin';
  await fetch('/api/refresh');
  allWines = [];
  await loadWines();
  icon.className = '';
}

// ── Filters & Sort ─────────────────────────────────────────
function applyFilters() {
  const search = document.getElementById('search').value.toLowerCase();
  const sortBy = document.getElementById('sort-by').value;
  const priceMax = parseFloat(document.getElementById('price-max').value);
  const ratingMin = parseFloat(document.getElementById('rating-min').value);

  let filtered = allWines.filter(w => {
    if (w.price > priceMax) return false;
    if (ratingMin > 0 && (!w.rating || w.rating < ratingMin)) return false;
    if (search && !w.name.toLowerCase().includes(search) &&
        !(w.vivino_name || '').toLowerCase().includes(search)) return false;
    return true;
  });

  if (sortBy === 'ratio')       filtered.sort((a,b) => (b.ratio||0)-(a.ratio||0));
  else if (sortBy === 'rating') filtered.sort((a,b) => (b.rating||0)-(a.rating||0));
  else if (sortBy === 'price_asc') filtered.sort((a,b) => a.price-b.price);
  else if (sortBy === 'price_desc') filtered.sort((a,b) => b.price-a.price);

  renderTable(filtered);
  updateStats(filtered);
}

function sortTable(col) {
  document.getElementById('sort-by').value = col;
  applyFilters();
}

// ── Render ─────────────────────────────────────────────────
function renderTable(wines) {
  const tbody = document.getElementById('wine-list');
  tbody.innerHTML = '';

  wines.forEach((w, i) => {
    const tr = document.createElement('tr');
    if (i < 3) tr.classList.add('top-3');

    const rankClass = i === 0 ? 'gold' : i === 1 ? 'silver' : i === 2 ? 'bronze' : '';
    const rankLabel = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : (i+1);

    // Stars
    const ratingHTML = w.rating
      ? `<div class="rating-cell">
           <span class="stars">${buildStars(w.rating)}</span>
           <span class="rating-num">${w.rating.toFixed(2)}</span>
           <span class="ratings-count">${(w.ratings_count||0).toLocaleString('fr')} avis</span>
         </div>`
      : `<span class="no-rating">non trouvé</span>`;

    // Ratio bar
    const pct = maxRatio > 0 ? Math.min(100, (w.ratio / maxRatio) * 100) : 0;
    const ratioHTML = w.ratio
      ? `<div class="ratio-bar-wrap">
           <div class="ratio-bar"><div class="ratio-fill" style="width:${pct}%"></div></div>
           <span class="ratio-num">${w.ratio.toFixed(2)}</span>
         </div>`
      : `<span class="no-rating">—</span>`;

    // Badges
    const badges = [];
    if (w.ratio > 0 && i < 5) badges.push(`<span class="badge badge-deal">🔥 Top ratio</span>`);
    if (w.rating >= 4.2)       badges.push(`<span class="badge badge-toprated">★ Top noté</span>`);

    // Wine name with link
    const nameHTML = `
      <div class="wine-name">
        ${w.url ? `<a href="${w.url}" target="_blank">${w.name}</a>` : w.name}
      </div>
      ${w.vivino_name && w.vivino_name !== w.name
        ? `<div class="wine-vivino-name">Vivino: ${w.vivino_name} ${w.vivino_vintage || ''}</div>`
        : ''}
    `;

    tr.innerHTML = `
      <td class="rank ${rankClass}">${rankLabel}</td>
      <td>${nameHTML}</td>
      <td class="price">${w.price.toFixed(2).replace('.',',')} €</td>
      <td>${ratingHTML}</td>
      <td class="ratio-cell">${ratioHTML}</td>
      <td>${badges.join(' ')}</td>
    `;

    // Animate with delay
    tr.style.animationDelay = `${i * 30}ms`;
    tbody.appendChild(tr);
  });
}

function buildStars(rating) {
  let html = '';
  for (let i = 1; i <= 5; i++) {
    if (rating >= i)           html += '<span class="star full">★</span>';
    else if (rating >= i-0.5) html += '<span class="star half">★</span>';
    else                       html += '<span class="star empty">★</span>';
  }
  return html;
}

// ── Stats ──────────────────────────────────────────────────
function updateStats(wines) {
  document.getElementById('stat-count').textContent = wines.length;

  const withPrice = wines.filter(w => w.price > 0);
  if (withPrice.length) {
    const avg = withPrice.reduce((s, w) => s + w.price, 0) / withPrice.length;
    document.getElementById('stat-avg-price').textContent = avg.toFixed(2).replace('.', ',') + ' €';
  }

  const withRating = wines.filter(w => w.rating);
  if (withRating.length) {
    const avg = withRating.reduce((s, w) => s + w.rating, 0) / withRating.length;
    document.getElementById('stat-avg-rating').textContent = '★ ' + avg.toFixed(2);
  }
}

function updatePriceLabel() {
  const v = document.getElementById('price-max').value;
  document.getElementById('price-label').textContent = v == 200 ? '∞' : v + ' €';
}

// ── Init ───────────────────────────────────────────────────
loadWines();
</script>
</body>
</html>

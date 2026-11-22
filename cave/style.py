"""Feuille de style de l'application (thème bordeaux / or)."""

CSS = r'''
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,700;0,900;1,700&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

/* ═══════════════ VARIABLES ═══════════════ */
:root{
  --bx:#6B1A2A;--bx-light:#8B2A3A;--gold:#C9A84C;--gold-light:#E5C97A;
  --ink:#1A0810;--muted:#8B6B72;--border:#EDE4E8;
  --card:#FFFFFF;--page:#FAF5F2;
  --sh:0 1px 3px rgba(26,8,16,.04),0 4px 16px rgba(26,8,16,.07);
  --sh-hover:0 6px 20px rgba(26,8,16,.11),0 16px 40px rgba(26,8,16,.06);
  --r:14px;
}

/* ═══════════════ CHROME STREAMLIT ═══════════════ */
.stApp,[data-testid="stAppViewContainer"]{background:var(--page) !important}
/* overflow-x géré par élément, pas globalement (cacherait la sidebar) */
[data-testid="stHeader"]{background:var(--page) !important}
footer{display:none !important}
/* #MainMenu conservé — nécessaire pour ouvrir la sidebar */
/* stToolbar conservé — contient le toggle sidebar */
[data-testid="stMainBlockContainer"]{padding-top:4rem !important}


/* ═══════════════ SIDEBAR ═══════════════ */
[data-testid="stSidebar"]{
  background:linear-gradient(175deg,#20080F 0%,#2E1018 50%,#1A0810 100%) !important;
  border-right:1px solid rgba(201,168,76,.18) !important}
[data-testid="stSidebar"] *{color:#E8D0D5 !important}
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3{color:#F5E6E8 !important;letter-spacing:.02em}
[data-testid="stSidebar"] .stMarkdown p{color:#C8A8B0 !important;font-size:.82rem}
[data-testid="stSidebar"] [data-testid="stSelectbox"] > div > div{
  background:rgba(255,255,255,.06) !important;
  border-color:rgba(201,168,76,.25) !important;color:#F5E6E8 !important;
  border-radius:8px !important}
[data-testid="stSidebar"] .stButton button{
  border-radius:8px !important;font-weight:500 !important;
  font-size:.8rem !important;transition:all .15s !important}
[data-testid="stSidebar"] .stButton button[kind="primary"]{
  background:var(--gold) !important;border:none !important;
  color:#1A0810 !important;font-weight:700 !important}
[data-testid="stSidebar"] .stButton button[kind="primary"]:hover{
  background:var(--gold-light) !important;transform:translateY(-1px) !important;
  box-shadow:0 4px 12px rgba(201,168,76,.35) !important}
[data-testid="stSidebar"] .stButton button[kind="secondary"]{
  background:rgba(255,255,255,.07) !important;
  border-color:rgba(232,208,213,.2) !important;color:#E8D0D5 !important}
[data-testid="stSidebar"] .stButton button[kind="secondary"]:hover{
  background:rgba(255,255,255,.12) !important}
[data-testid="stSidebar"] .stCheckbox label span{color:#C8A8B0 !important;font-size:.78rem}
[data-testid="stSidebar"] hr{border-color:rgba(201,168,76,.18) !important;margin:.7rem 0 !important}
[data-testid="stSidebar"] .stCaption{color:#A88090 !important}
[data-testid="stSidebar"] .stCaption strong{color:#E8D0D5 !important}
/* Champs de saisie : fond clair → texte foncé. La règle « * » plus haut rendait le texte
   rose clair sur fond clair (contraste 1.28:1, illisible) pour Prix min/max, Recherche, etc. */
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] textarea{
  color:#1A0810 !important;-webkit-text-fill-color:#1A0810 !important}

/* ═══════════════ EN-TÊTE PAGE ═══════════════ */
.main-title{
  font-family:'Playfair Display',serif;
  font-size:1.5rem;
  font-weight:900;color:var(--ink);line-height:1.2;letter-spacing:-.02em;
  word-break:break-word;white-space:normal}
.main-title span{color:var(--gold)}
.subtitle{
  color:var(--muted);font-size:.74rem;letter-spacing:.14em;
  text-transform:uppercase;margin-top:.25rem;margin-bottom:.1rem}
.title-rule{
  display:block;height:2px;width:3.5rem;
  background:linear-gradient(90deg,var(--gold),transparent);
  border:none;margin:.55rem 0 .8rem;border-radius:2px}

/* ═══════════════ MÉTRIQUES STREAMLIT ═══════════════ */
[data-testid="stMetric"]{
  background:var(--card);border-radius:12px;
  padding:.65rem .9rem;
  box-shadow:0 1px 3px rgba(26,8,16,.05),0 2px 8px rgba(26,8,16,.06);
  border:1px solid var(--border)}
[data-testid="stMetricLabel"]{
  font-size:.65rem !important;letter-spacing:.08em;
  text-transform:uppercase;color:var(--muted) !important;font-weight:600}
[data-testid="stMetricValue"]{
  font-family:'DM Mono',monospace;font-size:1.15rem !important;
  font-weight:700;color:var(--ink) !important;
  font-feature-settings:"tnum"}

/* ═══════════════ CARTE VIN ═══════════════ */
.wine-card{
  background:var(--card);border-radius:var(--r);
  padding:.9rem 1.1rem .9rem 1rem;margin-bottom:.35rem;
  border-left:3px solid var(--bx);box-shadow:var(--sh);
  display:grid;
  grid-template-columns:2.4rem 1fr 6rem 5.2rem 7.2rem;
  align-items:center;gap:.8rem;
  transition:box-shadow .18s,transform .18s,border-left-color .18s;
  position:relative}
.wine-card:hover{
  box-shadow:var(--sh-hover);transform:translateY(-1px);
  border-left-color:var(--bx-light)}

/* Podium */
.wine-card.top1{
  border-left:4px solid var(--gold);
  background:linear-gradient(108deg,#FFFBF0 0%,var(--card) 55%);
  box-shadow:0 2px 12px rgba(201,168,76,.12),0 6px 24px rgba(201,168,76,.06)}
.wine-card.top2{
  border-left:4px solid #B4B4B4;
  background:linear-gradient(108deg,#F5F5F7 0%,var(--card) 55%)}
.wine-card.top3{
  border-left:4px solid #C47540;
  background:linear-gradient(108deg,#FCF1E9 0%,var(--card) 55%)}
.wine-card.vintage-warn{box-shadow:var(--sh),inset 0 0 0 1px rgba(245,158,11,.3)}
.wine-card.unavailable{opacity:.28;filter:grayscale(55%)}
.wine-card.stale{border-left-style:dashed}

/* ── Rang ── */
.wine-rank{
  font-family:'DM Mono',monospace;font-size:1.1rem;
  text-align:center;line-height:1;flex-shrink:0;
  color:var(--muted)}

/* ── Info (col 2) ── */
.wine-info{min-width:0;position:relative}
.wine-bottle{
  position:absolute;right:0;top:50%;transform:translateY(-50%);
  width:32px;height:50px;object-fit:contain;
  border-radius:3px;opacity:.8;pointer-events:none}
.wine-info.has-img{padding-right:42px}
.wine-name{
  font-weight:600;font-size:.875rem;color:var(--ink);
  display:-webkit-box;-webkit-line-clamp:2;
  -webkit-box-orient:vertical;overflow:hidden;line-height:1.44}
.wine-name a{color:inherit;text-decoration:none;transition:color .12s}
.wine-name a:hover{color:var(--bx)}
.wine-year{
  color:var(--muted);font-size:.62rem;font-weight:400;
  margin-left:.28rem;font-style:italic;white-space:nowrap}
.wine-unavail{font-size:.58rem;color:#dc2626;margin-left:.28rem;white-space:nowrap}
.wine-sub{font-size:.65rem;color:var(--muted);margin-top:.12rem}

/* Pills de liens */
.wine-links{
  display:flex;gap:.22rem;margin-top:.32rem;flex-wrap:wrap;align-items:center}
.lnk{
  font-size:.57rem;text-decoration:none;border-radius:20px;
  padding:2px 8px;border:1px solid;white-space:nowrap;
  font-family:'DM Mono';font-weight:500;
  transition:background .12s,color .12s,border-color .12s}
.lnk-lec{color:#1d4ed8;border-color:rgba(29,78,216,.25)}
.lnk-lec:hover{background:rgba(29,78,216,.06);border-color:rgba(29,78,216,.4)}
.lnk-viv{color:#6b21a8;border-color:rgba(107,33,168,.25)}
.lnk-viv:hover{background:rgba(107,33,168,.06);border-color:rgba(107,33,168,.4)}

/* Barre confiance */
.conf-bar{
  height:2px;border-radius:2px;margin-top:5px;
  background:var(--border);overflow:hidden}
.conf-fill{height:100%;border-radius:2px}

/* Badges */
.badge{
  display:inline-block;padding:.08rem .45rem;border-radius:20px;
  font-size:.55rem;font-family:'DM Mono';font-weight:500;
  margin-right:.1rem;margin-top:.18rem;letter-spacing:.02em;
  vertical-align:middle;border:1px solid}
.b-deal {background:rgba(201,168,76,.10);color:#7a5520;border-color:rgba(201,168,76,.28)}
.b-top  {background:rgba(107,26,42,.07);color:#6B1A2A;border-color:rgba(107,26,42,.14)}
.b-reg  {background:rgba(29,78,216,.05);color:#1d4ed8;border-color:rgba(29,78,216,.15)}
.b-stale{background:rgba(245,158,11,.07);color:#92400e;border-color:rgba(245,158,11,.2)}
.b-nat  {background:rgba(22,163,74,.06);color:#166534;border-color:rgba(22,163,74,.18)}
.b-grape{background:rgba(109,40,217,.05);color:#5b21b6;border-color:rgba(109,40,217,.15)}
.b-style{background:rgba(8,145,178,.05);color:#0e7490;border-color:rgba(8,145,178,.15)}
.b-vol  {background:rgba(180,83,9,.06);color:#92400e;border-color:rgba(180,83,9,.15)}
.b-new  {background:rgba(22,163,74,.08);color:#166534;border-color:rgba(22,163,74,.22)}

/* ── Note (col 3) ── */
.wine-rating{text-align:center}
.stars{
  color:var(--gold);font-size:.76rem;letter-spacing:.2px;
  display:block;line-height:1.2}
.r-num{
  font-family:'DM Mono';font-size:1.05rem;font-weight:700;
  color:var(--ink);font-feature-settings:"tnum";
  display:block;margin-top:.05rem;line-height:1}
.r-cnt{
  font-size:.52rem;color:var(--muted);display:block;
  margin-top:.06rem;font-feature-settings:"tnum"}
.no-rat{
  font-size:.6rem;color:var(--muted);font-style:italic;
  text-align:center;opacity:.6}

/* ── Prix (col 4) ── */
.wine-price{
  font-family:'DM Mono',monospace;font-size:1.05rem;
  font-weight:700;color:var(--ink);text-align:right;
  white-space:nowrap;font-feature-settings:"tnum"}
.p-up  {color:#dc2626;font-size:.6rem;font-weight:700;margin-left:2px;vertical-align:middle}
.p-down{color:#16a34a;font-size:.6rem;font-weight:700;margin-left:2px;vertical-align:middle}
.p-eq  {color:#9ca3af;font-size:.6rem;margin-left:2px;vertical-align:middle}

/* ── Score Q/P (col 5) ── */
.score-wrap{
  text-align:right;position:relative}
.score-num{
  font-family:'DM Mono';font-size:1rem;color:var(--bx);
  font-weight:700;font-feature-settings:"tnum";line-height:1}
.score-lbl{
  font-size:.46rem;color:var(--muted);letter-spacing:.08em;
  text-transform:uppercase;margin-top:.05rem}
.score-bar{
  background:rgba(107,26,42,.07);border-radius:99px;
  height:4px;overflow:hidden;margin-top:7px}
.score-fill{
  height:100%;border-radius:99px;
  background:linear-gradient(90deg,var(--bx) 0%,#9C3050 50%,var(--gold) 100%)}

/* ═══════════════ DEALS ═══════════════ */
.deal-card{
  background:var(--card);border-radius:var(--r);padding:1rem 1.1rem;
  margin-bottom:.45rem;border-left:4px solid var(--gold);
  box-shadow:0 2px 12px rgba(201,168,76,.08);
  display:flex;align-items:center;gap:.9rem;
  transition:box-shadow .2s,transform .2s}
.deal-card:hover{
  box-shadow:0 8px 28px rgba(201,168,76,.2);transform:translateY(-2px)}
.deal-card.d-top{
  background:linear-gradient(108deg,#FFF9EC,var(--card));border-left-width:5px}
.deal-score{
  font-family:'DM Mono';font-size:1.4rem;font-weight:900;
  color:var(--bx);line-height:1;text-align:center;
  min-width:52px;font-feature-settings:"tnum"}
.deal-label{
  font-size:.52rem;color:var(--muted);text-transform:uppercase;
  letter-spacing:.07em;margin-top:.1rem}
.deal-body{flex:1;min-width:0}
.deal-name{
  font-weight:600;font-size:.9rem;color:var(--ink);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.deal-meta{font-size:.71rem;color:var(--muted);margin-top:.2rem}
.deal-price{
  font-family:'DM Mono';font-size:1.05rem;font-weight:700;
  color:var(--ink);white-space:nowrap;text-align:right;
  font-feature-settings:"tnum"}

/* ═══════════════ ONGLETS ═══════════════ */
[data-testid="stTabs"] [role="tablist"]{
  border-bottom:2px solid var(--border);gap:.1rem;padding-bottom:0}
[data-testid="stTabs"] [role="tab"]{
  font-family:'DM Sans',sans-serif;font-size:.76rem;font-weight:500;
  letter-spacing:.025em;color:var(--muted);border:none;
  border-radius:8px 8px 0 0;padding:.45rem .9rem;
  background:transparent;transition:color .15s,background .15s}
[data-testid="stTabs"] [role="tab"]:hover{
  color:var(--bx);background:rgba(107,26,42,.04)}
[data-testid="stTabs"] [aria-selected="true"]{
  color:var(--bx) !important;font-weight:700 !important;
  background:rgba(107,26,42,.05) !important;
  border-bottom:2.5px solid var(--bx) !important}

/* ═══════════════ PAGINATION ═══════════════ */
.page-info{
  font-size:.71rem;color:var(--muted);
  font-family:'DM Mono';text-align:center;padding:.35rem}

/* ═══════════════ SIDEBAR BRANDING ═══════════════ */
.sb-brand{
  padding:.9rem 1rem .7rem;
  border-bottom:1px solid rgba(201,168,76,.15);
  margin-bottom:.6rem}
.sb-brand-title{
  font-family:'Playfair Display',serif;
  font-size:1.05rem;font-weight:900;
  color:#F5E6E8;line-height:1.1;letter-spacing:-.01em}
.sb-brand-title span{color:var(--gold)}
.sb-brand-sub{
  font-size:.62rem;color:rgba(232,208,213,.45);
  letter-spacing:.1em;text-transform:uppercase;margin-top:.2rem}

/* Titres de section sidebar */
.sb-section{
  font-size:.62rem;color:rgba(201,168,76,.7);
  letter-spacing:.12em;text-transform:uppercase;
  font-weight:600;margin:.7rem 0 .3rem;
  padding-bottom:.25rem;
  border-bottom:1px solid rgba(201,168,76,.12)}

/* Barre de couverture Vivino */
.cov-wrap{margin:.35rem 0 .55rem}
.cov-label{
  display:flex;justify-content:space-between;
  font-size:.65rem;color:rgba(232,208,213,.7);margin-bottom:.28rem}
.cov-label strong{color:#E8D0D5}
.cov-bar{
  height:5px;background:rgba(255,255,255,.1);
  border-radius:99px;overflow:hidden}
.cov-fill{
  height:100%;border-radius:99px;
  transition:width .4s ease;
  background:linear-gradient(90deg,#8B2A3A,var(--gold))}
.cov-fill.cov-full{background:#16a34a}

/* ═══════════════ CARTE PROGRESSION SCRAPING ═══════════════ */
.job-card{
  background:rgba(255,255,255,.05);
  border-left:3px solid rgba(201,168,76,.6);
  border-radius:8px;
  padding:.6rem .75rem;margin:.4rem 0 .5rem}
.job-title{
  font-size:.7rem;font-weight:600;color:#E8D0D5;
  letter-spacing:.02em;margin-bottom:.45rem}
.job-bar{
  height:4px;background:rgba(255,255,255,.1);
  border-radius:99px;overflow:hidden;margin-bottom:.35rem}
.job-fill{
  height:100%;border-radius:99px;
  background:linear-gradient(90deg,#8B2A3A,var(--gold));
  transition:width .6s ease}
.job-meta{
  font-size:.58rem;color:rgba(200,168,180,.55);
  font-family:'DM Mono';letter-spacing:.03em}

/* ═══════════════ COMPTEUR FILTRES ═══════════════ */
.filter-counter{
  display:inline-flex;align-items:center;gap:.4rem;
  background:var(--card);border:1px solid var(--border);
  border-radius:20px;padding:.25rem .7rem;
  font-size:.7rem;color:var(--muted);
  font-family:'DM Mono';margin:.3rem 0 .6rem;
  box-shadow:0 1px 4px rgba(26,8,16,.05)}
.filter-counter strong{color:var(--ink);font-size:.78rem}
.filter-counter .sep{color:var(--border);margin:0 .1rem}
/* Variante "filtré" — bordure dorée discrète */
.filter-counter.is-filtered{
  border-color:rgba(201,168,76,.4);
  background:rgba(201,168,76,.04)}
.filter-counter.is-filtered strong{color:var(--bx)}

/* ═══════════════ ÉTAT VIDE ═══════════════ */
.empty-state{
  text-align:center;padding:3.5rem 1rem;
  color:var(--muted)}
.empty-state-icon{font-size:2.8rem;line-height:1;margin-bottom:.6rem}
.empty-state-title{
  font-family:'Playfair Display',serif;
  font-size:1.15rem;font-weight:700;color:var(--ink);
  margin-bottom:.4rem}
.empty-state-sub{font-size:.82rem;line-height:1.55}

/* ═══════════════ SECTION HEADERS ═══════════════ */
.tab-section{
  font-family:'Playfair Display',serif;
  font-size:1.05rem;font-weight:700;color:var(--ink);
  margin:.1rem 0 .8rem;letter-spacing:-.01em;
  display:flex;align-items:center;gap:.45rem}
.tab-section::after{
  content:'';flex:1;height:1px;
  background:linear-gradient(90deg,var(--border),transparent);
  margin-left:.2rem}
.tab-subsection{
  font-size:.68rem;font-weight:600;color:var(--muted);
  letter-spacing:.1em;text-transform:uppercase;
  margin:.9rem 0 .35rem;
  display:flex;align-items:center;gap:.4rem}
.tab-subsection::after{
  content:'';flex:1;height:1px;background:var(--border)}

/* ═══════════════ DEALS BANNER ═══════════════ */
.deals-banner{
  display:inline-flex;align-items:center;gap:.5rem;
  background:rgba(22,163,74,.07);border:1px solid rgba(22,163,74,.22);
  border-radius:10px;padding:.45rem .9rem;margin:.2rem 0 .7rem;
  font-size:.82rem;color:#166534}
.deals-banner strong{font-size:.9rem}
.deals-banner-warn{
  background:rgba(245,158,11,.06);border-color:rgba(245,158,11,.22);
  color:#92400e}

/* ═══════════════ ALTAIR TRANSPARENT ═══════════════ */
/* Force les SVG Vega-Lite à fond transparent pour dark mode */
.vega-embed,.vega-embed canvas,.vega-embed svg{
  background:transparent !important}

/* ═══════════════ PAGINATION ═══════════════ */
.pag-wrap{
  display:flex;align-items:center;justify-content:center;
  gap:.5rem;margin:.6rem 0 .2rem}
.pag-info{
  font-family:'DM Mono';font-size:.72rem;color:var(--muted);
  padding:0 .4rem}

/* ═══════════════ MOBILE ═══════════════ */
@media(max-width:640px){
  .wine-card{
    grid-template-columns:1.8rem 1fr;
    grid-template-rows:auto auto auto;gap:.3rem}
  .wine-info.has-img{padding-right:0}
  .wine-bottle{display:none}
  .wine-rating{
    grid-column:1/3;display:flex;align-items:center;
    gap:.6rem;justify-content:flex-start}
  .stars{display:inline}
  .wine-price{grid-column:1/3;text-align:left}
  .score-wrap{display:none}
  .wine-name{-webkit-line-clamp:3}
}

/* ═══════════════ DARK MODE ═══════════════ */
@media(prefers-color-scheme:dark){
  :root{
    --card:#1C1015;--page:#150A0D;--ink:#EDD5DA;--muted:#A89098;
    --border:#3A2030;--sh:0 2px 12px rgba(0,0,0,.4);
    --sh-hover:0 8px 28px rgba(0,0,0,.55)}
  .stApp,[data-testid="stAppViewContainer"]{background:var(--page) !important}
  [data-testid="stHeader"]{background:var(--page) !important}
  [data-testid="stMetric"]{background:#1C1015;border-color:#3A2030}
  [data-testid="stMetricLabel"]{color:var(--muted) !important}
  [data-testid="stMetricValue"]{color:var(--ink) !important}
  .wine-card{border-left-color:#5A2535}
  .wine-card:hover{border-left-color:#7A3550}
  .wine-card.top1{
    background:linear-gradient(108deg,#2A1800,#1C1015);
    border-left-color:var(--gold)}
  .wine-card.top2{
    background:linear-gradient(108deg,#181828,#1C1015);
    border-left-color:#909090}
  .wine-card.top3{
    background:linear-gradient(108deg,#1E1100,#1C1015);
    border-left-color:#B06035}
  .wine-card.unavailable{opacity:.22}
  .wine-name a{color:var(--ink) !important}
  .wine-sub{color:var(--muted)}
  .badge.b-reg  {background:#1A1438;color:#c4b5fd;border-color:rgba(196,181,253,.2)}
  .badge.b-top  {background:#38101E;color:#fda4af;border-color:rgba(253,164,175,.2)}
  .badge.b-stale{background:#20202E;color:#9ca3af;border-color:rgba(156,163,175,.16)}
  .badge.b-new  {background:rgba(22,163,74,.12);color:#86efac;border-color:rgba(22,163,74,.25)}
  .badge.b-nat  {background:#0E2014;color:#86efac;border-color:rgba(134,239,172,.18)}
  .badge.b-grape{background:#160A2C;color:#c4b5fd;border-color:rgba(196,181,253,.18)}
  .badge.b-style{background:#081C26;color:#67e8f9;border-color:rgba(103,232,249,.16)}
  .lnk-lec{color:#93c5fd;border-color:rgba(147,197,253,.22)}
  .lnk-lec:hover{background:rgba(147,197,253,.07);border-color:rgba(147,197,253,.4)}
  .lnk-viv{color:#d8b4fe;border-color:rgba(216,180,254,.22)}
  .lnk-viv:hover{background:rgba(216,180,254,.07);border-color:rgba(216,180,254,.4)}
  .conf-bar{background:#3A2030}
  .score-bar{background:rgba(255,255,255,.05)}
  .deal-card{border-left-color:var(--gold)}
  .deal-card.d-top{background:linear-gradient(108deg,#2C1A02,#1C1015)}
  .deal-name,.deal-price{color:var(--ink)}
  .deal-meta{color:var(--muted)}
  [data-testid="stTabs"] [role="tab"]{color:var(--muted)}
  [data-testid="stTabs"] [aria-selected="true"]{
    color:#fda4af !important;border-bottom-color:#fda4af !important;
    background:rgba(253,164,175,.05) !important}
  /* Altair : fond transparent déjà géré via CSS, textes adaptés */
  .vega-embed .mark-text text{fill:#EDD5DA !important}
  /* Deals banner */
  .deals-banner{background:rgba(22,163,74,.08);border-color:rgba(22,163,74,.2);color:#86efac}
  .deals-banner-warn{background:rgba(245,158,11,.07);border-color:rgba(245,158,11,.18);color:#fcd34d}
  /* Section headers */
  .tab-section{color:var(--ink)}
  .tab-subsection{color:var(--muted)}
  .tab-section::after,.tab-subsection::after{background:var(--border)}
  /* Filter counter */
  .filter-counter{background:var(--card);border-color:var(--border)}
  .filter-counter.is-filtered{background:rgba(201,168,76,.06);border-color:rgba(201,168,76,.3)}
}

/* ═══════════════ LISIBILITÉ DES CHAMPS (barre latérale) ═══════════════ */
[data-testid="stSidebar"] [data-testid="stSelectbox"] [role="group"]{
  background:#FFFFFF !important;border-color:rgba(201,168,76,.45) !important}
[data-testid="stSidebar"] [data-testid="stSelectbox"] [role="group"] *{
  color:#1A0810 !important;-webkit-text-fill-color:#1A0810 !important}
[data-testid="stSidebar"] [data-testid="stSelectbox"] svg{fill:#6B1A2A !important}

/* ═══════════════ MOBILE-FIRST : tout dans la page, plus de menu latéral ═══════════════ */
[data-testid="stSidebar"],[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"],[data-testid="stSidebarCollapseButton"]{display:none !important}
[data-testid="stMainBlockContainer"]{
  max-width:980px;margin:0 auto;padding:3.2rem 1rem 4rem !important}
[data-testid="stHeader"]{background:transparent}
.app-title{font-family:'Playfair Display',Georgia,serif;font-weight:700;font-size:1.55rem;
  color:var(--ink,#1A0810);line-height:1.15;margin:0 0 .6rem}
.app-title span{color:var(--gold,#C9A84C)}

/* segmented controls = navigation, pleine largeur, grandes cibles tactiles */
[data-testid="stButtonGroup"]{width:100%}
[data-testid="stButtonGroup"] button{min-height:2.6rem;font-size:.85rem;flex:1 1 0;
  white-space:nowrap;padding:0 .4rem}
[data-testid="stButtonGroup"] button[kind="segmented_controlActive"],
[data-testid="stButtonGroup"] button[aria-checked="true"]{
  background:var(--wine,#6B1A2A) !important;color:#fff !important;font-weight:600}

/* bandeau d'état */
.status-strip{background:#fff;border:1px solid rgba(107,26,42,.12);border-radius:10px;
  padding:.55rem .8rem;margin:.5rem 0 .9rem;box-shadow:0 1px 3px rgba(26,8,16,.05)}
.status-line{display:flex;flex-wrap:wrap;gap:.3rem .9rem;justify-content:space-between;
  font-size:.78rem;color:#5b4249}
.status-strip .cov-bar{margin-top:.45rem}
.chip{border-radius:999px;padding:.05rem .55rem;font-size:.7rem;font-weight:600}
.chip-run{background:#fff3d6;color:#8a5a00}

/* indicateurs compacts (3 colonnes même sur téléphone) */
.kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:.5rem;margin:.3rem 0 .8rem}
.kpi{background:#fff;border:1px solid rgba(107,26,42,.12);border-radius:10px;padding:.5rem .65rem;
  box-shadow:0 1px 3px rgba(26,8,16,.05)}
.kpi-label{font-size:.62rem;letter-spacing:.05em;text-transform:uppercase;color:#8B6B72}
.kpi-value{font-size:1.05rem;font-weight:700;color:#1A0810;font-variant-numeric:tabular-nums}
.kpi-sub{font-size:.62rem;color:#8B6B72;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* boutons et champs : taille tactile */
.stButton > button,.stDownloadButton > button{min-height:2.75rem;border-radius:10px}
[data-testid="stPopover"] > div > button,[data-testid="stPopoverButton"]{min-height:2.4rem}
input,textarea,[data-baseweb="select"] > div{font-size:16px !important}   /* évite le zoom iOS */

@media (max-width:640px){
  [data-testid="stMainBlockContainer"]{padding:2.8rem .65rem 4rem !important}
  .app-title{font-size:1.3rem}
  [data-testid="stButtonGroup"] button{font-size:.76rem;padding:0 .25rem}
  [data-testid="stHorizontalBlock"]{gap:.5rem !important}
  .kpi-value{font-size:.95rem}
  .kpi-label{font-size:.56rem}
  [data-testid="stMetric"]{padding:.55rem .65rem !important}
  [data-testid="stMetricValue"]{font-size:1.15rem !important}
  .tab-section{font-size:1.05rem}
}

/* compacité : barre Streamlit masquée, espacements réduits */
[data-testid="stToolbar"],[data-testid="stDecoration"],[data-testid="stHeader"]{display:none !important}
@media (max-width:640px){
  [data-testid="stMainBlockContainer"]{padding-top:.8rem !important}
  [data-testid="stVerticalBlock"]{gap:.55rem !important}
  [data-testid="stButtonGroup"] button{min-height:2.3rem}
  .app-title{margin-bottom:.15rem}
  .status-strip{padding:.4rem .65rem;margin:.1rem 0 .3rem}
  .kpis{margin:.1rem 0 .3rem}
  .kpi{padding:.4rem .55rem}
}

/* carte vin compacte sur téléphone : nom + prix en haut, note + score en dessous */
@media (max-width:640px){
  .wine-card{display:grid;grid-template-columns:1.7rem 1fr auto;
    grid-template-areas:"rank info price" ". rating score";
    gap:.25rem .55rem;padding:.7rem .75rem .65rem .7rem;margin-bottom:.15rem}
  .wine-card > .wine-rank{grid-area:rank;align-self:start;font-size:1rem;padding-top:.1rem}
  .wine-card > .wine-info{grid-area:info}
  .wine-card > div:nth-child(3){grid-area:rating;grid-column:auto}
  .wine-card > div:nth-child(4){grid-area:price;text-align:right;align-self:start}
  .wine-card > div:nth-child(5){grid-area:score}
  .wine-card .wine-price{font-size:.98rem;grid-column:auto;text-align:right}
  .wine-card .score-wrap{display:block;text-align:right}
  .wine-card .score-bar{display:none}
  .wine-card .score-num{font-size:.95rem}
  .wine-card .wine-rating{grid-column:auto}
  .wine-name{font-size:.88rem;-webkit-line-clamp:2}
  .wine-links{margin-top:.2rem}
  .app-title{margin-bottom:.5rem;padding-top:.3rem}
}
[data-testid="stPopover"]{margin:.1rem 0 .7rem}
@media (max-width:640px){
  .wine-card > div:nth-child(4) > div:nth-child(2){display:none}   /* libellé « prix » superflu */
  .wine-card .r-cnt{white-space:nowrap;font-size:.62rem}
  .wine-card .wine-rating{flex-wrap:nowrap;gap:.45rem}
  .wine-card .score-lbl{display:none}
}
</style>
'''

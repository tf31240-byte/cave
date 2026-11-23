"""Rendu : cartes de vins, tableaux, console de logs."""
import time
import streamlit as st
import pandas as pd
import html as _html
# AUTO-IMPORTS
from .vivino import build_query
from .scoring import fmt_count
from .config import VIVINO_LOW_CONFIDENCE
# END-AUTO

# ═══════════════════════════════════════════════════════════════════════════
# RENDU HTML
# ═══════════════════════════════════════════════════════════════════════════

def stars(r: float) -> str:
    r = max(0.0, min(5.0, float(r or 0)))
    return "".join("★" if r >= i else ("½" if r >= i - .5 else "☆") for i in range(1, 6))

# Constantes de rendu — définies une fois, partagées entre toutes les cartes
_RANK_CLS  = {1: "top1", 2: "top2", 3: "top3"}
_RANK_ICON = {1: "🥇",   2: "🥈",   3: "🥉"}

def wine_card_html(wine: dict, rank: int, max_score: float) -> str:
    cls  = _RANK_CLS.get(rank, "")
    if wine.get("vintage_match") is False: cls = (cls + " vintage-warn").strip()
    if not wine.get("available", True):    cls = (cls + " unavailable").strip()
    if wine.get("_stale"):                 cls = (cls + " stale").strip()
    icon = _RANK_ICON.get(rank, f"<span style='font-size:.75rem'>#{rank}</span>")

    # XSS : toutes les valeurs utilisateur échappées avant injection HTML
    name     = _html.escape(wine["name"])
    safe_url = _html.escape(wine.get("url") or "")
    safe_viv = _html.escape(wine.get("vivino_url") or "")
    safe_reg = _html.escape(wine.get("region") or "")

    name_html = (f'<a href="{safe_url}" target="_blank" '
                 f'style="color:var(--ink);text-decoration:none">{name}</a>'
                 ) if safe_url else name

    # Miniature bouteille — position absolue dans .wine-info (classe CSS, pas float)
    safe_img = _html.escape(wine.get("image") or "")
    img_html = (
        f'<img src="{safe_img}" alt="" class="wine-bottle" loading="lazy" '
        f'onerror="this.remove()" />'
    ) if safe_img else ""
    info_cls = "wine-info has-img" if safe_img else "wine-info"
    yr      = (f'<span class="wine-year">{wine["vintage"]}</span>'
               ) if wine.get("vintage") else ""
    unavail = ('<span class="wine-unavail">⛔</span>'
               if not wine.get("available", True) else "")
    mil = ""
    if wine.get("vivino_year") and wine.get("vintage") and wine["vivino_year"] != wine["vintage"]:
        mil = (f'<div class="wine-sub" style="color:#b87800">'
               f'⚠️ Vivino={wine["vivino_year"]} / Leclerc={wine["vintage"]}</div>')

    # Barre de confiance Vivino (couleur selon niveau)
    conf = wine.get("match_confidence") or 0
    conf_color = "#16a34a" if conf >= 0.7 else ("#f59e0b" if conf >= 0.4 else "#e5e7eb")
    conf_html = (
        f'<div class="conf-bar" title="Confiance Vivino : {conf:.0%}">'
        f'<div class="conf-fill" style="width:{conf*100:.0f}%;background:{conf_color}"></div>'
        f'</div>'
    ) if conf else ""

    links = []
    if safe_url:
        links.append(f'<a href="{safe_url}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>')
    if safe_viv:
        links.append(f'<a href="{safe_viv}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>')
    links_html = (f'<div class="wine-links">' + "".join(links) + conf_html + "</div>") if links or conf_html else ""

    score  = wine.get("score") or 0
    rating = wine.get("rating")
    badges = ""
    if rating and rating >= 4.3:             badges += '<span class="badge b-top">★ Top noté</span>'
    _fs = wine.get("first_seen")
    if _fs and time.time() - _fs < 14 * 86400:
        badges += '<span class="badge b-top">🆕 Nouveau</span>'
    _conf = wine.get("match_confidence")
    if wine.get("vivino_url") and _conf and _conf < VIVINO_LOW_CONFIDENCE and not wine.get("locked"):
        badges += '<span class="badge b-stale" title="Correspondance Vivino peu sûre">🔎 À vérifier</span>'
    if safe_reg:                             badges += f'<span class="badge b-reg">{safe_reg}</span>'
    if wine.get("_stale"):                   badges += f'<span class="badge b-stale">⏳ obsolète</span>'
    if wine.get("is_natural"):               badges += '<span class="badge b-nat">🌿 Naturel</span>'
    # Cépages — max 3, depuis Vivino ou hint Leclerc
    grapes = wine.get("grapes") or []
    if grapes:
        gstr = " · ".join(_html.escape(g) for g in grapes[:3])
        badges += f'<span class="badge b-grape">🍇 {gstr}</span>'
    # Style régional Vivino (ex: "Médoc Rouge", "Bourgogne Blanc")
    if wine.get("style_name"):
        badges += f'<span class="badge b-style">{_html.escape(wine["style_name"])}</span>'
    # Volume si différent de 75cl (Magnum, etc.)
    if wine.get("volume_cl") and wine["volume_cl"] != 75:
        badges += f'<span class="badge b-vol">{wine["volume_cl"]:.0f}cl</span>'

    if rating:
        cnt = wine.get("ratings_count") or 0
        rating_col = (f'<div class="wine-rating">'
                      f'<span class="stars">{stars(rating)}</span>'
                      f'<span class="r-num">{rating:.1f}</span>'
                      f'<span class="r-cnt">{fmt_count(cnt)} avis</span>'
                      f'</div>')
    else:
        rating_col = '<div class="no-rat">—<br>pas de note</div>'

    trend = wine.get("price_trend", "")
    trend_html = {"↑":'<span class="p-up">↑</span>',
                  "↓":'<span class="p-down">↓</span>',
                  "=":'<span class="p-eq">=</span>'}.get(trend, "")
    price_s = f'{wine.get("price") or 0:.2f} €'.replace(".", ",")
    # Prix en baisse : couleur verte pour attirer l'attention
    price_style = 'color:#16a34a' if trend == "↓" else ''
    price_col = (f'<div class="wine-price" style="{price_style}">'
                 f'{price_s}{trend_html}</div>')

    pct = min(100, (score / max_score) * 100) if max_score > 0 else 0
    score_col = (
        f'<div class="score-wrap">'
        f'<div class="score-num">{score:.2f}</div>'
        f'<div class="score-lbl">score</div>'
        f'<div class="score-bar"><div class="score-fill" style="width:{pct:.1f}%"></div></div>'
        f'</div>'
    ) if score else (
        f'<div class="score-wrap">'
        f'<div style="color:var(--muted);font-size:.68rem;text-align:right">—</div>'
        f'</div>'
    )

    # Prix avec label
    price_lbl = f'<div style="font-size:.44rem;color:var(--muted);letter-spacing:.07em;text-transform:uppercase;text-align:right;margin-top:.18rem">prix</div>'

    # Nom + millésime + indispo sur une ligne, badges en dessous
    name_line = (f'<div class="wine-name">{name_html}'
                 + (f' {yr}' if yr else '')
                 + (f' {unavail}' if unavail else '')
                 + '</div>')
    return (f'<div class="wine-card {cls}">'
            f'<div class="wine-rank">{icon}</div>'
            f'<div class="{info_cls}">'
            f'{img_html}'
            f'{name_line}'
            f'{mil}{links_html}<div>{badges}</div>'
            f'</div>'
            f'{rating_col}'
            f'<div>{price_col}{price_lbl}</div>'
            f'{score_col}'
            f'</div>')


_VINTAGE_MATCH_LABEL = {True: "✅", False: "⚠️", None: "—"}


def _make_wines_df(ws: list) -> "pd.DataFrame":
    """
    Fix I : fonction top-level (plus de redéfinition à chaque render dans tab_export).
    Colonnes communes pour tab_data ET tab_export — une seule source de vérité.
    """
    def _num(v):
        """None si absent/zéro-équivoque, sinon float. Arrow gère None en float64 nullable."""
        return float(v) if v is not None and v != "" and v is not False else None

    def _year(v):
        """Millésime → str '2021' ou '' — colonne TextColumn dans le config."""
        import math
        if v is None or (isinstance(v, float) and math.isnan(v)): return ""
        try: return str(int(v))
        except (ValueError, TypeError): return str(v) if v != "" else ""

    df = pd.DataFrame([{
        "Nom":              w["name"],
        "Région":           w.get("region") or "",
        "Millésime":        _year(w.get("vintage")),
        "Prix (€)":         _num(w.get("price")),
        "Volume (cl)":      _num(w.get("volume_cl") or 75),
        "Tendance":         w.get("price_trend") or "",
        "EAN":              w.get("ean") or "",
        "Note":             _num(w.get("rating")),
        "Nb avis":          _num(w.get("ratings_count")),
        "Nb avis (total)":  _num(w.get("ratings_count_all")),
        "Score":            _num(w.get("score")),
        "Cépages":          ", ".join(w.get("grapes") or []),
        "Style":            w.get("style_name") or "",
        "Domaine":          w.get("winery") or "",
        "Naturel":          "🌿" if w.get("is_natural") else "",
        "Tanin":            _num(w.get("tannin")),
        "Acidité":          _num(w.get("acidity")),
        "Sucrosité":        _num(w.get("sweetness")),
        "Corps":            _num(w.get("body")),
        "Mil. Vivino":      _year(w.get("vivino_year")),
        "Mil. OK":          _VINTAGE_MATCH_LABEL.get(w.get("vintage_match"), "—"),
        "Dispo":            "✅" if w.get("available", True) else "⛔",
        "Leclerc":          w.get("url") or "",
        "Vivino":           w.get("vivino_url") or "",
        "Query":            build_query(w["name"]),
    } for w in ws])
    return df

_DF_COL_CONFIG = {
    "Leclerc":        st.column_config.LinkColumn(display_text="🛒"),
    "Vivino":         st.column_config.LinkColumn(display_text="🍷"),
    "Prix (€)":       st.column_config.NumberColumn(format="%.2f"),
    "Note":           st.column_config.NumberColumn(format="%.1f"),
    "Score":          st.column_config.NumberColumn(format="%.2f"),
    "Nb avis":        st.column_config.NumberColumn(format="%d"),
    "Nb avis (total)":st.column_config.NumberColumn(format="%d"),
    "Tanin":          st.column_config.NumberColumn(format="%.2f"),
    "Acidité":        st.column_config.NumberColumn(format="%.2f"),
    "Sucrosité":      st.column_config.NumberColumn(format="%.2f"),
    "Corps":          st.column_config.NumberColumn(format="%.2f"),
    "Millésime":      st.column_config.TextColumn(),
    "Mil. Vivino":    st.column_config.TextColumn(),
}

def _make_logger(max_lines: int = 10):
    logs, box = [], st.empty()
    def _log(msg: str):
        logs.append(msg)
        box.markdown("\n\n".join(logs[-max_lines:]))
    return _log, box


def _fmt_log_line(line: str) -> str:
    """Formate une ligne de log pour la console HTML : timestamp grisé + coloration par niveau."""
    import html as _html
    # Extraire le timestamp [HH:MM:SS] si présent
    ts, rest = "", line
    if line.startswith("[") and len(line) > 9 and line[9] == "]":
        ts   = _html.escape(line[:10])
        rest = line[10:]
    else:
        rest = line
    rest_esc = _html.escape(rest)
    # Couleur selon le contenu
    if any(k in rest for k in ("✅", "succès", "terminé", "sauvegardé", "Cached")):
        color = "#3fb950"  # vert
    elif any(k in rest for k in ("❌", "Erreur", "ERROR", "Exception", "failed")):
        color = "#f85149"  # rouge
    elif any(k in rest for k in ("⚠️", "Warning", "obsolète", "manquant", "stale")):
        color = "#d29922"  # jaune
    elif any(k in rest for k in ("🌐", "Page ", "Chargement", "http")):
        color = "#79c0ff"  # bleu clair
    elif any(k in rest for k in ("🔎", "Vivino", "API", "fetch", "score")):
        color = "#d2a8ff"  # violet
    elif any(k in rest for k in ("📦", "Cache", "💾")):
        color = "#ffa657"  # orange
    else:
        color = "#c9d1d9"  # blanc cassé (défaut)
    ts_html   = f'<span style="color:#484f58">{ts}</span>' if ts else ""
    rest_html = f'<span style="color:{color}">{rest_esc}</span>'
    # Chaque ligne enveloppée dans un <div> : évite que st.markdown() collapse
    # le \n séparateur en texte littéral ou en espace entre deux <span> inline.
    return f'<div style="margin:0;min-height:1.5em">{ts_html}{rest_html}</div>'


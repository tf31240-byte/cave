"""Interface Streamlit : trois écrans — Classement, Mise à jour, Réglages."""
import html as _html
import json, math, threading, time
from datetime import datetime
import pandas as pd
import streamlit as st
# AUTO-IMPORTS
from .storage import REJECTION_REASONS, _gist_can_restore, _gist_is_configured, _invalidate_mem_cache, _lec_path, _price_hist_path, _viv_path, ckpt_finish, ckpt_load, gist_push, load_leclerc_cache, load_price_history, load_vivino_cache, load_vivino_rejections, restore_from_gist, save_vivino_cache, save_vivino_rejection
from .render import _DF_COL_CONFIG, _fmt_log_line, _make_logger, _make_wines_df, wine_card_html
from .jobs import get_price_drops, load_job_state, load_wines_from_cache, repair_zero_prices, run_check_stock, save_job_state, start_background_job
from .config import CACHE_DIR, CARDS_PER_PAGE, JOB_LOG_PATH, REJECTION_LOG_PATH, SORTS, STORE_CODE, VIVINO_CACHE_TTL_DAYS, WINE_TYPES
from .scoring import fmt_age, fmt_count, vivino_cache_type
from .regions import _fuzzy_match, extract_region
from .vivino import _PROGRESS_RE, build_query
# END-AUTO


SCREENS = ["🏅 Classement", "🔄 Mise à jour", "⚙️ Réglages"]
VIEWS = ["Liste", "Bonnes affaires", "Statistiques", "Tableau & export"]
_ALL_SLUGS = list(WINE_TYPES.values())
_JOB_ACTIVE = {"running", "queued"}


# ═══════════════════════════════════════════════════════════════════════════
# DÉMARRAGE : restauration / synchronisation Gist
# ═══════════════════════════════════════════════════════════════════════════

def _valid_vivino(d) -> bool:
    return isinstance(d, dict) and len(d) > 0


def _valid_leclerc(d) -> bool:
    return isinstance(d, dict) and len(d.get("wines", [])) > 0


def _file_is_valid(path, check) -> bool:
    if not path.exists():
        return False
    try:
        return check(json.loads(path.read_text("utf-8")))
    except Exception:
        return False


def _startup_restore() -> str | None:
    """Restaure les caches manquants ou vides : sauvegarde .bak locale d'abord, puis Gist."""
    checks = ([(f"vivino_{s}.json", _viv_path(s), _valid_vivino) for s in _ALL_SLUGS]
              + [(f"leclerc_{s}.json", _lec_path(s), _valid_leclerc) for s in _ALL_SLUGS])
    missing = [(f, p, c) for f, p, c in checks if not _file_is_valid(p, c)]
    if not missing:
        return None

    restored_local, still_missing = 0, []
    for fname, _path, check in missing:
        bak = (CACHE_DIR / fname).with_suffix(".bak")
        try:
            content = bak.read_text("utf-8")
            if check(json.loads(content)):
                (CACHE_DIR / fname).write_text(content, "utf-8")
                _invalidate_mem_cache(CACHE_DIR / fname)
                restored_local += 1
                continue
        except Exception:
            pass
        still_missing.append(fname)

    if not still_missing:
        return f"✅ {restored_local} fichier(s) restauré(s) depuis la sauvegarde locale"
    if not _gist_is_configured():
        return None
    n = restore_from_gist()
    if restored_local + n > 0:
        return f"✅ {restored_local + n} fichier(s) restauré(s) (Gist : {n}, local : {restored_local})"
    return f"⚠️ {len(still_missing)} fichier(s) non restauré(s) — vérifiez la configuration du Gist"


@st.cache_resource
def _startup_gist_sync():
    """Pousse les caches locaux VALIDES vers le Gist, une fois par démarrage du serveur."""
    if not _gist_is_configured():
        return 0
    pushed = 0
    for slug in _ALL_SLUGS:
        for path_fn, check in ((_viv_path, _valid_vivino), (_lec_path, _valid_leclerc)):
            p = path_fn(slug)
            if _file_is_valid(p, check):
                threading.Thread(target=gist_push, args=(p.name, p.read_text("utf-8")),
                                 daemon=False).start()
                pushed += 1
    return pushed


def _run_startup() -> None:
    if not st.session_state.get("_startup_done"):
        st.session_state["_startup_done"] = True
        msg = _startup_restore()
        if msg:
            st.toast(msg, icon="💾")
    _startup_gist_sync()


# ═══════════════════════════════════════════════════════════════════════════
# SUIVI EN TEMPS RÉEL
# ═══════════════════════════════════════════════════════════════════════════

def _poll() -> None:
    """Fragment : sauvegarde Gist horaire + rerun de l'app tant qu'une tâche tourne."""
    now = time.time()
    if _gist_is_configured() and now - st.session_state.get("_gist_backup_at", 0.0) > 3600:
        st.session_state["_gist_backup_at"] = now
        for slug in _ALL_SLUGS:
            for path_fn, check in ((_viv_path, _valid_vivino), (_lec_path, _valid_leclerc)):
                p = path_fn(slug)
                if _file_is_valid(p, check):
                    threading.Thread(target=gist_push, args=(p.name, p.read_text("utf-8")),
                                     daemon=False).start()
    if not st.session_state.get("live", True):
        return
    if now - st.session_state.get("_last_rerun", 0.0) < 1.8:
        return
    job = load_job_state()
    status = job.get("status")
    recently_finished = status in {"done", "error"} and now - (job.get("finished_at") or 0) < 6.0
    if status in _JOB_ACTIVE or recently_finished:
        st.session_state["_last_rerun"] = now
        st.rerun(scope="app")


def _start_polling(job: dict) -> None:
    active = job.get("status") in _JOB_ACTIVE or (time.time() - (job.get("finished_at") or 0) < 8.0)
    st.fragment(run_every=2 if active else 60)(_poll)()


# ═══════════════════════════════════════════════════════════════════════════
# DONNÉES
# ═══════════════════════════════════════════════════════════════════════════

def _mtime(p) -> float:
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


def get_wines(slug: str) -> list:
    """Vins enrichis (Leclerc + Vivino + prix). Recalculé seulement si un fichier de cache a changé."""
    key = (slug, _mtime(_lec_path(slug)), _mtime(_viv_path(slug)), _mtime(_price_hist_path()))
    if st.session_state.get("_wines_key") != key:
        st.session_state["_wines"] = load_wines_from_cache(slug)
        st.session_state["_wines_key"] = key
    return st.session_state.get("_wines") or []


def _force_reload() -> None:
    st.session_state.pop("_wines_key", None)


def coverage(slug: str, lc: dict | None, vc: dict) -> dict:
    """Chiffres de couverture Vivino pour l'écran de mise à jour et la barre latérale."""
    n_total = len(lc["wines"]) if lc else 0
    n_rated = sum(1 for v in vc.values() if v.get("rating") and not v.get("suppressed"))
    n_rated = min(n_rated, n_total) if n_total else 0
    return {
        "total": n_total,
        "rated": n_rated,
        "missing": max(0, n_total - n_rated),
        "stale": sum(1 for v in vc.values() if v.get("_stale")),
        "zero_price": sum(1 for w in (lc["wines"] if lc else []) if not w.get("price")),
        "pct": int(100 * n_rated / max(n_total, 1)),
    }


# ═══════════════════════════════════════════════════════════════════════════
# BARRE LATÉRALE
# ═══════════════════════════════════════════════════════════════════════════

def sidebar() -> str:
    """Affiche la barre latérale. Retourne le slug du type de vin choisi."""
    with st.sidebar:
        st.markdown(
            '<div class="sb-brand"><div class="sb-brand-title">🍷 Cave <span>Leclerc</span></div>'
            '<div class="sb-brand-sub">Blagnac × Vivino</div></div>', unsafe_allow_html=True)

        st.markdown('<div class="sb-section">🍾 Type de vin</div>', unsafe_allow_html=True)
        label = st.selectbox("Type de vin", list(WINE_TYPES), key="wine_type",
                             label_visibility="collapsed")
        slug = WINE_TYPES[label]

        st.markdown('<div class="sb-section">📍 Aller à</div>', unsafe_allow_html=True)
        st.radio("Écran", SCREENS, key="screen", label_visibility="collapsed")

        lc, vc = load_leclerc_cache(slug), load_vivino_cache(slug)
        cov = coverage(slug, lc, vc)
        if lc:
            cls = "cov-fill cov-full" if cov["pct"] == 100 else "cov-fill"
            st.markdown(
                f'<div class="cov-wrap">'
                f'<div class="cov-label"><span>📦 Catalogue <strong>{fmt_age(lc["cached_at"])}</strong></span>'
                f'<span>{cov["total"]} vins</span></div>'
                f'<div class="cov-label" style="margin-top:.35rem"><span>🍷 Notes Vivino '
                f'<strong>{cov["rated"]}/{cov["total"]}</strong></span>'
                f'<span style="color:rgba(201,168,76,.8)">{cov["pct"]}%</span></div>'
                f'<div class="cov-bar"><div class="{cls}" style="width:{cov["pct"]}%"></div></div>'
                f'</div>', unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="cov-wrap"><div class="cov-label"><span>📦 Catalogue</span>'
                '<span style="color:#dc6060">à charger</span></div></div>', unsafe_allow_html=True)

        job = load_job_state()
        if job.get("status") in _JOB_ACTIVE:
            m = _PROGRESS_RE.search(job.get("message", ""))
            pct = int(100 * int(m.group(1)) / max(int(m.group(2)), 1)) if m else 5
            st.markdown(
                f'<div class="job-card"><div class="job-title">⏳ Scraping en cours</div>'
                f'<div class="job-bar"><div class="job-fill" style="width:{pct}%"></div></div>'
                f'<div class="job-meta">{pct}% · détails dans « Mise à jour »</div></div>',
                unsafe_allow_html=True)

        st.checkbox("🟢 Suivi en temps réel", value=True, key="live",
                    help="Rafraîchit l'écran toutes les 2 s pendant un scraping.")
        st.markdown(
            f'<div style="font-size:.6rem;color:rgba(200,168,176,.4);margin-top:.4rem">'
            f'Leclerc Blagnac · magasin {STORE_CODE}</div>', unsafe_allow_html=True)
    return slug


# ═══════════════════════════════════════════════════════════════════════════
# ÉCRAN 1 — CLASSEMENT
# ═══════════════════════════════════════════════════════════════════════════

_NOTE_STEPS = [0.0, 2.5, 3.0, 3.2, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.1, 4.2, 4.3, 4.4, 4.5]


def _filter_keys(slug: str) -> list:
    return [f"f_{n}_{slug}" for n in ("search", "avail", "pmin", "pmax", "note", "regions",
                                      "grapes", "vintage")]


def apply_filters(wines: list, slug: str) -> list:
    """Barre de recherche + tri + filtres avancés. Retourne les vins filtrés et triés."""
    k = lambda n: f"f_{n}_{slug}"   # noqa: E731
    c1, c2, c3 = st.columns([3, 2, 2])
    search = c1.text_input("Rechercher", key=k("search"),
                           placeholder="Nom, domaine ou région : Bordeaux, Guigal, Pomerol…")
    c2.selectbox("Trier par", list(SORTS), key="sort_key")
    only_dispo = c3.toggle("🏪 Seulement les vins en rayon", value=True, key=k("avail"))

    price_max = max((w.get("price") or 0 for w in wines), default=200)
    ceil = max(200, int(math.ceil(price_max / 10) * 10))
    with st.expander("Plus de filtres : prix, note, région, cépage…"):
        a, b, c = st.columns(3)
        pmin = a.number_input("Prix min (€)", 0, ceil, 0, 1, key=k("pmin"))
        pmax = b.number_input("Prix max (€)", 0, ceil, ceil, 1, key=k("pmax"))
        note_min = c.select_slider("Note Vivino minimale", _NOTE_STEPS, value=0.0, key=k("note"),
                                   format_func=lambda x: "Toutes" if x == 0 else f"≥ {x:.1f} ★")
        regions = sorted({r for w in wines if (r := w.get("region") or extract_region(w["name"]))})
        grapes = sorted({g for w in wines for g in (w.get("grapes") or []) if g})
        d, e = st.columns(2)
        sel_regions = d.multiselect("Région", regions, key=k("regions"), placeholder="Toutes")
        sel_grapes = e.multiselect("Cépage", grapes, key=k("grapes"), placeholder="Tous") if grapes else []
        only_vintage = st.checkbox("✅ Millésime Leclerc confirmé par Vivino", key=k("vintage"))
        if st.button("↺ Réinitialiser les filtres", key=f"reset_{slug}"):
            for key in _filter_keys(slug):
                st.session_state.pop(key, None)
            st.rerun()

    lo, hi = min(pmin, pmax), max(pmin, pmax)
    out = [w for w in wines
           if lo <= (w.get("price") or 0) <= hi
           and (note_min == 0 or (w.get("rating") and w["rating"] >= note_min))
           and (not search or _fuzzy_match(search, w["name"]) or _fuzzy_match(search, w.get("region") or ""))
           and (not only_vintage or w.get("vintage_match") is True)
           and (not only_dispo or w.get("available", True))
           and (not sel_regions or w.get("region", "") in sel_regions)
           and (not sel_grapes or any(g in (w.get("grapes") or []) for g in sel_grapes))]
    out.sort(key=SORTS.get(st.session_state.get("sort_key"), next(iter(SORTS.values()))))

    n_dispo = sum(1 for w in out if w.get("available", True))
    if len(out) < len(wines):
        st.caption(f"🔍 **{len(out)}** vins correspondent sur {len(wines)} · {n_dispo} en rayon")
    else:
        st.caption(f"📋 **{len(wines)}** vins · {n_dispo} en rayon")
    return out


def _reject_form(w: dict, slug: str, uid: str) -> None:
    """Formulaire « ce lien Vivino est incorrect » (dans un popover sous la carte)."""
    reason = st.selectbox("Pourquoi ?", list(REJECTION_REASONS), key=f"reason_{uid}",
                          format_func=lambda r: REJECTION_REASONS[r])
    st.caption("Le vin sera masqué côté Vivino et ne sera plus rempli automatiquement. "
               "Vous pourrez corriger la note à la main dans Réglages.")
    if st.button("Confirmer", key=f"confirm_{uid}", type="primary"):
        vc = load_vivino_cache(slug)
        q = build_query(w["name"])
        old = vc.get(q, {})
        save_vivino_rejection(wine_name=w["name"], query=q, rejected_url=w.get("vivino_url", ""),
                              rejected_title=old.get("vivino_name") or old.get("vivino_url", ""),
                              reason=reason)
        vc[q] = {"rating": None, "ratings_count": 0, "vivino_url": "", "vivino_year": None,
                 "vintage_match": None, "match_confidence": None, "manual_override": True,
                 "suppressed": True, "locked": True, "cached_at": time.time()}
        save_vivino_cache(vc, slug)
        _force_reload()
        st.toast(f"Lien Vivino écarté · {REJECTION_REASONS[reason]}", icon="🚫")
        st.rerun()


def view_list(filtered: list, slug: str, drops: list) -> None:
    prices = [w["price"] for w in filtered if w.get("price")]
    rated = [w["rating"] for w in filtered if w.get("rating")]
    best = max(filtered, key=lambda w: w.get("score") or 0, default=None)
    m1, m2, m3 = st.columns(3)
    m1.metric("🏆 Meilleur rapport qualité/prix",
              f"{best['score']:.2f}" if best and best.get("score") else "—",
              help=best["name"][:60] if best else None)
    m2.metric("💶 Fourchette de prix",
              f"{min(prices):.2f} – {max(prices):.2f} €".replace(".", ",") if prices else "—")
    m3.metric("⭐ Meilleure note", f"★ {max(rated):.2f}" if rated else "—")

    dropped = [d for d in drops if d["wine"] in filtered]
    if dropped:
        lines = " · ".join(f"**{d['wine']['name'][:32]}** {d['prev_price']:.2f}€ → "
                           f"{d['curr_price']:.2f}€ (-{d['pct']:.0f}%)" for d in dropped[:4])
        st.success(f"📉 **{len(dropped)} vin(s) moins cher(s) qu'au dernier relevé** — {lines}"
                   + (" …" if len(dropped) > 4 else ""), icon=None)
    n_bad = sum(1 for w in filtered if w.get("vintage_match") is False)
    if n_bad:
        st.warning(f"⚠️ {n_bad} vin(s) dont le millésime diffère entre Leclerc et Vivino (bordure orange).")

    if not filtered:
        st.info("Aucun vin ne correspond aux filtres actifs.")
        return

    max_score = max((w.get("score") or 0 for w in filtered), default=1)
    page_key, size_key = f"rank_page_{slug}", f"rank_size_{slug}"
    if st.session_state.get(size_key) != len(filtered):
        st.session_state[page_key] = 0
        st.session_state[size_key] = len(filtered)
    page = st.session_state.get(page_key, 0)
    n_pages = max(1, math.ceil(len(filtered) / CARDS_PER_PAGE))
    page = min(page, n_pages - 1)
    start = page * CARDS_PER_PAGE

    for i, w in enumerate(filtered[start:start + CARDS_PER_PAGE]):
        uid = f"{slug}_{w.get('ean') or i}_{page}"
        st.markdown(wine_card_html(w, start + i + 1, max_score), unsafe_allow_html=True)
        if w.get("vivino_url"):
            with st.popover("🚫 Ce n'est pas le bon vin sur Vivino ?"):
                _reject_form(w, slug, uid)

    if n_pages > 1:
        p1, p2, p3 = st.columns([1, 2, 1])
        if page > 0 and p1.button("← Précédent", key=f"prev_{slug}", width="stretch"):
            st.session_state[page_key] = page - 1
            st.rerun()
        p2.markdown(f'<div class="page-info">Page {page + 1}/{n_pages} · {len(filtered)} vins</div>',
                    unsafe_allow_html=True)
        if page < n_pages - 1 and p3.button("Suivant →", key=f"next_{slug}", width="stretch"):
            st.session_state[page_key] = page + 1
            st.rerun()


def view_deals(filtered: list) -> None:
    """Vins très bien notés, bien documentés et abordables."""
    a, b, c = st.columns(3)
    min_rating = a.select_slider("Note minimale", [3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.1, 4.2, 4.3, 4.4, 4.5],
                                 value=4.0, format_func=lambda x: f"≥ {x:.1f} ★")
    top = min(int((max((w.get("price") or 0 for w in filtered), default=50)) + 5), 100)
    max_price = b.slider("Prix maximum (€)", 5, max(top, 10), min(15, max(top, 10)), step=5)
    min_avis = c.select_slider("Avis minimum", [50, 100, 200, 500, 1000, 2000, 5000], value=500,
                               format_func=lambda x: f"≥ {x:,}".replace(",", " "))

    def pick(r, p, n):
        return sorted([w for w in filtered if (w.get("rating") or 0) >= r
                       and (w.get("price") or 999) <= p and (w.get("ratings_count") or 0) >= n
                       and w.get("available", True)], key=lambda x: -(x.get("score") or 0))

    deals = pick(min_rating, max_price, min_avis)
    if deals:
        st.success(f"🎉 {len(deals)} bonne(s) affaire(s)")
    else:
        deals = pick(max(3.5, min_rating - 0.2), max_price + 5, max(50, min_avis // 5))[:20]
        if deals:
            st.warning(f"Critères assouplis automatiquement — {len(deals)} résultat(s)")
        else:
            st.info("Aucune bonne affaire pour l'instant. Complétez les notes Vivino dans « Mise à jour ».")

    for i, w in enumerate(deals[:30]):
        trend = {"↑": '<span class="p-up">↑</span>', "↓": '<span class="p-down">↓</span>',
                 "=": '<span class="p-eq">=</span>'}.get(w.get("price_trend", ""), "")
        lec = (f'<a href="{_html.escape(w["url"])}" target="_blank" class="lnk lnk-lec">🛒 Leclerc</a>'
               if w.get("url") else "")
        viv = (f'<a href="{_html.escape(w["vivino_url"])}" target="_blank" class="lnk lnk-viv">🍷 Vivino</a>'
               if w.get("vivino_url") else "")
        reg = f"🗺️ {_html.escape(w['region'])} · " if w.get("region") else ""
        icon = {0: "🥇", 1: "🥈", 2: "🥉"}.get(i, f"#{i + 1}")
        st.markdown(f"""
<div class="deal-card {'d-top' if i < 3 else ''}">
  <div style="text-align:center;min-width:56px"><div style="font-size:1.4rem;line-height:1">{icon}</div>
    <div class="deal-score">{(w.get('score') or 0):.2f}</div><div class="deal-label">score</div></div>
  <div class="deal-body"><div class="deal-name">{_html.escape(w['name'])}</div>
    <div class="deal-meta">{reg}★ {(w.get('rating') or 0):.1f} · {fmt_count(w.get('ratings_count', 0))} avis</div>
    <div class="wine-links" style="margin-top:.3rem">{lec}{viv}</div></div>
  <div class="deal-price"><strong>{(w.get('price') or 0):.2f} €</strong>{trend}</div>
</div>""", unsafe_allow_html=True)

    if deals:
        lines = []
        for i, w in enumerate(deals[:30], 1):
            parts = [p for p in (f"★{w['rating']:.1f}" if w.get("rating") else "",
                                 f"{w['ratings_count']:,}".replace(",", " ") + " avis" if w.get("ratings_count") else "",
                                 w.get("region", ""), f"{w.get('price', 0):.2f} €".replace(".", ",")) if p]
            lines.append(f"{i}. {w['name']} — {' · '.join(parts)}")
        st.text_area("📋 Liste à copier", "\n".join(lines), height=140)


def _bar(df: pd.DataFrame, x: str, y: str, color: str, horizontal: bool = False, height: int = 190):
    import altair as alt
    enc = (dict(y=alt.Y(f"{x}:O", sort="-x", title=None), x=alt.X(f"{y}:Q", title="Nb vins"))
           if horizontal else dict(x=alt.X(f"{x}:O", sort=None, title=None), y=alt.Y(f"{y}:Q", title="Nb vins")))
    return (alt.Chart(df).mark_bar(color=color, cornerRadiusTopRight=4, cornerRadiusTopLeft=4)
            .encode(tooltip=[f"{x}:O", f"{y}:Q"], **enc)
            .properties(height=height, background="transparent")
            .configure_view(strokeWidth=0, fill="transparent")
            .configure_axis(grid=False, labelColor="#8B6B72", titleColor="#8B6B72"))


def view_stats(filtered: list) -> None:
    if not filtered:
        st.info("Aucun vin à analyser : élargissez les filtres.")
        return
    rated = [w for w in filtered if w.get("rating")]
    priced = [w for w in filtered if w.get("price")]
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Vins analysés", len(filtered))
    s2.metric("Notés sur Vivino", f"{len(rated)}/{len(filtered)}")
    s3.metric("Prix médian", f"{sorted(w['price'] for w in priced)[len(priced) // 2]:.2f} €" if priced else "—")
    s4.metric("Note médiane", f"{sorted(w['rating'] for w in rated)[len(rated) // 2]:.1f} ★" if rated else "—")
    st.divider()

    a, b = st.columns(2)
    with a:
        st.markdown("**Répartition des notes Vivino**")
        if rated:
            cut = pd.cut(pd.Series([w["rating"] for w in rated]), bins=[2.5, 3.0, 3.3, 3.6, 3.9, 4.2, 4.5, 5.1],
                         labels=["2.5-3.0", "3.0-3.3", "3.3-3.6", "3.6-3.9", "3.9-4.2", "4.2-4.5", "4.5+"])
            counts = cut.value_counts().sort_index()
            st.altair_chart(_bar(pd.DataFrame({"Note": counts.index.astype(str), "Nb": counts.values}),
                                 "Note", "Nb", "#C9A84C"), width="stretch")
        else:
            st.caption("Aucune note disponible.")
    with b:
        st.markdown("**Répartition des prix**")
        if priced:
            pm = max(w["price"] for w in priced)
            step = 5 if pm <= 50 else (10 if pm <= 100 else 20)
            bins = list(range(0, int(pm) + step + 1, step))
            cut = pd.cut(pd.Series([w["price"] for w in priced]), bins=bins, right=False,
                         labels=[f"{bins[i]}-{bins[i + 1]}" for i in range(len(bins) - 1)])
            counts = cut.value_counts().sort_index()
            st.altair_chart(_bar(pd.DataFrame({"Tranche": counts.index.astype(str), "Nb": counts.values}),
                                 "Tranche", "Nb", "#6B1A2A"), width="stretch")

    st.divider()
    regs = pd.Series([w.get("region") or "Inconnue" for w in filtered]).value_counts().head(10)
    st.markdown("**Vins par région**")
    st.altair_chart(_bar(pd.DataFrame({"Région": regs.index, "Nb": regs.values}), "Région", "Nb",
                         "#6B1A2A", horizontal=True, height=240), width="stretch")

    ups = sum(1 for w in filtered if w.get("price_trend") == "↑")
    downs = sum(1 for w in filtered if w.get("price_trend") == "↓")
    eqs = sum(1 for w in filtered if w.get("price_trend") == "=")
    if ups + downs + eqs:
        st.divider()
        t1, t2, t3 = st.columns(3)
        t1.metric("📉 En baisse", downs)
        t2.metric("📈 En hausse", ups)
        t3.metric("➡️ Stables", eqs)
    _price_history_chart(filtered)


def _price_history_chart(filtered: list) -> None:
    ph = load_price_history()
    rows = [{"Nom": e.get("name", ean)[:50], "Date": r["date"], "Prix": r["price"]}
            for ean, e in ph.items() for r in e.get("history", [])]
    if not rows:
        return
    import altair as alt
    st.divider()
    st.markdown("**Historique des prix d'un vin**")
    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"])
    name = st.selectbox("Vin", sorted(df["Nom"].unique()), label_visibility="collapsed")
    sel = df[df["Nom"] == name].sort_values("Date")
    if len(sel) < 2:
        st.caption("Un seul relevé pour ce vin : revenez après la prochaine mise à jour du catalogue.")
        return
    st.altair_chart(alt.Chart(sel).mark_line(point=True, strokeWidth=2, color="#6B1A2A").encode(
        x=alt.X("Date:T", title=None), y=alt.Y("Prix:Q", title="Prix (€)", scale=alt.Scale(zero=False)),
        tooltip=["Date:T", "Prix:Q"]).properties(height=190), width="stretch")
    first, last = sel["Prix"].iloc[0], sel["Prix"].iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Premier prix", f"{first:.2f} €")
    c2.metric("Dernier prix", f"{last:.2f} €", delta=f"{last - first:+.2f} €")
    c3.metric("Relevés", len(sel))


def view_table(filtered: list, wines: list, slug: str) -> None:
    today = datetime.now().strftime("%Y%m%d")
    df_f, df_all = _make_wines_df(filtered), _make_wines_df(wines)
    st.markdown(f"**Vins filtrés** ({len(filtered)})")
    st.dataframe(df_f, width="stretch", hide_index=True, height=380, column_config=_DF_COL_CONFIG)
    c1, c2, c3 = st.columns(3)
    c1.download_button("⬇️ CSV (vins filtrés)",
                       df_f.drop(columns=["Query"], errors="ignore").to_csv(index=False, sep=";").encode("utf-8-sig"),
                       f"vins_{slug}_{today}.csv", "text/csv", width="stretch")
    c2.download_button("⬇️ CSV (tous les vins)",
                       df_all.drop(columns=["Query"], errors="ignore").to_csv(index=False, sep=";").encode("utf-8-sig"),
                       f"vins_{slug}_complet_{today}.csv", "text/csv", width="stretch")
    lines = []
    for i, w in enumerate(filtered[:50], 1):
        parts = [p for p in (f"★{w['rating']:.1f}" if w.get("rating") else "",
                             f"{w.get('price', 0):.2f} €".replace(".", ","), w.get("region", "")) if p]
        lines.append(f"{i}. {w['name']} — {' · '.join(parts)}")
    c3.download_button("📋 Liste texte", "\n".join(lines).encode("utf-8"), f"vins_{slug}_{today}.txt",
                       "text/plain", width="stretch", help="Prête à coller dans un message")


def screen_ranking(slug: str) -> None:
    wines = get_wines(slug)
    if not wines:
        st.markdown(
            '<div class="empty-state"><div class="empty-state-icon">🍷</div>'
            '<div class="empty-state-title">Aucun vin chargé</div>'
            '<div class="empty-state-sub">Ouvrez l\'écran <strong>🔄 Mise à jour</strong> '
            'et commencez par l\'étape 1 : charger le catalogue Leclerc.</div></div>',
            unsafe_allow_html=True)
        st.button("Aller à « Mise à jour »", type="primary",
                  on_click=lambda: st.session_state.__setitem__("screen", SCREENS[1]))
        return

    drops = get_price_drops(wines, load_price_history())
    if drops:
        top = drops[0]
        key = "_drops_toast_" + "_".join(d["wine"].get("ean", "") for d in drops[:5])
        if not st.session_state.get(key):
            st.session_state[key] = True
            st.toast(f"📉 {top['wine']['name'][:32]} : {top['prev_price']:.2f}€ → {top['curr_price']:.2f}€ "
                     f"(-{top['pct']:.0f}%)", icon="📉")

    filtered = apply_filters(wines, slug)
    view = st.segmented_control("Vue", VIEWS, default=VIEWS[0], key="view", label_visibility="collapsed") or VIEWS[0]
    if view == "Liste":
        view_list(filtered, slug, drops)
    elif view == "Bonnes affaires":
        view_deals(filtered)
    elif view == "Statistiques":
        view_stats(filtered)
    else:
        view_table(filtered, wines, slug)


# ═══════════════════════════════════════════════════════════════════════════
# ÉCRAN 2 — MISE À JOUR
# ═══════════════════════════════════════════════════════════════════════════

def _console(job: dict, height: int = 300, lines: int = 400) -> None:
    has_log = JOB_LOG_PATH.exists() and JOB_LOG_PATH.stat().st_size > 0
    try:
        text = JOB_LOG_PATH.read_text("utf-8") if has_log else ""
    except Exception:
        text = ""
    rows = text.strip().splitlines()[-lines:] or ["(aucun journal)"]
    st.markdown(
        f'<div style="background:#0d1117;color:#c9d1d9;font-family:Consolas,Monaco,monospace;'
        f'font-size:.72rem;line-height:1.5;padding:.75rem 1rem;border-radius:6px;border:1px solid #30363d;'
        f'height:{height}px;overflow-y:auto;white-space:pre-wrap;word-break:break-all">'
        + "".join(_fmt_log_line(r) for r in rows) + "</div>", unsafe_allow_html=True)


def _job_card(job: dict, slug: str) -> None:
    msg = job.get("message", "")
    m = _PROGRESS_RE.search(msg)
    label = next((k for k, v in WINE_TYPES.items() if v == job.get("slug")), job.get("slug", ""))
    if m:
        done, total = int(m.group(1)), int(m.group(2))
        pct = int(100 * done / max(total, 1))
        meta = f"{done} / {total} vins · {pct}% · {fmt_age(job.get('updated_at', 0))}"
    else:
        pct, meta = 5, f"démarrage… {fmt_age(job.get('updated_at', 0))}"
    st.markdown(
        f'<div class="job-card"><div class="job-title">⏳ Scraping Vivino en cours · {label}</div>'
        f'<div class="job-bar"><div class="job-fill" style="width:{pct}%"></div></div>'
        f'<div class="job-meta">{meta}</div></div>', unsafe_allow_html=True)
    if job.get("slug") == slug and st.button("⏹ Arrêter le scraping", type="secondary"):
        ckpt_finish(slug)
        save_job_state({"status": "done", "slug": slug, "mode": "stopped",
                        "message": "Arrêté par l'utilisateur.", "finished_at": time.time()})
        st.rerun()


def _step(num: int, title: str, text: str) -> None:
    st.markdown(f'<div class="tab-section">{num} · {title}</div>', unsafe_allow_html=True)
    st.caption(text)


def _launch(slug: str, mode: str, ok_msg: str) -> None:
    if start_background_job(slug, mode):
        st.toast(ok_msg, icon="🚀")
        st.rerun()
    else:
        st.warning("Une tâche est déjà en cours.")


def screen_update(slug: str) -> None:
    lc, vc = load_leclerc_cache(slug), load_vivino_cache(slug)
    cov, job = coverage(slug, lc, vc), load_job_state()
    running = job.get("status") in _JOB_ACTIVE

    c1, c2, c3 = st.columns(3)
    c1.metric("📦 Catalogue Leclerc", f"{cov['total']} vins" if lc else "—",
              help=f"Mis à jour {fmt_age(lc['cached_at'])}" if lc else "Pas encore chargé")
    c2.metric("🍷 Notes Vivino", f"{cov['rated']}/{cov['total']}" if lc else "—",
              delta=f"{cov['missing']} manquantes" if cov["missing"] else None, delta_color="off")
    c3.metric("⏳ Notes obsolètes", cov["stale"], help=f"Plus de {VIVINO_CACHE_TTL_DAYS} jours")

    if running:
        _job_card(job, slug)
        st.markdown("**Journal en direct**")
        _console(job)
        return

    if job.get("status") == "done" and job.get("slug") == slug:
        key = f"_done_toast_{job.get('finished_at', 0)}"
        if not st.session_state.get(key):
            st.session_state[key] = True
            st.toast(job.get("message", "✅ Terminé"), icon="✅")
    elif job.get("status") == "error":
        key = f"_err_toast_{job.get('finished_at', 0)}"
        if not st.session_state.get(key):
            st.session_state[key] = True
            st.toast(f"❌ {job.get('error', 'Erreur inconnue')}", icon="❌")

    # ── Reprise d'un scraping interrompu ─────────────────────────────────
    ckpt = ckpt_load(slug)
    if ckpt:
        n_done = len(ckpt["done_eans"])
        pct = int(100 * n_done / max(ckpt["total"], 1))
        st.markdown(
            f'<div class="job-card" style="border-left-color:#f59e0b">'
            f'<div class="job-title" style="color:#f59e0b">⚠️ Scraping Vivino interrompu</div>'
            f'<div class="job-bar"><div class="job-fill" style="width:{pct}%;background:#f59e0b"></div></div>'
            f'<div class="job-meta">{n_done}/{ckpt["total"]} vins traités ({pct}%) · '
            f'{fmt_age(ckpt["started_at"])} — vos notes déjà acquises sont conservées</div></div>',
            unsafe_allow_html=True)
        r1, r2 = st.columns(2)
        if r1.button("▶️ Reprendre là où ça s'est arrêté", type="primary", width="stretch"):
            _launch(slug, "resume", "Reprise du scraping Vivino lancée.")
        if r2.button("✖ Abandonner la reprise", width="stretch"):
            ckpt_finish(slug)
            st.rerun()

    # ── Étape 1 : catalogue ───────────────────────────────────────────────
    st.divider()
    _step(1, "Catalogue Leclerc",
          "Lit les vins du magasin de Blagnac : disponibilité, prix, nouveautés. "
          "Les notes Vivino déjà acquises ne sont pas touchées.")
    b1, b2 = st.columns([2, 1])
    if b1.button("🔄 Mettre à jour le catalogue", type="primary" if not lc else "secondary", width="stretch"):
        with st.status("Lecture du catalogue Leclerc…", expanded=True) as status:
            log, _ = _make_logger(10)
            try:
                raw = run_check_stock(slug, log=log)
            except Exception as e:
                status.update(label="Échec de la lecture du catalogue", state="error")
                st.error(f"❌ {e}\n\nSi Chromium est introuvable, vérifiez `packages.txt` "
                         "(chromium, chromium-driver).")
                st.stop()
            n_dispo = sum(1 for w in raw if w.get("available", True))
            status.update(label=f"✅ {n_dispo} vins en rayon sur {len(raw)}", state="complete")
        _force_reload()
        st.toast(f"✅ Catalogue à jour : {n_dispo} vins en rayon sur {len(raw)}", icon="🔄")
        st.rerun()
    if cov["zero_price"] and b2.button(f"🔧 Réparer {cov['zero_price']} prix à 0 €", width="stretch"):
        with st.status("Réparation des prix…", expanded=True) as status:
            log, _ = _make_logger(10)
            try:
                fixed = repair_zero_prices(slug, log=log)
                status.update(label=f"✅ {fixed} prix réparés" if fixed else "⚠️ Aucun prix récupéré",
                              state="complete" if fixed else "error")
                _force_reload()
            except Exception as e:
                st.error(f"❌ {e}")

    # ── Étape 2 : notes Vivino ────────────────────────────────────────────
    st.divider()
    _step(2, "Notes Vivino",
          "Cherche chaque vin sur Vivino (en arrière-plan : vous pouvez changer d'écran). "
          "Un scraping interrompu peut être repris ; les notes déjà trouvées sont conservées.")
    if not lc:
        st.info("Commencez par l'étape 1 pour charger le catalogue.")
    else:
        a, b, c = st.columns(3)
        with a:
            st.markdown("**Compléter les manquants**")
            st.caption(f"{cov['missing']} vin(s) sans note. Le plus rapide, à faire en premier.")
            if st.button("🔎 Compléter", key="go_fill", disabled=cov["missing"] == 0, width="stretch",
                         type="primary" if cov["missing"] else "secondary"):
                _launch(slug, "fill_missing", "Complétion des notes manquantes lancée.")
        with b:
            st.markdown("**Rafraîchir les obsolètes**")
            st.caption(f"{cov['stale']} note(s) de plus de {VIVINO_CACHE_TTL_DAYS} jours.")
            if st.button("⏳ Rafraîchir", key="go_stale", disabled=cov["stale"] == 0, width="stretch"):
                _launch(slug, "refresh_stale", "Rafraîchissement des notes obsolètes lancé.")
        with c:
            st.markdown("**Tout refaire**")
            st.caption("Relance la recherche pour tous les vins non verrouillés. Long.")
            if st.button("🍷 Tout rafraîchir", key="go_all", width="stretch"):
                ckpt_finish(slug)
                _launch(slug, "refresh_all", "Scraping Vivino complet lancé.")

    st.divider()
    with st.expander("🖥️ Journal du dernier scraping"):
        _console(job, height=260)


# ═══════════════════════════════════════════════════════════════════════════
# ÉCRAN 3 — RÉGLAGES
# ═══════════════════════════════════════════════════════════════════════════

def _backup_section() -> None:
    n_viv = sum(_file_is_valid(_viv_path(s), _valid_vivino) for s in _ALL_SLUGS)
    n_lec = sum(_file_is_valid(_lec_path(s), _valid_leclerc) for s in _ALL_SLUGS)
    can_push, can_restore = _gist_is_configured(), _gist_can_restore()
    st.caption(f"Données sur ce serveur : {n_lec}/4 catalogues · {n_viv}/4 fichiers de notes Vivino.")
    if not (can_push or can_restore):
        st.info("Sauvegarde Gist **non configurée** : les données sont perdues quand l'hébergement se met en veille.")
        url = st.text_input("URL d'un Gist à restaurer", key="gist_restore_url",
                            placeholder="https://gist.github.com/…")
        if url and st.button("↓ Restaurer depuis cette URL"):
            with st.spinner("Restauration…"):
                n = restore_from_gist()
            if n:
                st.session_state.pop("_startup_done", None)
                _force_reload()
                st.toast(f"✅ {n} fichier(s) restauré(s)", icon="💾")
                st.rerun()
            else:
                st.warning("Rien restauré : URL invalide ou Gist vide.")
        st.markdown("Pour activer la sauvegarde automatique : créez un token GitHub (scope `gist`) puis "
                    "ajoutez dans les secrets de l'app :\n```toml\n[gist]\ngithub_token = \"ghp_xxx\"\ngist_id = \"\"\n```")
        return
    a, b = st.columns(2)
    if can_push and a.button("↑ Sauvegarder maintenant", width="stretch"):
        with st.spinner("Sauvegarde…"):
            ok = 0
            paths = ([_lec_path(s) for s in _ALL_SLUGS] + [_viv_path(s) for s in _ALL_SLUGS]
                     + [_price_hist_path(), REJECTION_LOG_PATH])
            for p in paths:
                if p.exists():
                    ok += bool(gist_push(p.name, p.read_text("utf-8")))
        st.toast(f"☁️ {ok} fichier(s) sauvegardé(s)", icon="✅")
    if b.button("↓ Restaurer depuis le Gist", width="stretch", disabled=not can_restore):
        with st.spinner("Restauration…"):
            n = restore_from_gist()
        if n:
            st.session_state.pop("_startup_done", None)
            _force_reload()
            st.toast(f"✅ {n} fichier(s) restauré(s)", icon="💾")
            st.rerun()
        else:
            st.warning("Rien restauré : Gist vide ou inaccessible.")


def _rejections_section() -> None:
    rejs = load_vivino_rejections()
    if not rejs:
        st.caption("Aucun rejet enregistré. Utilisez « 🚫 Ce n'est pas le bon vin sur Vivino ? » sous un vin du classement.")
        return
    total = sum(len(v.get("history", [])) for v in rejs.values())
    hard = sum(1 for v in rejs.values() if v.get("hard_to_match"))
    a, b, c = st.columns(3)
    a.metric("Rejets", total)
    b.metric("Vins concernés", len(rejs))
    c.metric("Difficiles à trouver", hard, help="≥ 3 rejets : Vivino est ignoré pour ces vins.")
    rows = [{"Requête": q, "Nom Leclerc": h.get("wine_name", "")[:45], "URL rejetée": h.get("rejected_url", ""),
             "Raison": REJECTION_REASONS.get(h.get("reason", ""), h.get("reason", "")),
             "Date": time.strftime("%d/%m/%y %H:%M", time.localtime(h.get("ts", 0)))}
            for q, e in sorted(rejs.items()) for h in e.get("history", [])]
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True, height=300,
                 column_config={"URL rejetée": st.column_config.LinkColumn(display_text="🔗")})
    if st.button("🗑️ Effacer tous les rejets"):
        REJECTION_LOG_PATH.unlink(missing_ok=True)
        st.toast("Rejets effacés.", icon="🗑️")
        st.rerun()


def _manual_section(slug: str, wines: list) -> None:
    names = sorted({w["name"] for w in wines})
    if not names:
        st.caption("Chargez d'abord le catalogue.")
        return
    name = st.selectbox("Vin à corriger", names, key=f"manual_name_{slug}")
    vc = load_vivino_cache(slug)
    key = build_query(name)
    cur = vc.get(key, {})
    a, b, c = st.columns(3)
    rating = a.text_input("Note (0-5)", "" if cur.get("rating") is None else str(cur["rating"]), key=f"m_rating_{slug}_{key}")
    count = b.number_input("Nombre d'avis", 0, step=1, value=int(cur.get("ratings_count") or 0), key=f"m_count_{slug}_{key}")
    year = c.text_input("Millésime Vivino", "" if cur.get("vivino_year") is None else str(cur["vivino_year"]), key=f"m_year_{slug}_{key}")
    url = st.text_input("URL Vivino", cur.get("vivino_url", ""), key=f"m_url_{slug}_{key}")
    s1, s2, s3 = st.columns(3)
    if s1.button("💾 Enregistrer (verrouille)", width="stretch"):
        try:
            r = None if not rating.strip() else float(rating.replace(",", "."))
            if r is not None and not 0 <= r <= 5:
                raise ValueError("la note doit être entre 0 et 5")
            y = None if not year.strip() else int(year)
            if r is None and not url.strip():
                raise ValueError("indiquez au moins une note ou une URL (sinon « Supprimer »)")
            vc[key] = {"rating": r, "ratings_count": int(count or 0), "vivino_url": url.strip(),
                       "vivino_year": y, "vintage_match": None, "manual_override": True,
                       "suppressed": False, "locked": True, "cached_at": time.time()}
            save_vivino_cache(vc, slug)
            _force_reload()
            st.success("Correction enregistrée et verrouillée.")
        except Exception as e:
            st.error(f"Valeur invalide : {e}")
    if s2.button("🧹 Supprimer l'info Vivino", width="stretch"):
        vc[key] = {"rating": None, "ratings_count": 0, "vivino_url": "", "vivino_year": None,
                   "vintage_match": None, "manual_override": True, "suppressed": True,
                   "locked": True, "cached_at": time.time()}
        save_vivino_cache(vc, slug)
        _force_reload()
        st.success("Info Vivino supprimée et verrouillée (ne sera plus remplie automatiquement).")
    if s3.button("🔓 Déverrouiller", width="stretch"):
        if key in vc:
            vc[key] = {**vc[key], "locked": False, "suppressed": False, "manual_override": False}
            save_vivino_cache(vc, slug)
            _force_reload()
        st.success("Déverrouillé : le prochain scraping pourra le recalculer.")


def _cache_section(slug: str) -> None:
    vc = load_vivino_cache(slug)
    st.caption(f"{len(vc)} entrées · {sum(1 for v in vc.values() if v.get('rating'))} notes · "
               f"{sum(1 for v in vc.values() if v.get('vivino_url'))} URLs")
    num = lambda v: float(v) if v not in (None, "") else None   # noqa: E731
    df = pd.DataFrame([{
        "Requête": k, "Note": num(v.get("rating")), "Nb avis": num(v.get("ratings_count")),
        "Vivino": v.get("vivino_url") or "", "Confiance": num(v.get("match_confidence")),
        "Type": vivino_cache_type(v), "🔒": "🔒" if v.get("locked") else "",
        "Màj": fmt_age(v.get("cached_at", 0))} for k, v in vc.items()])
    st.dataframe(df, width="stretch", hide_index=True, height=360, column_config={
        "Vivino": st.column_config.LinkColumn(display_text="🍷"),
        "Note": st.column_config.NumberColumn(format="%.1f"),
        "Nb avis": st.column_config.NumberColumn(format="%d"),
        "Confiance": st.column_config.NumberColumn(format="%.2f")})


def screen_settings(slug: str) -> None:
    wines = get_wines(slug)
    st.markdown('<div class="tab-section">☁️ Sauvegarde des données</div>', unsafe_allow_html=True)
    _backup_section()
    st.divider()
    st.markdown('<div class="tab-section">✏️ Corriger une note Vivino</div>', unsafe_allow_html=True)
    _manual_section(slug, wines)
    st.divider()
    st.markdown('<div class="tab-section">🚫 Liens Vivino rejetés</div>', unsafe_allow_html=True)
    _rejections_section()
    st.divider()
    with st.expander("🗃️ Cache Vivino (détail technique)"):
        _cache_section(slug)


# ═══════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════════════════════

def run() -> None:
    st.markdown(
        '<div class="main-title">🍷 Cave <span>Leclerc Blagnac</span> × Vivino</div>'
        '<div class="subtitle">Les vins du rayon, classés par rapport qualité / prix</div>'
        '<hr class="title-rule">', unsafe_allow_html=True)
    _run_startup()
    st.session_state.setdefault("screen", SCREENS[0])
    slug = sidebar()
    _start_polling(load_job_state())
    screen = st.session_state.get("screen", SCREENS[0])
    if screen == SCREENS[1]:
        screen_update(slug)
    elif screen == SCREENS[2]:
        screen_settings(slug)
    else:
        screen_ranking(slug)

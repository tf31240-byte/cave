"""Orchestration : tâches de fond, mises à jour Leclerc / Vivino, reprise."""
import json
import time
import threading
# AUTO-IMPORTS
from .storage import _JOB_FLUSH_INTERVAL, _invalidate_mem_cache, _job_lock, _read_json_cached, ckpt_create, ckpt_finish, ckpt_load, ckpt_tick, get_rejected_urls, is_hard_to_match, load_leclerc_cache, load_price_history, load_vivino_cache, load_vivino_rejections, save_leclerc_cache, save_vivino_cache, update_price_history
from .vivino import _vivino_is_blocked, build_query, fetch_vivino, fetch_vivino_via_api
from .config import JOB_LOG_PATH, JOB_STATE_PATH, VIVINO_CACHE_TTL_DAYS, _VIVINO_SEL_WORKERS
from .leclerc import _scrape_all_leclerc_pages, _set_store_cookie, check_availability, scrape_leclerc_full
from .regions import extract_region
from .browser import make_driver
from .scoring import _merge_vivino, fmt_age, fmt_count
# END-AUTO

def load_job_state() -> dict:
    data = _read_json_cached(JOB_STATE_PATH, ttl=0.5)  # TTL court : réactivité temps réel console
    return data if isinstance(data, dict) else {}


def save_job_state(state: dict) -> None:
    """Écriture best-effort : les erreurs sont silencieuses pour ne pas
    bloquer le thread de scraping si le FS est temporairement indisponible."""
    tmp = JOB_STATE_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(JOB_STATE_PATH)
        _invalidate_mem_cache(JOB_STATE_PATH)
    except Exception:
        tmp.unlink(missing_ok=True)


def _set_job_state(**kwargs) -> None:
    """
    Mise à jour du job state avec flush différé (≤1 écriture disque/s).
    Seuls status=done/error forcent un flush immédiat pour ne pas perdre
    le résultat final si le process se termine juste après.
    """
    global _job_buf, _job_buf_last_flush
    with _job_lock:
        _job_buf.update(kwargs)
        _job_buf["updated_at"] = time.time()
        force = kwargs.get("status") in {"done", "error"}
        now   = time.time()
        if force or now - _job_buf_last_flush >= _JOB_FLUSH_INTERVAL:
            state = load_job_state()
            state.update(_job_buf)
            save_job_state(state)
            _job_buf_last_flush = now


def _background_job(slug: str, mode: str) -> None:
    global _job_buf, _job_buf_last_flush
    # Fix 1 : vider le buffer du job précédent pour ne pas contaminer
    with _job_lock:
        _job_buf.clear()
        _job_buf_last_flush = 0.0
    # Effacer le log précédent au démarrage d'un nouveau job
    try:
        JOB_LOG_PATH.write_text("", "utf-8")
    except Exception:
        pass
    _set_job_state(status="running", slug=slug, mode=mode, message="Démarrage…", error="")

    def _log(msg: str):
        _set_job_state(message=msg)
        # Écrire chaque sous-ligne séparément → évite le \n littéral dans la console.
        # Certains messages contiennent des \n embarqués (ex. résumé multi-lignes).
        try:
            ts = time.strftime("%H:%M:%S")
            with JOB_LOG_PATH.open("a", encoding="utf-8") as f:
                for sub in msg.splitlines():
                    f.write(f"[{ts}] {sub}\n")
        except Exception:
            pass

    try:
        if mode == "refresh_all":
            raw = run_refresh_vivino(slug, resume=False, log=_log)
        elif mode == "fill_missing":
            raw = run_fill_missing_vivino(slug, log=_log)
        elif mode == "resume":
            raw = run_refresh_vivino(slug, resume=True, log=_log)
        elif mode == "refresh_stale":
            raw = run_refresh_stale_vivino(slug, log=_log)
        else:
            raise ValueError(f"Mode inconnu: {mode}")

        n_rated = sum(1 for w in raw if w.get("rating"))
        _set_job_state(status="done", message=f"✅ Terminé · {n_rated} vins notés", finished_at=time.time())
    except Exception as e:
        _set_job_state(status="error", error=str(e), message=f"❌ {e}", finished_at=time.time())


def start_background_job(slug: str, mode: str) -> bool:
    global _job_thread
    with _job_lock:
        current = load_job_state()
        if current.get("status") in {"running", "queued"}:
            return False
        save_job_state({
            "status": "queued",
            "slug": slug,
            "mode": mode,
            "started_at": time.time(),
            "message": "Mise en file…",
            "error": "",
            "updated_at": time.time(),
        })

    _job_thread = threading.Thread(target=_background_job, args=(slug, mode), daemon=True)
    _job_thread.start()
    return True


# ═══════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════

def load_wines_from_cache(slug: str) -> list:
    lc = load_leclerc_cache(slug)
    if not lc: return []
    return _merge_vivino(lc["wines"], load_vivino_cache(slug), load_price_history())


def run_check_stock(slug: str, log=None) -> list:
    lc = load_leclerc_cache(slug)
    vc = load_vivino_cache(slug)
    if lc:
        if log: log(f"📦 Cache Leclerc ({fmt_age(lc['cached_at'])}) — vérif. stock…")
        wines = check_availability(slug, lc["wines"], log=log)
        save_leclerc_cache(slug, wines)
    else:
        if log: log("🚀 Pas de cache — scrape Leclerc complet…")
        wines = scrape_leclerc_full(slug, log=log)
        for w in wines: w["available"] = True
        save_leclerc_cache(slug, wines)
        if log: log(f"💾 Cache Leclerc sauvegardé ({len(wines)} vins)")
    return _merge_vivino(wines, vc, load_price_history())


def _api_lookup_wine(wine: dict, slug: str,
                     rejections: dict | None = None) -> tuple[str, str, dict]:
    """Appel API Vivino pour un vin (exécuté en parallèle)."""
    key    = build_query(wine["name"])
    region = extract_region(wine["name"])
    _rej   = rejections or {}
    # Vérifier IP bloquée avant de gaspiller une requête
    if _vivino_is_blocked():
        return key, region, None
    if is_hard_to_match(key, _rej):
        return key, region, None
    rejected = get_rejected_urls(key, _rej)
    # Délai léger entre requêtes pour éviter de déclencher le rate-limiter
    time.sleep(0.15)
    result = fetch_vivino_via_api(key, wine.get("vintage"), slug=slug,
                                  rejected_urls=rejected,
                                  grapes_hint=wine.get("grapes_hint") or [])
    return key, region, result


def _make_vc_entry(data: dict) -> dict:
    """Wraps un résultat Vivino avec les métadonnées de cache standard."""
    return {**data, "cached_at": time.time(),
            "locked": False, "manual_override": False, "suppressed": False}


def _scrape_vivino_list(slug, wines, todo, vc, log):
    """
    Boucle de scraping Vivino avec stratégie deux phases :

    Scraping Vivino via Selenium uniquement.
    L'API publique Vivino ignore le paramètre de recherche — Selenium donne
    des résultats précis et fiables.

    Gestion des reprise via checkpoint, sauvegarde incrémentale, vins verrouillés.
    """
    found = 0
    done_count = len(wines) - len(todo)
    interrupted = False
    driver = None

    # ── Précalculer build_query une seule fois pour chaque vin ───────────────
    # Évite 5-7 appels build_query par vin dans la boucle de scraping.
    _key_of: dict[int, str] = {id(w): build_query(w["name"]) for w in todo}

    # ── Séparer vins verrouillés / à traiter ─────────────────────────────────
    locked, to_process = [], []
    for _w in todo:
        (locked if vc.get(_key_of[id(_w)], {}).get("locked") else to_process).append(_w)

    for w in locked:
        ean = w.get("ean") or _key_of[id(w)]
        ckpt_tick(slug, ean)   # Fix 2 : enregistré dans done_eans → sauté proprement en cas de reprise
        done_count += 1
        if log: log(f"  🔒 [{done_count}/{len(wines)}] {w['name'][:38]} (correction manuelle conservée)")

    if not to_process:
        ckpt_finish(slug)
        if log: log(f"✅ Terminé — {found} notes · {len(vc)} entrées cache")
        return

    # Charger les rejets 1× pour toute la session de scraping
    _rejections = load_vivino_rejections()
    n_skip_hard = sum(1 for w in to_process if is_hard_to_match(_key_of[id(w)], _rejections))
    if n_skip_hard and log:
        log(f"  ⚠️ {n_skip_hard} vins skippés (trop de rejets précédents)")

    # ── Phase 1 (API) désactivée ──────────────────────────────────────────────
    # L'API publique Vivino (/api/explore/explore) ignore le paramètre q et retourne
    # les vins les plus populaires globalement (Marqués de Riscal, Meiomi…) sans
    # rapport avec la query. Selenium donne des résultats corrects et précis.
    need_selenium: list[tuple[dict, str]] = [
        (w, extract_region(w["name"])) for w in to_process
    ]

    # ── Selenium (seule phase active) ─────────────────────────────────────
    if not need_selenium:
        ckpt_finish(slug)
        if log: log(f"✅ Terminé — {found} notes · {len(vc)} entrées cache")
        return

    n_w = min(_VIVINO_SEL_WORKERS, len(need_selenium))
    if log: log(f"🌐 Selenium ×{n_w} workers — {len(need_selenium)} vins…")
    _t0 = time.time()

    # _results : key → vd (écrit par workers, lu par thread principal)
    # Thread-safe : workers n'écrivent que dans _results (protégé par lock),
    # le thread principal est le seul à lire/écrire vc.
    _results_lock = threading.Lock()
    _results: dict[str, dict] = {}

    # _pending : key → (wine, region) — construit avec id() unique pour éviter
    # les collisions de build_query sur deux vins au nom similaire
    _pending: dict[str, tuple] = {}
    for w, r in need_selenium:
        k = _key_of[id(w)]
        # Si collision de query (deux vins → même key), on préfixe avec l'EAN
        if k in _pending:
            k = f"{k}__{w.get('ean') or id(w)}"
        _pending[k] = (w, r)
    # Reconstruire need_selenium avec les clés dé-dupliquées
    _ns_keyed: list[tuple[str, dict, str]] = []
    _seen_ids: dict[int, str] = {}
    for w, r in need_selenium:
        base_k = _key_of[id(w)]
        k = base_k if _pending.get(base_k, (w,))[0] is w else f"{base_k}__{w.get('ean') or id(w)}"
        _seen_ids[id(w)] = k
        _ns_keyed.append((k, w, r))

    def _worker_scrape(chunk: list[tuple[str,dict,str]], worker_id: int) -> None:
        """Un worker scrape son chunk avec son propre driver Chrome."""
        _drv = None
        try:
            _drv = make_driver(fast=True)
            for key, wine, region in chunk:
                try:
                    _ku = vc.get(_key_of[id(wine)], {})
                    _ku = _ku.get("vivino_url", "") if not _ku.get("rating") else ""
                    vd = fetch_vivino(_drv, wine["name"], wine.get("vintage"),
                                      slug=slug, region=region, known_url=_ku)
                except Exception:
                    vd = {"rating": None, "ratings_count": 0, "vivino_url": "",
                          "vivino_year": None, "vintage_match": None, "match_confidence": 0.0}
                with _results_lock:
                    _results[key] = vd
        except Exception as _we:
            # Worker planté → marquer tous ses vins non traités comme vides
            # pour ne pas bloquer la boucle de polling
            with _results_lock:
                for _k, _w, _r in chunk:
                    if _k not in _results:
                        _results[_k] = {"rating": None, "ratings_count": 0,
                                        "vivino_url": "", "vivino_year": None,
                                        "vintage_match": None, "match_confidence": 0.0}
        finally:
            if _drv:
                try: _drv.quit()
                except Exception: pass

    # Découper en chunks round-robin pour équilibrer la charge
    chunks: list[list] = [[] for _ in range(n_w)]
    for i, item in enumerate(_ns_keyed):
        chunks[i % n_w].append(item)

    threads = [threading.Thread(target=_worker_scrape, args=(chunk, i), daemon=True)
               for i, chunk in enumerate(chunks) if chunk]
    for t in threads: t.start()

    # Polling : traiter les résultats au fur et à mesure
    _processed: set[str] = set()
    _total = len(_ns_keyed)
    try:
        while len(_processed) < _total:
            # Arrêt anticipé si tous les threads sont morts ET plus rien à venir
            if all(not t.is_alive() for t in threads):
                with _results_lock:
                    _ready_now = {k: v for k, v in _results.items() if k not in _processed}
                # Traiter ce qui reste puis sortir
                for key, vd in _ready_now.items():
                    if key not in _pending: continue
                    wine, region = _pending[key]
                    ean = wine.get("ean") or key
                    _existing = vc.get(_key_of[id(wine)], {})
                    _new_has  = bool(vd.get("rating") or vd.get("vivino_url"))
                    _had_data = bool(_existing.get("rating") or _existing.get("vivino_url"))
                    vc[_key_of[id(wine)]] = _make_vc_entry(vd) if _new_has else (
                        {**_existing, "cached_at": _existing.get("cached_at", time.time())}
                        if _had_data else _make_vc_entry(vd))
                    ckpt_tick(slug, ean)
                    done_count += 1; _processed.add(key)
                    if vd.get("rating"): found += 1
                # Marquer les non-traités comme interrompus
                if len(_processed) < _total:
                    interrupted = True
                break

            with _results_lock:
                _ready = {k: v for k, v in _results.items() if k not in _processed}
            for key, vd in _ready.items():
                if key not in _pending: continue
                wine, region = _pending[key]
                ean = wine.get("ean") or key
                orig_key = _key_of[id(wine)]
                _existing = vc.get(orig_key, {})
                _new_has  = bool(vd.get("rating") or vd.get("vivino_url"))
                _had_data = bool(_existing.get("rating") or _existing.get("vivino_url"))
                if _new_has:
                    vc[orig_key] = _make_vc_entry(vd)
                elif _had_data:
                    vc[orig_key] = {**_existing, "cached_at": _existing.get("cached_at", time.time())}
                else:
                    vc[orig_key] = _make_vc_entry(vd)
                ckpt_tick(slug, ean)
                done_count += 1
                _processed.add(key)
                if vd.get("rating"):
                    found += 1
                    if log: log(f"  ✅ [{done_count}/{len(wines)}] {wine['name'][:38]}\n"
                                f"     ★ {vd['rating']} · {fmt_count(vd.get('ratings_count'))} avis")
                    if found % 20 == 0:
                        save_vivino_cache(vc, slug)
                elif _new_has:
                    if log: log(f"  🔗 [{done_count}/{len(wines)}] {wine['name'][:38]} — URL trouvée")
                elif _had_data:
                    if log: log(f"  ⚠️ [{done_count}/{len(wines)}] {wine['name'][:38]} — non trouvé, données conservées")
                else:
                    if log: log(f"  🔍 [{done_count}/{len(wines)}] {wine['name'][:45]} — aucune note")
            if len(_processed) < _total:
                time.sleep(0.5)
        for t in threads: t.join(timeout=10)   # laisser le temps aux drivers de se fermer
    except Exception as e:
        interrupted = True
        for t in threads: t.join(timeout=3)
        if log: log(f"⚠️ Interrompu à [{done_count}/{len(wines)}] : {e}")
    finally:
        save_vivino_cache(vc, slug, _force_gist=True)
        remaining = len(wines) - done_count
        if interrupted or remaining > 0:
            if log: log(f"\n⚠️ {done_count}/{len(wines)} traités · {remaining} restants\n"
                        f"💡 Cliquez **▶️ Reprendre** pour continuer")
        else:
            ckpt_finish(slug)
            if log: log(f"✅ Terminé — {found} notes · {len(vc)} entrées cache")
        if log and _processed:
            _el = time.time() - _t0
            log(f"⏱️ {_el:.0f} s pour {len(_processed)} vins · {_el/len(_processed):.1f} s/vin "
                f"({n_w} navigateurs)")



def repair_zero_prices(slug: str, log=None) -> int:
    """
    Rescrape uniquement les vins dont price=0 ou price=None dans le cache.
    Utilise Selenium avec cookie magasin pour récupérer les vrais prix.
    Retourne le nombre de vins corrigés.
    """
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("❌ Pas de cache Leclerc.")
        return 0

    zero_price = [w for w in lc["wines"] if not w.get("price")]
    if not zero_price:
        if log: log("✅ Aucun vin avec prix manquant !")
        return 0

    if log: log(f"🔧 {len(zero_price)} vins avec prix=0 détectés, rescraping…")
    driver = None
    fixed = 0
    try:
        driver = make_driver()
        _set_store_cookie(driver)
        fresh_cards = _scrape_all_leclerc_pages(driver, slug, log)
        fresh_by_ean  = {c["ean"]:  c["price"] for c in fresh_cards if c.get("ean") and c.get("price") and c["price"] > 0}
        fresh_by_name = {c["name"]: c["price"] for c in fresh_cards if c.get("price") and c["price"] > 0}

        # Patcher les vins avec prix=0
        wines = [dict(w) for w in lc["wines"]]
        for w in wines:
            if not w.get("price"):
                new_price = fresh_by_ean.get(w.get("ean","")) or fresh_by_name.get(w["name"])
                if new_price:
                    w["price"] = new_price
                    fixed += 1
                    if log: log(f"  ✅ {w['name'][:40]} → {new_price:.2f} €")

        if fixed:
            save_leclerc_cache(slug, wines)
            update_price_history(wines)
            if log: log(f"✅ {fixed}/{len(zero_price)} prix réparés et sauvegardés")
        else:
            if log: log(f"⚠️ Aucun prix récupéré — le site n'a peut-être pas chargé les prix")
    except Exception as e:
        if log: log(f"❌ Erreur : {e}")
    finally:
        if driver is not None:
            try: driver.quit()
            except Exception: pass
    return fixed


def get_price_drops(wines: list, ph: dict) -> list[dict]:
    """
    Retourne les vins dont le prix a baissé depuis le dernier relevé.
    Seuil : ≥ 0,50 € ou ≥ 3% de réduction.
    """
    drops = []
    for w in wines:
        ean = w.get("ean", "")
        price = w.get("price")
        if not ean or not price:
            continue
        hist = ph.get(ean, {}).get("history", [])
        if len(hist) < 2:
            continue
        # Dernier prix connu AVANT le relevé actuel
        prev_price = None
        for entry in reversed(hist[:-1]):
            if entry.get("price") and entry["price"] != price:
                prev_price = entry["price"]
                break
        if prev_price is None:
            continue
        diff  = prev_price - price
        pct   = diff / prev_price * 100
        if diff >= 0.50 and pct >= 3.0:
            drops.append({
                "wine":       w,
                "prev_price": prev_price,
                "curr_price": price,
                "diff":       diff,
                "pct":        pct,
            })
    drops.sort(key=lambda x: x["pct"], reverse=True)
    return drops

def run_refresh_vivino(slug: str, resume: bool = False, log=None) -> list:
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("🚀 Pas de cache Leclerc — scrape complet…")
        wines = scrape_leclerc_full(slug, log=log)
        for w in wines: w["available"] = True
        save_leclerc_cache(slug, wines)
    else:
        wines = [dict(w) for w in lc["wines"]]  # copie — ne mute pas lc["wines"]
        for w in wines: w.setdefault("available", True)
    vc   = load_vivino_cache(slug)
    ckpt = ckpt_load(slug) if resume else None
    if ckpt:
        done_eans = set(ckpt["done_eans"])
        n_done    = len(done_eans)
        if log: log(f"🔁 Reprise : {n_done}/{len(wines)} ({int(100*n_done/max(len(wines),1))}%) déjà traités")
    else:
        done_eans = set()
        # ⑥ CORRIGÉ : ckpt_create appelle désormais ckpt_finish() en interne
        # pour nettoyer tout checkpoint stale avant d'en créer un nouveau
        ckpt_create(slug, len(wines))
    # Exclure d'abord les vins du checkpoint (déjà traités dans cette session)
    todo = [w for w in wines if (w.get("ean") or build_query(w["name"])) not in done_eans]
    # Exclure ensuite les vins déjà bien renseignés (note valide + non-stale + non-locked)
    # pour ne pas écraser de bonnes données lors d'un rafraîchissement partiel.
    def _is_fresh(w):
        e = vc.get(build_query(w["name"]), {})
        return bool(e.get("rating")) and not e.get("_stale") and not e.get("suppressed")
    # Note : si done_eans vide (nouveau scrape) on respecte quand même les données fraîches
    if not done_eans:
        already_good = [w for w in todo if _is_fresh(w)]
        todo = [w for w in todo if not _is_fresh(w)]
        if already_good and log:
            log(f"  ℹ️ {len(already_good)} vins déjà à jour ignorés (note valide, non-obsolète)")
    if not todo:
        if log: log("✅ Tous les vins sont déjà dans le cache !")
        ckpt_finish(slug)
    else:
        n_skip = len(wines) - len(todo)
        if log: log(f"🍷 {len(todo)} vins à scraper" + (f" ({n_skip} ignorés)" if n_skip else "") + "…")
        _scrape_vivino_list(slug, wines, todo, vc, log)
    return _merge_vivino(wines, vc, load_price_history())


def _run_vivino_subset(slug: str, selector, empty_msg: str, count_msg, log=None) -> list:
    """
    Facteur commun pour run_fill_missing_vivino et run_refresh_stale_vivino.

    selector(wines, vc) → list[dict]  sous-ensemble à scraper.
    load_price_history est appelé une seule fois, qu'il y ait scraping ou non.
    """
    lc = load_leclerc_cache(slug)
    if not lc:
        if log: log("❌ Pas de cache Leclerc. Lancez d'abord 🔄 Vérifier disponibilité.")
        return []
    vc    = load_vivino_cache(slug)
    wines = [dict(w) for w in lc["wines"]]
    for w in wines: w.setdefault("available", True)
    ph     = load_price_history()          # 1× — évite le double appel branch/fin
    subset = selector(wines, vc)
    if not subset:
        if log: log(empty_msg)
        return _merge_vivino(wines, vc, ph)
    if log: log(count_msg(subset, wines))
    ckpt_create(slug, len(subset))
    _scrape_vivino_list(slug, subset, subset, vc, log)
    return _merge_vivino(wines, vc, ph)


def run_fill_missing_vivino(slug: str, log=None) -> list:
    """Scrape uniquement les vins sans note ET sans URL Vivino."""
    def _sel(wines, vc):
        result = []
        for w in wines:
            e = vc.get(build_query(w["name"]), {})
            if not e.get("locked") and not e.get("rating") and not e.get("vivino_url"):
                result.append(w)
        return result
    return _run_vivino_subset(
        slug, _sel,
        "✅ Tous les vins ont déjà une note ou un lien Vivino !",
        lambda s, w: f"🔍 {len(s)}/{len(w)} vins sans données Vivino…",
        log,
    )


def run_refresh_stale_vivino(slug: str, log=None) -> list:
    """Rafraîchit uniquement les entrées Vivino marquées obsolètes (> TTL jours)."""
    def _sel(wines, vc):
        return [w for w in wines if vc.get(build_query(w["name"]), {}).get("_stale")]
    return _run_vivino_subset(
        slug, _sel,
        f"✅ Aucune entrée obsolète (TTL = {VIVINO_CACHE_TTL_DAYS} jours) !",
        lambda s, _: f"⏳ {len(s)} entrées obsolètes à rafraîchir…",
        log,
    )


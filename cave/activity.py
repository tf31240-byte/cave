"""Journal d'activité persistant et rapports de passage.

Chaque ligne de log d'un scraping (catalogue ou Vivino) devient un événement horodaté,
conservé dans `.cache/activity.jsonl` : il survit aux rechargements de page et au
redémarrage de l'app (tant que le disque est conservé) et n'est pas effacé au lancement
d'une nouvelle tâche. Les rapports de fin de passage (`report_*.json`) résument ce qui
s'est passé : vins ajoutés / retirés, changements de prix, résultat Vivino de chaque vin.
"""
import json
import threading
import time
from pathlib import Path

from .config import CACHE_DIR
# AUTO-IMPORTS
# END-AUTO

ACTIVITY_PATH = CACHE_DIR / "activity.jsonl"
_MAX_BYTES = 1_500_000      # au-delà : on ne garde que les 3 000 derniers événements
_KEEP_LINES = 3000
_lock = threading.Lock()
_run_id = ""

KINDS = {"catalogue": "📦 Catalogue", "vivino": "🍷 Vivino", "systeme": "⚙️ Système"}
LEVELS = {"ok": "✅ Succès", "info": "ℹ️ Info", "warn": "⚠️ Alertes", "error": "❌ Erreurs"}


def level_of(msg: str) -> str:
    """Déduit le niveau d'un message à partir de ses pictogrammes."""
    if any(k in msg for k in ("❌", "Erreur", "échoué", "Exception")):
        return "error"
    if any(k in msg for k in ("⚠️", "non trouvé", "introuvable", "Interrompu", "peu sûre", "aucune note")):
        return "warn"
    if any(k in msg for k in ("✅", "➕", "🆕", "💾", "🔗")):
        return "ok"
    return "info"


def new_run(kind: str, **info) -> str:
    """Ouvre un « passage » : tous les événements suivants lui sont rattachés."""
    global _run_id
    _run_id = f"{kind}-{time.strftime('%Y%m%d-%H%M%S')}"
    add_event(kind, f"▶️ Début : {info.get('label') or kind}", "info", start=True, **info)
    return _run_id


def current_run() -> str:
    return _run_id


def add_event(kind: str, msg: str, level: str | None = None, **data) -> None:
    ev = {"t": time.time(), "run": _run_id, "kind": kind,
          "level": level or level_of(msg), "msg": msg}
    if data:
        ev["data"] = data
    try:
        with _lock:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with ACTIVITY_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            if ACTIVITY_PATH.stat().st_size > _MAX_BYTES:
                lines = ACTIVITY_PATH.read_text("utf-8").splitlines()[-_KEEP_LINES:]
                ACTIVITY_PATH.write_text("\n".join(lines) + "\n", "utf-8")
    except Exception:
        pass    # le journal ne doit jamais faire échouer un scraping


def make_log(kind: str, mirror=None):
    """Fonction `log(msg)` : une ligne = un événement ; `mirror` reçoit aussi le message brut."""
    def _log(msg: str, **data):
        for i, line in enumerate(str(msg).replace("**", "").splitlines()):
            if line.strip():
                add_event(kind, line.strip(), **(data if i == 0 else {}))
        if mirror:
            try:
                mirror(msg)
            except Exception:
                pass
    return _log


def read_events(limit: int = 500, kinds=None, levels=None, run: str | None = None,
                contains: str = "") -> list:
    """Derniers événements (du plus ancien au plus récent), filtrés."""
    try:
        raw = ACTIVITY_PATH.read_text("utf-8").splitlines()
    except Exception:
        return []
    out = []
    needle = contains.lower().strip()
    for line in reversed(raw):
        try:
            ev = json.loads(line)
        except Exception:
            continue
        if kinds and ev.get("kind") not in kinds:
            continue
        if levels and ev.get("level") not in levels:
            continue
        if run and ev.get("run") != run:
            continue
        if needle and needle not in ev.get("msg", "").lower():
            continue
        out.append(ev)
        if len(out) >= limit:
            break
    out.reverse()
    return out


def format_event(ev: dict) -> str:
    return f"[{time.strftime('%d/%m %H:%M:%S', time.localtime(ev.get('t', 0)))}] {ev.get('msg', '')}"


# ── Rapports de fin de passage ────────────────────────────────────────────────

def _report_path(name: str) -> Path:
    return CACHE_DIR / f"report_{name}.json"


def save_report(name: str, data: dict) -> None:
    data = {**data, "saved_at": time.time(), "run": _run_id}
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _report_path(name).with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), "utf-8")
        tmp.replace(_report_path(name))
    except Exception:
        pass


def load_report(name: str) -> dict:
    try:
        return json.loads(_report_path(name).read_text("utf-8"))
    except Exception:
        return {}

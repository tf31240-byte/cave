"""Cave Leclerc Blagnac × Vivino — point d'entrée Streamlit.

Toute la logique est dans le paquet `cave/` :
  config · storage · regions · vivino · browser · leclerc · scoring · jobs · render · screens
"""
import logging

import streamlit as st

st.set_page_config(page_title="Cave Leclerc Blagnac × Vivino", page_icon="🍷",
                   layout="wide", initial_sidebar_state="collapsed")


class _NoScriptRunContext(logging.Filter):
    """Masque l'avertissement « missing ScriptRunContext » émis par les threads de fond."""
    def filter(self, record):
        return "ScriptRunContext" not in record.getMessage()


for _h in logging.getLogger().handlers:
    _h.addFilter(_NoScriptRunContext())
logging.getLogger("streamlit").addFilter(_NoScriptRunContext())

from cave.style import CSS      # noqa: E402
from cave import screens        # noqa: E402

st.markdown(CSS, unsafe_allow_html=True)
screens.run()

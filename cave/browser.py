# AUTO-IMPORTS
# END-AUTO
"""Navigateur Chromium headless (Selenium)."""

def make_driver(fast: bool = False):
    """
    ③ CORRIGÉ (indirectement) : les appelants initialisent désormais
    driver = None avant d'appeler make_driver(), ce qui évite le NameError
    dans les blocs finally si make_driver() lève une exception.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import os
    opts = Options()
    for arg in ["--headless","--no-sandbox","--disable-dev-shm-usage",
                "--disable-gpu","--window-size=1280,900",
                "--disable-blink-features=AutomationControlled"]:
        opts.add_argument(arg)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    if fast:
        # Rapidité (Vivino uniquement) : pas d'images ni de polices, chargement « eager ».
        # Leclerc garde le mode normal : son catalogue Angular/anti-bot est plus fragile.
        opts.page_load_strategy = "eager"
        opts.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.fonts": 2,
        })
        opts.add_argument("--blink-settings=imagesEnabled=false")
    for b in ["/usr/bin/chromium","/usr/bin/chromium-browser",
              "/usr/bin/google-chrome","/usr/bin/google-chrome-stable"]:
        if os.path.exists(b): opts.binary_location = b; break
    for d in ["/usr/bin/chromedriver","/usr/lib/chromium/chromedriver",
              "/usr/lib/chromium-browser/chromedriver"]:
        if os.path.exists(d): return webdriver.Chrome(service=Service(d), options=opts)
    return webdriver.Chrome(options=opts)


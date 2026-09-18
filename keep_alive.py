"""
keep_alive.py

Keeps the Streamlit Community Cloud app from going to sleep.

A plain HTTP request is not enough: Streamlit Cloud only counts a visit that
opens a real app session, and a sleeping app shows a page whose wake-up
button has to be clicked. So this loads the site in headless Chromium,
clicks the wake button if the app is asleep, and waits for the dashboard
to render.

The ?keepalive=1 parameter tells the dashboard to skip the GoatCounter
pixel, so these visits don't inflate the traffic stats.

Exits 1 if the dashboard never renders, so a site that is actually down
shows up as a failed workflow run.
"""

import re
import sys
import time

from playwright.sync_api import sync_playwright

URL = "https://bonddataquality.streamlit.app/?nav=abs&keepalive=1"
LOADED_MARKER = "Bond Data Quality"
WAKE_BUTTON = re.compile(r"get this app back up", re.IGNORECASE)
DEADLINE_SECONDS = 240


def app_rendered(page) -> bool:
    for frame in page.frames:
        try:
            if LOADED_MARKER in frame.inner_text("body", timeout=2000):
                return True
        except Exception:
            continue
    return False


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(URL, timeout=90_000, wait_until="domcontentloaded")

        woke = False
        start = time.monotonic()
        while time.monotonic() - start < DEADLINE_SECONDS:
            wake = page.get_by_role("button", name=WAKE_BUTTON)
            if not woke and wake.count():
                print("App was asleep - clicking wake button.")
                wake.first.click()
                woke = True
            if app_rendered(page):
                elapsed = time.monotonic() - start
                print(f"App is awake and rendered ({elapsed:.0f}s)"
                      + (" after waking it." if woke else "."))
                browser.close()
                return 0
            time.sleep(5)

        print(f"App did not render within {DEADLINE_SECONDS}s - the site may be down.")
        browser.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())

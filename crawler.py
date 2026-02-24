import asyncio
import json
import os
import hashlib
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from brain import detect_platform


def _generate_test_email() -> str:
    """Generate a unique test email that passes most server-side validators.
    Uses @gmail.com domain to pass MX/domain validation checks."""
    uid = uuid.uuid4().hex[:8]
    return f"gtmtest.{uid}@gmail.com"

CACHE_FILE = "submitted_forms_cache.json"
CRAWLER_VERSION = "2.3" # Incrementing to invalidate old failed shadow form caches
MAX_DATALAYER_EVENTS = 500  # P1-1: Cap to prevent unbounded memory growth

def get_form_hash(url: str, form_html: str) -> str:
    """Generate a unique hash for a form on a specific URL, versioned by crawler logic."""
    content = f"v{CRAWLER_VERSION}:{url}:{form_html}".encode('utf-8')
    return hashlib.md5(content).hexdigest()

def _get_cache_path(session_id: str | None = None) -> str:
    """Return session-scoped cache path, or global default for backward compat."""
    if session_id:
        cache_dir = os.path.join(".cache", session_id)
        os.makedirs(cache_dir, exist_ok=True)
        return os.path.join(cache_dir, "submitted_forms.json")
    return CACHE_FILE

def load_cache(session_id: str | None = None) -> set:
    """Load submitted form hashes from cache. P2-11: File locking for concurrent safety."""
    import fcntl
    path = _get_cache_path(session_id)
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    return set(json.load(f))
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except (json.JSONDecodeError, OSError):
            return set()
    # For session-scoped caches, also load global cache as baseline
    if session_id and os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    return set(json.load(f))
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except (json.JSONDecodeError, OSError):
            pass
    return set()

def save_cache(cache: set, session_id: str | None = None):
    """Save submitted form hashes to cache. P2-11: Atomic write via tempfile + os.replace()."""
    import tempfile
    path = _get_cache_path(session_id)
    dir_name = os.path.dirname(path) or "."
    os.makedirs(dir_name, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, 'w') as f:
            json.dump(list(cache), f)
        os.replace(tmp_path, path)  # Atomic on POSIX
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

# ── BrowserSession: shared browser lifecycle for all crawler functions ────────

# Common dataLayer + CF7 interception script injected via add_init_script
_DATALAYER_INIT_SCRIPT = """
(function() {
    let dl = window.dataLayer || [];

    function setInterceptor(array) {
        const originalPush = array.push;
        array.push = function() {
            const res = originalPush.apply(this, arguments);
            window.dispatchEvent(new CustomEvent('datalayer_push', { detail: arguments }));
            return res;
        };
    }

    setInterceptor(dl);

    Object.defineProperty(window, 'dataLayer', {
        get: function() { return dl; },
        set: function(val) {
            if (Array.isArray(val)) {
                val.forEach(item => dl.push(item));
            }
        },
        configurable: true
    });

    window.gtag = window.gtag || function() {
        window.dataLayer.push(arguments);
    };

    document.addEventListener('wpcf7mailsent', (e) => {
        window.dataLayer.push({
            event: 'wpcf7mailsent',
            cf7_form_id: e.detail.contactFormId,
            cf7_container_post_id: e.detail.containerPostId,
            cf7_unit_tag: e.detail.unitTag
        });
    }, false);

    document.addEventListener('wpcf7submit', (e) => {
        window.dataLayer.push({
            event: 'wpcf7submit',
            cf7_form_id: e.detail.contactFormId,
            status: e.detail.status
        });
    }, false);
})();
"""

# Common scroll-to-footer script
_SCROLL_TO_BOTTOM_SCRIPT = """async () => {
    await new Promise((resolve) => {
        let totalHeight = 0;
        let distance = 300;
        let timer = setInterval(() => {
            let scrollHeight = document.body.scrollHeight;
            window.scrollBy(0, distance);
            totalHeight += distance;
            if(totalHeight >= scrollHeight){
                clearInterval(timer);
                resolve();
            }
        }, 100);
    });
}"""


@asynccontextmanager
async def browser_session(
    url: str,
    log,
    browser_pool=None,
    navigation_timeout: int = 60000,
    intercept_datalayer: bool = False,
):
    """
    Shared browser lifecycle for all crawler functions (P0-1 + P0-2).

    Handles:
      1. Browser context acquisition (pool vs standalone)
      2. Page creation + stealth
      3. Optional dataLayer interception (for measure/crawl)
      4. Navigation + wait
      5. Consent banner + popup dismissal
      6. Platform detection
      7. Contact link detection
      8. Scroll to footer for lazy-loaded forms
      9. Guaranteed page.close() in finally (P0-1: no page leaks)

    Yields a dict with:
      - page: Playwright Page handle
      - platform: detected platform string
      - has_phone_links / has_email_links: bool
      - datalayer_events: list (only if intercept_datalayer=True)
      - nav_failed: bool (True if navigation failed — caller should return early)
    """
    # ── Browser context: pool or standalone ───────────────────
    if browser_pool:
        _ctx_mgr = browser_pool.acquire_context()
    else:
        @asynccontextmanager
        async def _standalone():
            async with async_playwright() as pw:
                br = await pw.chromium.launch(headless=True)
                ctx = await br.new_context()
                try:
                    yield ctx
                finally:
                    await br.close()
        _ctx_mgr = _standalone()

    async with _ctx_mgr as context:
        page = await context.new_page()
        try:
            # Stealth
            await stealth_async(page)

            # DataLayer interception (only for measure/crawl phases)
            datalayer_events = []
            if intercept_datalayer:
                await page.add_init_script(_DATALAYER_INIT_SCRIPT)

                async def _on_dl_push(event):
                    if len(datalayer_events) < MAX_DATALAYER_EVENTS:
                        datalayer_events.append(event)

                await page.expose_function("onDatalayerPush", _on_dl_push)
                await page.evaluate("""
                    window.addEventListener('datalayer_push', (e) => {
                        const args = Array.from(e.detail);
                        window.onDatalayerPush(args);
                    });
                    const captureCF7 = (eventName) => {
                        document.addEventListener(eventName, (e) => {
                            window.onDatalayerPush([{
                                event: eventName,
                                cf7_form_id: e.detail ? e.detail.contactFormId : 'unknown',
                                cf7_unit_tag: e.detail ? e.detail.unitTag : 'unknown'
                            }]);
                        }, false);
                    };
                    captureCF7('wpcf7mailsent');
                    captureCF7('wpcf7submit');
                """)

            # Build session dict (yielded to caller)
            session = {
                "page": page,
                "platform": "unknown",
                "has_phone_links": False,
                "has_email_links": False,
                "has_contact_links": False,
                "datalayer_events": datalayer_events,
                "nav_failed": False,
            }

            # Navigate
            try:
                log(f"Navigating to {url}...")
                await page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout)
                await page.wait_for_load_state("load", timeout=navigation_timeout // 2)
                await page.wait_for_timeout(3000)
            except Exception as e:
                log(f"Error navigating to {url}: {e}")
                session["nav_failed"] = True
                yield session
                return

            # Wait for dynamic content
            await page.wait_for_timeout(6000)

            # Consent + popups
            await auto_accept_consent(page, log)
            await sweep_popups(page, log)
            await page.wait_for_timeout(1000)

            # Platform detection
            try:
                page_html = await page.content()
                session["platform"] = detect_platform(page_html, url)
                log(f"Platform detected: {session['platform']}")
            except Exception:
                session["platform"] = "unknown"

            # Contact links
            phone_links = await page.locator('a[href^="tel:"]').count()
            email_links = await page.locator('a[href^="mailto:"]').count()
            session["has_phone_links"] = phone_links > 0
            session["has_email_links"] = email_links > 0
            session["has_contact_links"] = phone_links > 0 or email_links > 0

            # Scroll to footer to discover lazy-loaded forms
            log("Scrolling to discover footer forms...")
            await page.evaluate(_SCROLL_TO_BOTTOM_SCRIPT)
            await page.wait_for_timeout(2000)

            yield session
        finally:
            # P0-1: Guarantee page cleanup to prevent resource leaks
            try:
                await page.close()
            except Exception:
                pass


# ── #10 Multi-Page Crawl — Discover form-likely pages ────────────────────────
FORM_PAGE_KEYWORDS = [
    # English - General & Marketing
    "contact", "form", "book", "booking", "inquiry", "quote", "apply",
    "application", "register", "signup", "sign-up", "appointment", "request",
    "consult", "demo", "trial", "subscribe", "join", "enrol", "enroll",
    "lead", "newsletter", "mailing", "support", "help", "feedback",
    "message", "touch", "reach", "callback", "request-a-quote",
    
    # English - Account & E-commerce
    "login", "signin", "sign-in", "account", "profile", "checkout", "cart",
    "basket", "order", "purchase", "billing", "shipping",
    
    # English - Careers & Resources
    "career", "job", "vacancy", "resume", "cv", "volunteer", "download",
    "whitepaper", "resource", "event", "rsvp", "seminar", "webinar",

    # Bulgarian (Cyrillic & Latin)
    "kontakt", "kontakti", "контакт", "контакти",
    "forma", "форма",
    "zapitvane", "запитване",
    "rezervacia", "rezervaciya", "резервация",
    "abonament", "абонамент",
    "registracia", "registratsiya", "регистрация",
    "vhod", "вход",
    "porachka", "поръчка",
    "kupuvane", "купуване",
    "podkrep", "подкрепа",
    "pomosht", "помощ",
    "karieri", "кариери",
    "sybitie", "събитие",
    "anketa", "анкета"
]

async def discover_form_pages(base_url: str, max_pages: int = 4, browser_pool=None, navigation_timeout: int = 60000) -> list[str]:
    """Crawl the base URL and return a list of internal URLs likely to contain forms."""
    from urllib.parse import urljoin, urlparse
    parsed = urlparse(base_url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"
    discovered = []

    async def _discover_with_context(context):
        page = await context.new_page()
        try:
            # Optimize navigation: Use domcontentloaded + explicit wait for load state
            await page.goto(base_url, wait_until="domcontentloaded", timeout=navigation_timeout)
            await page.wait_for_load_state("load", timeout=navigation_timeout // 2)
            await page.wait_for_timeout(3000) # Let dynamic elements settle
            links = await page.locator("a[href]").all()
            for link in links:
                try:
                    href = await link.get_attribute("href") or ""
                    if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                        continue
                    full_url = urljoin(base_domain, href)
                    if urlparse(full_url).netloc != parsed.netloc:
                        continue
                    link_text = await link.inner_text() or ""
                    path = urlparse(full_url).path.lower()
                    semantic_context = f"{path} {link_text.lower()}"
                    if any(kw in semantic_context for kw in FORM_PAGE_KEYWORDS):
                        if full_url not in discovered and full_url != base_url:
                            discovered.append(full_url)
                            if len(discovered) >= max_pages:
                                break
                except Exception:
                    continue
        except Exception:
            pass
        finally:
            await page.close()  # BUG-02: Guarantee page cleanup to prevent resource leaks

    if browser_pool:
        async with browser_pool.acquire_context() as context:
            await _discover_with_context(context)
    else:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await _discover_with_context(context)
            await browser.close()

    return discovered

async def auto_accept_consent(page, log) -> bool:
    """Attempts to find and click 'Accept All' on common Cookie Consent Banners."""
    log("Checking for Cookie Consent Banners...")
    
    # CSS Selectors for common Consent Management Platforms (CMPs)
    cmp_selectors = [
        "#onetrust-accept-btn-handler",        # OneTrust
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll", # Cookiebot
        ".cky-btn-accept",                     # CookieYes
        "#didomi-notice-agree-button",         # Didomi
        ".osano-cm-accept-all",                # Osano
        "#truste-consent-button",              # TrustArc
        ".qc-cmp2-b-right",                    # Quantcast Choice
        ".cmplz-accept",                       # Complianz
        ".brlbs-btn-accept-all",               # Borlabs
        "#cookie-law-info-bar ~ * .cli-plugin-button", # Cookie Law Info
        "[id*='cookie'] button:has-text('Accept')", # Generic fallback
        "[class*='cookie'] button:has-text('Accept')",
        "button:has-text('Accept All')",
        "button:has-text('Allow All')",
        "button:has-text('Prihvati')",
        "button:has-text('Съгласен съм')"
    ]
    
    for selector in cmp_selectors:
        try:
            # Check if element exists and is visible quickly
            elements = await page.locator(selector).all()
            for el in elements:
                if await el.is_visible():
                    await el.click(timeout=3000)
                    log(f"✅ Clicked consent banner using selector: `{selector}`")
                    # Wait a moment for the banner to disappear and DOM to settle
                    await page.wait_for_timeout(1000)
                    return True
        except Exception:
            continue
            
    log("No recognizable cookie banner found or accepted.")
    return False

async def sweep_popups(page, log):
    """Closes common promotional popups (Poptin, OptinMonster, etc.) that might block forms."""
    popup_close_selectors = [
        "button[class*='poptin-close']",
        "div[class*='poptin-close']",
        ".om-close-button",
        ".poptin-close-x",
        "svg[class*='close']",
        "span:has-text('✕')",
        "button:has-text('No thanks')",
        "button:has-text('Към магазина')" # common in BG for popups
    ]
    for selector in popup_close_selectors:
        try:
            elements = await page.locator(selector).all()
            for el in elements:
                if await el.is_visible():
                    await el.click(timeout=2000)
                    log(f"🧹 Closed popup using selector: `{selector}`")
                    await page.wait_for_timeout(500)
        except Exception:
            continue

async def discover_forms(
    url: str,
    log_callback=None,
    debug_dir=".debug",
    session_id: str | None = None,
    browser_pool=None,
    navigation_timeout: int = 60000,
    screenshot_timeout: int = 5000,
) -> dict:
    """
    Phase 1: Passive DOM scan. Discovers all forms and shadow forms,
    extracts metadata and screenshots. NO interaction (no filling, no submitting).
    Returns a DiscoveryOutput-compatible dict.
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)

    result = {
        "url": url,
        "page_path": urlparse(url).path or "/",
        "platform": "unknown",
        "forms_discovered": [],
        "detected_iframes": [],
        "has_phone_links": False,
        "has_email_links": False,
        "has_contact_links": False,
        "data_layer_events": [],
    }

    async with browser_session(
        url, log,
        browser_pool=browser_pool,
        navigation_timeout=navigation_timeout,
        intercept_datalayer=False,
    ) as session:
        if session["nav_failed"]:
            return result

        page = session["page"]
        result["platform"] = session["platform"]
        result["has_phone_links"] = session["has_phone_links"]
        result["has_email_links"] = session["has_email_links"]
        result["has_contact_links"] = session["has_contact_links"]

        # iFrame scanning (discover-only, not in shared session)
        detected_iframes = []
        try:
            iframe_elements = await page.locator("iframe").all()
            page_netloc = urlparse(url).netloc
            for iframe_el in iframe_elements:
                try:
                    src = await iframe_el.get_attribute("src") or ""
                    if not src:
                        continue
                    iframe_netloc = urlparse(src).netloc
                    is_cross = bool(iframe_netloc and iframe_netloc != page_netloc)
                    detected_iframes.append({
                        "src": src,
                        "is_cross_origin": is_cross,
                        "netloc": iframe_netloc or page_netloc,
                    })
                except Exception:
                    continue
            if detected_iframes:
                log(f"[Discover] Detected {len(detected_iframes)} iframe(s): "
                    f"{sum(1 for i in detected_iframes if i['is_cross_origin'])} cross-origin")
        except Exception:
            pass
        result["detected_iframes"] = detected_iframes

        # ── Discover <form> elements ─────────────────────────────────
        forms_count = await page.locator('form').count()
        log(f"[Discover] Found {forms_count} form(s) after scroll.")

        for i in range(forms_count):
            form = page.locator('form').nth(i)
            form_data = {
                "form_index": i,
                "html_attributes": {},
                "cf7_form_id": None,
                "page_path": urlparse(url).path or "/",
                "is_shadow_form": False,
            }

            # Get HTML attributes of the form
            try:
                html = await form.evaluate("el => el.outerHTML")
            except Exception:
                continue

            attributes = await form.evaluate("""el => {
                const attrs = {};
                for (const attr of el.attributes) {
                    attrs[attr.name] = attr.value;
                }
                return attrs;
            }""")

            form_hash = get_form_hash(url, html)
            form_data["html_attributes"] = attributes
            form_data["form_hash"] = form_hash
            form_data["form_id"] = attributes.get("id")
            form_data["form_classes"] = attributes.get("class")
            form_data["form_action"] = attributes.get("action")

            # Optional screenshot
            try:
                os.makedirs(debug_dir, exist_ok=True)
                screenshot_path = os.path.join(debug_dir, f"form_{i}.png")
                try:
                    await form.scroll_into_view_if_needed(timeout=1500)
                except Exception:
                    pass
                try:
                    box = await form.bounding_box()
                    if box and (box['width'] < 400 or box['height'] < 100):
                        parent = await form.evaluate_handle("el => el.parentElement")
                        await parent.screenshot(path=screenshot_path, timeout=3000, force=True)
                    else:
                        await form.screenshot(path=screenshot_path, timeout=3000, force=True)
                except Exception:
                    pass
            except Exception:
                pass

            # Extract form context: labels, parent hierarchy, fields, buttons
            form_metadata = await form.evaluate("""el => {
                const data = {
                    field_labels: {},
                    parent_context: [],
                    fields: [],
                    buttons: [],
                    position_on_page: null,
                };

                // 1. Label Harvester
                const inputs = el.querySelectorAll('input, select, textarea');
                inputs.forEach(input => {
                    const id = input.id;
                    const name = input.name;
                    const key = name || id;

                    let labelText = '';
                    if (id) {
                        const lbl = document.querySelector(`label[for="${id}"]`);
                        if (lbl) labelText = lbl.innerText.trim();
                    }
                    if (!labelText) {
                        let parentLabel = input.closest('label');
                        if (parentLabel) labelText = parentLabel.innerText.trim();
                    }
                    if (!labelText) {
                        let prev = input.previousElementSibling;
                        if (prev && prev.tagName === 'LABEL') labelText = prev.innerText.trim();
                    }

                    if (key) data.field_labels[key] = labelText;

                    // Structured field data
                    data.fields.push({
                        name: input.name || '',
                        type: input.type || input.tagName.toLowerCase(),
                        tag: input.tagName.toLowerCase(),
                        id: input.id || '',
                        label_text: labelText,
                        placeholder: input.placeholder || '',
                        required: input.required || false,
                        is_hidden: input.type === 'hidden',
                        is_consent: input.type === 'checkbox' && /consent|gdpr|agree/i.test(input.name || ''),
                    });
                });

                // 2. Button Harvester
                el.querySelectorAll('button, input[type="submit"]').forEach(btn => {
                    data.buttons.push({
                        text: (btn.innerText || btn.value || '').trim(),
                        type: btn.type || '',
                        tag: btn.tagName.toLowerCase(),
                        class: btn.className || '',
                    });
                });

                // 3. Hierarchy Harvester
                let current = el.parentElement;
                while (current && current.tagName !== 'BODY') {
                    data.parent_context.push({
                        tag: current.tagName.toLowerCase(),
                        id: current.id,
                        classes: current.className
                    });
                    current = current.parentElement;
                }

                // 4. Position detection (header/main/footer)
                let posEl = el;
                while (posEl) {
                    const tag = posEl.tagName.toLowerCase();
                    if (tag === 'footer' || (posEl.className && /footer/i.test(posEl.className))) {
                        data.position_on_page = 'footer';
                        break;
                    }
                    if (tag === 'header' || (posEl.className && /header|nav/i.test(posEl.className))) {
                        data.position_on_page = 'header';
                        break;
                    }
                    if (tag === 'main' || tag === 'article') {
                        data.position_on_page = 'main';
                        break;
                    }
                    posEl = posEl.parentElement;
                }
                if (!data.position_on_page) data.position_on_page = 'main';

                return data;
            }""")

            form_data["field_labels"] = form_metadata["field_labels"]
            form_data["parent_context"] = form_metadata["parent_context"]
            form_data["fields"] = form_metadata["fields"]
            form_data["buttons"] = form_metadata["buttons"]
            form_data["position_on_page"] = form_metadata["position_on_page"]

            # Extract CF7 form ID
            cf7_id = attributes.get("data-id")
            if not cf7_id:
                for parent in form_data["parent_context"]:
                    parent_id = parent.get("id", "")
                    if "wpcf7-f" in parent_id:
                        import re
                        match = re.search(r"wpcf7-f(\d+)", parent_id)
                        if match:
                            cf7_id = match.group(1)
                            break
            if not cf7_id:
                for cls in attributes.get("class", "").split():
                    if cls.startswith("wpcf7-f"):
                        cf7_id = cls.replace("wpcf7-f", "")
                        break
            if cf7_id:
                form_data["cf7_form_id"] = cf7_id

            # Extract form title
            form_data["form_title"] = await form.evaluate("""el => {
                let title = el.querySelector('legend, h1, h2, h3, h4, h5, h6')?.innerText;
                if (!title) {
                    let prev = el.previousElementSibling;
                    while (prev && !prev.tagName.match(/^H[1-6]$/)) {
                        prev = prev.previousElementSibling;
                    }
                    if (prev) title = prev.innerText;
                }
                if (!title) {
                    let parent = el.parentElement;
                    while (parent && !title) {
                        const heading = parent.querySelector('h1, h2, h3, h4, h5, h6');
                        if (heading && heading.innerText.trim()) {
                            title = heading.innerText;
                        }
                        parent = parent.parentElement;
                        if (parent && /^(SECTION|MAIN|BODY|FOOTER)$/.test(parent.tagName)) break;
                    }
                }
                return title ? title.trim() : null;
            }""")

            # Extract surrounding context
            form_data["surrounding_context"] = await form.evaluate("""el => {
                const context = [];
                const checkElement = (node) => {
                    if (node && node.tagName.match(/^(H[1-6]|P|DIV)$/)) {
                        const text = node.innerText.trim();
                        if (text && text.length > 5 && text.length < 500) {
                            context.push({tag: node.tagName.toLowerCase(), text: text});
                            return true;
                        }
                    }
                    return false;
                };
                let prev = el.previousElementSibling;
                let count = 0;
                while (prev && count < 3) {
                    if (checkElement(prev)) count++;
                    prev = prev.previousElementSibling;
                }
                if (context.length === 0 && el.parentElement) {
                    let pSib = el.parentElement.previousElementSibling;
                    while (pSib && count < 3) {
                        if (checkElement(pSib)) count++;
                        pSib = pSib.previousElementSibling;
                    }
                }
                return context;
            }""")

            log(f"[Discover] Form #{i+1}: title={form_data['form_title']}, "
                f"fields={len(form_data['fields'])}, buttons={len(form_data['buttons'])}, "
                f"position={form_data['position_on_page']}")
            result["forms_discovered"].append(form_data)

        # ── Shadow Form Discovery ─────────────────────────────────────
        try:
            loose_emails = await page.locator(
                'input[type="email"], input[name*="email"], '
                'input[placeholder*="имейл"], input[placeholder*="email"]'
            ).all()
            for i, el in enumerate(loose_emails):
                if not await el.is_visible():
                    continue
                in_form = await el.evaluate("el => !!el.closest('form')")
                if in_form:
                    continue

                container_handle = await el.evaluate_handle(
                    "el => el.closest('.elementor-widget, .et_pb_module, .wp-block-group, "
                    "div[class*=\"newsletter\"], div[class*=\"form\"]') || el.parentElement"
                )
                html = await container_handle.evaluate("el => el.outerHTML")
                form_hash = get_form_hash(url, html + "_shadow")

                log(f"[Discover] Found 'Shadow Form' interaction cluster (no <form> tag).")

                attributes = await container_handle.evaluate("""el => {
                    const attrs = {};
                    for (const attr of el.attributes) { attrs[attr.name] = attr.value; }
                    return attrs;
                }""")

                # Extract fields and buttons from shadow form container
                shadow_metadata = await container_handle.evaluate("""el => {
                    const fields = [];
                    el.querySelectorAll('input, select, textarea').forEach(input => {
                        fields.push({
                            name: input.name || '',
                            type: input.type || input.tagName.toLowerCase(),
                            tag: input.tagName.toLowerCase(),
                            id: input.id || '',
                            label_text: '',
                            placeholder: input.placeholder || '',
                            required: input.required || false,
                            is_hidden: input.type === 'hidden',
                            is_consent: false,
                        });
                    });
                    const buttons = [];
                    el.querySelectorAll('button, input[type="submit"]').forEach(btn => {
                        buttons.push({
                            text: (btn.innerText || btn.value || '').trim(),
                            type: btn.type || '',
                            tag: btn.tagName.toLowerCase(),
                            class: btn.className || '',
                        });
                    });

                    // Surrounding text
                    let surroundingText = '';
                    let prev = el.previousElementSibling;
                    if (prev) surroundingText = prev.innerText?.trim() || '';

                    // Visible labels
                    const labels = [];
                    fields.forEach(f => { if (f.label_text) labels.push(f.label_text); });
                    buttons.forEach(b => { if (b.text) labels.push(b.text); });

                    return { fields, buttons, surroundingText, labels };
                }""")

                shadow_data = {
                    "form_index": f"shadow_{i}",
                    "is_shadow_form": True,
                    "html_attributes": attributes,
                    "form_id": attributes.get("id"),
                    "form_classes": attributes.get("class"),
                    "form_hash": form_hash,
                    "page_path": urlparse(url).path or "/",
                    "fields": shadow_metadata["fields"],
                    "buttons": shadow_metadata["buttons"],
                    "field_labels": {},
                    "parent_context": [],
                    "form_title": None,
                    "surrounding_context": [{"tag": "p", "text": shadow_metadata["surroundingText"]}] if shadow_metadata["surroundingText"] else [],
                    "cf7_form_id": None,
                    "form_action": None,
                    "position_on_page": "footer",
                }

                # Shadow form screenshot
                try:
                    screenshot_path = os.path.join(debug_dir, f"form_shadow_{i}.png")
                    try:
                        await container_handle.scroll_into_view_if_needed(timeout=screenshot_timeout)
                        await container_handle.screenshot(path=screenshot_path, timeout=screenshot_timeout, force=True)
                    except Exception:
                        pass
                except Exception:
                    pass

                result["forms_discovered"].append(shadow_data)
                log(f"[Discover] Shadow Form added (Classes: {attributes.get('class', 'none')[:30]}...)")
        except Exception as e:
            log(f"[Discover] Shadow Form detection error: {e}")

    log(f"[Discover] Total: {len(result['forms_discovered'])} forms discovered.")
    return result


async def measure_forms(
    url: str,
    approved_forms: list[dict],
    log_callback=None,
    ignore_cache: bool = False,
    debug_dir: str = ".debug",
    session_id: str | None = None,
    browser_pool=None,
    navigation_timeout: int = 60000,
    screenshot_timeout: int = 5000,
) -> dict:
    """
    Phase 2: Targeted measurement. Fills, submits, and captures signals
    for ONLY the approved forms from discover_forms().

    Returns a crawl_site()-compatible dict with forms_processed[].
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)

    result = {
        "url": url,
        "page_path": urlparse(url).path or "/",
        "platform": "unknown",
        "forms_processed": [],
        "datalayer_events": [],
        "gtag_events": [],
        "has_phone_links": False,
        "has_email_links": False,
        "skipped_forms": [],  # P1-3: Track unlocatable forms
    }

    if not approved_forms:
        log("[Measure] No approved forms to measure.")
        return result

    cache = load_cache(session_id=session_id)

    async with browser_session(
        url, log,
        browser_pool=browser_pool,
        navigation_timeout=navigation_timeout,
        intercept_datalayer=True,
    ) as session:
        if session["nav_failed"]:
            return result

        page = session["page"]
        datalayer_events = session["datalayer_events"]
        result["platform"] = session["platform"]
        result["has_phone_links"] = session["has_phone_links"]
        result["has_email_links"] = session["has_email_links"]

        log(f"[Measure] Measuring {len(approved_forms)} approved form(s)...")

        # Process each approved form
        for af in approved_forms:
            is_shadow = af.get("is_shadow_form", False)
            form_idx = af.get("form_index")
            target_hash = af.get("form_hash", "")

            # Ensure we are on the base URL
            if urlparse(page.url).netloc != urlparse(url).netloc or page.url != url:
                log(f"[Measure] Redirect detected. Returning to base URL.")
                await page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout)
                await page.wait_for_load_state("load", timeout=navigation_timeout // 2)
                await page.wait_for_timeout(3000)
                await sweep_popups(page, log)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(3000)

            if is_shadow:
                # Shadow forms don't get submitted — they are discovery-only
                # Create a compatible form_data entry with element_visibility tracking
                shadow_data = dict(af)
                shadow_data["form_submitted"] = False
                shadow_data["is_successful_submission"] = True
                shadow_data["dom_payload_keys"] = [af["fields"][0]["name"]] if af.get("fields") else ["email"]
                shadow_data["available_tracking_methods"] = [{
                    "method": "element_visibility",
                    "priority": 1,
                    "reason": "Interaction cluster detected outside of standard <form> tag. Tracking via element visibility of the container.",
                    "trigger_condition": {
                        "selector": f"#{af.get('form_id')}" if af.get('form_id')
                        else f".{af.get('form_classes', '').replace(' ', '.')}" if af.get('form_classes')
                        else "div"
                    },
                }]
                result["forms_processed"].append(shadow_data)
                log(f"[Measure] Shadow form '{form_idx}' added with element_visibility tracking.")
                continue

            # P0-3: Atomic snapshot of all form handles with hashes
            # (prevents race condition from stale forms_count)
            form_handles = []
            for form_el in await page.locator('form').all():
                try:
                    f_html = await form_el.evaluate("el => el.outerHTML")
                    fh = get_form_hash(url, f_html)
                    form_handles.append((fh, form_el))
                except Exception:
                    continue

            matched_form = None
            matched_index = None

            # Match by hash first (stable across DOM changes)
            if target_hash:
                for j, (fh, form_el) in enumerate(form_handles):
                    if fh == target_hash:
                        matched_form = form_el
                        matched_index = j
                        break

            # Fallback to positional index within the snapshot
            if matched_form is None and isinstance(form_idx, int) and form_idx < len(form_handles):
                matched_form = form_handles[form_idx][1]
                matched_index = form_idx

            if matched_form is None:
                log(f"[Measure] ⚠️ Could not locate form '{form_idx}' on page. Skipping.")
                result["skipped_forms"].append({
                    "form_index": form_idx,
                    "reason": "Form not found on page after re-navigation. DOM may have changed.",
                })
                continue

            form = matched_form
            log(f"[Measure] Measuring form #{matched_index + 1} (hash match: {bool(target_hash)})...")

            form_data = dict(af)  # Start with discovery data
            form_data["form_index"] = matched_index
            form_data["form_submitted"] = False
            form_data["is_ajax_submission"] = False
            form_data["redirect_url"] = None
            form_data["dom_payload_keys"] = []

            # ── Comprehensive form filling ────────────────────────────
            test_email = _generate_test_email()
            log(f"[Measure] Using test email: {test_email}")

            # Email inputs
            for el in await form.locator('input[type="email"]').all():
                try:
                    await el.fill(test_email)
                except Exception:
                    pass

            # Text / Tel / Number inputs
            for el in await form.locator('input[type="text"], input[type="tel"], input[type="number"], input:not([type])').all():
                try:
                    name = (await el.get_attribute("name") or
                            await el.get_attribute("placeholder") or "field").lower()
                    if any(k in name for k in ["phone", "tel", "mob", "gsm"]):
                        await el.fill("0896248833")
                    elif any(k in name for k in ["name", "first", "last", "names", "ime"]):
                        await el.fill("Ivan Ivanov")
                except Exception:
                    pass

            # All text/email/tel/textarea — dummy data with dom_payload_keys
            for el in await form.locator("input[type='text'], input[type='email'], input[type='tel'], textarea").all():
                try:
                    name = await el.get_attribute("name") or "field"
                    if name not in form_data["dom_payload_keys"] and name != "field":
                        form_data["dom_payload_keys"].append(name)
                    if "email" in name.lower() or (await el.get_attribute("type")) == "email":
                        await el.fill(test_email)
                    else:
                        await el.fill(f"test_{name}")
                except Exception:
                    pass

            # Select dropdowns
            for el in await form.locator("select").all():
                try:
                    name = await el.get_attribute("name") or "select_field"
                    if name not in form_data["dom_payload_keys"] and name != "select_field":
                        form_data["dom_payload_keys"].append(name)
                    options = await el.locator("option").all()
                    for opt in options:
                        val = await opt.get_attribute("value") or ""
                        if val.strip():
                            await el.select_option(val)
                            break
                except Exception:
                    pass

            # Checkboxes
            for el in await form.locator('input[type="checkbox"]').all():
                try:
                    if not await el.is_checked():
                        await el.check()
                except Exception:
                    pass

            # Radio buttons
            seen_radio_names = set()
            for el in await form.locator('input[type="radio"]').all():
                try:
                    name = await el.get_attribute("name") or ""
                    if name not in form_data["dom_payload_keys"] and name != "":
                        form_data["dom_payload_keys"].append(name)
                    if name not in seen_radio_names:
                        await el.check()
                        seen_radio_names.add(name)
                except Exception:
                    pass

            # Hidden inputs
            for el in await form.locator('input[type="hidden"]').all():
                try:
                    name = await el.get_attribute("name") or ""
                    if name and not name.startswith("_wp") and not name.startswith("_wpcf7") and name not in form_data["dom_payload_keys"]:
                        form_data["dom_payload_keys"].append(name)
                except Exception:
                    pass

            log(f"[Measure] All fields filled for form #{matched_index + 1}")

            # ── Submit form ───────────────────────────────────────────
            # P2-1: Per-form scoped AJAX listeners (prevents cross-form event leakage)
            ajax_requests = []
            ajax_responses = []
            req_handler = lambda req: ajax_requests.append(req.url) if req.method in ["POST", "PUT", "PATCH"] else None
            res_handler = lambda res: ajax_responses.append({"url": res.request.url, "status": res.status}) if res.request.method in ["POST", "PUT", "PATCH"] else None
            page.on("request", req_handler)
            page.on("response", res_handler)

            dl_start_len = len(datalayer_events)

            # DataLayer Diff — snapshot before submit
            try:
                dl_before_keys = set(await page.evaluate("""
                    () => {
                        const flat = {};
                        (window.dataLayer || []).forEach(obj => {
                            if (typeof obj === 'object' && !Array.isArray(obj))
                                Object.keys(obj).forEach(k => flat[k] = true);
                        });
                        return Object.keys(flat);
                    }
                """))
            except Exception:
                dl_before_keys = set()

            try:
                form_was_connected = await form.evaluate("el => el.isConnected")
            except Exception:
                form_was_connected = False

            try:
                # CANONICAL SUBMISSION STRATEGY (BUG-03 + CROSS-03: separated click from navigation wait)
                submit_btn = form.locator('button[type="submit"], input[type="submit"], button:has-text("Изпрати"), button:has-text("Submit"), button:has-text("Send"), .wpcf7-submit')
                did_click = False
                if await submit_btn.count() > 0:
                    try:
                        log(f"[Measure] Attempting button click...")
                        await submit_btn.first.click(timeout=2000)
                        did_click = True
                    except Exception as e:
                        log(f"[Measure] Click failed: {e}")

                if not did_click:
                    try:
                        log(f"[Measure] Trying requestSubmit()...")
                        await form.evaluate("el => el.requestSubmit()")
                        did_click = True
                    except Exception:
                        pass

                if not did_click:
                    try:
                        log(f"[Measure] Falling back to el.submit()...")
                        await form.evaluate("el => el.submit()")
                    except Exception:
                        pass

                # Wait for navigation or AJAX completion (replaces deprecated expect_navigation)
                try:
                    await page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass

                current_full_url = page.url
                if "#wpcf7" in current_full_url or "#confirmation" in current_full_url:
                    form_data["redirect_url"] = current_full_url
                else:
                    form_data["redirect_url"] = current_full_url if current_full_url.split('#')[0] != url.split('#')[0] else None
            except Exception as e:
                log(f"[Measure] Submission error: {e}")

            # BUG-01: Wrap post-submission processing in try/finally to guarantee listener cleanup
            try:
                # Evaluate AJAX activity
                successful_ajax = [res for res in ajax_responses if 200 <= res["status"] < 300]
                if not form_data["redirect_url"] and len(ajax_requests) > 0:
                    log(f"[Measure] Detected {len(ajax_requests)} background requests ({len(successful_ajax)} successful).")
                    form_data["is_ajax_submission"] = True
                    form_data["has_successful_ajax"] = len(successful_ajax) > 0
                    if successful_ajax:
                        form_data["ajax_endpoint"] = successful_ajax[0]["url"]

                try:
                    form_is_connected = await form.evaluate("el => el.isConnected")
                except Exception:
                    form_is_connected = False

                form_data["is_spa_unmounted"] = form_was_connected and not form_is_connected
                if form_data["is_spa_unmounted"]:
                    log("[Measure] SPA Form Unmount detected.")

                form_data["datalayer_events"] = datalayer_events[dl_start_len:]

                # DataLayer Diff — after submit
                try:
                    dl_after_keys = set(await page.evaluate("""
                        () => {
                            const flat = {};
                            (window.dataLayer || []).forEach(obj => {
                                if (typeof obj === 'object' && !Array.isArray(obj))
                                    Object.keys(obj).forEach(k => flat[k] = true);
                            });
                            return Object.keys(flat);
                        }
                    """))
                    dl_ignored = {"event", "gtm.uniqueEventId", "gtm.start", "gtm.element",
                                  "gtm.elementClasses", "gtm.elementId", "gtm.elementTarget", "gtm.elementUrl"}
                    form_data["datalayer_diff"] = {
                        "added_keys": list((dl_after_keys - dl_before_keys) - dl_ignored),
                    }
                    if form_data["datalayer_diff"]["added_keys"]:
                        log(f"[Measure] DataLayer Diff: {form_data['datalayer_diff']['added_keys']} new keys")
                except Exception:
                    form_data["datalayer_diff"] = {"added_keys": []}

                # Dynamic redirect detection
                if form_data.get("redirect_url"):
                    import re as _re
                    redirect_path = urlparse(form_data["redirect_url"]).path
                    full_redirect = form_data["redirect_url"]
                    if _re.search(r'[?&][a-z_]+=\d+', full_redirect) or _re.search(r'/\d+(/|$)', redirect_path):
                        form_data["redirect_is_dynamic"] = True
                    else:
                        form_data["redirect_is_dynamic"] = False

                # Data Type Inference
                dl_events_after = datalayer_events[dl_start_len:]
                payload_schema = {}
                for ev in dl_events_after:
                    if isinstance(ev, list) and len(ev) > 0 and isinstance(ev[0], dict):
                        for k, v in ev[0].items():
                            if k in {"event", "gtm.uniqueEventId", "gtm.start"}:
                                continue
                            if isinstance(v, bool):      payload_schema[k] = "boolean"
                            elif isinstance(v, int):     payload_schema[k] = "integer"
                            elif isinstance(v, float):   payload_schema[k] = "number"
                            elif isinstance(v, list):    payload_schema[k] = "array"
                            elif isinstance(v, dict):    payload_schema[k] = "object"
                            else:                        payload_schema[k] = "string"
                form_data["payload_schema"] = payload_schema

                # Success element detection
                SUCCESS_SELECTORS = [
                    # CF7 (classic + v5.6+)
                    ".wpcf7-mail-sent-ok",
                    ".wpcf7 form.sent .wpcf7-response-output",
                    ".wpcf7-response-output[role='alert']",
                    # Gravity Forms
                    ".gform_confirmation_message",
                    # WPForms
                    ".wpforms-confirmation",
                    # Formidable Forms
                    ".frm_message",
                    # MailChimp embedded
                    ".mce-success-response[style*='display: block']",
                    "#mce-success-response:not([style*='display: none'])",
                    # Generic success patterns
                    "[class*='success'][style*='display: block']",
                    "[class*='thank-you']",
                    "[class*='confirmation']",
                ]
                form_data["success_element_selector"] = None
                form_data["is_successful_submission"] = False
                for selector in SUCCESS_SELECTORS:
                    try:
                        count = await page.locator(selector).count()
                        if count > 0:
                            is_visible = await page.locator(selector).first.is_visible()
                            if is_visible:
                                form_data["success_element_selector"] = selector
                                form_data["success_message_text"] = await page.locator(selector).first.inner_text()
                                form_data["is_successful_submission"] = True
                                log(f"[Measure] Success element: {selector}")
                                break
                    except Exception:
                        pass

                # Determine if submission was successful
                has_dl_event = any(isinstance(e, list) and len(e) > 0 and isinstance(e[0], dict) and e[0].get("event") for e in form_data.get("datalayer_events", []))
                if form_data.get("redirect_url") or form_data.get("success_element_selector") or has_dl_event or form_data.get("has_successful_ajax") or form_data.get("is_spa_unmounted"):
                    form_data["is_successful_submission"] = True

                # Build available_tracking_methods
                form_data["available_tracking_methods"] = []

                if has_dl_event:
                    dl_obj = next((e[0] for e in form_data.get("datalayer_events", []) if isinstance(e, list) and len(e) > 0 and isinstance(e[0], dict) and e[0].get("event")), {})
                    custom_event_name = dl_obj.get("event", "custom_event")
                    cond = {"event": custom_event_name}
                    if form_data.get("cf7_form_id"):
                        cond["cf7_form_id"] = form_data["cf7_form_id"]
                    elif form_data.get("form_id"):
                        cond["form_id"] = form_data["form_id"]
                    ignored_keys = {"event", "gtm.uniqueEventId", "gtm.start", "gtm.element", "gtm.elementClasses", "gtm.elementId", "gtm.elementTarget", "gtm.elementUrl"}
                    payload_keys = [k for k in dl_obj.keys() if k not in ignored_keys]
                    form_data["available_tracking_methods"].append({
                        "method": "custom_event", "priority": 1,
                        "reason": "Found explicit custom dataLayer event during submission.",
                        "trigger_condition": cond, "payload_keys": payload_keys
                    })

                if form_data.get("is_spa_unmounted"):
                    form_data["available_tracking_methods"].append({
                        "method": "ajax_complete", "priority": 3,
                        "reason": "SPA: Form was unmounted from DOM upon success.",
                        "trigger_condition": {"event": "ajaxComplete"}
                    })

                if form_data.get("success_element_selector"):
                    form_data["available_tracking_methods"].append({
                        "method": "element_visibility", "priority": 2,
                        "reason": "Detected visible success message after submission.",
                        "trigger_condition": {"selector": form_data["success_element_selector"]},
                        "payload_keys": form_data.get("dom_payload_keys", [])
                    })

                if form_data.get("redirect_url"):
                    parsed_redir = urlparse(form_data["redirect_url"])
                    cond = {"page_path": parsed_redir.path}
                    if parsed_redir.fragment:
                        cond["fragment"] = parsed_redir.fragment
                    form_data["available_tracking_methods"].append({
                        "method": "page_view", "priority": 3,
                        "reason": "User redirected to new URL after submission.",
                        "trigger_condition": cond,
                        "payload_keys": form_data.get("dom_payload_keys", [])
                    })

                if not form_data.get("is_ajax_submission") and (form_data.get("form_id") or form_data.get("form_classes")):
                    cond = {}
                    if form_data.get("form_id"):
                        cond = {"key": "id", "value": form_data["form_id"]}
                    elif form_data.get("form_classes"):
                        cond = {"key": "class", "value": form_data["form_classes"]}
                    if cond:
                        form_data["available_tracking_methods"].append({
                            "method": "form_submission", "priority": 4,
                            "reason": "Standard HTML form submission with identifying attributes.",
                            "trigger_condition": cond,
                            "payload_keys": form_data.get("dom_payload_keys", [])
                        })

                if form_data.get("has_successful_ajax"):
                    form_data["available_tracking_methods"].append({
                        "method": "ajax_complete", "priority": 2,
                        "reason": "AJAX submission succeeded. Utilizing Bounteous AJAX listener.",
                        "trigger_condition": {"event": "ajaxComplete"}
                    })

                form_data["form_submitted"] = True
                result["forms_processed"].append(form_data)
            except Exception as e:
                log(f"[Measure] Post-submission processing error: {e}")
            finally:
                # Cache submitted form regardless of post-processing errors (prevents duplicate submissions)
                if target_hash:
                    cache.add(target_hash)
                    save_cache(cache, session_id=session_id)
                # P2-1: Guaranteed removal of per-form AJAX listeners (BUG-01)
                page.remove_listener("request", req_handler)
                page.remove_listener("response", res_handler)

            await page.wait_for_timeout(2000)

        result["datalayer_events"] = datalayer_events

    log(f"[Measure] Done. {len(result['forms_processed'])} form(s) measured.")
    return result


async def crawl_site(url: str, log_callback=None, ignore_cache=False, debug_dir=".debug",
                     session_id: str | None = None, browser_pool=None,
                     navigation_timeout: int = 60000, screenshot_timeout: int = 5000) -> dict:
    """
    Legacy all-in-one crawl: discovers + fills + submits ALL forms on the page.
    Now uses browser_session() to eliminate duplicated boilerplate (P0-1 + P0-2).
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)

    result = {
        "url": url,
        "page_path": urlparse(url).path or "/",
        "platform": "unknown",
        "forms_processed": [],
        "datalayer_events": [],
        "gtag_events": [],
        "has_phone_links": False,
        "has_email_links": False
    }

    cache = load_cache(session_id=session_id)

    async with browser_session(
        url, log,
        browser_pool=browser_pool,
        navigation_timeout=navigation_timeout,
        intercept_datalayer=True,
    ) as session:
        if session["nav_failed"]:
            return result

        page = session["page"]
        datalayer_events = session["datalayer_events"]
        result["platform"] = session["platform"]
        result["has_phone_links"] = session["has_phone_links"]
        result["has_email_links"] = session["has_email_links"]

        # iFrame scanning
        detected_iframes = []
        try:
            iframe_elements = await page.locator("iframe").all()
            page_netloc = urlparse(url).netloc
            for iframe_el in iframe_elements:
                try:
                    src = await iframe_el.get_attribute("src") or ""
                    if not src:
                        continue
                    iframe_netloc = urlparse(src).netloc
                    is_cross = bool(iframe_netloc and iframe_netloc != page_netloc)
                    detected_iframes.append({
                        "src": src,
                        "is_cross_origin": is_cross,
                        "netloc": iframe_netloc or page_netloc,
                    })
                except Exception:
                    continue
            if detected_iframes:
                log(f"Detected {len(detected_iframes)} iframe(s): "
                    f"{sum(1 for i in detected_iframes if i['is_cross_origin'])} cross-origin")
        except Exception:
            pass
        result["detected_iframes"] = detected_iframes

        forms_count = await page.locator('form').count()
        log(f"Found {forms_count} form(s) after scroll.")
        for i in range(forms_count):
            # Ensure we are on the base page if a previous form navigated away
            if urlparse(page.url).netloc != urlparse(url).netloc or page.url != url:
                log(f"Redirect detected during crawl. Returning to base URL: {url}")
                await page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout)
                await page.wait_for_load_state("load", timeout=navigation_timeout // 2)
                await page.wait_for_timeout(3000)
                await sweep_popups(page, log)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(3000)

                # BUG-05: Re-validate form count after re-navigation (DOM may have changed)
                new_forms_count = await page.locator('form').count()
                if i >= new_forms_count:
                    log(f"Form #{i+1} no longer exists after re-navigation ({new_forms_count} form(s) now). Stopping.")
                    break

            form = page.locator('form').nth(i)
            form_data = {
                "form_index": i,
                "html_attributes": {},
                "cf7_form_id": None,
                "page_path": urlparse(url).path or "/",
                "redirect_url": None,
                "is_ajax_submission": False,
                "form_submitted": False
            }

            # Get HTML attributes of the form
            html = await form.evaluate("el => el.outerHTML")
            attributes = await form.evaluate("""el => {
                const attrs = {};
                for (const attr of el.attributes) {
                    attrs[attr.name] = attr.value;
                }
                return attrs;
            }""")

            form_hash = get_form_hash(url, html)
            if form_hash in cache and not ignore_cache:
                log(f"Form {form_hash[:8]} already submitted previously according to cache. Skipping.")
                continue
            elif form_hash in cache and ignore_cache:
                log(f"Form {form_hash[:8]} in cache, but ignoring due to `ignore_cache=True`.")

            log(f"Found new form (#{i+1}). Interacting and filling inputs...")
            form_data["html_attributes"] = attributes

            # Screenshot
            try:
                os.makedirs(debug_dir, exist_ok=True)
                screenshot_path = os.path.join(debug_dir, f"form_{i}.png")
                try:
                    await form.scroll_into_view_if_needed(timeout=1500)
                except Exception:
                    pass
                try:
                    box = await form.bounding_box()
                    if box and (box['width'] < 400 or box['height'] < 100):
                        parent = await form.evaluate_handle("el => el.parentElement")
                        await parent.screenshot(path=screenshot_path, timeout=3000, force=True)
                    else:
                        await form.screenshot(path=screenshot_path, timeout=3000, force=True)
                except Exception as e:
                    log(f"  ↳ [Vision AI] Skipped screenshot for form #{i} (timeout/hidden). Relying on DOM context.")
            except Exception:
                pass

            # Extract form context (labels and hierarchy)
            form_metadata = await form.evaluate("""el => {
                const data = {
                    field_labels: {},
                    parent_context: []
                };
                const inputs = el.querySelectorAll('input, select, textarea');
                inputs.forEach(input => {
                    const id = input.id;
                    const name = input.name;
                    const key = name || id;
                    if (!key) return;
                    if (id) {
                        const lbl = document.querySelector(`label[for="${id}"]`);
                        if (lbl) { data.field_labels[key] = lbl.innerText.trim(); return; }
                    }
                    let parentLabel = input.closest('label');
                    if (parentLabel) { data.field_labels[key] = parentLabel.innerText.trim(); return; }
                    let prev = input.previousElementSibling;
                    if (prev && prev.tagName === 'LABEL') {
                        data.field_labels[key] = prev.innerText.trim();
                    }
                });
                let current = el.parentElement;
                while (current && current.tagName !== 'BODY') {
                    data.parent_context.push({
                        tag: current.tagName.toLowerCase(),
                        id: current.id,
                        classes: current.className
                    });
                    current = current.parentElement;
                }
                return data;
            }""")

            form_data["field_labels"] = form_metadata["field_labels"]
            form_data["parent_context"] = form_metadata["parent_context"]

            # CF7 form ID extraction
            cf7_id = attributes.get("data-id")
            if not cf7_id:
                for parent in form_data["parent_context"]:
                    parent_id = parent.get("id", "")
                    if "wpcf7-f" in parent_id:
                        import re
                        match = re.search(r"wpcf7-f(\d+)", parent_id)
                        if match:
                            cf7_id = match.group(1)
                            break
            if not cf7_id:
                 for cls in attributes.get("class", "").split():
                     if cls.startswith("wpcf7-f"):
                         cf7_id = cls.replace("wpcf7-f", "")
                         break

            if cf7_id:
                form_data["cf7_form_id"] = cf7_id
                log(f"  ↳ Identified as CF7 Form (ID: {cf7_id})")
            if form_data["field_labels"]:
                log(f"  → Labels captured: {list(form_data['field_labels'].values())[:3]}")

            # Form title
            form_data["form_title"] = await form.evaluate("""el => {
                let title = el.querySelector('legend, h1, h2, h3, h4, h5, h6')?.innerText;
                if (!title) {
                    let prev = el.previousElementSibling;
                    while (prev && !prev.tagName.match(/^H[1-6]$/)) { prev = prev.previousElementSibling; }
                    if (prev) title = prev.innerText;
                }
                if (!title) {
                    let parent = el.parentElement;
                    while (parent && !title) {
                        const heading = parent.querySelector('h1, h2, h3, h4, h5, h6');
                        if (heading && heading.innerText.trim()) { title = heading.innerText; }
                        parent = parent.parentElement;
                        if (parent && /^(SECTION|MAIN|BODY|FOOTER)$/.test(parent.tagName)) break;
                    }
                }
                return title ? title.trim() : null;
            }""")

            # Surrounding context
            form_data["surrounding_context"] = await form.evaluate("""el => {
                const context = [];
                const checkElement = (node) => {
                    if (node && node.tagName.match(/^(H[1-6]|P|DIV)$/)) {
                        const text = node.innerText.trim();
                        if (text && text.length > 5 && text.length < 500) {
                            context.push({tag: node.tagName.toLowerCase(), text: text});
                            return true;
                        }
                    }
                    return false;
                };
                let prev = el.previousElementSibling;
                let count = 0;
                while (prev && count < 3) { if (checkElement(prev)) count++; prev = prev.previousElementSibling; }
                if (context.length === 0 && el.parentElement) {
                    let pSib = el.parentElement.previousElementSibling;
                    while (pSib && count < 3) { if (checkElement(pSib)) count++; pSib = pSib.previousElementSibling; }
                }
                return context;
            }""")

            # ── Comprehensive form filling ──────────────────────────────────────
            test_email = _generate_test_email()
            for el in await form.locator('input[type="email"]').all():
                try: await el.fill(test_email)
                except Exception: pass

            for el in await form.locator('input[type="text"], input[type="tel"], input[type="number"], input:not([type])').all():
                try:
                    name = (await el.get_attribute("name") or
                            await el.get_attribute("placeholder") or "field").lower()
                    if any(k in name for k in ["phone", "tel", "mob", "gsm"]):
                        await el.fill("0896248833")
                    elif any(k in name for k in ["name", "first", "last", "names", "ime"]):
                        await el.fill("Ivan Ivanov")
                except Exception:
                    pass
            log(f"Filling form #{i+1}...")
            form_data["dom_payload_keys"] = []

            for el in await form.locator("input[type='text'], input[type='email'], input[type='tel'], textarea").all():
                try:
                    name = await el.get_attribute("name") or "field"
                    if name not in form_data["dom_payload_keys"] and name != "field":
                        form_data["dom_payload_keys"].append(name)
                    if "email" in name.lower() or (await el.get_attribute("type")) == "email":
                        await el.fill(test_email)
                    else:
                        await el.fill(f"test_{name}")
                except Exception: pass

            for el in await form.locator("select").all():
                try:
                    name = await el.get_attribute("name") or "select_field"
                    if name not in form_data["dom_payload_keys"] and name != "select_field":
                        form_data["dom_payload_keys"].append(name)
                    options = await el.locator("option").all()
                    for opt in options:
                        val = await opt.get_attribute("value") or ""
                        if val.strip():
                            await el.select_option(val)
                            break
                except Exception: pass

            for el in await form.locator('input[type="checkbox"]').all():
                try:
                    if not await el.is_checked():
                        await el.check()
                except Exception: pass

            seen_radio_names = set()
            for el in await form.locator('input[type="radio"]').all():
                try:
                    name = await el.get_attribute("name") or ""
                    if name not in form_data["dom_payload_keys"] and name != "":
                        form_data["dom_payload_keys"].append(name)
                    if name not in seen_radio_names:
                        await el.check()
                        seen_radio_names.add(name)
                except Exception: pass

            for el in await form.locator('input[type="hidden"]').all():
                try:
                    name = await el.get_attribute("name") or ""
                    if name and not name.startswith("_wp") and not name.startswith("_wpcf7") and name not in form_data["dom_payload_keys"]:
                        form_data["dom_payload_keys"].append(name)
                except Exception: pass

            log(f"  All fields filled for form #{i+1} (Title: {form_data['form_title']} | ID: {form_data['cf7_form_id']})")

            # Submit form — P2-1: scoped AJAX listeners
            ajax_requests = []
            ajax_responses = []
            req_handler = lambda req: ajax_requests.append(req.url) if req.method in ["POST", "PUT", "PATCH"] else None
            res_handler = lambda res: ajax_responses.append({"url": res.request.url, "status": res.status}) if res.request.method in ["POST", "PUT", "PATCH"] else None
            page.on("request", req_handler)
            page.on("response", res_handler)

            dl_start_len = len(datalayer_events)

            try:
                dl_before_keys = set(await page.evaluate("""
                    () => {
                        const flat = {};
                        (window.dataLayer || []).forEach(obj => {
                            if (typeof obj === 'object' && !Array.isArray(obj))
                                Object.keys(obj).forEach(k => flat[k] = true);
                        });
                        return Object.keys(flat);
                    }
                """))
            except Exception:
                dl_before_keys = set()

            try:
                form_was_connected = await form.evaluate("el => el.isConnected")
            except Exception:
                form_was_connected = False

            try:
                # CANONICAL SUBMISSION STRATEGY (BUG-03 + CROSS-03: separated click from navigation wait)
                submit_btn = form.locator('button[type="submit"], input[type="submit"], button:has-text("Изпрати"), button:has-text("Submit"), button:has-text("Send"), .wpcf7-submit')
                did_click = False
                if await submit_btn.count() > 0:
                    try:
                        log(f"  → Attempting button click (Selector: {await submit_btn.first.evaluate('el => el.className')})")
                        await submit_btn.first.click(timeout=2000)
                        did_click = True
                    except Exception as e:
                        log(f"  ↳ Click failed: {e}")

                if not did_click:
                    try:
                        log(f"  → Standard click failed. Trying requestSubmit().")
                        await form.evaluate("el => el.requestSubmit()")
                        did_click = True
                    except Exception:
                        pass

                if not did_click:
                    try:
                        log(f"  → All canonical methods failed. Falling back to direct el.submit().")
                        await form.evaluate("el => el.submit()")
                    except Exception:
                        pass

                # Wait for navigation or AJAX completion (replaces deprecated expect_navigation)
                try:
                    await page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass

                current_full_url = page.url
                if "#wpcf7" in current_full_url or "#confirmation" in current_full_url:
                    form_data["redirect_url"] = current_full_url
                else:
                    form_data["redirect_url"] = current_full_url if current_full_url.split('#')[0] != url.split('#')[0] else None
            except Exception as e:
                log(f"  ⚠️ Submission error for form #{i+1}: {e}")

            # BUG-06: Wrap post-submission processing in try/finally to guarantee listener cleanup
            try:
                # Evaluate AJAX activity
                successful_ajax = [res for res in ajax_responses if 200 <= res["status"] < 300]
                if not form_data["redirect_url"] and len(ajax_requests) > 0:
                    log(f"Detected {len(ajax_requests)} background API requests ({len(successful_ajax)} successful) after clicking submit.")
                    form_data["is_ajax_submission"] = True
                    form_data["has_successful_ajax"] = len(successful_ajax) > 0
                    if successful_ajax:
                        form_data["ajax_endpoint"] = successful_ajax[0]["url"]

                try:
                    form_is_connected = await form.evaluate("el => el.isConnected")
                except Exception:
                    form_is_connected = False

                form_data["is_spa_unmounted"] = form_was_connected and not form_is_connected
                if form_data["is_spa_unmounted"]:
                    log("  → SPA Form Unmount detected (form removed from DOM after submission).")

                form_data["datalayer_events"] = datalayer_events[dl_start_len:]

                try:
                    dl_after_keys = set(await page.evaluate("""
                        () => {
                            const flat = {};
                            (window.dataLayer || []).forEach(obj => {
                                if (typeof obj === 'object' && !Array.isArray(obj))
                                    Object.keys(obj).forEach(k => flat[k] = true);
                            });
                            return Object.keys(flat);
                        }
                    """))
                    dl_ignored = {"event", "gtm.uniqueEventId", "gtm.start", "gtm.element",
                                  "gtm.elementClasses", "gtm.elementId", "gtm.elementTarget", "gtm.elementUrl"}
                    form_data["datalayer_diff"] = {
                        "added_keys": list((dl_after_keys - dl_before_keys) - dl_ignored),
                    }
                    if form_data["datalayer_diff"]["added_keys"]:
                        log(f"  → DataLayer Diff: {form_data['datalayer_diff']['added_keys']} new keys after submit")
                except Exception:
                    form_data["datalayer_diff"] = {"added_keys": []}

                if form_data.get("redirect_url"):
                    import re as _re
                    redirect_path = urlparse(form_data["redirect_url"]).path
                    full_redirect = form_data["redirect_url"]
                    if _re.search(r'[?&][a-z_]+=\d+', full_redirect) or _re.search(r'/\d+(/|$)', redirect_path):
                        form_data["redirect_is_dynamic"] = True
                        log(f"  → Dynamic redirect detected. Delegating Regex generation to AI.")
                    else:
                        form_data["redirect_is_dynamic"] = False

                dl_events_after = datalayer_events[dl_start_len:]
                payload_schema = {}
                for ev in dl_events_after:
                    if isinstance(ev, list) and len(ev) > 0 and isinstance(ev[0], dict):
                        for k, v in ev[0].items():
                            if k in {"event", "gtm.uniqueEventId", "gtm.start"}: continue
                            if isinstance(v, bool):      payload_schema[k] = "boolean"
                            elif isinstance(v, int):     payload_schema[k] = "integer"
                            elif isinstance(v, float):   payload_schema[k] = "number"
                            elif isinstance(v, list):    payload_schema[k] = "array"
                            elif isinstance(v, dict):    payload_schema[k] = "object"
                            else:                        payload_schema[k] = "string"
                form_data["payload_schema"] = payload_schema
                if payload_schema:
                    log(f"  → Type inference: {payload_schema}")

                form_data["form_id"] = attributes.get("id")
                form_data["form_classes"] = attributes.get("class")
                form_data["form_action"] = attributes.get("action")

                SUCCESS_SELECTORS = [
                    # CF7 (classic + v5.6+)
                    ".wpcf7-mail-sent-ok",
                    ".wpcf7 form.sent .wpcf7-response-output",
                    ".wpcf7-response-output[role='alert']",
                    # Gravity Forms
                    ".gform_confirmation_message",
                    # WPForms
                    ".wpforms-confirmation",
                    # Formidable Forms
                    ".frm_message",
                    # MailChimp embedded
                    ".mce-success-response[style*='display: block']",
                    "#mce-success-response:not([style*='display: none'])",
                    # Generic success patterns
                    "[class*='success'][style*='display: block']",
                    "[class*='thank-you']",
                    "[class*='confirmation']",
                ]
                form_data["success_element_selector"] = None
                form_data["is_successful_submission"] = False
                for selector in SUCCESS_SELECTORS:
                    try:
                        count = await page.locator(selector).count()
                        if count > 0:
                            is_visible = await page.locator(selector).first.is_visible()
                            if is_visible:
                                form_data["success_element_selector"] = selector
                                form_data["success_message_text"] = await page.locator(selector).first.inner_text()
                                form_data["is_successful_submission"] = True
                                log(f"  → Success element detected: {selector} (Text: {form_data['success_message_text'][:30]}...)")
                                break
                    except Exception:
                        pass

                has_dl_event = any(isinstance(e, list) and len(e)>0 and isinstance(e[0], dict) and e[0].get("event") for e in form_data["datalayer_events"])
                if form_data["redirect_url"] or form_data["success_element_selector"] or has_dl_event or form_data.get("has_successful_ajax") or form_data.get("is_spa_unmounted"):
                    form_data["is_successful_submission"] = True

                form_data["available_tracking_methods"] = []

                if has_dl_event:
                    dl_obj = next((e[0] for e in form_data["datalayer_events"] if isinstance(e, list) and len(e)>0 and isinstance(e[0], dict) and e[0].get("event")), {})
                    custom_event_name = dl_obj.get("event", "custom_event")
                    cond = {"event": custom_event_name}
                    if form_data.get("cf7_form_id"):
                        cond["cf7_form_id"] = form_data["cf7_form_id"]
                    elif form_data.get("form_id"):
                        cond["form_id"] = form_data["form_id"]
                    ignored_keys = {"event", "gtm.uniqueEventId", "gtm.start", "gtm.element", "gtm.elementClasses", "gtm.elementId", "gtm.elementTarget", "gtm.elementUrl"}
                    payload_keys = [k for k in dl_obj.keys() if k not in ignored_keys]
                    form_data["available_tracking_methods"].append({
                        "method": "custom_event",
                        "priority": 1,
                        "reason": "Found explicit custom dataLayer event during submission.",
                        "trigger_condition": cond,
                        "payload_keys": payload_keys
                    })

                if form_data.get("is_spa_unmounted"):
                    form_data["available_tracking_methods"].append({
                        "method": "ajax_complete",
                        "priority": 3,
                        "reason": "Single Page Application (SPA): Form was unmounted from the DOM upon success.",
                        "trigger_condition": {"event": "ajaxComplete"}
                    })

                if form_data.get("success_element_selector"):
                    form_data["available_tracking_methods"].append({
                        "method": "element_visibility",
                        "priority": 2,
                        "reason": "Detected a visible success message/element after submission.",
                        "trigger_condition": {"selector": form_data["success_element_selector"]},
                        "payload_keys": form_data.get("dom_payload_keys", [])
                    })

                if form_data.get("redirect_url"):
                    parsed_redir = urlparse(form_data["redirect_url"])
                    cond = {"page_path": parsed_redir.path}
                    if parsed_redir.fragment:
                        cond["fragment"] = parsed_redir.fragment
                    form_data["available_tracking_methods"].append({
                        "method": "page_view",
                        "priority": 3,
                        "reason": "User was redirected to a new URL or hash fragment after submission.",
                        "trigger_condition": cond,
                        "payload_keys": form_data.get("dom_payload_keys", [])
                    })

                if not form_data.get("is_ajax_submission") and (form_data.get("form_id") or form_data.get("form_classes")):
                    cond = {}
                    if form_data.get("form_id"):
                        cond = {"key": "id", "value": form_data["form_id"]}
                    elif form_data.get("form_classes"):
                        cond = {"key": "class", "value": form_data["form_classes"]}
                    if cond:
                        form_data["available_tracking_methods"].append({
                            "method": "form_submission",
                            "priority": 4,
                            "reason": "Standard HTML form submission detected with identifying attributes.",
                            "trigger_condition": cond,
                            "payload_keys": form_data.get("dom_payload_keys", [])
                        })

                if form_data.get("has_successful_ajax"):
                    form_data["available_tracking_methods"].append({
                        "method": "ajax_complete",
                        "priority": 2,
                        "reason": "AJAX submission succeeded. Utilizing Bounteous AJAX listener.",
                        "trigger_condition": {"event": "ajaxComplete"}
                    })

                form_data["form_submitted"] = True
                result["forms_processed"].append(form_data)
            except Exception as e:
                log(f"  ⚠️ Post-submission processing error for form #{i+1}: {e}")
            finally:
                # Cache submitted form regardless of post-processing errors (prevents duplicate submissions)
                cache.add(form_hash)
                save_cache(cache, session_id=session_id)
                # P2-1: Guaranteed removal of per-form AJAX listeners (BUG-06)
                page.remove_listener("request", req_handler)
                page.remove_listener("response", res_handler)

            await page.wait_for_timeout(2000)

        result["datalayer_events"] = datalayer_events

        # ── Shadow Form Hunter ──────────────────────────────────────
        try:
            loose_emails = await page.locator('input[type="email"], input[name*="email"], input[placeholder*="имейл"], input[placeholder*="email"]').all()
            for i, el in enumerate(loose_emails):
                if not await el.is_visible(): continue

                in_form = await el.evaluate("el => !!el.closest('form')")
                if in_form: continue

                container_handle = await el.evaluate_handle("el => el.closest('.elementor-widget, .et_pb_module, .wp-block-group, div[class*=\"newsletter\"], div[class*=\"form\"]') || el.parentElement")
                html = await container_handle.evaluate("el => el.outerHTML")

                form_hash = get_form_hash(url, html + "_shadow")
                if form_hash in cache: continue

                log(f"Found potential 'Shadow Form' interaction cluster (no <form> tag).")

                attributes = await container_handle.evaluate("""el => {
                    const attrs = {};
                    for (const attr of el.attributes) { attrs[attr.name] = attr.value; }
                    return attrs;
                }""")

                shadow_data = {
                    "form_index": f"shadow_{i}",
                    "is_shadow_form": True,
                    "is_successful_submission": True,
                    "html_attributes": attributes,
                    "form_id": attributes.get("id"),
                    "form_classes": attributes.get("class"),
                    "page_path": urlparse(url).path or "/",
                    "form_submitted": False,
                    "dom_payload_keys": [await el.get_attribute("name") or "email"],
                    "available_tracking_methods": [
                        {
                            "method": "element_visibility",
                            "priority": 1,
                            "reason": "Interaction cluster detected outside of standard <form> tag. Tracking via element visibility of the container.",
                            "trigger_condition": {"selector": f"#{attributes.get('id')}" if attributes.get('id') else f".{attributes.get('class', '').replace(' ', '.')}" if attributes.get('class') else "div"}
                        }
                    ]
                }

                try:
                    screenshot_path = os.path.join(debug_dir, f"form_shadow_{i}.png")
                    try:
                        await container_handle.scroll_into_view_if_needed(timeout=screenshot_timeout)
                        await container_handle.screenshot(path=screenshot_path, timeout=screenshot_timeout, force=True)
                    except Exception:
                        pass
                except Exception:
                    pass
                result["forms_processed"].append(shadow_data)
                log(f"  → Shadow Form added (Classes: {attributes.get('class', 'none')[:30]}...)")
        except Exception as e:
            log(f"  → Shadow Form detection error: {e}")

    return result

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        res = asyncio.run(crawl_site(sys.argv[1]))
        print(json.dumps(res, indent=2))

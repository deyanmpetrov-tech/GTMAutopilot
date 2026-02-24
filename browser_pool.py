"""
browser_pool.py — Singleton Playwright browser with BrowserContext pooling.

A single Chromium process serves all concurrent crawl sessions via isolated
BrowserContexts (~2 MB each vs ~100-150 MB per full browser launch).
An asyncio.Semaphore caps the number of simultaneous contexts to prevent OOM.

Usage::

    pool = BrowserPool.get_instance(max_contexts=4)

    async with pool.acquire_context() as context:
        page = await context.new_page()
        await page.goto("https://example.com")

    # On shutdown:
    await pool.shutdown()
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from playwright.async_api import async_playwright, Browser, BrowserContext, Playwright


class BrowserPool:
    """
    Singleton Playwright browser manager with semaphore-guarded context pooling.
    """

    _instance: BrowserPool | None = None

    def __init__(self, max_contexts: int = 4):
        self._max_contexts = max_contexts
        self._semaphore = asyncio.Semaphore(max_contexts)
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._lock = asyncio.Lock()
        self._active_contexts: int = 0

    # ── Singleton ───────────────────────────────────────────────────────────

    @classmethod
    def get_instance(cls, max_contexts: int = 4) -> BrowserPool:
        if cls._instance is None:
            cls._instance = cls(max_contexts=max_contexts)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (useful for testing)."""
        cls._instance = None

    # ── Lazy Browser Initialization ─────────────────────────────────────────

    async def _ensure_browser(self) -> Browser:
        """Start Playwright + Chromium once; auto-reconnect on crash."""
        if self._browser and self._browser.is_connected():
            return self._browser

        async with self._lock:
            # Double-check after acquiring lock
            if self._browser and self._browser.is_connected():
                return self._browser

            if self._playwright is None:
                self._playwright = await async_playwright().start()

            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-dev-shm-usage",    # Prevent /dev/shm exhaustion in Docker
                    "--no-sandbox",
                    "--disable-gpu",
                ],
            )
            return self._browser

    # ── Context Acquisition ─────────────────────────────────────────────────

    @asynccontextmanager
    async def acquire_context(self):
        """
        Acquire a semaphore slot, create an isolated BrowserContext,
        yield it, then guarantee cleanup.

        Blocks if ``max_contexts`` contexts are already active.
        """
        await self._semaphore.acquire()
        self._active_contexts += 1
        context: BrowserContext | None = None
        try:
            browser = await self._ensure_browser()
            context = await browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            yield context
        finally:
            if context:
                try:
                    await context.close()
                except Exception:
                    pass
            self._active_contexts -= 1
            self._semaphore.release()

    # ── Shutdown ────────────────────────────────────────────────────────────

    async def shutdown(self) -> None:
        """Graceful shutdown.  Call on application exit (e.g. FastAPI lifespan)."""
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    # ── Stats ───────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        return {
            "max_contexts": self._max_contexts,
            "active_contexts": self._active_contexts,
            "browser_connected": (
                self._browser.is_connected() if self._browser else False
            ),
        }

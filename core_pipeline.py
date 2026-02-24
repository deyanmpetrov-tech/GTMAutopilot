"""
core_pipeline.py — Central PipelineOrchestrator & event-listener protocol.

Encapsulates the full GTM analysis flow so that both the Streamlit UI (app.py)
and the FastAPI backend (api.py) can run the *same* pipeline with different
event listeners for UI updates vs. silent background state mutations.

Four-phase "User-in-the-Loop" execution::

    config = PipelineConfig(target_url=..., gtm_data=..., gemini_api_key=...)
    orchestrator = PipelineOrchestrator(config, listener=my_listener)

    result = await orchestrator.discover()              # Phase 1: passive scan + AI classify
    # ... user selects forms ...
    result = await orchestrator.measure(approved_forms)  # Phase 2: targeted crawl + signal extraction
    # ... user reviews extracted_signals, selects tracking method per form ...
    result = await orchestrator.analyze_with_methods(user_methods)  # Phase 3: AI plan (constrained)
    # ... user reviews result.draft_plan ...
    final  = await orchestrator.compile(approved_plan)   # Phase 4: compile + export

Legacy two-phase mode (backward compat):

    result = await orchestrator.analyze()      # Phase 1+2+3 combined (crawl_site)
    final  = await orchestrator.compile(approved_plan)
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


# ── Pipeline Stages ─────────────────────────────────────────────────────────

class PipelineStage(str, Enum):
    CONTEXT_EXTRACT = "context_extract"
    PASSIVE_CRAWL = "passive_crawl"
    AI_CLASSIFY = "ai_classify"
    DISCOVERY_CHECKPOINT = "discovery_checkpoint"
    TARGETED_CRAWL = "targeted_crawl"
    SIGNAL_EXTRACT = "signal_extract"
    METHOD_SCORING = "method_scoring"
    AI_ANALYZE = "ai_analyze"
    REVIEW_CHECKPOINT = "review_checkpoint"
    COMPILE = "compile"
    HEAL = "heal"
    VALIDATE = "validate"
    EXPORT = "export"
    # Legacy alias (kept for backward compat with API listeners)
    CRAWL = "crawl"


# ── Configuration ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for a single pipeline run."""

    target_url: str
    gtm_data: dict
    gemini_api_key: str
    model: str = "gemini-2.5-flash"
    include_shadow_forms: bool = True
    include_iframes: bool = True
    ignore_cache: bool = False
    navigation_timeout: int = 60000      # ms — Playwright page.goto timeout
    screenshot_timeout: int = 5000       # ms — form screenshot capture timeout
    export_mode: str = "delta"  # "delta" or "full"
    scan_scope: str = "single_url"  # "single_url" or "entire_domain"
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def __post_init__(self):
        if self.export_mode not in ("delta", "full"):
            raise ValueError(f"export_mode must be 'delta' or 'full', got '{self.export_mode}'")
        if self.scan_scope not in ("single_url", "entire_domain"):
            raise ValueError(f"scan_scope must be 'single_url' or 'entire_domain', got '{self.scan_scope}'")


# ── Event Listener Protocol ─────────────────────────────────────────────────

class PipelineEventListener(Protocol):
    """Protocol that frontends implement to receive live pipeline events."""

    def on_step_start(self, stage: PipelineStage, message: str) -> None: ...
    def on_step_complete(self, stage: PipelineStage, message: str) -> None: ...
    def on_log(self, message: str) -> None: ...
    def on_error(self, stage: PipelineStage, error: Exception) -> None: ...
    def on_discovery_ready(self, discovered_forms: list[dict], classifications: dict) -> None: ...
    def on_review_ready(self, draft_plan: list[dict], crawler_data: dict) -> None: ...
    def on_complete(self, result: "PipelineResult") -> None: ...


class _NullListener:
    """No-op listener for CLI / testing."""

    def on_step_start(self, stage, message): pass
    def on_step_complete(self, stage, message): pass
    def on_log(self, message): pass
    def on_error(self, stage, error): pass
    def on_discovery_ready(self, discovered_forms, classifications): pass
    def on_review_ready(self, draft_plan, crawler_data): pass
    def on_complete(self, result): pass


class _TeeListener:
    """Wraps a PipelineEventListener and tees all messages to pipeline.log."""

    def __init__(self, inner: PipelineEventListener, log_path: str):
        self._inner = inner
        self._log_path = log_path
        import time as _time
        self._time = _time

    def _tee(self, msg: str):
        try:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(f"[{self._time.strftime('%H:%M:%S')}] {msg}\n")
        except Exception:
            pass  # never fail the pipeline over logging

    def on_step_start(self, stage, message):
        self._tee(f"── {stage.value}: {message}")
        self._inner.on_step_start(stage, message)

    def on_step_complete(self, stage, message):
        self._tee(f"   ↳ ✅ {message}")
        self._inner.on_step_complete(stage, message)

    def on_log(self, message):
        self._tee(message)
        self._inner.on_log(message)
    on_log._tees_to_pipeline_log = True

    def on_error(self, stage, error):
        self._tee(f"   ↳ ❌ {stage.value}: {error}")
        self._inner.on_error(stage, error)

    def on_discovery_ready(self, discovered_forms, classifications):
        self._tee(f"── discovery_ready: {len(discovered_forms or [])} form(s)")
        self._inner.on_discovery_ready(discovered_forms, classifications)

    def on_review_ready(self, draft_plan, crawler_data):
        self._tee(f"── review_ready: {len(draft_plan or [])} plan item(s)")
        self._inner.on_review_ready(draft_plan, crawler_data)

    def on_complete(self, result):
        self._tee(f"── complete: error={result.error or 'none'}")
        self._inner.on_complete(result)


# ── Pipeline Result ─────────────────────────────────────────────────────────

@dataclass
class PipelineResult:
    """Structured output of a completed (or failed) pipeline run."""

    session_id: str
    discovered_forms: list[dict] | None = None
    form_classifications: dict | None = None
    draft_plan: list[dict] | None = None
    compiled_gtm: dict | None = None
    validation_report: dict | None = None
    pipeline_warnings: dict | None = None
    debug_dir: str | None = None
    crawler_data: dict | None = None
    gtm_context: dict | None = None
    mechanical_ids: dict | None = None
    tracking_spec_md: str | None = None
    extracted_signals: list[dict] | None = None  # Per-form signal data for UI
    output_filename: str | None = None
    api_call_count: int | None = None  # Total Gemini API calls used
    error: str | None = None


# ── Orchestrator ────────────────────────────────────────────────────────────

class PipelineOrchestrator:
    """
    Central orchestrator that encapsulates the full GTM analysis pipeline.

    Four-phase "User-in-the-Loop" execution:
      1. ``discover()``              — passive crawl + AI classification → user selects forms
      2. ``measure(approved_forms)`` — targeted crawl + signal extraction → user selects tracking methods
      3. ``analyze_with_methods()``  — AI analysis constrained to user's methods → user reviews plan
      4. ``compile(approved_plan)``  — inject tags → heal → validate → export

    Legacy ``analyze()`` method combines phases 1+2+3 for backward compatibility.

    Both frontends (Streamlit, FastAPI) use the same orchestrator with
    different ``PipelineEventListener`` implementations.
    """

    def __init__(
        self,
        config: PipelineConfig,
        listener: PipelineEventListener | None = None,
        browser_pool=None,
    ):
        self._config = config
        self._result = PipelineResult(session_id=config.session_id)
        self._full_gtm = copy.deepcopy(config.gtm_data)
        self._browser_pool = browser_pool

        # ── Persistent session log (created from Phase 1 onwards) ──
        from brain import get_debug_dir
        self._debug_dir = get_debug_dir(config.session_id)
        os.makedirs(self._debug_dir, exist_ok=True)
        self._log_path = os.path.join(self._debug_dir, "pipeline.log")

        raw_listener = listener or _NullListener()
        self._listener = _TeeListener(raw_listener, self._log_path)

    @property
    def result(self) -> PipelineResult:
        return self._result

    # P2-10: Public setter for listener (avoids direct _listener mutation)
    def set_listener(self, listener: PipelineEventListener) -> None:
        """Replace the pipeline event listener (e.g. between phases in Streamlit)."""
        self._listener = _TeeListener(listener, self._log_path)

    # ── Phase 1: Discover ──────────────────────────────────────────────────

    async def discover(self) -> PipelineResult:
        """
        Phase 1: Passive discovery + AI classification.
        Scans the page for forms without interaction, classifies them via AI,
        then pauses at the discovery checkpoint for user form selection.
        """
        from brain import classify_forms
        from main import extract_gtm_context
        from crawler import discover_forms

        os.environ["GEMINI_API_KEY"] = self._config.gemini_api_key

        # ── Stage: Context Extraction ───────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.CONTEXT_EXTRACT, "Extracting GTM context..."
        )
        try:
            gtm_context = extract_gtm_context(self._config.gtm_data)
            self._result.gtm_context = {"ai_context": gtm_context["ai_context"]}
            self._result.mechanical_ids = gtm_context["mechanical_ids"]
            self._listener.on_step_complete(
                PipelineStage.CONTEXT_EXTRACT, "GTM context extracted."
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.CONTEXT_EXTRACT, e)
            self._result.error = str(e)
            return self._result

        # ── Stage: Passive Crawl ────────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.PASSIVE_CRAWL, "Scanning page for forms (no interaction)..."
        )
        self._result.debug_dir = self._debug_dir

        try:
            discovery = await discover_forms(
                self._config.target_url,
                log_callback=self._listener.on_log,
                debug_dir=self._debug_dir,
                session_id=self._config.session_id,
                browser_pool=self._browser_pool,
                navigation_timeout=self._config.navigation_timeout,
                screenshot_timeout=self._config.screenshot_timeout,
            )
            self._result.discovered_forms = discovery.get("forms_discovered", [])
            self._result.crawler_data = discovery
            forms_count = len(self._result.discovered_forms)
            self._listener.on_step_complete(
                PipelineStage.PASSIVE_CRAWL,
                f"Discovered {forms_count} form(s).",
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.PASSIVE_CRAWL, e)
            self._result.error = str(e)
            return self._result

        # ── Multi-Page Discovery (Domain Mode) ─────────────────────────
        if self._config.scan_scope == "entire_domain":
            from crawler import discover_form_pages
            self._listener.on_log("🌐 Domain mode: discovering additional form pages...")
            try:
                sub_urls = await discover_form_pages(
                    self._config.target_url,
                    max_pages=4,
                    browser_pool=self._browser_pool,
                    navigation_timeout=self._config.navigation_timeout,
                )
                self._listener.on_log(f"   ↳ Found {len(sub_urls)} subpage(s) with potential forms.")
                for sub_idx, sub_url in enumerate(sub_urls):
                    self._listener.on_log(f"   ↳ Scanning: {sub_url}")
                    try:
                        sub_discovery = await discover_forms(
                            sub_url,
                            log_callback=self._listener.on_log,
                            debug_dir=self._debug_dir,
                            session_id=self._config.session_id,
                            browser_pool=self._browser_pool,
                            navigation_timeout=self._config.navigation_timeout,
                            screenshot_timeout=self._config.screenshot_timeout,
                        )
                        for fi, sub_form in enumerate(sub_discovery.get("forms_discovered", [])):
                            sub_form["form_index"] = f"p{sub_idx}_{sub_form.get('form_index', fi)}"
                            sub_form["source_url"] = sub_url
                            self._result.discovered_forms.append(sub_form)
                    except Exception as sub_e:
                        self._listener.on_log(f"   ↳ ⚠️ Failed to scan {sub_url}: {sub_e}")
                total = len(self._result.discovered_forms)
                self._listener.on_step_complete(
                    PipelineStage.PASSIVE_CRAWL,
                    f"Domain scan complete. {total} form(s) across {1 + len(sub_urls)} page(s).",
                )
            except Exception as e:
                self._listener.on_log(f"   ↳ ⚠️ Domain discovery failed: {e}. Using single-page results.")
            # Sync merged forms back into discovery dict for classification
            discovery["forms_discovered"] = list(self._result.discovered_forms)

        # ── Stage: AI Classification ────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.AI_CLASSIFY, "Classifying discovered forms..."
        )
        try:
            loop = asyncio.get_running_loop()
            classifications = await loop.run_in_executor(
                None,
                lambda: classify_forms(
                    discovery_data=discovery,
                    model=self._config.model,
                    log_callback=self._listener.on_log,
                ),
            )
            self._result.form_classifications = classifications
            classified_count = len(classifications)
            self._listener.on_step_complete(
                PipelineStage.AI_CLASSIFY,
                f"Classified {classified_count} form(s).",
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.AI_CLASSIFY, e)
            self._result.error = str(e)
            return self._result

        # ── Discovery Checkpoint ────────────────────────────────────────
        self._listener.on_discovery_ready(
            self._result.discovered_forms, classifications
        )
        return self._result

    # ── Phase 2: Measure (crawler-only) ──────────────────────────────────

    async def measure(self, approved_forms: list[dict]) -> PipelineResult:
        """
        Phase 2: Targeted measurement ONLY — fills, submits, captures signals.
        Does NOT run AI analysis — that happens in analyze_with_methods()
        after the user reviews extracted signals and selects tracking methods.

        Populates ``result.extracted_signals`` with per-form signal data for UI.
        """
        # P1-9: Phase guard — discover() must be called first
        if self._result.discovered_forms is None:
            raise RuntimeError(
                "measure() requires discover() to be called first. "
                "No discovered forms available."
            )

        from crawler import measure_forms

        # ── Stage: Targeted Crawl ───────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.TARGETED_CRAWL,
            f"Measuring {len(approved_forms)} approved form(s)...",
        )
        try:
            enriched = await measure_forms(
                self._config.target_url,
                approved_forms=approved_forms,
                log_callback=self._listener.on_log,
                ignore_cache=self._config.ignore_cache,
                debug_dir=self._result.debug_dir or ".debug",
                session_id=self._config.session_id,
                browser_pool=self._browser_pool,
                navigation_timeout=self._config.navigation_timeout,
                screenshot_timeout=self._config.screenshot_timeout,
            )
            # Merge enriched data into crawler_data for downstream pipeline
            if self._result.crawler_data:
                self._result.crawler_data["forms_processed"] = enriched.get("forms_processed", [])
                self._result.crawler_data["datalayer_events"] = enriched.get("datalayer_events", [])
            else:
                self._result.crawler_data = enriched

            measured_count = len(enriched.get("forms_processed", []))
            # P1-3: Surface skipped forms feedback
            skipped = enriched.get("skipped_forms", [])
            if skipped:
                for sf in skipped:
                    self._listener.on_log(
                        f"   ↳ ⚠️ Form #{sf['form_index']} skipped: {sf['reason']}"
                    )
            self._listener.on_step_complete(
                PipelineStage.TARGETED_CRAWL,
                f"Measured {measured_count} form(s)."
                + (f" ({len(skipped)} skipped)" if skipped else ""),
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.TARGETED_CRAWL, e)
            self._result.error = str(e)
            return self._result

        # ── Stage: Signal Extraction ────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.SIGNAL_EXTRACT,
            "Extracting tracking signals from measurement data...",
        )
        try:
            self._result.extracted_signals = []
            for form in enriched.get("forms_processed", []):
                _fi_str = str(form.get("form_index"))
                _cls = (self._result.form_classifications or {}).get(_fi_str, {})
                self._result.extracted_signals.append({
                    "form_index": form.get("form_index"),
                    "form_title": form.get("form_title", f"Form #{form.get('form_index')}"),
                    "is_successful_submission": form.get("is_successful_submission", False),
                    "available_tracking_methods": form.get("available_tracking_methods", []),
                    "datalayer_events": form.get("datalayer_events", []),
                    "success_element_selector": form.get("success_element_selector"),
                    "redirect_url": form.get("redirect_url"),
                    "is_ajax_submission": form.get("is_ajax_submission", False),
                    "ajax_endpoint": form.get("ajax_endpoint"),
                    # Recipe matching: propagate platform info from AI classification
                    "platform": _cls.get("platform", ""),
                    "technology_signals": _cls.get("technology_signals", []),
                    "cf7_form_id": form.get("cf7_form_id"),
                })

            signal_count = len(self._result.extracted_signals)
            methods_total = sum(
                len(s.get("available_tracking_methods", []))
                for s in self._result.extracted_signals
            )
            self._listener.on_step_complete(
                PipelineStage.SIGNAL_EXTRACT,
                f"Extracted {signal_count} signal set(s) with {methods_total} tracking method(s).",
            )
            self._listener.on_log(
                f"   ↳ Scoring methods before user selection..."
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.SIGNAL_EXTRACT, e)
            self._result.error = str(e)
            return self._result

        # ── Stage: Method Scoring (REQUIRES successful test submit) ──
        self._listener.on_step_start(
            PipelineStage.METHOD_SCORING,
            "AI evaluating method resilience (based on test submit data)...",
        )
        try:
            from brain import score_tracking_methods

            os.environ["GEMINI_API_KEY"] = self._config.gemini_api_key
            scored_count = 0
            for signal in self._result.extracted_signals:
                # GUARD: Score ONLY forms with successful test submission
                if not signal.get("is_successful_submission"):
                    self._listener.on_log(
                        f"   ↳ ⚠️ Form #{signal.get('form_index', '?')}: "
                        "Skipping scoring — no successful test submission."
                    )
                    signal["method_scores"] = []
                    continue

                methods = signal.get("available_tracking_methods", [])
                if not methods:
                    signal["method_scores"] = []
                    continue

                classification = (self._result.form_classifications or {}).get(
                    str(signal["form_index"]), {}
                )
                scored = score_tracking_methods(
                    form_data=signal,
                    available_methods=methods,
                    form_classification=classification,
                    model=self._config.model,
                    log_callback=self._listener.on_log,
                )
                signal["method_scores"] = scored
                scored_count += 1

            self._listener.on_step_complete(
                PipelineStage.METHOD_SCORING,
                f"Scored methods for {scored_count} form(s). Awaiting user selection.",
            )
        except Exception as e:
            # Non-fatal: if scoring fails, proceed with unscored methods
            self._listener.on_log(
                f"   ↳ ⚠️ Method scoring failed: {e}. Proceeding without scores."
            )
            for signal in self._result.extracted_signals:
                if "method_scores" not in signal:
                    signal["method_scores"] = []

        return self._result

    # ── Phase 3: Analyze with User-Selected Methods ────────────────────────

    async def analyze_with_methods(
        self, user_selected_methods: list[dict]
    ) -> PipelineResult:
        """
        Phase 3: AI analysis constrained to user-selected tracking methods.

        Injects the user's method choices into crawler_data so brain.py's
        Step 2 (Signal Validation) respects them, then runs the full AI
        pipeline (Steps 1-5).

        Args:
            user_selected_methods: list of dicts, each with:
                - form_index: int — which form this applies to
                - method: str — e.g. "custom_event", "element_visibility"
                - trigger_condition: dict — the condition from available_tracking_methods
                - payload_keys: list[str] — optional data keys
        """
        if self._result.crawler_data is None:
            raise RuntimeError(
                "analyze_with_methods() requires measure() to be called first. "
                "No crawler data available."
            )

        from brain import generate_tracking_plan

        # Ensure API key is set (may have been lost across Streamlit reruns)
        os.environ["GEMINI_API_KEY"] = self._config.gemini_api_key

        # ── Separate recipe forms from AI forms ──────────────────────────
        from recipes import build_recipe_plan_item

        recipe_plan_items: list[dict] = []
        ai_methods: list[dict] = []

        for m in user_selected_methods:
            if m.get("use_recipe"):
                item = build_recipe_plan_item(
                    m["use_recipe"], m["form_index"],
                    cf7_form_id=m.get("cf7_form_id"),
                    form_type=m.get("form_type"),
                )
                recipe_plan_items.append(item)
                self._listener.on_log(
                    f"   \u21b3 \U0001f373 Form #{m['form_index']}: "
                    f"{m['use_recipe']} recipe applied (AI skipped)"
                )
            else:
                ai_methods.append(m)

        # ── Inject AI method selections into crawler_data ────────────────
        for form in self._result.crawler_data.get("forms_processed", []):
            fi = form.get("form_index")
            user_choice = next(
                (m for m in ai_methods if m["form_index"] == fi),
                None,
            )
            if user_choice:
                # Override available_tracking_methods with ONLY the user's selection
                form["available_tracking_methods"] = [{
                    "method": user_choice["method"],
                    "trigger_condition": user_choice.get("trigger_condition", {}),
                    "payload_keys": user_choice.get("payload_keys", []),
                    "reason": "User-selected method",
                    "priority": 1,
                }]
                form["_user_selected_method"] = user_choice["method"]

        # ── Stage: AI Analysis (Steps 1-5) — only for non-recipe forms ──
        ai_plan_items: list[dict] = []

        if ai_methods:
            self._listener.on_step_start(
                PipelineStage.AI_ANALYZE,
                "Running AI analysis with selected methods...",
            )

            # Inject cached Step 1 classification from discovery phase
            # (saves 1+ API calls by skipping redundant platform analysis)
            if self._result.form_classifications:
                self._result.crawler_data["_cached_step1"] = self._result.form_classifications

            try:
                gtm_context = self._result.gtm_context or {}
                loop = asyncio.get_running_loop()
                plan = await loop.run_in_executor(
                    None,
                    lambda: generate_tracking_plan(
                        crawler_data=self._result.crawler_data,
                        model=self._config.model,
                        gtm_data={"ai_context": gtm_context.get("ai_context", [])},
                        log_callback=self._listener.on_log,
                        include_shadow_forms=self._config.include_shadow_forms,
                        include_iframes=self._config.include_iframes,
                        session_id=self._config.session_id,
                    ),
                )
                ai_plan_items = plan.get("tracking_plan", [])
                self._result.pipeline_warnings = plan.get("pipeline_warnings")
                self._result.tracking_spec_md = plan.get("tracking_spec_md")
                self._result.api_call_count = plan.get("api_call_count")

                # Propagate pipeline-internal errors (e.g. Step 1 FATAL)
                plan_error = plan.get("error")
                if plan_error:
                    self._result.error = plan_error
                    self._listener.on_error(
                        PipelineStage.AI_ANALYZE, Exception(plan_error)
                    )
                    return self._result

                api_calls = self._result.api_call_count or "?"
                self._listener.on_step_complete(
                    PipelineStage.AI_ANALYZE,
                    f"Generated {len(ai_plan_items)} AI tags. ({api_calls} API calls used)",
                )
            except Exception as e:
                self._listener.on_error(PipelineStage.AI_ANALYZE, e)
                self._result.error = str(e)
                return self._result
        else:
            self._listener.on_log(
                "   \u21b3 All forms resolved via recipes. AI analysis skipped."
            )

        # ── Merge: recipe items + AI items ───────────────────────────────
        self._result.draft_plan = recipe_plan_items + ai_plan_items

        total = len(self._result.draft_plan)
        recipe_count = len(recipe_plan_items)
        if recipe_count:
            self._listener.on_log(
                f"   \u21b3 Total plan: {total} item(s) "
                f"({recipe_count} from recipes, {len(ai_plan_items)} from AI)"
            )

        # ── Review Checkpoint ───────────────────────────────────────────
        self._listener.on_review_ready(
            self._result.draft_plan, self._result.crawler_data
        )
        return self._result

    # ── Legacy: Analyze (backward compat) ─────────────────────────────────

    async def analyze(self) -> PipelineResult:
        """
        Legacy Phase 1+2 combined: crawl + AI analysis in one step.
        Uses the original crawl_site() which discovers AND interacts with all forms.
        Kept for backward compatibility with debug_delta_logic.py and API.
        """
        from brain import generate_tracking_plan, get_debug_dir
        from main import extract_gtm_context
        from crawler import crawl_site
        from models import CrawlerOutput
        from pydantic import ValidationError

        os.environ["GEMINI_API_KEY"] = self._config.gemini_api_key

        # ── Stage: Context Extraction ───────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.CONTEXT_EXTRACT, "Extracting GTM context..."
        )
        try:
            gtm_context = extract_gtm_context(self._config.gtm_data)
            self._result.gtm_context = {"ai_context": gtm_context["ai_context"]}
            self._result.mechanical_ids = gtm_context["mechanical_ids"]
            self._listener.on_step_complete(
                PipelineStage.CONTEXT_EXTRACT, "GTM context extracted."
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.CONTEXT_EXTRACT, e)
            self._result.error = str(e)
            return self._result

        # ── Stage: Crawl (legacy full crawl) ────────────────────────────
        self._listener.on_step_start(
            PipelineStage.CRAWL, "Launching headless browser..."
        )
        debug_dir = get_debug_dir(self._config.session_id)
        self._result.debug_dir = debug_dir
        os.makedirs(debug_dir, exist_ok=True)

        try:
            raw_data = await crawl_site(
                self._config.target_url,
                log_callback=self._listener.on_log,
                ignore_cache=self._config.ignore_cache,
                debug_dir=debug_dir,
                session_id=self._config.session_id,
                browser_pool=self._browser_pool,
                navigation_timeout=self._config.navigation_timeout,
                screenshot_timeout=self._config.screenshot_timeout,
            )

            try:
                validated = CrawlerOutput.model_validate(raw_data)
                crawler_data = validated.model_dump()
            except ValidationError as ve:
                self._listener.on_log(
                    f"WARNING: Crawler output validation: {ve}. Using raw data."
                )
                crawler_data = raw_data

            self._result.crawler_data = crawler_data
            forms_count = len(crawler_data.get("forms_processed", []))
            self._listener.on_step_complete(
                PipelineStage.CRAWL,
                f"Crawling complete. Found {forms_count} forms.",
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.CRAWL, e)
            self._result.error = str(e)
            return self._result

        # ── Stage: AI Analysis ──────────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.AI_ANALYZE,
            "Running AI analysis pipeline...",
        )
        try:
            loop = asyncio.get_running_loop()
            plan = await loop.run_in_executor(
                None,
                lambda: generate_tracking_plan(
                    crawler_data=crawler_data,
                    model=self._config.model,
                    gtm_data={"ai_context": gtm_context["ai_context"]},
                    log_callback=self._listener.on_log,
                    include_shadow_forms=self._config.include_shadow_forms,
                    include_iframes=self._config.include_iframes,
                    session_id=self._config.session_id,
                ),
            )
            self._result.draft_plan = plan.get("tracking_plan", [])
            self._result.pipeline_warnings = plan.get("pipeline_warnings")
            self._result.tracking_spec_md = plan.get("tracking_spec_md")
            self._result.api_call_count = plan.get("api_call_count")

            # Propagate pipeline-internal errors (e.g. Step 1 FATAL)
            plan_error = plan.get("error")
            if plan_error:
                self._result.error = plan_error
                self._listener.on_error(
                    PipelineStage.AI_ANALYZE, Exception(plan_error)
                )
                return self._result

            tag_count = len(self._result.draft_plan)
            api_calls = self._result.api_call_count or "?"
            self._listener.on_step_complete(
                PipelineStage.AI_ANALYZE,
                f"Generated {tag_count} recommended tags. ({api_calls} API calls used)",
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.AI_ANALYZE, e)
            self._result.error = str(e)
            return self._result

        # ── Review Checkpoint ───────────────────────────────────────────
        self._listener.on_review_ready(self._result.draft_plan, crawler_data)
        return self._result

    # ── Phase 3: Compile ────────────────────────────────────────────────────

    async def compile(self, approved_plan: list[dict]) -> PipelineResult:
        """
        Phase 3: inject approved tags → heal → validate → export.
        """
        # P1-9: Phase guard — measure() or analyze() must be called first
        if self._result.crawler_data is None:
            raise RuntimeError(
                "compile() requires measure() or analyze() to be called first. "
                "No crawler data available."
            )

        from main import add_tag_and_trigger, export_delta_recipe
        from healer import heal_gtm_container
        from validator import validate_gtm_container

        # ── Stage: Compile ──────────────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.COMPILE, "Injecting approved tags..."
        )
        try:
            updated_gtm = copy.deepcopy(self._full_gtm)

            # ── Inject recipe listeners BEFORE tag loop ──────────────
            from recipes import inject_recipe_listener
            cv = updated_gtm.get("containerVersion", {})
            _acct = cv.get("container", {}).get("accountId", "0")
            _cid = cv.get("container", {}).get("containerId", "0")

            injected_recipes: set[str] = set()
            for item in approved_plan:
                rk = item.get("use_recipe")
                if rk and rk not in injected_recipes:
                    if inject_recipe_listener(cv, _acct, _cid, rk):
                        injected_recipes.add(rk)
                        self._listener.on_log(
                            f"  ++ Recipe: Injected {rk} listener tag"
                        )

            # ── Inject AJAX listener if needed (BUG FIX: missing from pipeline) ──
            from main import inject_ajax_listener
            if any(i.get("trigger_type") == "ajax_complete" for i in approved_plan):
                inject_ajax_listener(cv, _acct, _cid)

            # ── Inject tags + triggers (existing logic) ──────────────
            for item in approved_plan:
                updated_gtm = add_tag_and_trigger(updated_gtm, item)
                self._listener.on_log(
                    f"Injected tag: GA4 Event - {item.get('event_name', '?')}"
                )
            # ── Inject feature tags (Session Harvester, Guardian, Cross-Domain) ──
            from main import inject_session_harvester, inject_html5_guardian, inject_cross_domain_linker

            harvester_keys: list[str] = []
            guardian_selectors: list[str] = []
            external_domains: list[str] = []

            for item in approved_plan:
                ttype = item.get("trigger_type")
                if ttype in ("element_visibility", "page_view", "form_submission"):
                    harvester_keys.extend(item.get("gtm_payload_keys", []))
                elif ttype == "html5_validation_guardian":
                    cond = item.get("trigger_condition", {})
                    if cond.get("key") == "id" and cond.get("value"):
                        guardian_selectors.append(f"#{cond['value']}")
                    elif cond.get("key") == "class" and cond.get("value"):
                        guardian_selectors.append(f".{cond['value'].replace(' ', '.')}")
                elif ttype == "cross_domain_redirect":
                    ext = item.get("trigger_condition", {}).get("external_domain")
                    if ext and ext not in external_domains:
                        external_domains.append(ext)

            if harvester_keys:
                inject_session_harvester(cv, _acct, _cid, harvester_keys)
                self._listener.on_log("  ++ Injected Session Storage Harvester")
            if guardian_selectors:
                inject_html5_guardian(cv, _acct, _cid, guardian_selectors)
                self._listener.on_log("  ++ Injected HTML5 Validation Guardian")
            if external_domains:
                inject_cross_domain_linker(cv, _acct, _cid, external_domains)
                self._listener.on_log("  ++ Injected Cross-Domain Linker")

            self._listener.on_step_complete(
                PipelineStage.COMPILE,
                f"Injected {len(approved_plan)} tags.",
            )
        except Exception as e:
            self._listener.on_error(PipelineStage.COMPILE, e)
            self._result.error = str(e)
            return self._result

        # ── Stage: Heal ─────────────────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.HEAL, "Healing container schema..."
        )
        updated_gtm = heal_gtm_container(updated_gtm)
        self._listener.on_step_complete(PipelineStage.HEAL, "Healing complete.")

        # ── Stage: Validate ─────────────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.VALIDATE, "Validating compiled output..."
        )
        report = validate_gtm_container(updated_gtm)
        self._result.validation_report = report
        self._listener.on_step_complete(
            PipelineStage.VALIDATE, f"Score: {report.get('score', '?')}/100"
        )

        # ── Stage: Export ───────────────────────────────────────────────
        self._listener.on_step_start(
            PipelineStage.EXPORT, "Generating export..."
        )
        if self._config.export_mode == "delta":
            final_output = export_delta_recipe(updated_gtm, self._full_gtm)
            # Delta-specific structural validation
            from validator import validate_delta
            delta_report = validate_delta(final_output, self._full_gtm)
            if not delta_report["passed"]:
                self._listener.on_log(
                    f"⚠️ Delta issues: {'; '.join(delta_report['errors'][:3])}"
                )
            self._result.validation_report.setdefault("delta_checks", delta_report["checks"])
            self._result.validation_report.setdefault("delta_errors", delta_report["errors"])
            self._result.validation_report.setdefault("delta_warnings", delta_report["warnings"])
        else:
            final_output = updated_gtm

        self._result.compiled_gtm = final_output
        self._listener.on_step_complete(PipelineStage.EXPORT, "Export ready.")

        # ── Done ────────────────────────────────────────────────────────
        self._listener.on_complete(self._result)
        return self._result

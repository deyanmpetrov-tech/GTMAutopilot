import streamlit as st
import asyncio
import copy
import json
import os
import urllib.parse
from enum import Enum
from pathlib import Path
from datetime import datetime
# Force-reload pipeline modules so code changes take effect without restarting Streamlit
import importlib as _il, sys as _sys
for _mod in ("brain", "crawler", "core_pipeline"):
    if _mod in _sys.modules:
        _il.reload(_sys.modules[_mod])
from core_pipeline import PipelineOrchestrator, PipelineConfig, PipelineStage, PipelineResult


# P2-3: Phase enum replaces boolean flags
class AppPhase(str, Enum):
    IDLE = "idle"
    DISCOVERY_REVIEW = "discovery_review"
    SIGNAL_REVIEW = "signal_review"       # After measure() — user selects tracking methods
    PLAN_REVIEW = "plan_review"
    COMPILED = "compiled"

CONFIG_FILE = Path(__file__).parent / ".autogtm_config.json"

def load_config() -> dict:
    """Load persisted API key and URL from config file."""
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            pass
    return {}

def save_config(api_key: str, url: str, model: str = "gemini-2.0-flash"):
    """Persist API key, URL and model for next session."""
    CONFIG_FILE.write_text(json.dumps({"api_key": api_key, "url": url, "model": model}, indent=2))

GEMINI_MODELS = {
    "gemini-2.5-flash-lite":  "Gemini 2.5 Flash-Lite  (free, висок лимит) ⭐",
    "gemini-2.0-flash-lite":  "Gemini 2.0 Flash-Lite  (free, бърз)",
    "gemini-2.0-flash":       "Gemini 2.0 Flash       (free)",
    "gemini-2.5-flash":       "Gemini 2.5 Flash       (free, 20 req/day)",
    "gemini-2.5-pro":         "Gemini 2.5 Pro         (paid tier)",
}

FORM_TYPES_MAP = {
    "newsletter": "📩 Newsletter / Signup",
    "contact_form": "📝 Contact Form",
    "lead": "🎯 Lead / Conversion",
    "ecommerce_reserved": "🛒 E-Commerce (Reserved)",
    "unknown": "❓ Other (Not Tracked)",
}
FORM_TYPES = list(FORM_TYPES_MAP.keys())

# ─── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="AutoGTM Builder", page_icon="🏷️", layout="wide")
st.title("🏷️ AutoGTM Builder")
st.markdown("Automate GA4 tracking plans with Playwright and Gemini.")

# ─── Session state ─────────────────────────────────────────────────────────────
_defaults = {
    "phase": AppPhase.IDLE,  # P2-3: Single phase enum replaces discovery_mode + review_mode
    "compiled_gtm": None,
    "output_path": None,
    "validation_report": None,
    "draft_plan": None,
    "edited_plan": None,  # P2-2: Deep copy of draft_plan for safe editing
    "gtm_context": None,
    "crawler_data": None,
    "pipeline_warnings": None,
    # Two-phase discovery
    "discovered_forms": None,
    "form_classifications": None,
    "extracted_signals": None,  # Per-form signal data for signal review UI
}
for key, default in _defaults.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ─── Load saved config ─────────────────────────────────────────────────────────
_cfg = load_config()

# ─── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuration")
    gemini_key = st.text_input(
        "Gemini API Key",
        value=_cfg.get("api_key", ""),
        type="password"
    )
    if gemini_key:
        os.environ["GEMINI_API_KEY"] = gemini_key
    elif "GEMINI_API_KEY" in os.environ:
        st.success("API Key loaded from environment.")
    else:
        st.warning("Please provide a Gemini API Key.")

    st.divider()
    saved_model = _cfg.get("model", "gemini-2.0-flash")
    model_keys = list(GEMINI_MODELS.keys())
    selected_model = st.selectbox(
        "Gemini Model",
        options=model_keys,
        index=model_keys.index(saved_model) if saved_model in model_keys else 0,
        format_func=lambda m: GEMINI_MODELS[m],
        help="2.0 Flash / 1.5 Flash have 1500 free requests/day. 2.5 Flash has only 20/day."
    )

    st.divider()
    st.subheader("🌐 Scan Scope")
    scan_scope = st.radio(
        "Discovery Mode",
        options=["Single URL", "Entire Domain (Coming Soon)"],
        index=0,
        help="'Single URL' scans only the target page. 'Entire Domain' crawls subpages to find additional forms.",
        horizontal=True,
    )
    scan_scope_value = "single_url"  # Domain mode wired but feature-flagged for now

    st.divider()
    st.subheader("🔍 Advanced Discovery")
    discover_shadow = st.toggle(
        "AI Shadow Hunter",
        value=True,
        help="Detect interaction clusters (email + button) even without a <form> tag."
    )
    discover_iframes = st.toggle(
        "iFrame & Shadow DOM",
        value=True,
        help="Analyze iframes and Shadow DOM boundaries for hidden forms."
    )
    ignore_cache = st.toggle(
        "Ignore Submission Cache",
        value=False,
        help="Allow re-testing of forms that have already been submitted in previous runs."
    )

    st.divider()
    st.subheader("📁 Recent Exports")
    exports_dir = os.path.join(os.path.dirname(__file__), "exports")
    if os.path.exists(exports_dir):
        files = sorted(
            [f for f in os.listdir(exports_dir) if f.endswith(".json")],
            key=lambda x: os.path.getmtime(os.path.join(exports_dir, x)),
            reverse=True
        )[:5]
        if files:
            for f in files:
                st.write(f"📄 `{f}`")
        else:
            st.info("No exports yet.")
    else:
        st.info("No exports yet.")


# ─── Shared Log Handler ───────────────────────────────────────────────────────

def _make_log_handler(status_box):
    """Creates a log handler function for Streamlit status display."""
    def handle_log(msg: str):
        if msg.startswith("── Step"):
            parts = msg.split("—")
            title = parts[-1].replace("⏳", "").strip() if len(parts) > 1 else msg.replace("── ", "")
            step_num = msg.split(":")[0].replace("── ", "").strip()
            status_box.update(label=f"⏳ {step_num}: {title}...")
            st.markdown(f"#### {step_num}: {title}")
        elif msg.startswith("   ↳") or msg.startswith("   🔄"):
            clean = msg.replace("   ↳", "").replace("   🔄", "🔄").strip()
            if "✅" in clean or "✓" in clean:
                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;↳ :green[{clean}]")
            elif "❌" in clean or "🚨" in clean or "⚠️" in clean or "⚠" in clean:
                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;↳ :red[{clean}]")
            else:
                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;↳ :gray[{clean}]")
        elif "🔄 [Loop]" in msg:
            status_box.update(label=f"🔄 AI self-correcting...")
            st.markdown(f"**{msg.strip()}**")
        elif "✅" in msg or "✓" in msg:
            st.markdown(f":green[{msg}]")
        elif "❌" in msg or "🚨" in msg or "⚠" in msg:
            st.markdown(f":red[{msg}]")
        else:
            st.markdown(f":gray[{msg}]")
    return handle_log


class _StreamlitListener:
    """Streamlit pipeline event listener."""
    def __init__(self, log_handler):
        self._log = log_handler

    def on_step_start(self, stage, message):
        self._log(f"── {stage.value}: {message}")
    def on_step_complete(self, stage, message):
        self._log(f"   ↳ ✅ {message}")
    def on_log(self, message):
        self._log(f"   ↳ {message}")
    def on_error(self, stage, error):
        self._log(f"   ↳ ❌ {stage.value}: {error}")
    def on_discovery_ready(self, discovered_forms, classifications):
        """Extension point: called after discovery + classification complete."""
        pass  # State stored via PipelineResult in session_state
    def on_review_ready(self, draft_plan, crawler_data):
        """Extension point: called after measurement + AI analysis complete."""
        pass  # State stored via PipelineResult in session_state
    def on_complete(self, result):
        """Extension point: called after compile + export complete."""
        pass  # State stored via PipelineResult in session_state


def _format_method_label(method: dict, score_data: dict | None = None) -> str:
    """Format a tracking method for display in the signal review UI."""
    m = method.get("method", "unknown")
    reason = method.get("reason", "")
    cond = method.get("trigger_condition", {})

    # Resilience score bar (from AI scoring)
    if score_data:
        rs = score_data.get("resilience_score", 0)
        fp = score_data.get("false_positive_risk", "?")
        cx = score_data.get("gtm_complexity", "?")
        bar = "\u2588" * int(rs * 10) + "\u2591" * (10 - int(rs * 10))
        score_prefix = f"{bar} {rs:.0%} "
        score_suffix = f" | {cx} setup, FP risk: {fp}"
    else:
        priority = method.get("priority", 99)
        score_prefix = ""
        score_suffix = f" (Priority {priority})"

    if m == "custom_event":
        event_name = cond.get("event", "unknown")
        return f"{score_prefix}\U0001f4ca Custom Event: `{event_name}`{score_suffix}"
    elif m == "element_visibility":
        selector = cond.get("selector", "unknown")
        return f"{score_prefix}\U0001f441\ufe0f Element Visibility: `{selector}`{score_suffix}"
    elif m == "ajax_complete":
        return f"{score_prefix}\U0001f504 AJAX Complete{score_suffix}"
    elif m == "page_view":
        path = cond.get("page_path", cond.get("page_path_regex", "unknown"))
        return f"{score_prefix}\U0001f4c4 Page View: `{path}`{score_suffix}"
    elif m == "form_submission":
        return f"{score_prefix}\U0001f4dd Form Submission{score_suffix}"
    elif m == "click" or m == "click_links":
        link_type = cond.get("filter_value", cond.get("click_id", "element"))
        return f"{score_prefix}\U0001f517 Click: `{link_type}`{score_suffix}"
    elif m == "dom_ready":
        return f"{score_prefix}\U0001f310 DOM Ready{score_suffix}"
    else:
        return f"{score_prefix}\U0001f527 {m}{score_suffix}"


# ─── Main Layout ───────────────────────────────────────────────────────────────
tab_builder, tab_validator = st.tabs(['🏷️ AutoGTM Builder', '🛡️ Consent Validator'])

with tab_builder:
    col1, col2 = st.columns([1, 1])

    with col1:
        st.header("1. Input Data")
        target_url = st.text_input(
            "Target URL",
            value=_cfg.get("url", ""),
            placeholder="https://example.com"
        )
        gtm_file = st.file_uploader("Upload base GTM Container (JSON)", type="json")

        st.divider()
        st.subheader("📦 Export Settings")
        export_mode = st.radio(
            "Export Format",
            options=["Delta Recipe (Recommended)", "Full Container (Legacy)"],
            index=0,
            help="'Delta Recipe' contains only the new tags (cleaner for import). 'Full Container' returns your entire file with the additions."
        )

        start_button = st.button(
            "🔍 Discover Forms",
            type="primary",
            disabled=not (target_url and gtm_file and os.environ.get("GEMINI_API_KEY"))
        )

    with col2:
        st.header("2. Execution Log")
        log_placeholder = st.empty()

    # ─── Phase 1: Discover Forms ────────────────────────────────────────────────
    if start_button:
        # P2-4: Reset ALL pipeline-related state (not just _defaults)
        for key in list(st.session_state.keys()):
            if key not in ("gemini_api_key",):
                del st.session_state[key]
        for key, default in _defaults.items():
            st.session_state[key] = default
        save_config(gemini_key, target_url, selected_model)

        if "GEMINI_API_KEY" not in os.environ or not os.environ["GEMINI_API_KEY"]:
            st.error("Missing Gemini API Key!")
            st.stop()

        try:
            gtm_data = json.load(gtm_file)
        except json.JSONDecodeError:
            st.error("Invalid GTM JSON file format.")
            st.stop()

        with st.status("🔍 Discovering forms (no interaction)...", expanded=True) as status_box:
            handle_log = _make_log_handler(status_box)
            listener = _StreamlitListener(handle_log)

            config = PipelineConfig(
                target_url=target_url,
                gtm_data=gtm_data,
                gemini_api_key=gemini_key,
                model=selected_model,
                scan_scope=scan_scope_value,
                include_shadow_forms=discover_shadow,
                include_iframes=discover_iframes,
                ignore_cache=ignore_cache,
                export_mode="delta" if export_mode.startswith("Delta") else "full",
            )
            orchestrator = PipelineOrchestrator(config, listener)

            try:
                loop = asyncio.new_event_loop()
                # P1-7: Timeout to prevent indefinite UI freeze
                result = loop.run_until_complete(
                    asyncio.wait_for(orchestrator.discover(), timeout=300)
                )
            except asyncio.TimeoutError:
                status_box.update(label="⏰ Discovery Timed Out!", state="error", expanded=True)
                st.error("Discovery timed out after 5 minutes. The target site may be too slow or unresponsive.")
                if st.button("🔄 Retry Discovery"):
                    st.session_state.phase = AppPhase.IDLE
                    st.rerun()
                st.stop()
            finally:
                loop.close()

            if result.error:
                status_box.update(label="❌ Discovery Failed!", state="error", expanded=True)
                st.error(f"Discovery failed: {result.error}")
                # P2-6: Retry button instead of st.stop()
                if st.button("🔄 Retry Discovery"):
                    st.session_state.phase = AppPhase.IDLE
                    st.rerun()
                st.stop()

            # Store results in session state
            st.session_state.orchestrator = orchestrator
            st.session_state.discovered_forms = result.discovered_forms
            st.session_state.form_classifications = result.form_classifications
            st.session_state.crawler_data = result.crawler_data
            st.session_state.session_id = config.session_id
            st.session_state.debug_dir = result.debug_dir
            st.session_state.gtm_context = {
                "ai_context": result.gtm_context,
                "full_gtm": gtm_data,
                "mechanical_ids": result.mechanical_ids,
            }
            st.session_state.phase = AppPhase.DISCOVERY_REVIEW  # P2-3
            status_box.update(
                label=f"✅ Discovered {len(result.discovered_forms or [])} form(s)! Select which to measure.",
                state="complete", expanded=False
            )

    # ─── Phase 1b: Discovery Review UI (Form Selection) ───────────────────────────
    if st.session_state.phase == AppPhase.DISCOVERY_REVIEW:

        st.divider()
        st.subheader("🔍 Discovered Forms")
        discovered = st.session_state.discovered_forms
        classifications = st.session_state.form_classifications or {}

        if not discovered:
            st.warning("No forms were found on the page.")
        else:
            st.info(
                f"Found **{len(discovered)}** form(s) on the page. "
                "Select which forms to measure (fill + submit + capture signals)."
            )

            selected_forms = []
            for idx, form in enumerate(discovered):
                fi = form.get("form_index")
                fi_str = str(fi)
                cls_info = classifications.get(fi_str, {})
                form_type = cls_info.get("form_type", "unknown")
                form_role = cls_info.get("form_role", "")
                confidence = cls_info.get("confidence", 0.0)
                is_shadow = form.get("is_shadow_form", False)

                # Pre-check actionable forms (exclude ecommerce and unknown)
                type_label = FORM_TYPES_MAP.get(form_type, "❓ Unknown")
                is_actionable = form_type not in ("ecommerce_reserved", "unknown")
                is_ecommerce = form_type == "ecommerce_reserved"

                with st.container(border=True):
                    col_check, col_info, col_screenshot = st.columns([1, 3, 2])

                    with col_check:
                        include = st.checkbox(
                            "Measure",
                            value=is_actionable and not is_ecommerce,
                            key=f"p1_discover_{fi_str}",  # P1-5+P1-6: stable form_index + phase namespace
                            help="Check to fill, submit, and capture tracking signals for this form.",
                            disabled=is_ecommerce,
                        )
                        st.metric("Confidence", f"{confidence:.0%}")

                    with col_info:
                        title = form.get("form_title") or f"Form #{fi}"
                        st.markdown(f"### {title}")
                        st.markdown(f"**Type:** {type_label} &nbsp; | &nbsp; **Role:** _{form_role}_")

                        # Badges
                        badges = []
                        if is_shadow:
                            badges.append("🛡️ Shadow Form")
                        if cls_info.get("is_iframe_embedded"):
                            badges.append("🖼️ iFrame")
                        if cls_info.get("contains_pii"):
                            badges.append("🔒 PII Detected")
                        position = form.get("position_on_page", "")
                        if position:
                            badges.append(f"📍 {position}")
                        if badges:
                            st.caption(" &nbsp;|&nbsp; ".join(badges))

                        # Field summary
                        fields = form.get("fields", [])
                        visible_fields = [f for f in fields if not f.get("is_hidden")]
                        if visible_fields:
                            field_names = [f.get("name") or f.get("type") for f in visible_fields]
                            st.markdown(f"**Fields:** `{'`, `'.join(field_names[:6])}`"
                                        + (f" + {len(field_names) - 6} more" if len(field_names) > 6 else ""))

                        # Buttons
                        buttons = form.get("buttons", [])
                        if buttons:
                            btn_texts = [b.get("text", "?") for b in buttons if b.get("text")]
                            if btn_texts:
                                st.markdown(f"**Buttons:** {', '.join(btn_texts[:3])}")

                        if is_ecommerce:
                            st.info("🛒 E-commerce tracking will be handled by a dedicated module.")

                    with col_screenshot:
                        # P3-2: Lazy-load screenshots in expander to reduce initial render
                        debug_dir = st.session_state.get("debug_dir", ".debug")
                        if is_shadow:
                            img_path = os.path.join(debug_dir, f"form_shadow_{fi.replace('shadow_', '') if isinstance(fi, str) else fi}.png")
                        else:
                            img_path = os.path.join(debug_dir, f"form_{fi}.png")
                        if os.path.exists(img_path):
                            with st.expander("View Screenshot", expanded=False):
                                st.image(img_path, caption=f"Form #{fi}", width="stretch")
                        else:
                            st.caption("No screenshot available")

                    if include and not is_ecommerce:
                        selected_forms.append(form)

            st.warning(f"**{len(selected_forms)}** form(s) selected for measurement.")

            # ─── Phase 2: Measure Selected Forms ───────────────────────────────
            measure_button = st.button(
                f"🎯 Measure {len(selected_forms)} Selected Form(s)",
                type="primary",
                width="stretch",
                disabled=len(selected_forms) == 0,
            )
            if measure_button:
                orchestrator = st.session_state.get("orchestrator")
                if not orchestrator:
                    st.error("No active pipeline session. Please re-run discovery.")
                    st.stop()

                with st.status(f"🎯 Measuring {len(selected_forms)} form(s)...", expanded=True) as meas_status:
                    handle_log = _make_log_handler(meas_status)
                    orchestrator.set_listener(_StreamlitListener(handle_log))  # P2-10

                    try:
                        loop = asyncio.new_event_loop()
                        # P1-7: Timeout to prevent indefinite UI freeze
                        result = loop.run_until_complete(
                            asyncio.wait_for(orchestrator.measure(selected_forms), timeout=600)
                        )
                    except asyncio.TimeoutError:
                        meas_status.update(label="⏰ Measurement Timed Out!", state="error", expanded=True)
                        st.error("Measurement timed out after 10 minutes. The target site may be too slow.")
                        if st.button("🔄 Retry Measurement"):
                            st.session_state.phase = AppPhase.DISCOVERY_REVIEW
                            st.rerun()
                        st.stop()
                    finally:
                        loop.close()

                    if result.error:
                        meas_status.update(label="❌ Measurement Failed!", state="error", expanded=True)
                        st.error(f"Measurement failed: {result.error}")
                        # P2-6: Retry button instead of st.stop()
                        if st.button("🔄 Retry Measurement"):
                            st.session_state.phase = AppPhase.DISCOVERY_REVIEW
                            st.rerun()
                        st.stop()

                    st.session_state.crawler_data = result.crawler_data
                    st.session_state.extracted_signals = result.extracted_signals
                    st.session_state.phase = AppPhase.SIGNAL_REVIEW  # Go to signal review, not plan review

                    signal_count = len(result.extracted_signals or [])
                    methods_total = sum(
                        len(s.get("available_tracking_methods", []))
                        for s in (result.extracted_signals or [])
                    )
                    meas_status.update(
                        label=f"✅ Measurement complete! Found {methods_total} tracking method(s) across {signal_count} form(s). Select methods below.",
                        state="complete", expanded=False
                    )
                st.rerun()

    # ─── Phase 2b: Signal Review UI (Method Selection) ─────────────────────────────
    if st.session_state.phase == AppPhase.SIGNAL_REVIEW:
        st.divider()
        st.subheader("3. Signal Review — Select Tracking Method")
        st.info(
            "The crawler submitted your selected form(s) and captured these success signals. "
            "Choose the tracking method for each form below."
        )

        signals = st.session_state.get("extracted_signals") or []
        if not signals:
            st.warning("No signals were extracted. The form submissions may have failed.")
        else:
            user_methods = []
            for signal in signals:
                fi = signal["form_index"]
                title = signal.get("form_title", f"Form #{fi}")
                methods = signal.get("available_tracking_methods", [])

                with st.container(border=True):
                    st.markdown(f"### {title}")

                    # Show raw signal data in expander
                    with st.expander("🔬 Raw Signal Data", expanded=False):
                        sig_summary = {
                            "is_successful_submission": signal.get("is_successful_submission", False),
                            "is_ajax_submission": signal.get("is_ajax_submission", False),
                            "ajax_endpoint": signal.get("ajax_endpoint"),
                            "redirect_url": signal.get("redirect_url"),
                            "success_element_selector": signal.get("success_element_selector"),
                            "datalayer_events": signal.get("datalayer_events", []),
                        }
                        st.json(sig_summary)

                    if not signal.get("is_successful_submission", False):
                        st.error(
                            "⚠️ No success signals detected. The form submission may have failed. "
                            "You can retry measurement or proceed with available methods (if any)."
                        )

                    if not methods:
                        st.warning(
                            "No tracking methods available for this form. "
                            "The AI will attempt to determine the best approach."
                        )
                        # Still allow proceeding — AI can try fallback methods
                        user_methods.append({
                            "form_index": fi,
                            "method": "auto",
                            "trigger_condition": {},
                            "payload_keys": [],
                        })
                        continue

                    # ── Recipe check: offer recipe if platform matches ──
                    from recipes import get_recipe_for_platform
                    _platform = signal.get("platform", "")
                    _tech_sig = signal.get("technology_signals", [])
                    _recipe_match = get_recipe_for_platform(_platform, _tech_sig)

                    if _recipe_match:
                        _rkey, _recipe = _recipe_match
                        _dl_event = _recipe["plan_item"]["trigger_condition"]["event"]

                        _use_recipe = st.checkbox(
                            f"\U0001f373 Use {_recipe['name']} Recipe (`{_dl_event}`)",
                            value=True,
                            key=f"recipe_{fi}",
                        )

                        if _use_recipe:
                            st.caption(
                                f"\U0001f4a1 Full template: listener + trigger + GA4 tag. "
                                f"AI analysis will be skipped for this form."
                            )
                            _fc = st.session_state.get("form_classifications", {})
                            _ft = _fc.get(str(fi), {}).get("form_type")
                            user_methods.append({
                                "form_index": fi,
                                "method": "custom_event",
                                "trigger_condition": _recipe["plan_item"]["trigger_condition"],
                                "payload_keys": _recipe["plan_item"]["gtm_payload_keys"],
                                "use_recipe": _rkey,
                                "cf7_form_id": signal.get("cf7_form_id"),
                                "form_type": _ft,
                            })
                            continue  # Skip method selection — recipe approved

                        # User unchecked → fall through to normal method selection

                    # Build score lookup from AI resilience scoring
                    method_scores = signal.get("method_scores", [])
                    scores_by_method = {s["method"]: s for s in method_scores}

                    # Sort by resilience_score DESC if available, else by priority ASC
                    if scores_by_method:
                        def _sort_key(m):
                            sd = scores_by_method.get(m.get("method"), {})
                            return -sd.get("resilience_score", 0)
                        methods_sorted = sorted(methods, key=_sort_key)
                    else:
                        methods_sorted = sorted(methods, key=lambda m: m.get("priority", 99))

                    # Build radio options with resilience scores
                    options = []
                    for m in methods_sorted:
                        sd = scores_by_method.get(m.get("method"))
                        label = _format_method_label(m, score_data=sd)
                        options.append(label)

                    selected_label = st.radio(
                        f"Tracking method for {title}",
                        options=options,
                        index=0,  # Best score pre-selected
                        key=f"signal_method_{fi}",
                    )

                    selected_idx = options.index(selected_label)
                    selected_method = methods_sorted[selected_idx]

                    # Show selected method details + AI reasoning
                    sd = scores_by_method.get(selected_method.get("method"))
                    if sd:
                        reasoning = sd.get("resilience_reasoning", "")
                        built_ins = sd.get("recommended_built_ins", [])
                        if reasoning:
                            st.caption(f"💡 AI: {reasoning}")
                        if built_ins:
                            st.caption(f"📋 Built-ins: {', '.join(built_ins)}")

                    cond = selected_method.get("trigger_condition", {})
                    if cond:
                        st.caption(f"Trigger condition: `{json.dumps(cond)}`")

                    payload_keys = selected_method.get("payload_keys", [])
                    if payload_keys:
                        st.caption(f"Payload keys: `{', '.join(payload_keys)}`")

                    user_methods.append({
                        "form_index": fi,
                        "method": selected_method.get("method", "auto"),
                        "trigger_condition": cond,
                        "payload_keys": payload_keys,
                    })

            st.warning(f"**{len(user_methods)}** form(s) ready for AI analysis.")

            # "Generate Tracking Plan" button
            analyze_button = st.button(
                "🧠 Generate Tracking Plan",
                type="primary",
                width="stretch",
                disabled=len(user_methods) == 0,
            )

            if analyze_button:
                orchestrator = st.session_state.get("orchestrator")
                if not orchestrator:
                    st.error("No active pipeline session. Please re-run discovery.")
                    st.stop()

                with st.status("🧠 Running AI analysis with selected methods...", expanded=True) as analysis_status:
                    handle_log = _make_log_handler(analysis_status)
                    orchestrator.set_listener(_StreamlitListener(handle_log))

                    try:
                        loop = asyncio.new_event_loop()
                        result = loop.run_until_complete(
                            asyncio.wait_for(
                                orchestrator.analyze_with_methods(user_methods),
                                timeout=600,
                            )
                        )
                    except asyncio.TimeoutError:
                        analysis_status.update(label="⏰ Analysis Timed Out!", state="error", expanded=True)
                        st.error("AI analysis timed out after 10 minutes.")
                        if st.button("🔄 Retry Analysis"):
                            st.session_state.phase = AppPhase.SIGNAL_REVIEW
                            st.rerun()
                        st.stop()
                    finally:
                        loop.close()

                    if result.error:
                        analysis_status.update(label="❌ Analysis Failed!", state="error", expanded=True)
                        st.error(f"AI analysis failed: {result.error}")
                        if st.button("🔄 Retry Analysis"):
                            st.session_state.phase = AppPhase.SIGNAL_REVIEW
                            st.rerun()
                        st.stop()

                    # Guard: empty plan (API returned no tags, but no explicit error)
                    if not result.draft_plan:
                        analysis_status.update(
                            label="⚠️ No tracking tags generated!", state="error", expanded=True
                        )
                        st.error(
                            "The AI pipeline returned no tracking tags. "
                            "This usually means the Gemini API failed or reached its quota limit."
                        )
                        warnings = result.pipeline_warnings
                        if warnings:
                            with st.expander("Pipeline Warnings", expanded=True):
                                st.json(warnings)
                        if st.button("🔄 Retry Analysis"):
                            st.session_state.phase = AppPhase.SIGNAL_REVIEW
                            st.rerun()
                        st.stop()

                    st.session_state.draft_plan = result.draft_plan
                    st.session_state.edited_plan = copy.deepcopy(result.draft_plan)
                    st.session_state.pipeline_warnings = result.pipeline_warnings
                    st.session_state.phase = AppPhase.PLAN_REVIEW

                    tag_count = len(result.draft_plan or [])
                    api_calls = getattr(result, "api_call_count", None) or "?"
                    if tag_count > 0:
                        analysis_status.update(
                            label=f"✅ AI analysis complete! Generated {tag_count} recommended tags. ({api_calls} API calls used)",
                            state="complete", expanded=False
                        )
                    else:
                        analysis_status.update(
                            label=f"⚠️ AI finished but generated 0 tags. ({api_calls} API calls used)",
                            state="error", expanded=True
                        )
                st.rerun()

    # ─── Phase 3: Tracking Plan Review UI ─────────────────────────────────────────
    if st.session_state.phase == AppPhase.PLAN_REVIEW:
        st.divider()
        st.subheader("🕵️ Tracking Plan Review")
        st.info("The AI has analyzed the measured forms and proposed tracking events. Review or skip specific events before compiling the JSON.")

        # P2-2: Work on edited_plan (deep copy), preserving original draft_plan
        if st.session_state.edited_plan is None:
            st.session_state.edited_plan = copy.deepcopy(st.session_state.draft_plan or [])

        edited_plan = []
        for idx, item in enumerate(st.session_state.edited_plan):
            with st.container(border=True):
                # ── Side-by-Side Visualization ──
                col_viz_a, col_viz_b = st.columns([2, 3])

                with col_viz_a:
                    st.markdown("### 📄 HTML Context")
                    form_idx = item.get("form_index")
                    crawler_forms = st.session_state.get("crawler_data", {}).get("forms_processed", [])
                    target_form = next((f for f in crawler_forms if f.get("form_index") == form_idx), None)

                    if target_form:
                        attrs = target_form.get("html_attributes", {})
                        if attrs:
                            st.code(json.dumps(attrs, indent=2), language="json")
                        else:
                            st.warning("No HTML attributes available for this form.")
                    else:
                        st.info("Form details extracted by AI (Conceptual Form).")

                with col_viz_b:
                    st.markdown("### 👁️ Vision AI View")
                    fi = item.get("form_index")
                    if fi is not None:
                        debug_dir = st.session_state.get("debug_dir", ".debug")
                        img_path = os.path.join(debug_dir, f"form_{fi}.png")
                        if os.path.exists(img_path):
                            st.image(img_path, caption=f"Verified Form #{fi}", width="stretch")
                        else:
                            st.info("No screenshot available for this specific form.")

                st.divider()

                colA, colB = st.columns([1, 4])
                with colA:
                    conf = item.get("confidence", 0.0)
                    fi_key = item.get("form_index", idx)
                    include = st.checkbox("Include Tag", value=True, key=f"p2_review_{fi_key}")  # P1-5+P1-6
                    st.metric("AI Confidence", f"{conf:.0%}")
                    if conf < 0.7:
                        st.warning("Low confidence")
                    # P0-4: Show auditor removal flag
                    if item.get("_auditor_would_remove"):
                        st.warning("⚠️ AI auditor flagged for removal")
                with colB:
                    new_evt = st.text_input("GA4 Event Name", value=item.get("event_name", "form_submit"), key=f"p2_evt_{fi_key}")  # P1-5+P1-6
                    item["event_name"] = new_evt

                    # Advanced Discovery Badges
                    badges = []
                    if item.get("is_shadow_form"):
                        badges.append("🛡️ **AI Shadow Discovery** (No standard <form> tag)")
                    if item.get("is_iframe_embedded"):
                        badges.append("🖼️ **iFrame Embedded**")
                    if badges:
                        st.info(" | ".join(badges))

                    st.markdown(f"**Trigger Method:** `{item.get('trigger_type')}`  \n**Reason:** _{item.get('confidence_reason')}_")

                    # Schema Mapping Preview
                    mapping = item.get("semantic_mapping")
                    if mapping:
                        with st.expander("🧬 Vision Comparison & Field Mapping", expanded=True):
                            st.markdown("### 🎨 Color-Coded Field Mapping")
                            cols = st.columns(len(mapping) if len(mapping) < 4 else 4)
                            for i, (orig, final) in enumerate(mapping.items()):
                                color = "#1E90FF"  # Blue (Default)
                                if any(kw in orig.lower() or kw in final.lower() for kw in ["mail", "email"]):
                                    color = "#FFD700"  # Yellow (Email)
                                elif any(kw in orig.lower() or kw in final.lower() for kw in ["button", "submit", "send"]):
                                    color = "#32CD32"  # Green (Action)
                                elif any(kw in orig.lower() or kw in final.lower() for kw in ["name", "user"]):
                                    color = "#FF4500"  # Orange (Identity)

                                with cols[i % 4]:
                                    st.markdown(
                                        f'<div style="background:{color}; padding:5px 10px; border-radius:15px; '
                                        f'color:white; font-size:12px; font-weight:bold; text-align:center; margin-bottom:5px;">'
                                        f'{final.upper()}</div>',
                                        unsafe_allow_html=True
                                    )
                                    st.caption(f"mapped from: `{orig}`")

                            st.divider()
                            st.markdown("**Tabular View:**")
                            df_map = [{"Original Field": k, "GA4 Parameter": v} for k, v in mapping.items()]
                            st.dataframe(df_map, width="stretch", hide_index=True)

            if include:
                edited_plan.append(item)

        st.warning(f"**{len(edited_plan)}** triggers selected for container injection.")

        # ─── Phase 3: Compile ──────────────────────────────────────────────────────
        compile_button = st.button("🏗️ Approve & Compile Container", type="primary", width="stretch")
        if compile_button:
            # P2-5: Pre-compile validation
            if not edited_plan:
                st.warning("No tags selected. Select at least one tracking event to compile.")
            else:
                orchestrator = st.session_state.get("orchestrator")
                if not orchestrator:
                    st.error("No active pipeline session. Please re-run the analysis.")
                    st.stop()

                with st.status("🏗️ Compiling GTM Container...", expanded=True) as comp_status:
                    try:
                        loop = asyncio.new_event_loop()
                        # P1-7: Timeout to prevent indefinite UI freeze
                        result = loop.run_until_complete(
                            asyncio.wait_for(orchestrator.compile(edited_plan), timeout=120)
                        )
                    except asyncio.TimeoutError:
                        comp_status.update(label="⏰ Compilation Timed Out!", state="error", expanded=True)
                        st.error("Compilation timed out after 2 minutes.")
                        if st.button("🔄 Retry Compilation"):
                            st.session_state.phase = AppPhase.PLAN_REVIEW
                            st.rerun()
                        st.stop()
                    finally:
                        loop.close()

                    if result.error:
                        comp_status.update(label="❌ Compilation Failed!", state="error", expanded=True)
                        st.error(f"Compilation failed: {result.error}")
                        # P2-6: Retry button instead of st.stop()
                        if st.button("🔄 Retry Compilation"):
                            st.session_state.phase = AppPhase.PLAN_REVIEW
                            st.rerun()
                        st.stop()

                    compiled_str = json.dumps(result.compiled_gtm, indent=2, ensure_ascii=False)
                    domain = urllib.parse.urlparse(target_url).netloc or "unknown_domain"
                    domain = domain.replace("www.", "").replace(".", "_")

                    # Save locally
                    exports_dir = os.path.join(os.path.dirname(__file__), "exports")
                    os.makedirs(exports_dir, exist_ok=True)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"AutoGTM_{domain}_{timestamp}.json"
                    output_path = os.path.join(exports_dir, filename)
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(compiled_str)

                    st.session_state.compiled_gtm = compiled_str
                    st.session_state.output_path = output_path
                    st.session_state.output_filename = filename
                    st.session_state.validation_report = result.validation_report
                    st.session_state.phase = AppPhase.COMPILED  # P2-3

                    report = result.validation_report or {}
                    if report.get("passed"):
                        comp_status.update(label="✅ Successfully Compiled & Validated!", state="complete", expanded=False)
                        st.toast("🎉 Container ready! Scroll down to download.", icon="✅")
                    else:
                        comp_status.update(label="⚠️ Compiled with Validation Warnings!", state="error", expanded=True)
                        st.toast("⚠️ Compiled with warnings. Check details below.", icon="⚠️")
                st.rerun()

    # ─── Validation Report ─────────────────────────────────────────────────────────
    if st.session_state.validation_report:
        report = st.session_state.validation_report
        st.divider()
        st.subheader("4. Validation Report")

        stats = report["stats"]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Score", f"{report['score']}/100")
        m2.metric("✅ Passed", stats["pass"])
        m3.metric("⚠️ Warnings", stats["warn"])
        m4.metric("❌ Errors", stats["fail"])

        if report["passed"]:
            st.success("🎉 Validation passed — container is ready to import into GTM!")
        else:
            st.error("❌ Validation failed — fix the errors below before importing into GTM.")

        for check in report["checks"]:
            if check["status"] == "pass":
                st.success(f"**{check['name']}** — {check['detail']}")
            elif check["status"] == "warn":
                st.warning(f"**{check['name']}** — {check['detail']}")
            else:
                st.error(f"**{check['name']}** — {check['detail']}")

        # QA Checklist
        st.info(
            "🛡️ **Industry Standard QA Checklist:**\n\n"
            "- **Deduplication Check:** Use GTM Preview Mode to ensure the tracking plan doesn't fire twice (e.g. once for DataLayer and once for Thank You Page).\n"
            "- **Anomaly Monitoring:** In GA4, compare the total `generate_lead` events to your actual CRM emails. A 5-10% discrepancy is normal (AdBlockers). >20% indicates broken tracking/spam."
        )

    # ─── Download ──────────────────────────────────────────────────────────────────
    if st.session_state.compiled_gtm:
        st.divider()
        st.subheader("5. Download")

        # P3-3: Single download button (removed redundant base64 fallback)
        filename = st.session_state.get("output_filename", "AutoGTM_Enhanced_Container.json")
        st.download_button(
            label="⬇️ Download GTM Container",
            data=st.session_state.compiled_gtm.encode("utf-8"),
            file_name=filename,
            mime="application/json",
            type="primary",
            width="stretch",
        )

        if st.session_state.output_path:
            st.info(f"📁 File also saved locally at:\n`{st.session_state.output_path}`")

        st.balloons()

"""
brain.py — AI Tracking Plan Generator (Phase 12: 5-Step Expert Pipeline)
Uses google-genai SDK (v1 API).

Pipeline Steps:
  Step 1: analyze_platform_and_forms()  — Platform & Form Analyst
  Step 2: validate_success_signals()    — Success Signal Validator
  Step 3: architect_variables()         — Variable Architect
  Step 4: plan_gtm_strategy()           — GTM Strategy Planner
  Step 5: audit_and_compile()           — Compiler & Auditor (Self-Review)
"""
import os
import json
from dataclasses import dataclass, field as dc_field
from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator
from google import genai
from google.genai import types


# ═══════════════════════════════════════════════════════════════
# §0 — PIPELINE CONTEXT (Error/Warning Accumulator)
# ═══════════════════════════════════════════════════════════════

class Severity(str, Enum):
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"

@dataclass
class PipelineEvent:
    step: str
    form_index: Any | None
    severity: Severity
    message: str

@dataclass
class PipelineContext:
    events: list[PipelineEvent] = dc_field(default_factory=list)
    api_call_count: int = 0
    _sticky_model: str | None = dc_field(default=None, repr=False)
    _sticky_call_count: int = dc_field(default=0, repr=False)

    def record(self, step: str, form_index: int | None,
               severity: Severity, message: str):
        self.events.append(PipelineEvent(
            step=step, form_index=form_index,
            severity=severity, message=message,
        ))

    def has_fatal(self) -> bool:
        return any(e.severity == Severity.FATAL for e in self.events)

    def to_summary(self) -> dict:
        return {
            "errors": [
                {"step": e.step, "form": e.form_index, "msg": e.message}
                for e in self.events
                if e.severity in (Severity.ERROR, Severity.FATAL)
            ],
            "warnings": [
                {"step": e.step, "form": e.form_index, "msg": e.message}
                for e in self.events if e.severity == Severity.WARNING
            ],
            "skipped_forms": sorted(set(
                e.form_index for e in self.events
                if e.form_index is not None and e.severity == Severity.ERROR
            )),
        }


# ═══════════════════════════════════════════════════════════════
# §0.5 — CONSTANTS
# ═══════════════════════════════════════════════════════════════

PII_GA4_PARAMS = frozenset({
    "email", "phone_number", "user_name", "first_name", "last_name",
    "street", "city", "region", "postal_code", "country",
})


# ═══════════════════════════════════════════════════════════════
# §1 — PYDANTIC SCHEMAS (One per pipeline step)
# ═══════════════════════════════════════════════════════════════

# ── Shared primitive: key-value pair for schema-safe dicts ──────
class KeyValuePair(BaseModel):
    key: str
    value: str

class TriggerCondition(BaseModel):
    """Flexible trigger condition — only populated fields are used by the compiler."""
    event: str | None = None               # custom_event
    cf7_form_id: str | None = None         # CF7 filter
    selector: str | None = None            # element_visibility / click
    page_path: str | None = None           # page_view exact
    page_path_regex: str | None = None     # page_view dynamic
    filter_key: str | None = None          # form_submission: 'id' or 'class'
    filter_value: str | None = None        # form_submission: actual id/class value
    url_contains: str | None = None        # generic URL filter
    observe_dom_changes: bool | None = None # element_visibility uses MutationObserver
    click_id: str | None = None            # click precision
    click_class: str | None = None         # click precision

# ── Step 3B Output (CSS Critic) — defined early to resolve forward reference ──
class CSSCriticReview(BaseModel):
    is_robust: bool
    fragility_score: int = Field(..., ge=1, le=10, description="1 to 10 scale. 10 is extremely fragile/brittle.")
    reasoning: str
    suggested_selector: str | None = Field(None, description="A completely rewritten, robust CSS selector if the original was fragile.")
    is_dynamic_element: bool = Field(False, description="True if the context implies this form is injected dynamically after load (e.g. modals, popups).")

    @field_validator("fragility_score", mode="before")
    @classmethod
    def clamp_fragility(cls, v):
        if isinstance(v, (int, float)):
            return max(1, min(10, int(v)))
        return v

# ── Step 1 Output ───────────────────────────────────────────────
class FormAnalysis(BaseModel):
    form_index: Any
    platform: str
    platform_confidence: float = Field(..., ge=0.0, le=1.0)
    form_type: Literal["newsletter", "contact_form", "lead", "ecommerce_reserved", "unknown"] = "unknown"
    form_role: str
    technology_signals: list[str]
    contains_pii: bool = Field(False, description="True if the form requests sensitive PII like passwords, SSN, health data, or full credit card numbers.")
    funnel_step: str | None = Field(None, description="For e-commerce/booking only: e.g. 'view_cart', 'begin_checkout', 'add_shipping_info'.")
    # ── iFrame/Shadow DOM Detection ──
    is_iframe_embedded: bool = Field(False, description="True if the form is inside an iframe element.")
    iframe_src: str | None = None
    is_cross_origin_iframe: bool = Field(False, description="True if the iframe src is a different origin.")
    is_shadow_dom: bool = Field(False, description="True if the form is inside a Shadow DOM boundary.")
    is_shadow_form: bool = Field(False, description="True if this is a 'Conceptual Form' (input/button cluster) without a <form> tag.")
    shadow_trigger_hint: str | None = Field(None, description="Suggested trigger method for shadow forms (e.g. 'Click on Buy Button' or 'Visibility of div.newsletter').")

    @field_validator("platform_confidence", mode="before")
    @classmethod
    def clamp_platform_confidence(cls, v):
        if isinstance(v, (int, float)):
            return max(0.0, min(1.0, float(v)))
        return v

    @field_validator("form_type", mode="before")
    @classmethod
    def coerce_form_type(cls, v):
        allowed = {"newsletter", "contact_form", "lead", "ecommerce_reserved", "unknown"}
        # Backward compat: map old type names to current 5-type system
        migration = {
            "booking": "lead", "checkout": "ecommerce_reserved", "application": "lead",
            "login": "unknown", "other": "unknown",
            "cart": "ecommerce_reserved", "payment": "ecommerce_reserved",
        }
        if isinstance(v, str):
            v = v.lower().strip()
            if v in allowed:
                return v
            if v in migration:
                return migration[v]
        return "unknown"

class PlatformAnalysis(BaseModel):
    platform: str
    platform_confidence: float = Field(..., ge=0.0, le=1.0)
    forms: list[FormAnalysis]

    @field_validator("platform_confidence", mode="before")
    @classmethod
    def clamp_platform_confidence(cls, v):
        if isinstance(v, (int, float)):
            return max(0.0, min(1.0, float(v)))
        return v

# ── Method Resilience Scoring Output ────────────────────────────
class MethodScore(BaseModel):
    """AI-evaluated resilience score for a single tracking method on a specific form."""
    method: Literal["custom_event", "ajax_complete", "element_visibility",
                     "page_view", "form_submission", "click", "dom_ready"]
    resilience_score: float = Field(..., ge=0.0, le=1.0,
        description="How resilient this method is for THIS specific form. 1.0 = bulletproof.")
    resilience_reasoning: str = Field(...,
        description="1-2 sentences explaining the score for this form.")
    false_positive_risk: Literal["low", "medium", "high"]
    gtm_complexity: Literal["simple", "medium", "complex"]
    data_capture_ability: bool = Field(...,
        description="True if method can capture form field values via dataLayer.")
    recommended_built_ins: list[str] = Field(default_factory=list,
        description="GTM built-in variables to activate for this method.")
    trigger_condition: TriggerCondition

    @field_validator("resilience_score", mode="before")
    @classmethod
    def clamp_resilience(cls, v):
        if isinstance(v, (int, float)):
            return max(0.0, min(1.0, float(v)))
        return v

# ── Step 2 Output ───────────────────────────────────────────────
class SuccessValidation(BaseModel):
    form_index: Any
    is_genuine_success: bool
    rejection_reason: str | None = None
    best_method: Literal[
        "custom_event", "ajax_complete", "element_visibility",
        "page_view", "form_submission", "click", "none"
    ]
    method_confidence: float = Field(..., ge=0.0, le=1.0)
    trigger_condition: TriggerCondition
    fallback_method: str | None = Field(None, description="A secondary method to create a resilient Trigger Group if the primary is not 100% reliable.")
    url_parameters: list[str] = Field(default_factory=list, description="For page_view. List of URL parameters (like 'order_id') to extract as GTM variables.")
    network_payload_success: bool | None = Field(None, description="True if the provided AJAX response payload explicitly confirms success (e.g. status: 'ok', success: true).")
    conversion_value_detected: bool = Field(False, description="True if a currency symbol and number are found near the success message or payload.")
    conversion_value: float | None = None
    conversion_currency: str | None = None
    selector_critic: CSSCriticReview | None = None

    @field_validator("method_confidence", mode="before")
    @classmethod
    def clamp_method_confidence(cls, v):
        if isinstance(v, (int, float)):
            return max(0.0, min(1.0, float(v)))
        return v

# ── Step 2B Output (Fallback Injector) ───────────────────────────
class FallbackScript(BaseModel):
    is_needed: bool = Field(description="True if we should inject a fallback listener for this form")
    js_code: str | None = Field(None, description="The Custom JavaScript snippet that adds an event listener or intercepts fetch/XHR to push a dataLayer event on successful submission. RETURN ONLY RAW JS, NO <script> TAGS.")
    suggested_event_name: str | None = Field(None, description="The dataLayer event name the script pushes (e.g. 'form_submit_fallback')")

# ── Step 3 Output ───────────────────────────────────────────────
class GTMVariable(BaseModel):
    gtm_var_name: str
    dl_key: str
    var_type: Literal["string", "integer", "boolean"] = "string"
    ga4_param: str

    @field_validator("var_type", mode="before")
    @classmethod
    def coerce_var_type(cls, v):
        allowed = {"string", "integer", "boolean"}
        if isinstance(v, str) and v.lower() in allowed:
            return v.lower()
        return "string"

class VariableArchitecture(BaseModel):
    form_index: Any
    event_name: str = Field(..., description="MUST be a GA4 Recommended Event (e.g. 'generate_lead', 'sign_up', 'login', 'purchase'). Use snake_case.")
    orphaned_bridge: bool
    variables: list[GTMVariable]

# ── Step 4 Output ───────────────────────────────────────────────
class GTMStrategy(BaseModel):
    form_index: Any
    skip: bool
    skip_reason: str | None = None
    tag_name: str
    trigger_name: str
    variables_to_create: list[GTMVariable]
    built_ins_to_activate: list[str]
    proximity_filter: str | None = Field(None, description="CSS selector for a unique parent element (e.g. '.site-footer') if there are multiple similar forms on the same page.")
    notes: str | None = Field(None, description="Auto-documentation explaining WHY this trigger method was chosen.")
    is_global_element: bool = Field(False, description="True if this form appears on multiple/all pages (like a footer newsletter or sidebar contact form). We should track it globally rather than per-page.")
    requires_trigger_group: bool = Field(False, description="True if tracking requires multiple conditions to be met (e.g. multi-step forms).")

# ── Step 5 Output (Final compiled plan — matches what main.py expects) ──────
class TrackingItem(BaseModel):
    event_name: str
    trigger_type: Literal[
        "custom_event", "ajax_complete", "element_visibility",
        "page_view", "click_links", "form_submission", "click", "custom_html"
    ]
    # ── Fields inherited from Step 4 (kept so Step 5 doesn't strip them) ──
    tag_name: str
    trigger_name: str
    variables_to_create: list[GTMVariable]
    built_ins_to_activate: list[str]
    # ── Layer 4: Consent Mode ──
    consent_requirements: list[str] = Field(default_factory=list)
    # ───────────────────────────────────────────────────────────────────────
    gtm_payload_keys: list[str]
    payload_schema: list[KeyValuePair] | None = None
    semantic_mapping: list[KeyValuePair] | None = None
    trigger_condition: TriggerCondition
    confidence: float = Field(..., ge=0.0, le=1.0)
    confidence_reason: str
    notes: str | None = None
    orphaned_bridge: bool = False
    proximity_filter: str | None = None
    custom_html_script: str | None = Field(None, description="Raw JavaScript code if trigger type is custom_html")
    # ── PII / Enhanced Conversions ──
    user_provided_data: list[KeyValuePair] | None = Field(None, description="Enhanced Conversions user-provided data fields. Hashed by GTM before sending to Google.")
    # ── Phase 12+: CSS Critic & Failure Predictor ──
    selector_critic: CSSCriticReview | None = None
    failure_risks: list[str] = Field(default_factory=list)
    qa_test_steps: list[str] = Field(default_factory=list)

    @field_validator("confidence", mode="before")
    @classmethod
    def clamp_confidence(cls, v):
        if isinstance(v, (int, float)):
            return max(0.0, min(1.0, float(v)))
        return v

class TrackingPlan(BaseModel):
    is_valid: bool = Field(..., description="True if the plan is completely valid without logic or schema errors.")
    errors: list[str] = Field(default_factory=list, description="If is_valid is false, list the structural or logical errors found.")
    health_score: int = Field(100, ge=0, le=100, description="0 to 100 score rating the container's performance and leanness. Subtract points for heavy JS, overly complex regex, or too many separate tags.")
    health_suggestions: list[str] = Field(default_factory=list, description="Suggestions for improving the container health score.")
    qa_test_plan: str | None = Field(None, description="Markdown bullet points instructing a QA human on how to test each tag based on its trigger conditions.")
    tracking_plan: list[TrackingItem]

    @field_validator("health_score", mode="before")
    @classmethod
    def clamp_health_score(cls, v):
        if isinstance(v, (int, float)):
            return max(0, min(100, int(v)))
        return v

class SOPDocument(BaseModel):
    markdown_content: str = Field(description="The full Markdown documentation explaining the tracking plan.")



# ═══════════════════════════════════════════════════════════════
# §2 — SYSTEM PROMPTS (One per pipeline step)
# ═══════════════════════════════════════════════════════════════

PROMPT_STEP1_PLATFORM = """
You are an expert web platform fingerprinting analyst.
Your task is to identify the CMS/platform used by a website and classify each form on the page.

## Platform Detection Signals
- `wpcf7`, `wp-content`, `wp_ajax` → wordpress_cf7
- `woocommerce`, `wc-cart`, `wc_checkout` → woocommerce
- `cdn.shopify.com`, `shopify`, `checkout.liquid` → shopify
- `hs-form`, `hs_form_guid`, `hubspotforms` → hubspot
- `webflow.com`, `w-form` → webflow
- `squarespace`, `ss-form` → squarespace
- Custom or no recognizable signals → custom

## Form Analysis Rules:
1. **Classification Goal**: Identify the primary purpose. MUST be one of:
   - `newsletter`: Newsletter signups, mailing list enrollment, subscription forms.
   - `contact_form`: Contact forms, general inquiries, feedback, support requests.
   - `lead`: Booking, appointment, quote request, job application — any high-value non-transactional business conversion.
   - `ecommerce_reserved`: E-commerce checkout forms, shopping carts, payment forms, add-to-cart buttons.
     Signals: credit card / CVV / expiry fields; cart totals, coupon codes; /checkout, /cart, /payment in URL;
     WooCommerce checkout, Shopify checkout, any transactional purchase flow.
   - `unknown`: Login, search boxes, generic inputs with no business tracking value.

2. **Context Utilization (CRITICAL)**:
   - Use `form_title` and `surrounding_context` (headings/paragraphs found by crawler) as the primary classification signal.
   - If `form_title` contains "Newsletter", "Бюлетин", "Subscribe", or "Абониране" → Classify as `newsletter`.
   - If only an email input exists but the heading indicates subscription → Classify as `newsletter`.

3. **Shadow Forms**: If `is_shadow_form` is true, look at `labels` and `context` very closely.
   - **Field Pattern**: A form with exactly ONE text/email field is 90% likely to be a newsletter.
   - **Button Pattern**: Button text containing "Абонирай се", "Subscribe", "Join", "Newsletter" is a strong signal.
   - **Surrounding Context**: Pay close attention to `surrounding_context`. Headings like "Бюлетин", "Stay Updated", or "Get Discounts" often precede subscription forms.

4. **E-commerce Boundary (CRITICAL)**:
   - Any form that is part of an e-commerce transaction flow (checkout, cart, payment, add-to-cart) MUST be `ecommerce_reserved`.
   - If the platform is `woocommerce` or `shopify` and the form is on a checkout page, classify as `ecommerce_reserved`.
   - Forms with `funnel_step` like `begin_checkout`, `add_payment_info`, `add_shipping_info`, `view_cart` → `ecommerce_reserved`.
   - Do NOT classify transactional forms as `lead`. The `lead` type is strictly for non-transactional conversions (quote requests, booking inquiries).

## Advanced Enrichments
1. **PII Scanner**: Set `contains_pii` to true ONLY if the form asks for Highly Sensitive Data (Passwords, SSN/EGN, Health Data, Full Credit Card). Standard name/email/phone is NOT considered sensitive PII here (we handle those via hashing later).
2. **Funnel Context**: If e-commerce/booking, infer the `funnel_step` (e.g. `view_cart`, `begin_checkout', `add_shipping_info`, `add_payment_info`).

## Search Form Recognition (Low Priority)
1. **Field Pattern**: Inputs with names like `s`, `search`, `q`, `query` or placeholders like "Търси...", "Search products...".
2. **Context**: Often in header or top bar. Classified as `other` or `search`.
3. **Action**: These are NOT business conversions. Do NOT classify them as `lead` or `newsletter`.

## AI Shadow Form Hunter (#43 Upgrade)
If the crawler flags a form with `is_shadow_form: true`, it means it's a standalone interaction cluster (like a newsletter input and button not wrapped in a <form> tag).
1. **Validation**: Confirm if the cluster's HTML and context indeed represent a business goal (mostly `newsletter`).
2. **Trigger Hint**: Suggest the most robust trigger type for this shadow form (e.g., "Element Visibility of div.newsletter-box" or "Click on Button 'Subscribe'"). Populate `shadow_trigger_hint`.

Return only valid JSON matching the schema. No explanations.
"""

PROMPT_SCORE_METHODS = """
You are a GTM measurement specialist. You are given REAL data from an actual test
submission of this form — including observed dataLayer events, visible success elements,
AJAX responses, URL redirects, and DOM changes.

For EACH tracking method in `available_tracking_methods`, evaluate its **resilience score**
(0.0 to 1.0) considering this SPECIFIC form's characteristics.

## Evaluation Criteria (apply ALL independently to EACH method)

1. **Form Complexity**: How many fields does this form have?
   - 1 field (email only = newsletter) → element_visibility is simple and sufficient
   - 3+ fields (contact/lead) → data capture needed → custom_event or ajax preferred

2. **False Positive Risk**: Does the success indicator ALSO show errors?
   - Generic response divs (e.g. `.wpcf7-response-output`) show both success AND errors → risk for multi-field forms
   - Scoped selectors (e.g. `form.sent .wpcf7-response-output`) only fire on actual success → low risk
   - Custom dataLayer events fire only on true success → lowest risk

3. **Server-Side Confirmation**: Does the method verify actual backend success?
   - `custom_event` from CF7 (`wpcf7mailsent`) → fires after wp_mail() succeeds ✅
   - `element_visibility` → fires on DOM change only, no server confirmation ❌
   - `ajax_complete` → verifies HTTP 200 response ✅

4. **Data Capture Need**: Does the business goal require capturing field values?
   - Newsletter → NO (just the subscription event is enough)
   - Contact form → YES (email, name, message for CRM integration)
   - Lead form → YES (phone, company, etc.)

5. **GTM Setup Complexity**: How easy is it to set up and maintain?
   - element_visibility → 1 trigger, no variables. Simplest.
   - custom_event → trigger + dataLayer variables. Medium.
   - ajax_complete → requires AJAX listener tag. Complex.
   - form_submission → trigger + Form built-in vars. Simple.

6. **Signal Stability**: Will this method survive theme/plugin updates?
   - CSS class selectors → fragile if theme changes
   - DataLayer events from plugins → stable unless plugin is replaced
   - URL-based → stable but fragile to URL rewrites

## Recommended Built-In Variables per Method
For each method, populate `recommended_built_ins` with the relevant GTM built-in variables:

- **form_submission** → ["Form Element", "Form Classes", "Form ID", "Form Target", "Form URL", "Form Text"]
- **click** → ["Click Element", "Click Classes", "Click ID", "Click Target", "Click URL", "Click Text"]
- **element_visibility** → ["Page URL", "Page Path"]
- **custom_event** → ["Page URL", "Page Path"]
- **page_view** → ["Page URL", "Page Path", "Page Hostname", "Referrer"]
- **dom_ready** → ["Page URL", "Page Path"]
- **ajax_complete** → ["Page URL", "Page Path"]

## Known Platform Recipes
If a platform recipe exists (CF7, HubSpot, etc.), the system will auto-inject
the listener tag during compilation. This guarantees that custom_event will fire.
Factor this into your resilience evaluation for custom_event.

## IMPORTANT
Do NOT follow a fixed priority order. Evaluate each method ON ITS OWN MERITS for THIS form.
A newsletter form may score element_visibility higher than custom_event if simplicity
outweighs server-side confirmation for that use case.

Return ONLY valid JSON matching the schema. No explanations.
"""

PROMPT_STEP2_VALIDATE = """
You are a meticulous GTM QA specialist, focused on validating successful form submissions.
Your task is to analyze the available tracking signals and determine the BEST measurement method.

## Priority Order (pick the one available with the HIGHEST priority)
1. `custom_event` — A named DataLayer event fires on success (e.g. "wpcf7mailsent"). MOST RELIABLE.
2. `ajax_complete` — AJAX request completes with a detectable success response. RELIABLE.
3. `element_visibility` — A success element becomes visible AFTER submission. RELIABLE.
4. `page_view` — Browser redirects to a static Thank You page. RELIABLE.
5. `form_submission` — Native GTM form trigger (lowest priority — use only as last resort).

## Sentiment Analysis (Critical Safety Check)
Inspect `success_message_text`. If it contains negative words like:
"error", "invalid", "failed", "incorrect", "problem", "грешка", "невалиден"
→ Set `is_genuine_success: false` and return `rejection_reason`

## Advanced Capabilities
1. **Signal Triangulation**: If you choose `ajax_complete` or `element_visibility` but there is a reliable secondary signal (e.g., both AJAX and Element Visibility are present), specify the secondary method in `fallback_method`. This helps build resilient Trigger Groups later.
2. **URL Parameter Intelligence**: If the method is `page_view` (e.g. redirected to `/thank-you?order_id=123&status=ok`), extract and list the keys (`order_id`, `status`) in `url_parameters`.
3. **Network Payload Analysis**: If `ajax_response_body` or network logs are provided, analyze the JSON. If it contains explicit success flags (e.g. `{"status": "success"}`, `user_id: 123`), set `network_payload_success: true`. This is absolute proof of success.
4. **Conversion Value Extractor**: Actively look for currency symbols ($, €, лв) and numbers near the success message or in the payload. If it's a checkout or high-value form, and a value is found, set `conversion_value_detected: true` and populate `conversion_value` (as float) and `conversion_currency` (e.g. 'EUR', 'BGN', 'USD').

- page_view → {"page_path": "<path>"} or {"page_path_regex": "<pattern>"} (Include "fragment": "<anchor>" if provided in crawler data for "same-page" redirects).

## Self-Healing Regex Rules (#11 Upgrade)
If `redirect_is_dynamic` is true or if the URL contains IDs/session data, you MUST generate a **Self-Healing PCRE Regex**:
1. **Parameter Resilience**: Use `(\\?.*)?$` at the end of every path-based regex to ensure it ignores any appended query parameters (UTM tags, IDs) unless they are part of the match.
2. **Specific ID Matchers**: Replace numeric segments with `[0-9]+` and alphanumeric IDs with `[a-zA-Z0-9_-]+`. Avoid `.*` if a more specific class exists.
3. **Partial Anchoring**: Ensure the regex starts with the base path but is flexible enough for common CMS variations (e.g. trailing slashes).
4. **Example**: For `/checkout/123/success?utm=fb`, use `^/checkout/[0-9]+/success/?(\\?.*)?$`

If NO valid tracking signal exists, set best_method to "none" and is_genuine_success to false.
Return only valid JSON. No explanations.
"""

PROMPT_STEP2B_FALLBACK = """
You are the "Fallback Injector" module of an AI GTM Architect.
The previous validation step determined that standard tracking signals (like URL redirects, success messages, or generic dataLayer events) were weak or non-existent for this form.
However, we DID detect that a background network request (AJAX/Fetch) fired when the form was submitted.

Your job is to write a bulletproof Vanilla JavaScript snippet that does the following:
1. Intercepts `fetch` or `XMLHttpRequest`.
2. Checks if the request URL matches the known API endpoint for this form's submission (`ajax_endpoint`).
3. Checks if the response status is successful (e.g., 200-299).
4. Pushes a highly robust dataLayer event (using `suggested_event_name` like 'form_submit_fallback') when the success response is received.
5. Ensures the interception doesn't break the original site functionality (always call original fetch/xhr methods).
6. Uses `window.dataLayer = window.dataLayer || []; window.dataLayer.push(...)`.

If writing an interceptor isn't viable based on the provided data, set `is_needed` to false.
Otherwise, provide the raw JS code inside `js_code` (no ```javascript or <script> tags, JUST the JS code).
"""

PROMPT_STEP3_VARIABLES = """
You are a GA4 Data Architect. Your task is to design the optimal variable schema for GTM.
You receive a single form's success signal validation and its raw data.

## Your Rules

### 1. Variable Extraction Priority
1. `datalayer_diff.added_keys` — Keys that appeared ONLY after submit (cleanest)
2. `datalayer_events` — Keys from event payloads
3. `dom_payload_keys` / `field_labels` — HTML input names and their labels
4. Inferred: form_id, form_name, page_location

### 2. Label-to-Variable Mapping (Critical for clarity)
Use `field_labels` to rename cryptic keys. Examples:
- Key: `input_1`, Label: "Your Name" → `dl_key: "input_1"`, `ga4_param: "user_name"`
- Key: `field_23`, Label: "Email address" → `dl_key: "field_23"`, `ga4_param: "email"`

### 3. GA4 Semantic Mapping Table
Apply these mappings when keys match semantically:
- name, full_name, your-name, cnt_name → `user_name`
- email, your-email, cnt_email, user_email → `email`
- phone, tel, your-phone, cnt_phone → `phone_number`
- message, comment, your-message, cnt_msg → `message`
- order_id, booking_id, ref_no, transaction_id → `transaction_id`
- revenue, total, value, price, sum → `value`
- currency, curr, currency_code → `currency`
- product_name, item_name, item_title → `item_name`

### 4. GTM Variable Naming Convention
Name as: `DLV - <Readable Name>`, e.g. `DLV - Email`, `DLV - Order ID`

### 5. Nested Objects (Dot-Notation)
If a DataLayer value is an object, use dot-notation for the `dl_key`:
- `{user: {id: 1}}` → dl_key: `user.id`

### 6. Orphaned Bridge Flag
Set `orphaned_bridge: true` if BOTH of these are true:
- datalayer_diff.added_keys is empty AND datalayer_events is empty
- BUT field_labels or dom_payload_keys exist (form has data we can scrape from DOM)

### 7. Variable Types (from payload_schema)
- "string" → var_type: "string"
- "integer" / "number" → var_type: "integer"
- "boolean" → var_type: "boolean"

### 8. Event Naming
Contact/Lead → `generate_lead` | Booking → `book_appointment` | Newsletter → `sign_up`
Checkout → `purchase` | Quote → `request_quote` | Application → `submit_application`

### 9. Newsletter Specifics
For `newsletter` forms, set the event name to `sign_up` and ensure `method` is set to 'email' in the semantic mapping if an email field is present.

### 10. PII Routing for Enhanced Conversions
Fields that map semantically to: email, phone_number, user_name, first_name, last_name, street, city, region, postal_code, country:
- These MUST NOT be mapped as standard GA4 event parameters (sending raw PII to GA4 violates Google ToS).
- Still include them in the `variables` list so the pipeline can route them to the Enhanced Conversions `user_provided_data` object automatically (with SHA-256 hashing by GTM).
- If the ONLY extractable data is PII (no non-PII fields), set `orphaned_bridge: true`.

Return only valid JSON. No explanations.
"""

PROMPT_STEP3B_CSS_CRITIC = """
You are a Senior CSS Architect and GTM Robustness Critic.
Your job is to evaluate CSS selectors for brittleness, preventing tracking from breaking when a website is updated.

## Rules for Fragility Score (1 to 10):
1. Very Fragile (Score 8-10): Uses nth-child, deep generic tag chains (e.g. `body > div > div > form`), or long utility class chains (like Tailwind).
2. Moderate (Score 4-7): Uses structural classes like `.footer-form` but still has some tag-level nesting.
3. Robust (Score 1-3): Uses unique `id` attributes, specific `data-*` attributes, or generic tags combined with highly specific functional `name` or `action` attributes.

## Advanced Capabilities
4. **Mutation Observer Check**: Check the `parent_context` or `html_attributes`. If the form appears to be in a modal, popup, or dynamically injected container (e.g. React/Vue roots, classes like `modal-content`, `popup`), set `is_dynamic_element: true`.

If the score is >= 7, you MUST provide a `suggested_selector` that is much shorter and more robust, relying on IDs or attributes found in the provided HTML context.
Return only valid JSON matching the schema. No explanations.
"""

PROMPT_STEP4_STRATEGY = """
You are a Senior GTM Implementation Architect. You create the final implementation blueprint.
You receive the full context: platform, success validation, variable architecture, and existing container.

## Your Responsibilities

### 1. Double-Firing Audit (Strict Deduplication)
Compare against `existing_gtm_elements`.
If an existing Tag has an IDENTICAL `event_name` AND a compatible `trigger_type` (e.g., both track on 'click' or 'form_submit'), set `skip: true` with a detailed `skip_reason`.
DO NOT propose a new tag if the tracking goal is already met by an existing tag.

### 2. Naming Convention (STRICT TEMPLATES)
All names MUST follow these exact patterns. The "Auto - " prefix marks AutoGTM-managed elements.

- **Tags:** `Auto - GA4 Event - {event_name}`
  Examples: "Auto - GA4 Event - sign_up", "Auto - GA4 Event - generate_lead"

- **Triggers (by type):**
  - custom_event:        `Auto - CE - {dataLayer_event_name}`
    Example: "Auto - CE - wpcf7mailsent"
  - ajax_complete:       `Auto - CE - ajaxComplete - {event_name}`
    Example: "Auto - CE - ajaxComplete - sign_up"
  - form_submission:     `Auto - Form Submit - {form_identifier}`
    Example: "Auto - Form Submit - newsletter"
  - element_visibility:  `Auto - EV - {selector_summary}`
    Example: "Auto - EV - .wpcf7-mail-sent-ok"
  - page_view:           `Auto - PV - {page_path}`
    Example: "Auto - PV - /thank-you"
  - click_links:         `Auto - Click - {link_type}`
    Example: "Auto - Click - contact_links"

- **Variables:** `DLV - {dataLayer_key_name}` for Data Layer Variables

NEVER use vague or ambiguous identifiers. Always include business context where applicable.

### 3. Built-In Variables to Activate
Based on trigger_type, specify required built-in variables:
- form_submission → ["Form ID", "Form Classes", "Form Target"]
- click → ["Click ID", "Click Classes", "Click URL", "Click Text"]
- page_view → ["Page Path", "Page URL"]

### 4. Proximity Filter
If `parent_context` has unique identifiers (like `.site-footer`, `#hero-section`),
provide a `proximity_filter` CSS selector string to disambiguate multiple forms.

### 5. Global Element Deduplicator
If the parent_context strongly suggests this form is part of a global layout (e.g. `footer`, `header`, `sidebar`, `nav`) rather than specific page content, set `is_global_element: true`. This tells the compiler to merge identical forms across pages into a single global Tag/Trigger.

### 6. Trigger Grouping
If the form is clearly a multi-step flow requiring multiple valid signals, set `requires_trigger_group: true`.

### 8. Shadow Form Strategy
If `is_shadow_form` is true:
- Avoid `form_submission` triggers (they won't fire).
- Prefer `element_visibility` (on the success message) or `click` (on the submit button).
- Use `click_id` or `click_class` in the Trigger Condition for high precision.
- Reference the `shadow_trigger_hint` from Step 1.

### 9. Filter Search Forms
If the form is clearly a site search (as noted in Step 1 or by fields like `s`, `q`, `search`):
- Set `skip: true`.
- Reason: "Internal site search detected. Not a business conversion form."

### 10. CRITICAL OVERRIDE — Newsletter & Subscription Forms
Even if similar tags exist (e.g., a generic "form_submit", "contact_form_submit", or "sign_up"),
if the form is classified as a Newsletter or Subscription form, you MUST generate a dedicated
Newsletter tag with event_name "sign_up" UNLESS an EXACT match exists with identical event_name
AND identical trigger selector targeting the same DOM element. The existing "form_submit" or
"contact_form_submit" tracks a DIFFERENT business goal (contact inquiries) — Newsletter opt-in
is a separate conversion. Do NOT skip newsletter forms based on semantic similarity to other
form-related events.

Return only valid JSON. No explanations.
"""

PROMPT_STEP5_AUDIT = """
You are a strict GTM Container Validation Engineer performing a final audit.
Review the complete tracking plan and return a CORRECTED version.

## Check Each Item For:
1. `trigger_condition` must match `trigger_type`:
   - element_visibility → must have "selector"
   - page_view → must have "page_path" or "page_path_regex"
   - custom_event → must have "event"
   - form_submission → must have "key" and "value"
2. All `gtm_payload_keys` must be snake_case or dot.notation
3. No duplicate `event_name` values (add _hero / _footer suffix if needed)
4. `confidence` must match the signal quality (custom_event ≥ 0.9, page_view ≤ 0.75)
5. `semantic_mapping` must be present and map all keys in `gtm_payload_keys`

6. **Self-Healing Regex Audit**: Verify any `page_path_regex`. It MUST be resilient to query parameters and trailing slashes. If you see a brittle pattern like `/success$`, change it to `/success/?(\\?.*)?$`.

7. **GA4 Strict Schema Enforcer**: If `event_name` is `purchase`, it MUST include `transaction_id`, `value`, and `currency` in the payload. If any are missing, you MUST set `is_valid: False` and explicitly list the missing e-commerce parameters in the `errors` array, instructing the user to build a fallback scraper.

8. **Failure Predictor & QA Guide**: For EVERY item, analyze technical risks (iFrames, Shadow DOM, SPAs, slow-loading elements).
   - Populate `failure_risks` with concise descriptions of technical dangers.
   - Populate `qa_test_steps` with specific, step-by-step instructions for a human to verify this specific tag.

Flag any item with confidence < 0.5 by prefixing its notes with "⚠️ LOW CONFIDENCE: "

## Advanced Post-Processing
1. **Container Health Score**: Calculate a `health_score` (0-100). Subtract points for overly complex regex, reliance on brittle CSS text selectors, or having too many highly specific tags instead of a global tag. Provide `health_suggestions` to improve performance.
2. **Automated QA Test Generation**: Provide a `qa_test_plan` in Markdown format, listing explicit chronological steps a human QA tester must perform on the website to verify each Tag actually fires.

Return ONLY the corrected JSON matching the exact schema. No explanations.
"""

PROMPT_STEP6_SOP = """
You are a Technical Writer and GTM Expert.
Your task is to generate a beautiful, easy-to-read Markdown Standard Operating Procedure (SOP) based on the final Tracking Plan.

## Document Structure
1. **Executive Summary**: Brief overview of what this container tracking implementation covers.
2. **Global Variables & Configs**: List of DataLayer variables, Consent rules, and settings.
3. **Trigger Definitions & QA**: For each trigger, explain exactly HOW it fires and WHAT the human QA tester needs to do on the website to verify it (e.g. "To test this, fill out the form at /contact and click Submit. Look for the DataLayer event X").
4. **Advanced Logic**: Document any Fallback Interceptors, Custom HTML scripts, or specific e-commerce Strict Schema enforcements.

Return only valid JSON matching the schema containing the `markdown_content`. No explanations.
"""


# ═══════════════════════════════════════════════════════════════
# §3 — HELPER
# ═══════════════════════════════════════════════════════════════

def get_debug_dir(session_id: str = None) -> str:
    """Returns a unique session-based debug directory."""
    if not session_id:
        import uuid as _uuid
        session_id = _uuid.uuid4().hex[:12]
    return os.path.join(".debug", session_id)

def _save_debug(step_name: str, data, session_id: str = None) -> None:
    """Persist the output of a pipeline step to .debug/<session_id>/<step_name>.json for inspection."""
    debug_dir = get_debug_dir(session_id)
    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, f"{step_name}.json")
    
    # Convert Pydantic models to dict for JSON serialization
    if hasattr(data, "model_dump"):
        dump_data = data.model_dump()
    elif isinstance(data, list) and len(data) > 0 and hasattr(data[0], "model_dump"):
        dump_data = [d.model_dump() for d in data]
    else:
        dump_data = data
        
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dump_data, f, indent=2, ensure_ascii=False)

FREE_FALLBACK_MODELS = [
    "gemini-2.5-flash-lite",   # Free, high limit ⭐
    "gemini-2.0-flash-lite",   # Free, fast
    "gemini-2.0-flash",        # Free
    "gemini-2.5-flash",        # Free, thinking model (highest quota: 500/day)
]


def _call_gemini(client, model: str, system: str, prompt: str, schema=None,
                 image_parts: list = None, ctx: PipelineContext = None,
                 log_fn=print) -> dict:
    import time

    cfg = dict(
        system_instruction=system,
        temperature=0.1,
        response_mime_type="application/json"
    )
    if schema:
        cfg["response_schema"] = schema

    # Construct contents (multimodal)
    contents = [prompt]
    if image_parts:
        contents.extend(image_parts)

    def _parse(response):
        """Parse response using SDK native parser or JSON fallback."""
        if schema and hasattr(response, 'parsed') and response.parsed:
            return response.parsed
        raw = response.text or ""
        if raw.startswith("```json"):
            raw = raw.replace("```json", "").replace("```", "").strip()
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _try_model(m: str) -> dict | None:
        """Single attempt with one model. Increments api_call_count."""
        if ctx:
            ctx.api_call_count += 1
        response = client.models.generate_content(
            model=m,
            contents=contents,
            config=types.GenerateContentConfig(**cfg)
        )
        return _parse(response)

    def _is_retriable(error_msg: str) -> bool:
        return any(tok in error_msg for tok in ("429", "quota", "exhausted", "503", "500"))

    # ── Sticky model: reuse last successful fallback to skip cascade ──
    effective_model = model
    if ctx and ctx._sticky_model and ctx._sticky_model != model:
        ctx._sticky_call_count += 1
        if ctx._sticky_call_count >= 5:
            # Periodically retry primary model to check if it's back
            ctx._sticky_call_count = 0
            ctx._sticky_model = None
        else:
            effective_model = ctx._sticky_model

    # ── Attempt 1: Primary (or sticky) model ──
    try:
        result = _try_model(effective_model)
        if result is not None:
            return result
    except Exception as e:
        error_msg = str(e).lower()

        if not _is_retriable(error_msg):
            log_fn(f"  [❌ API Error] Неочаквана грешка с {effective_model}: {e}")
            if ctx:
                ctx.record("api_call", None, Severity.ERROR,
                           f"Non-retriable error on {effective_model}: {e}")
            return None

        # ── Attempt 2: Retry after short delay ──
        log_fn(f"  [⚠ 429/Quota] {effective_model} — повторен опит след 2 сек...")
        if ctx:
            ctx.record("api_call", None, Severity.WARNING,
                       f"429/Quota on {effective_model}, retrying...")
        time.sleep(2)
        try:
            result = _try_model(effective_model)
            if result is not None:
                return result
        except Exception:
            log_fn(f"  [⚠ Retry Failed] {effective_model} — преминаване към резервни модели...")

    # Clear sticky if it just failed, so we search fresh
    if ctx and ctx._sticky_model:
        ctx._sticky_model = None

    # ── Attempt 3+: Cycle through ALL free fallback models ──
    for fallback in FREE_FALLBACK_MODELS:
        if fallback == effective_model:
            continue  # skip the model that already failed
        log_fn(f"  [🔄 Fallback] Опит с {fallback}...")
        try:
            result = _try_model(fallback)
            if result is not None:
                log_fn(f"  [✅ Fallback OK] {fallback} успя!")
                # Sticky: remember this model for subsequent calls
                if ctx:
                    ctx._sticky_model = fallback
                return result
        except Exception as fb_err:
            fb_msg = str(fb_err).lower()
            log_fn(f"  [❌ Fallback Failed] {fallback}: {fb_msg[:120]}")
            if ctx:
                ctx.record("api_call", None, Severity.WARNING,
                           f"Fallback {fallback} failed: {fb_msg[:120]}")
            if _is_retriable(fb_msg):
                continue  # try next fallback
            else:
                return None  # non-retriable error, stop

    log_fn("  [❌ ALL MODELS FAILED] Всички безплатни модели са изчерпани.")
    if ctx:
        ctx.record("api_call", None, Severity.ERROR,
                   "All fallback models exhausted (429 on all)")
    return None


# ═══════════════════════════════════════════════════════════════
# §3.4 — TRIGGER CONDITION FORMAT BRIDGE
# ═══════════════════════════════════════════════════════════════

def _map_crawler_trigger_condition(raw_cond: dict) -> dict:
    """
    Map crawler's trigger_condition format to TriggerCondition Pydantic field names.

    The crawler produces form_submission conditions as {"key": "class", "value": "..."}
    but TriggerCondition expects {"filter_key": "class", "filter_value": "..."}.
    Other method types (custom_event, page_view, element_visibility) already use
    the correct field names and pass through unchanged.
    """
    if not raw_cond:
        return {}
    mapped = {}
    for k, v in raw_cond.items():
        if k == "key":
            mapped["filter_key"] = v
        elif k == "value":
            mapped["filter_value"] = v
        else:
            mapped[k] = v
    return mapped


# ── Deterministic Step 3 Fallback Helpers ───────────────────────

_FORM_TYPE_TO_EVENT = {
    "newsletter": "sign_up",
    "contact_form": "generate_lead",
    "lead": "generate_lead",
    "booking": "book_appointment",
    "quote": "request_quote",
    "application": "submit_application",
    "unknown": "generate_lead",
}

_FIELD_TO_GA4_PARAM = {
    "name": "user_name", "full_name": "user_name", "your-name": "user_name", "cnt_name": "user_name",
    "email": "email", "your-email": "email", "cnt_email": "email", "user_email": "email",
    "phone": "phone_number", "tel": "phone_number", "your-phone": "phone_number", "cnt_phone": "phone_number",
    "message": "message", "comment": "message", "your-message": "message", "cnt_msg": "message",
    "order_id": "transaction_id", "booking_id": "transaction_id", "transaction_id": "transaction_id",
    "revenue": "value", "total": "value", "price": "value",
    "currency": "currency", "currency_code": "currency",
}


def _infer_event_name(form_type: str) -> str:
    """Deterministic event name inference from form type (mirrors PROMPT_STEP3 Rule 8)."""
    return _FORM_TYPE_TO_EVENT.get(form_type, "generate_lead")


def _extract_variables_from_form(form_data: dict) -> list[GTMVariable]:
    """
    Deterministic variable extraction from form DOM data.
    Uses dom_payload_keys + field_labels to construct GTMVariable objects.
    Mirrors the AI's logic from PROMPT_STEP3_VARIABLES but without an API call.
    """
    variables = []
    seen = set()
    dom_keys = form_data.get("dom_payload_keys", [])
    labels = form_data.get("field_labels", {})

    for raw_key in dom_keys:
        label = labels.get(raw_key, "").lower().strip()
        # Match against known semantic mappings (label first, then raw key)
        ga4_param = None
        for pattern, param in _FIELD_TO_GA4_PARAM.items():
            if pattern in label or pattern in raw_key.lower():
                ga4_param = param
                break
        if not ga4_param:
            ga4_param = raw_key.lower().replace("-", "_").replace(" ", "_")

        if ga4_param in seen:
            continue
        seen.add(ga4_param)

        readable = labels.get(raw_key) or ga4_param.replace("_", " ").title()
        variables.append(GTMVariable(
            gtm_var_name=f"DLV - {readable}",
            dl_key=raw_key,
            var_type="string",
            ga4_param=ga4_param,
        ))
    return variables


# ═══════════════════════════════════════════════════════════════
# §3.5 — DETERMINISTIC MICRO-FIXERS (Pre-audit corrections)
# ═══════════════════════════════════════════════════════════════

def _fix_regex_patterns(items: list[dict]) -> list[dict]:
    """Ensure all page_path_regex are resilient to query params and trailing slashes."""
    for item in items:
        tc = item.get("trigger_condition", {})
        regex = tc.get("page_path_regex")
        if regex and not regex.endswith("(\\?.*)?$"):
            regex = regex.rstrip("$")
            if not regex.endswith("/?"):
                regex += "/?"
            regex += "(\\?.*)?$"
            tc["page_path_regex"] = regex
    return items

def _fix_trigger_conditions(items: list[dict]) -> list[dict]:
    """Ensure trigger_condition has required fields for its trigger_type."""
    REQUIRED = {
        "element_visibility": ("selector",),
        "custom_event": ("event",),
        "page_view": ("page_path", "page_path_regex"),
    }
    for item in items:
        tt = item.get("trigger_type")
        tc = item.get("trigger_condition", {})
        fields = REQUIRED.get(tt, ())
        if fields and not any(tc.get(f) for f in fields):
            item.setdefault("failure_risks", []).append(
                f"trigger_condition missing required field for {tt}: needs one of {list(fields)}"
            )
            item["confidence"] = min(item.get("confidence", 0.5), 0.3)
            item["notes"] = (item.get("notes") or "") + " [WARNING: trigger_condition incomplete]"
    return items

def _fix_ecommerce_schema(items: list[dict]) -> list[dict]:
    """Verify purchase events have transaction_id, value, currency in payload."""
    for item in items:
        if item.get("event_name") == "purchase":
            payload_keys = set(item.get("gtm_payload_keys", []))
            sm = item.get("semantic_mapping") or {}
            mapped_params = set(sm.values()) if isinstance(sm, dict) else set()
            required = {"transaction_id", "value", "currency"}
            missing = required - (payload_keys | mapped_params)
            if missing:
                item.setdefault("failure_risks", []).append(
                    f"GA4 purchase event missing required params: {', '.join(sorted(missing))}"
                )
                item["confidence"] = min(item.get("confidence", 0.5), 0.4)
    return items


# ═══════════════════════════════════════════════════════════════
# §4 — PIPELINE STEPS
# ═══════════════════════════════════════════════════════════════

def _step1_analyze_platform(client, model: str, crawler_data: dict,
                             ctx: PipelineContext, log_fn=print) -> PlatformAnalysis:
    """Step 1: Platform & Form Analyst with Vision Support."""
    log_fn("── Step 1: analyze_platform_and_forms()  — Platform & Form Analyst... ⏳")
    
    # Vision logic: load form screenshots if they exist
    image_parts = []
    forms = crawler_data.get("forms_processed", [])
    for f in forms:
        fi = f.get("form_index")
        img_path = f".debug/form_{fi}.png"
        if os.path.exists(img_path):
            try:
                with open(img_path, "rb") as img_file:
                    img_data = img_file.read()
                    image_parts.append(types.Part.from_bytes(data=img_data, mime_type="image/png"))
            except Exception:
                pass
    
    if image_parts:
        log_fn(f"   ↳ 👁️ Vision AI: Attached {len(image_parts)} form screenshot(s) for verification.")

    prompt = f"Analyze this crawler data and identify the platform and each form:\n{json.dumps(crawler_data, indent=2)}"
    result: PlatformAnalysis | None = _call_gemini(client, model, PROMPT_STEP1_PLATFORM, prompt, PlatformAnalysis, image_parts=image_parts, ctx=ctx, log_fn=log_fn)
    
    if not result:
        ctx.record("step1_platform", None, Severity.FATAL, "Platform analysis API call failed — all downstream steps will lack platform context.")
        return PlatformAnalysis(platform="unknown", platform_confidence=0.0, forms=[])

    log_fn(f"✓ Platform: **{result.platform}** ({result.platform_confidence:.0%} confidence)")
    for f in result.forms:
        log_fn(f"   ↳ Form #{f.form_index}: `{f.form_type}` — {f.form_role}")
    return result


def _step2_validate_signals(client, model: str, crawler_data: dict, platform_result: PlatformAnalysis,
                             ctx: PipelineContext, log_fn=print) -> list[SuccessValidation]:
    """Step 2: Success Signal Validator — one call per form."""
    log_fn("── Step 2: validate_success_signals()    — Success Signal Validator... ⏳")
    validations: list[SuccessValidation] = []
    forms_processed = crawler_data.get("forms_processed", [])

    for i, form_data in enumerate(forms_processed):
        _raw_fi = form_data.get("form_index", i)
        try:
            fi = int(_raw_fi)
        except (ValueError, TypeError):
            fi = i  # Fallback for non-numeric indices like "shadow_0"
        is_shadow = form_data.get("is_shadow_form", False)

        # ── User-Selected Method Passthrough ─────────────────────────
        # If the user already chose a tracking method in the Signal Review UI,
        # skip the LLM call entirely and construct a SuccessValidation directly.
        user_method = form_data.get("_user_selected_method")
        user_atm_list = form_data.get("available_tracking_methods") or []
        if user_method and user_method != "auto" and user_atm_list:
            user_atm = user_atm_list[0]
            user_cond = _map_crawler_trigger_condition(user_atm.get("trigger_condition", {}))
            if any(v is not None for v in user_cond.values()):
                result = SuccessValidation(
                    form_index=fi,
                    is_genuine_success=True,
                    best_method=user_method,
                    method_confidence=1.0,  # User-selected = full confidence
                    trigger_condition=TriggerCondition(**user_cond),
                )
                log_fn(f"   ↳ ✅ Form #{fi}: `{user_method}` (100% — user-selected)")
                validations.append(result)
                continue

        if not is_shadow and not form_data.get("is_successful_submission"):
            log_fn(f"   ↳ Form #{fi or '?'} skipped — no success signal detected by crawler")
            ctx.record("step2_validate", fi, Severity.WARNING, "No success signal detected by crawler — form skipped.")
            continue

        if is_shadow:
            log_fn(f"   ↳ Form #{fi or '?'} is a shadow form — bypassing success signal gate.")

        prompt = f"""Platform context: {platform_result.model_dump_json(indent=2)}

Form data for validation:
{json.dumps(form_data, indent=2)}"""
        result: SuccessValidation | None = _call_gemini(client, model, PROMPT_STEP2_VALIDATE, prompt, SuccessValidation, ctx=ctx, log_fn=log_fn)
        if not result:
            ctx.record("step2_validate", fi, Severity.ERROR, f"Gemini API failed for form #{fi} — form dropped from pipeline.")
            continue

        flag = "✅" if result.is_genuine_success else "❌ REJECTED"
        reason = result.rejection_reason or ""
        method = result.best_method
        conf   = result.method_confidence
        log_fn(f"   ↳ {flag} Form #{result.form_index}: `{method}` ({conf:.0%}) {reason}")
        validations.append(result)
    return validations


def _step2b_fallback_injector(client, model: str, crawler_data: dict, validations: list[SuccessValidation],
                              ctx: PipelineContext, log_fn=print) -> dict[int, FallbackScript]:
    """Step 2B: Fallback Injector — generates custom JS for tricky AJAX forms."""
    log_fn("── Step 2B: generate_fallback_scripts()  — Fallback Injector... ⏳")
    fallback_scripts = {}

    for val in validations:
        # Only trigger fallback injector if method confidence is critically low (< 0.5)
        # and we know an AJAX request succeeded.
        form_data = next((f for f in crawler_data.get("forms_processed", []) if f.get("form_index") == val.form_index), None)
        if not form_data: continue

        needs_fallback = val.method_confidence < 0.5 and form_data.get("has_successful_ajax") and form_data.get("ajax_endpoint")

        if not needs_fallback:
            continue

        log_fn(f"  [Form #{val.form_index}] ⚠️ Weak signals detected (Confidence: {val.method_confidence}). Generating Fallback Interceptor...")

        prompt = f"""Form Index: {val.form_index}
Forms Data:
{json.dumps(form_data, indent=2)}
"""
        result = _call_gemini(client, model, PROMPT_STEP2B_FALLBACK, prompt, FallbackScript, ctx=ctx, log_fn=log_fn)
        if result and result.is_needed and result.js_code:
            fallback_scripts[val.form_index] = result
            log_fn(f"   ↳ ✅ Fallback script generated! Event: {result.suggested_event_name}")
        else:
            ctx.record("step2b_fallback", val.form_index, Severity.WARNING, f"AI declined to generate fallback for form #{val.form_index} despite weak signals.")
            log_fn(f"   ↳ ❌ AI declined to generate a fallback snippet.")
            
    return fallback_scripts

def _step3_architect_variables(client, model: str, crawler_data: dict,
                                platform_result: PlatformAnalysis, validations: list[SuccessValidation],
                                ctx: PipelineContext, existing_elements: list | None = None, log_fn=print) -> list[VariableArchitecture]:
    """Step 3: Variable Architect — one call per validated form."""
    existing_elements = existing_elements or []
    log_fn("── Step 3: architect_variables()         — Variable Architect... ⏳")
    architectures: list[VariableArchitecture] = []
    forms_by_index = {f.get("form_index"): f for f in crawler_data.get("forms_processed", [])}

    for validation in validations:
        if not validation.is_genuine_success:
            continue
        form_index = validation.form_index
        form_data = forms_by_index.get(form_index, {})
        # Look up by the new form_index field (fallback to 'lead' if not found)
        form_type = "lead"
        for f in platform_result.forms:
            if getattr(f, "form_index", None) == form_index:
                form_type = getattr(f, "form_type", "lead")
                break

        prompt = f"""Platform: {platform_result.platform}
Form type: {form_type}

Success validation:
{validation.model_dump_json(indent=2)}

Raw form data:
{json.dumps(form_data, indent=2)}"""
        result: VariableArchitecture | None = _call_gemini(client, model, PROMPT_STEP3_VARIABLES, prompt, VariableArchitecture, ctx=ctx, log_fn=log_fn)
        if not result:
            # Deterministic fallback: build minimal architecture from form metadata
            event_name = _infer_event_name(form_type)
            variables = _extract_variables_from_form(form_data)
            has_datalayer = bool(form_data.get("datalayer_diff", {}).get("added_keys")) or bool(form_data.get("datalayer_events"))
            result = VariableArchitecture(
                form_index=form_index,
                event_name=event_name,
                orphaned_bridge=not has_datalayer,
                variables=variables,
            )
            ctx.record("step3_variables", form_index, Severity.WARNING,
                       f"AI failed for form #{form_index} — deterministic fallback "
                       f"(event={event_name}, {len(variables)} vars)")
            log_fn(f"   ↳ ⚠️ Form #{form_index}: deterministic fallback — "
                   f"`{event_name}`, {len(variables)} variable(s)")
            
        n_vars = len(result.variables)
        bridge = result.orphaned_bridge
        log_fn(f"   ↳ Form #{result.form_index}: event=`{result.event_name}`, {n_vars} variable(s), bridge={bridge}")
        architectures.append(result)
    return architectures


def _step3b_css_critic(client, model: str, validations: list[SuccessValidation], crawler_data: dict,
                        ctx: PipelineContext, log_fn=print) -> None:
    """Step 3B: CSS Robustness Critic. Reviews selectors and mutates validations if fragile."""
    log_fn("── Step 3B: css_critic()                 — CSS Robustness Critic... ⏳")
    forms_by_index = {f.get("form_index"): f for f in crawler_data.get("forms_processed", [])}

    for validation in validations:
        if not validation.is_genuine_success:
            continue

        tc = validation.trigger_condition
        selector = tc.selector or tc.filter_value
        # Only critic if we are relying on a CSS selector
        if not selector or validation.best_method not in ["element_visibility", "click"]:
            continue

        form_index = validation.form_index
        form_data = forms_by_index.get(form_index, {})

        prompt = f"""Evaluate this CSS selector for the form element:
Selector: `{selector}`

Form Context (use to find better selectors):
{json.dumps(form_data.get('html_attributes', {}), indent=2)}

Parent Context:
{json.dumps(form_data.get('parent_context', []), indent=2)}"""

        result: CSSCriticReview | None = _call_gemini(client, model, PROMPT_STEP3B_CSS_CRITIC, prompt, CSSCriticReview, ctx=ctx, log_fn=log_fn)
        if not result:
            ctx.record("step3b_css_critic", form_index, Severity.WARNING, f"CSS critic API failed for form #{form_index} — selector not reviewed.")
            continue
            
        score = result.fragility_score
        
        if score >= 7 and result.suggested_selector:
            log_fn(f"   ↳ ⚠️ Form #{form_index}: Selector `{selector}` rejected (Fragility: {score}/10). Replaced with: `{result.suggested_selector}`")
            # Mutate the validation dictionary in place
            if tc.selector:
                tc.selector = result.suggested_selector
            elif tc.filter_value:
                tc.filter_value = result.suggested_selector
        else:
            log_fn(f"   ↳ ✅ Form #{form_index}: Selector `{selector}` is robust (Fragility: {score}/10).")

        if result.is_dynamic_element:
            log_fn(f"   ↳ 🔄 Form #{form_index}: Detected dynamic element (modal/popup). Enabling DOM Mutation Observer.")
            tc.observe_dom_changes = True
            
        # Store the critic review in the validation object for inclusion in Step 5
        validation.selector_critic = result


def _step4_plan_strategy(client, model: str, crawler_data: dict,
                          validations: list[SuccessValidation], architectures: list[VariableArchitecture],
                          existing_gtm_elements: list[dict],
                          ctx: PipelineContext, log_fn=print) -> list[GTMStrategy]:
    """Step 4: GTM Strategy Planner — one call per validated form."""
    log_fn("── Step 4: plan_gtm_strategy()           — GTM Strategy Planner... ⏳")
    strategies: list[GTMStrategy] = []
    validations_by_index = {v.form_index: v for v in validations}
    forms_by_index = {f.get("form_index"): f for f in crawler_data.get("forms_processed", [])}

    for arch in architectures:
        form_index = arch.form_index
        validation = validations_by_index.get(form_index)
        if not validation:
            continue
        form_data = forms_by_index.get(form_index, {})

        parent_context = form_data.get('parent_context', [])
        prompt = f"""Existing GTM elements (for audit & style):
{json.dumps(existing_gtm_elements, indent=2)}

Validation result:
{validation.model_dump_json(indent=2)}

Variable architecture:
{arch.model_dump_json(indent=2)}

Parent context (for proximity filter):
{json.dumps(parent_context, indent=2)}"""

        result: GTMStrategy | None = _call_gemini(client, model, PROMPT_STEP4_STRATEGY, prompt, GTMStrategy, ctx=ctx, log_fn=log_fn)
        if not result:
            ctx.record("step4_strategy", form_index, Severity.ERROR, f"Gemini API failed for form #{form_index} strategy — form will not appear in final plan.")
            continue
            
        if result.skip:
            log_fn(f"   ↳ ❌ Form #{result.form_index} SKIPPED: {result.skip_reason}")
        else:
            log_fn(f"   ↳ ✅ Tag `{result.tag_name}` / Trigger `{result.trigger_name}`")
        strategies.append(result)
    return strategies


def _step5_audit_and_compile(client, model: str,
                              strategies: list[GTMStrategy], architectures: list[VariableArchitecture],
                              validations: list[SuccessValidation], crawler_data: dict,
                              existing_gtm_elements: list[dict],
                              fallback_scripts: dict[int, FallbackScript] = None,
                              ctx: PipelineContext = None, log_fn=print) -> dict:
    """Step 5: Compiler & Auditor — builds the final TrackingPlan and self-corrects via AI loop."""
    ctx = ctx or PipelineContext()
    log_fn("── Step 5: audit_and_compile()           — Compiler & Auditor (Self-Review)... ⏳")
    compiled_plan = []
    arch_by_index = {a.form_index: a for a in architectures}
    val_by_index = {v.form_index: v for v in validations}

    for strategy in strategies:
        if strategy.skip:
            continue
        form_index = strategy.form_index
        arch = arch_by_index.get(form_index)
        validation = val_by_index.get(form_index)
        if not arch or not validation:
            continue

        # Convert TriggerCondition (Pydantic) → dict for main.py
        tc_raw = validation.trigger_condition.model_dump()
        trigger_cond = {k: v for k, v in tc_raw.items() if v is not None}

        # Map filter_key/filter_value → what main.py expects for form_submission
        if "filter_key" in trigger_cond:
            trigger_cond[trigger_cond.pop("filter_key")] = trigger_cond.pop("filter_value", "")

        item = {
            "_source_form_index": form_index,  # internal: stripped before return
            "event_name": arch.event_name or "form_submit",
            "trigger_type": validation.best_method or "form_submission",
            # ── Step 4 fields forwarded through the audit ──
            "tag_name": strategy.tag_name or f"GA4 - Event - {arch.event_name}",
            "trigger_name": strategy.trigger_name or "Trigger",
            "variables_to_create": [v.model_dump() for v in strategy.variables_to_create] if strategy.variables_to_create else [],
            "built_ins_to_activate": strategy.built_ins_to_activate or [],
            # ───────────────────────────────────────────────
            "gtm_payload_keys": [v.dl_key for v in arch.variables] if arch.variables else [],
            "payload_schema": {v.dl_key: v.var_type for v in arch.variables} if arch.variables else None,
            "semantic_mapping": {v.dl_key: v.ga4_param for v in arch.variables} if arch.variables else None,
            "trigger_condition": trigger_cond,
            "confidence": validation.method_confidence or 0.5,
            "confidence_reason": f"Step 2 signal: {validation.best_method}",
            "notes": strategy.notes,
            "orphaned_bridge": arch.orphaned_bridge,
            "proximity_filter": strategy.proximity_filter,
            "is_global_element": strategy.is_global_element,
            "requires_trigger_group": strategy.requires_trigger_group,
            # ── Phase 12+: CSS Critic & Failure Predictor ──
            "selector_critic": validation.selector_critic.model_dump() if validation.selector_critic else None,
            "failure_risks": [],
            "qa_test_steps": []
        }

        # ── PII Routing for Enhanced Conversions ──
        sm = item.get("semantic_mapping") or {}
        if isinstance(sm, dict):
            pii_fields = {}
            clean_sm = {}
            for raw_key, ga4_param in sm.items():
                if ga4_param in PII_GA4_PARAMS:
                    pii_fields[raw_key] = ga4_param
                else:
                    clean_sm[raw_key] = ga4_param
            if pii_fields:
                item["user_provided_data"] = [
                    {"key": k, "value": v} for k, v in pii_fields.items()
                ]
                item["semantic_mapping"] = clean_sm or None
                item["gtm_payload_keys"] = [
                    k for k in item.get("gtm_payload_keys", []) if k not in pii_fields
                ]
                # P1-8: Also filter PII fields from variables_to_create
                # to prevent raw PII DLVs from being created in the container
                pii_ga4_params = set(pii_fields.values())
                item["variables_to_create"] = [
                    v for v in item.get("variables_to_create", [])
                    if v.get("ga4_param") not in pii_ga4_params
                ]

        # ── Global Element Deduplicator (Layer 4 Upgrade) ──
        if item["is_global_element"]:
            is_duplicate = False
            for existing in compiled_plan:
                if existing.get("is_global_element") and existing.get("event_name") == item["event_name"] and existing.get("trigger_type") == item["trigger_type"]:
                    is_duplicate = True
                    if str(form_index) not in str(existing.get("notes", "")):
                        existing["notes"] = str(existing.get("notes", "")) + f" (Merged with form #{form_index})"
                    break
            
            if is_duplicate:
                log_fn(f"   ↳ ⏭️ Merged global element `{item['event_name']}` (Form #{form_index}) into existing global tag.")
                continue

        compiled_plan.append(item)
        
    # ── Final Collision Audit ──
    # Ensure we are not proposing a tag name that ALREADY exists in GTM exactly
    existing_names = set(e.get("name") for e in existing_gtm_elements)
    for it in compiled_plan:
        if it.get("tag_name") in existing_names:
            log_fn(f"   ↳ ⚠️ Collision detected: `{it['tag_name']}` already exists. Main.py will auto-rename, but verify if this is a duplicate.")

    # ── Inject Fallback Scripts into Output Plan ──
    if fallback_scripts:
        for form_index, script in fallback_scripts.items():
            if script.is_needed and script.js_code:
                event_name = script.suggested_event_name or "form_submit_fallback"
                compiled_plan.append({
                    "event_name": event_name,
                    "trigger_type": "custom_html",
                    "tag_name": f"Auto - GA4 Script - Fallback Interceptor ({event_name})",
                    "trigger_name": "Auto - DOM Ready (Fallback Interceptor)",
                    "variables_to_create": [],
                    "built_ins_to_activate": [],
                    "gtm_payload_keys": [],
                    "payload_schema": None,
                    "semantic_mapping": None,
                    "trigger_condition": {"event": "gtm.dom"},  # DOM Ready triggers Custom HTML
                    "confidence": 0.9,
                    "confidence_reason": "AI-generated Fallback Interceptor",
                    "notes": f"Injected JS to intercept AJAX API for form #{form_index}.",
                    "orphaned_bridge": False,
                    "proximity_filter": None,
                    "is_global_element": False,
                    "requires_trigger_group": False,
                    "custom_html_script": script.js_code
                })

        # ── Rewire original items to listen for fallback events ──
        for item in compiled_plan:
            fi = item.get("_source_form_index")
            if fi is not None and fi in fallback_scripts:
                script = fallback_scripts[fi]
                if script.is_needed and script.suggested_event_name:
                    item["trigger_type"] = "custom_event"
                    item["trigger_condition"] = {"event": script.suggested_event_name}
                    item["confidence"] = 0.7
                    item["confidence_reason"] = f"Rewired to fallback event: {script.suggested_event_name}"
                    item["notes"] = (item.get("notes") or "") + (
                        f" [Fallback active: original signal unreliable, "
                        f"now listening for '{script.suggested_event_name}' "
                        f"pushed by injected interceptor.]"
                    )

    # ── Self-Review via AI (Internal Correction Loop) ─────────────────────────────────────
    if not compiled_plan:
        print("  ⚠ No items to audit. Returning empty plan.")
        return {"tracking_plan": []}

    # ── Preserve _source_form_index before AI audit (BUG-2 fix) ──
    # Use (event_name, position_among_same_event) as composite key to handle
    # multiple forms sharing the same event_name (e.g. two "form_submit" forms).
    _fi_map: dict[tuple[str, int], object] = {}
    _event_counter: dict[str, int] = {}
    for item in compiled_plan:
        fi = item.get("_source_form_index")
        if fi is not None:
            ename = item.get("event_name", "")
            pos = _event_counter.get(ename, 0)
            _event_counter[ename] = pos + 1
            _fi_map[(ename, pos)] = fi

    # ── Phase A: Deterministic micro-fixers (cheap, no API calls) ──
    compiled_plan = _fix_regex_patterns(compiled_plan)
    compiled_plan = _fix_trigger_conditions(compiled_plan)
    compiled_plan = _fix_ecommerce_schema(compiled_plan)

    # ── Phase B: AI audit (max 2 passes: initial + 1 retry) ──
    max_retries = 1
    final_raw = None

    for attempt in range(max_retries + 1):
        if attempt > 0:
            log_fn(f"   🔄 [Loop] Auditor executing revision pass (Attempt {attempt}/{max_retries})...")
            # Re-apply deterministic fixers to AI-corrected output
            compiled_plan = _fix_regex_patterns(compiled_plan)
            compiled_plan = _fix_trigger_conditions(compiled_plan)

        review_prompt = f"""Review and correct this GTM tracking plan:
{json.dumps(compiled_plan, indent=2)}

Rules:
- trigger_condition fields must match trigger_type
- event_name must be snake_case
- confidence must be plausible for the signal quality"""

        if attempt > 0 and final_raw and final_raw.errors:
            review_prompt += f"\n\n🚨 FIX THESE SPECIFIC ERRORS FROM PREVIOUS PASS:\n" + "\n".join(f"- {e}" for e in final_raw.errors)

        final_raw = _call_gemini(client, model, PROMPT_STEP5_AUDIT, review_prompt, TrackingPlan, ctx=ctx, log_fn=log_fn)
        if not final_raw:
            log_fn("  [❌ API Error] Auditor failed to generate TrackingPlan. Returning un-audited plan.")
            ctx.record("step5_audit", None, Severity.WARNING, "Auditor API call failed — returning un-audited plan without self-correction.")
            return {"tracking_plan": compiled_plan}

        is_valid = final_raw.is_valid

        # In-place correction logic: Feed the AI's corrected models back into compiled_plan
        corrected_plans = []
        for it in final_raw.tracking_plan:
            tc = it.trigger_condition.model_dump(exclude_none=True)
            if "filter_key" in tc:
                tc[tc.pop("filter_key")] = tc.pop("filter_value", "")

            sm_raw = it.semantic_mapping
            sm = {kv.key: kv.value for kv in sm_raw} if isinstance(sm_raw, list) else (sm_raw or {})
            ps_raw = it.payload_schema
            ps = {kv.key: kv.value for kv in ps_raw} if isinstance(ps_raw, list) else (ps_raw or {})

            corrected_plans.append({
                "event_name": it.event_name,
                "trigger_type": it.trigger_type,
                "tag_name": it.tag_name,
                "trigger_name": it.trigger_name,
                "variables_to_create": [v.model_dump() for v in it.variables_to_create] if it.variables_to_create else [],
                "built_ins_to_activate": it.built_ins_to_activate,
                "gtm_payload_keys": it.gtm_payload_keys,
                "payload_schema": ps or None,
                "semantic_mapping": sm or None,
                "trigger_condition": tc,
                "confidence": it.confidence,
                "confidence_reason": it.confidence_reason,
                "notes": it.notes,
                "orphaned_bridge": it.orphaned_bridge,
                "proximity_filter": it.proximity_filter,
                "is_global_element": getattr(it, "is_global_element", False),
                "requires_trigger_group": getattr(it, "requires_trigger_group", False),
                "custom_html_script": getattr(it, "custom_html_script", None),
                "selector_critic": it.selector_critic.model_dump() if it.selector_critic else None,
                "failure_risks": it.failure_risks,
                "qa_test_steps": it.qa_test_steps,
                "is_shadow_form": False,        # AI audit doesn't preserve per-form metadata
                "is_iframe_embedded": False,     # Same — use pre-audit values for display
                "_source_form_index": None,  # Restored below from pre-audit map; stripped before return
            })

        # ── Restore _source_form_index from pre-audit map (BUG-2 fix) ──
        _restore_counter: dict[str, int] = {}
        for item in corrected_plans:
            ename = item.get("event_name", "")
            pos = _restore_counter.get(ename, 0)
            _restore_counter[ename] = pos + 1
            if item.get("_source_form_index") is None:
                item["_source_form_index"] = _fi_map.get((ename, pos))

        # ── Guard against AI item reduction (P0-4) ──
        if len(corrected_plans) < len(compiled_plan):
            corrected_events = {it["event_name"] for it in corrected_plans}
            dropped = [it for it in compiled_plan if it["event_name"] not in corrected_events]

            for d in dropped:
                d["_auditor_would_remove"] = True
                d["_removal_note"] = "AI auditor flagged for removal — review manually"

            compiled_plan = corrected_plans + dropped  # Keep all, but flag dropped items

            log_fn(f"   ↳ ⚠️ Auditor flagged {len(dropped)} item(s) for removal: "
                   f"{[d['event_name'] for d in dropped]}")
            ctx.record("step5_audit", None, Severity.WARNING,
                       f"Auditor flagged {len(dropped)} items for removal: "
                       f"{[d['event_name'] for d in dropped]}. Items preserved but flagged for review.")
        else:
            compiled_plan = corrected_plans

        if is_valid:
            if attempt > 0:
                log_fn("   ↳ ✅ Auditor successfully corrected the plan!")
            break
        else:
            log_fn(f"   ↳ ⚠️ Auditor flagged {len(final_raw.errors)} error(s).")
            for err in final_raw.errors[:2]:
                log_fn(f"      - {err}")
            if len(final_raw.errors) > 2:
                log_fn(f"      - ...and {len(final_raw.errors) - 2} more.")

    # ── Compilation & Final Output ──────────────────────────────────────────────────────────
    health_score = final_raw.health_score if final_raw else 100
    health_suggestions = final_raw.health_suggestions if final_raw else []
    
    log_fn(f"   ↳ 🏥 Container Health: {health_score}/100")
    if health_suggestions:
        for suggestion in health_suggestions[:2]:
            log_fn(f"      - {suggestion}")

    final = {
        "is_valid": final_raw.is_valid if final_raw else True,
        "errors": final_raw.errors if final_raw else [],
        "health_score": health_score,
        "health_suggestions": health_suggestions,
        "qa_test_plan": final_raw.qa_test_plan if final_raw else None,
        "tracking_plan": compiled_plan
    }
    
    for plain in compiled_plan:
        c = plain["confidence"]
        flag = "✅" if c >= 0.7 else "⚠️ LOW"
        log_fn(f"   ↳ {flag} [{c:.0%}] `{plain['event_name']}` via `{plain['trigger_type']}` — {plain['confidence_reason']}")

    # ── Strip internal tracking fields before returning ──
    for item in final.get("tracking_plan", []):
        item.pop("_source_form_index", None)

    return final


def _step6_generate_sop(client, model: str, final_plan: dict,
                         ctx: PipelineContext, log_fn=print) -> str:
    """Step 6: Auto-Documentation (SOP) Generator."""
    log_fn("── Step 6: generate_tracking_spec()      — Auto-Documentation... ⏳")
    prompt = f"Tracking Plan to document:\n{json.dumps(final_plan, indent=2)}"

    result = _call_gemini(client, model, PROMPT_STEP6_SOP, prompt, SOPDocument, ctx=ctx, log_fn=log_fn)
    if result and result.markdown_content:
        log_fn("   ↳ ✅ SOP Generated: tracking_spec.md")
        return result.markdown_content
    ctx.record("step6_sop", None, Severity.WARNING, "SOP generation failed — documentation will not be included.")
    log_fn("   ↳ ❌ Failed to generate SOP.")
    return ""



# ═══════════════════════════════════════════════════════════════
# §5 — PUBLIC ENTRY POINT
# ═══════════════════════════════════════════════════════════════

# ── Heuristic fallback for method scoring (when API quota exhausted) ──

def _heuristic_method_scores(
    methods: list[dict],
    field_count: int,
    platform: str = "",
    technology_signals: list[str] | None = None,
) -> list[dict]:
    """Fallback resilience scoring when AI API call fails (e.g. 429 quota)."""
    HEURISTIC = {
        "custom_event":       {"base": 0.90, "fp": "low",    "cx": "medium",
                               "data_cap": True,
                               "builtins": ["Page URL", "Page Path"]},
        "ajax_complete":      {"base": 0.75, "fp": "low",    "cx": "complex",
                               "data_cap": True,
                               "builtins": ["Page URL", "Page Path"]},
        "element_visibility": {"base": 0.70, "fp": "medium", "cx": "simple",
                               "data_cap": False,
                               "builtins": ["Page URL", "Page Path"]},
        "page_view":          {"base": 0.65, "fp": "low",    "cx": "simple",
                               "data_cap": False,
                               "builtins": ["Page URL", "Page Path", "Page Hostname", "Referrer"]},
        "form_submission":    {"base": 0.50, "fp": "medium", "cx": "simple",
                               "data_cap": False,
                               "builtins": ["Form Element", "Form Classes", "Form ID", "Form URL"]},
        "click":              {"base": 0.60, "fp": "medium", "cx": "simple",
                               "data_cap": False,
                               "builtins": ["Click Element", "Click Classes", "Click ID", "Click Text"]},
        "dom_ready":          {"base": 0.40, "fp": "low",    "cx": "complex",
                               "data_cap": False,
                               "builtins": ["Page URL", "Page Path"]},
    }
    scored = []
    for m in methods:
        method_name = m.get("method", "unknown")
        h = HEURISTIC.get(method_name, {"base": 0.50, "fp": "medium", "cx": "medium",
                                         "data_cap": False, "builtins": []})
        score = h["base"]
        # EV bonus for simple forms (newsletter), penalty for complex forms
        if method_name == "element_visibility":
            if field_count <= 2:
                score = min(1.0, score + 0.15)   # Newsletter bonus
            elif field_count >= 4:
                score = max(0.0, score - 0.20)    # Multi-field false positive penalty
        # Form submission penalty for AJAX forms
        if method_name == "form_submission" and field_count <= 1:
            score = max(0.0, score - 0.10)

        # Recipe boost: known listener available → custom_event is guaranteed
        if method_name == "custom_event" and (platform or technology_signals):
            from recipes import get_recipe_for_platform
            if get_recipe_for_platform(platform, technology_signals):
                score = min(1.0, score + 0.05)

        scored.append({
            "method": method_name,
            "resilience_score": round(score, 2),
            "resilience_reasoning": f"Heuristic score (API unavailable). "
                                    f"Base={h['base']}, fields={field_count}.",
            "false_positive_risk": h["fp"],
            "gtm_complexity": h["cx"],
            "data_capture_ability": h["data_cap"],
            "recommended_built_ins": h["builtins"],
            "trigger_condition": m.get("trigger_condition", {}),
        })
    scored.sort(key=lambda x: x["resilience_score"], reverse=True)
    return scored


def score_tracking_methods(
    form_data: dict,
    available_methods: list[dict],
    form_classification: dict | None = None,
    model: str = "gemini-2.5-flash",
    log_callback=None,
) -> list[dict]:
    """
    AI evaluates resilience score for each tracking method for a specific form.
    Called AFTER successful test submit, BEFORE Signal Review UI.
    Returns list of MethodScore dicts sorted by resilience_score DESC.
    Falls back to heuristic scoring if API call fails.
    """
    log_fn = log_callback if callable(log_callback) else print

    _platform = (form_classification or {}).get("platform", form_data.get("platform", ""))
    _tech_sig = form_data.get("technology_signals", [])

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        log_fn("   ↳ ⚠️ No API key — using heuristic scoring.")
        field_count = len(form_data.get("dom_payload_keys", []))
        return _heuristic_method_scores(available_methods, field_count, _platform, _tech_sig)

    client = genai.Client(api_key=api_key)
    ctx = PipelineContext()

    # Build context for AI: form info + all available methods
    form_context = {
        "form_index": form_data.get("form_index"),
        "form_title": form_data.get("form_title", "Unknown"),
        "form_type": (form_classification or {}).get("form_type", "unknown"),
        "form_role": (form_classification or {}).get("form_role", "unknown"),
        "field_count": len(form_data.get("dom_payload_keys", [])),
        "dom_payload_keys": form_data.get("dom_payload_keys", []),
        "is_successful_submission": form_data.get("is_successful_submission", False),
        "success_element_selector": form_data.get("success_element_selector"),
        "datalayer_events": form_data.get("datalayer_events", []),
        "redirect_url": form_data.get("redirect_url"),
        "is_ajax_submission": form_data.get("is_ajax_submission", False),
        "available_tracking_methods": available_methods,
    }

    # Add recipe context if available
    recipe_note = ""
    from recipes import get_recipe_for_platform
    _recipe_match = get_recipe_for_platform(_platform, _tech_sig)
    if _recipe_match:
        _rkey, _recipe = _recipe_match
        recipe_note = (
            f"\nKNOWN RECIPE: '{_recipe['name']}' listener is available as a full "
            f"template. DL event: '{_recipe['plan_item']['trigger_condition']['event']}'. "
            f"custom_event is guaranteed for this platform."
        )

    prompt = (
        f"Evaluate resilience of each tracking method for this form "
        f"(data from REAL test submission):\n{json.dumps(form_context, indent=2, default=str)}"
        f"{recipe_note}"
    )

    log_fn(f"   ↳ 🎯 Scoring {len(available_methods)} method(s) for Form #{form_data.get('form_index')}...")

    result = _call_gemini(
        client, model, PROMPT_SCORE_METHODS, prompt,
        schema=list[MethodScore], ctx=ctx, log_fn=log_fn,
    )

    if not result:
        log_fn("   ↳ ⚠️ AI scoring failed — using heuristic fallback.")
        field_count = len(form_data.get("dom_payload_keys", []))
        return _heuristic_method_scores(available_methods, field_count, _platform, _tech_sig)

    # Convert Pydantic models to dicts, sort by resilience_score DESC
    scored = [ms.model_dump() if hasattr(ms, "model_dump") else ms for ms in result]
    scored.sort(key=lambda x: x.get("resilience_score", 0), reverse=True)

    for ms in scored:
        score = ms.get("resilience_score", 0)
        method = ms.get("method", "?")
        fp = ms.get("false_positive_risk", "?")
        log_fn(f"      {score:.0%} — {method} (FP risk: {fp})")

    return scored


def classify_forms(
    discovery_data: dict,
    model: str = "gemini-2.5-flash",
    log_callback=None,
) -> dict:
    """
    Phase 1 classification: Runs Step 1 (platform + form type) on passive discovery data.

    Returns dict mapping form_index → {"form_type": str, "form_role": str, "confidence": float, ...}.
    """
    log_fn = log_callback if callable(log_callback) else print

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is missing.")

    client = genai.Client(api_key=api_key)

    # Build a crawler_data-compatible dict from discovery data
    # This adapts forms_discovered → forms_processed for Step 1 compatibility
    # P2-9: Add explicit defaults for fields that Step 1 might reference
    crawler_compat = {
        "url": discovery_data.get("url", ""),
        "page_path": discovery_data.get("page_path", "/"),
        "platform": discovery_data.get("platform", "unknown"),
        "forms_processed": [
            {
                **f,
                "form_index": (int(f.get("form_index", i)) if str(f.get("form_index", i)).isdigit() else i),  # P1-2: safe int normalize
                "datalayer_events": f.get("datalayer_events", []),
                "is_successful_submission": f.get("is_successful_submission", False),
                "dom_payload_keys": f.get("dom_payload_keys", []),
                "available_tracking_methods": f.get("available_tracking_methods", []),
            }
            for i, f in enumerate(discovery_data.get("forms_discovered", []))
        ],
        "datalayer_events": discovery_data.get("data_layer_events", []),
        "has_phone_links": discovery_data.get("has_phone_links", False),
        "has_email_links": discovery_data.get("has_email_links", False),
    }

    ctx = PipelineContext()
    platform_result = _step1_analyze_platform(client, model, crawler_compat, ctx=ctx, log_fn=log_fn)

    # ── Quota-aware error: raise with a clear message so the UI can display it ──
    if ctx.has_fatal():
        all_exhausted = any(
            "All fallback models exhausted" in e.message
            for e in ctx.events if e.step == "api_call"
        )
        if all_exhausted:
            raise RuntimeError(
                "Дневният лимит на безплатния план е изчерпан за всички модели. "
                "Опитайте отново утре или преминете на платен план. "
                f"Опитани модели: {', '.join(FREE_FALLBACK_MODELS)}"
            )
        raise RuntimeError("Класификацията на формите се провали. Проверете API ключа и квотата.")

    return {
        str(f.form_index): {  # String keys — app.py accesses via str(fi)
            "form_type": f.form_type,
            "form_role": f.form_role,
            "confidence": f.platform_confidence,
            "platform": f.platform,
            "technology_signals": f.technology_signals,
            "is_shadow_form": f.is_shadow_form,
            "contains_pii": f.contains_pii,
            "is_iframe_embedded": f.is_iframe_embedded,
        }
        for f in platform_result.forms
    }


def generate_tracking_plan(
    crawler_data: dict,
    model: str = "gemini-2.5-flash",  # Starts with smarter model, falls back to Lite on quota limit
    gtm_data: dict | None = None,
    log_callback=None,
    include_shadow_forms: bool = True,
    include_iframes: bool = True,
    session_id: str | None = None,
) -> dict:
    """
    Orchestrates the 5-step expert pipeline to generate a GTM tracking plan.
    This is the only public function called by app.py and main.py.

    log_callback: optional callable(str) for streaming step progress to Streamlit UI.
                  Defaults to print() for CLI / debug usage.
    session_id: optional session ID for grouping debug files + pipeline.log.
    """
    import time as _time
    log_fn = log_callback if callable(log_callback) else print

    # ── Per-session pipeline.log: tee every log_fn call to a file ──
    # Skip if caller already tees to pipeline.log (e.g. core_pipeline._TeeListener)
    if not getattr(log_fn, '_tees_to_pipeline_log', False):
        _debug_dir = get_debug_dir(session_id)
        os.makedirs(_debug_dir, exist_ok=True)
        _log_path = os.path.join(_debug_dir, "pipeline.log")
        _original_log = log_fn
        def log_fn(msg: str):
            _original_log(msg)
            try:
                with open(_log_path, "a", encoding="utf-8") as _f:
                    _f.write(f"[{_time.strftime('%H:%M:%S')}] {msg}\n")
            except Exception:
                pass  # never fail the pipeline over logging

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is missing.")

    client = genai.Client(api_key=api_key)

    # ── Extract existing container metadata ───────────────────
    existing_elements: list[dict] = []
    if gtm_data:
        if "ai_context" in gtm_data:
            existing_elements = gtm_data["ai_context"]
        else:
            cv = gtm_data.get("containerVersion", gtm_data)
            for tag in cv.get("tag", []):
                el = {"name": tag.get("name"), "type": "tag"}
                for p in tag.get("parameter", []):
                    if p.get("key") == "eventName" and p.get("value"):
                        el["event_name"] = p["value"]
                existing_elements.append(el)
            for trig in cv.get("trigger", []):
                existing_elements.append({
                    "name": trig.get("name"),
                    "type": "trigger",
                    "trigger_type": trig.get("type")
                })
    if existing_elements:
        log_fn(f"[Context] {len(existing_elements)} existing GTM element(s) loaded for audit & style.")

    ctx = PipelineContext()

    try:
        # ── Step 1 ──────────────────────────────────────────────
        cached_step1 = crawler_data.pop("_cached_step1", None)
        if cached_step1:
            # Reconstruct PlatformAnalysis from Discovery-phase classification
            log_fn("── Step 1: analyze_platform_and_forms()  — ♻️ Reusing Discovery cache (0 API calls)")
            cached_forms = []
            for fi_str, info in cached_step1.items():
                cached_forms.append(FormAnalysis(
                    form_index=fi_str,
                    platform=info.get("platform", "unknown"),
                    platform_confidence=info.get("confidence", 0.5),
                    form_type=info.get("form_type", "unknown"),
                    form_role=info.get("form_role", "lead form"),
                    technology_signals=info.get("technology_signals", []),
                    contains_pii=info.get("contains_pii", False),
                    is_shadow_form=info.get("is_shadow_form", False),
                    is_iframe_embedded=info.get("is_iframe_embedded", False),
                ))
            # Determine overall platform from first form or crawler_data
            overall_platform = cached_forms[0].platform if cached_forms else "unknown"
            overall_conf = max((f.platform_confidence for f in cached_forms), default=0.0)
            platform_result = PlatformAnalysis(
                platform=overall_platform,
                platform_confidence=overall_conf,
                forms=cached_forms,
            )
            log_fn(f"   ↳ ♻️ Cached: platform={overall_platform} ({overall_conf:.0%}), {len(cached_forms)} form(s)")
        else:
            platform_result = _step1_analyze_platform(client, model, crawler_data, ctx=ctx, log_fn=log_fn)
        _save_debug("step1_platform", platform_result, session_id=session_id)

        # P1-4: Short-circuit if Step 1 failed (prevents silent empty plans)
        if ctx.has_fatal():
            log_fn("   ↳ ❌ Platform analysis failed. Cannot generate tracking plan.")
            # Detect quota exhaustion vs. other errors for a clearer message
            all_exhausted = any(
                "All fallback models exhausted" in e.message
                for e in ctx.events if e.step == "api_call"
            )
            if all_exhausted:
                error_msg = (
                    "Дневният лимит на безплатния план е изчерпан за всички модели. "
                    "Опитайте отново утре или преминете на платен план. "
                    f"Опитани модели: {', '.join(FREE_FALLBACK_MODELS)}"
                )
            else:
                error_msg = "Step 1 (Platform Analysis) failed. Check API key and quota."
            return {
                "tracking_plan": [],
                "pipeline_warnings": ctx.to_summary(),
                "error": error_msg,
                "api_call_count": ctx.api_call_count,
            }

        # ── Form Discovery Filtering ───────────────────────────
        if not include_shadow_forms or not include_iframes:
            original_count = len(crawler_data.get("forms_processed", []))
            crawler_data["forms_processed"] = [
                f for f in crawler_data.get("forms_processed", [])
                if (include_shadow_forms or not f.get("is_shadow_form")) and
                   (include_iframes or not f.get("is_iframe_embedded"))
            ]
            new_count = len(crawler_data["forms_processed"])
            if new_count < original_count:
                log_fn(f"   ↳ ✂️ Filtered out {original_count - new_count} form(s) based on Discovery settings.")

        # ── Step 2 ──────────────────────────────────────────────
        validations = _step2_validate_signals(client, model, crawler_data, platform_result, ctx=ctx, log_fn=log_fn)
        _save_debug("step2_validations", validations, session_id=session_id)
        if not validations:
            log_fn("⚠ No valid form submissions found. Returning empty plan.")
            return {"tracking_plan": [], "pipeline_warnings": ctx.to_summary(), "api_call_count": ctx.api_call_count}

        # ── Step 2B: Fallback ───────────────────────────────────
        fallback_scripts = _step2b_fallback_injector(client, model, crawler_data, validations, ctx=ctx, log_fn=log_fn)

        # ── Step 3 ──────────────────────────────────────────────
        architectures = _step3_architect_variables(client, model, crawler_data, platform_result, validations, ctx=ctx, existing_elements=existing_elements, log_fn=log_fn)
        _save_debug("step3_variables", architectures, session_id=session_id)

        # ── Step 3B: CSS Critic ─────────────────────────────────
        _step3b_css_critic(client, model, validations, crawler_data, ctx=ctx, log_fn=log_fn)
        _save_debug("step3b_validations_after_critic", validations, session_id=session_id)

        # ── Step 4 ──────────────────────────────────────────────
        strategies = _step4_plan_strategy(
            client, model, crawler_data, validations, architectures,
            existing_elements, ctx=ctx, log_fn=log_fn
        )
        _save_debug("step4_strategies", strategies, session_id=session_id)

        # ── Step 5 (Internal Self-Correction Loop) ──────────────────────────────────────────────
        final = _step5_audit_and_compile(
            client, model, strategies, architectures, validations,
            crawler_data, existing_elements, fallback_scripts, ctx=ctx, log_fn=log_fn
        )
        _save_debug("step5_final_plan", final, session_id=session_id)

        # ── Step 6 (Auto-Documentation) ─────────────────────────────────────────────────────────
        sop_md = _step6_generate_sop(client, model, final, ctx=ctx, log_fn=log_fn)
        if sop_md:
            with open("tracking_spec.md", "w", encoding="utf-8") as f:
                f.write(sop_md)
            final["tracking_spec_md"] = sop_md

        # ── Attach pipeline diagnostics ─────────────────────────
        final["pipeline_warnings"] = ctx.to_summary()
        final["api_call_count"] = ctx.api_call_count
        return final

    except Exception as e:
        log_fn(f"❌ Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        ctx.record("pipeline", None, Severity.FATAL, str(e))
        return {"tracking_plan": [], "pipeline_warnings": ctx.to_summary(), "api_call_count": ctx.api_call_count}




# ═══════════════════════════════════════════════════════════════
# §6 — LEGACY COMPAT: detect_platform still exported for crawler.py
# ═══════════════════════════════════════════════════════════════

def detect_platform(page_html: str, url: str) -> str:
    """Fast heuristic for initial platform guessing in the crawler (pre-AI step)."""
    html = page_html.lower() if page_html else ""
    u = url.lower()
    if "woocommerce" in html or "wc-cart" in html:   return "woocommerce"
    if "wpcf7" in html or "wp-content" in html:      return "wordpress_cf7"
    if "shopify" in u or "cdn.shopify.com" in html:  return "shopify"
    if "hubspot" in html or "hs-form" in html:       return "hubspot"
    if "wordpress" in html:                          return "wordpress"
    return "unknown"


if __name__ == "__main__":
    dummy = {
        "url": "https://example.com",
        "platform": "wordpress_cf7",
        "forms_processed": [{
            "form_index": 0,
            "is_successful_submission": True,
            "datalayer_diff": {"added_keys": ["cf7_form_id", "your-email"]},
            "datalayer_events": [{"event": "wpcf7mailsent", "cf7_form_id": "42", "your-email": "test@test.com"}],
            "payload_schema": {"cf7_form_id": "string", "your-email": "string"},
            "field_labels": {"your-email": "Email Address", "your-name": "Full Name"},
            "dom_payload_keys": ["your-name", "your-email", "your-message"],
            "available_tracking_methods": [{"method": "custom_event", "priority": 1,
                                            "trigger_condition": {"event": "wpcf7mailsent", "cf7_form_id": "42"}}],
            "success_element_selector": ".wpcf7-mail-sent-ok",
            "success_message_text": "Thank you for your message!",
            "redirect_is_dynamic": False,
            "parent_context": [{"tag": "div", "id": "contact-section", "classes": "section-hero"}]
        }],
        "has_phone_links": True,
        "has_email_links": False,
    }
    if os.environ.get("GEMINI_API_KEY"):
        print(json.dumps(generate_tracking_plan(dummy), indent=2))
    else:
        print("Set GEMINI_API_KEY to test.")

"""
models.py — Pydantic schemas enforcing strict data boundaries.

CrawlerOutput validates the dict returned by crawler.crawl_site() at the
boundary between the crawler and the AI pipeline, catching missing fields,
type mismatches, and structural errors before expensive LLM tokens are spent.
"""
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator
from typing import Any


# ── Crawler Sub-Models ──────────────────────────────────────────────────────

class ParentContext(BaseModel):
    tag: str
    id: str = ""
    classes: str = ""


class SurroundingContext(BaseModel):
    tag: str
    text: str = ""


class TrackingMethod(BaseModel):
    method: str
    priority: int
    reason: str = ""
    trigger_condition: dict = Field(default_factory=dict)
    payload_keys: list[str] = Field(default_factory=list)


class DetectedIframe(BaseModel):
    src: str = ""
    is_cross_origin: bool = False
    netloc: str = ""


class CrawledForm(BaseModel):
    """Validated schema for a single processed form from the crawler."""

    form_index: int | str
    html_attributes: dict = Field(default_factory=dict)
    cf7_form_id: str | None = None
    page_path: str = "/"
    redirect_url: str | None = None
    is_ajax_submission: bool = False
    form_submitted: bool = False

    field_labels: dict[str, str] = Field(default_factory=dict)
    parent_context: list[ParentContext] = Field(default_factory=list)
    form_title: str | None = None
    surrounding_context: list[SurroundingContext] = Field(default_factory=list)
    dom_payload_keys: list[str] = Field(default_factory=list)

    form_id: str | None = None
    form_classes: str | None = None
    form_action: str | None = None

    datalayer_events: list = Field(default_factory=list)
    datalayer_diff: dict = Field(default_factory=lambda: {"added_keys": []})
    payload_schema: dict[str, str] = Field(default_factory=dict)

    redirect_is_dynamic: bool = False
    success_element_selector: str | None = None
    success_message_text: str | None = None
    is_successful_submission: bool = False
    is_spa_unmounted: bool = False
    has_successful_ajax: bool = False
    ajax_endpoint: str | None = None

    available_tracking_methods: list[TrackingMethod] = Field(default_factory=list)
    detected_iframes: list[DetectedIframe] = Field(default_factory=list)
    is_shadow_form: bool = False
    shadow_trigger_hint: str | None = None

    @field_validator("form_index", mode="before")
    @classmethod
    def coerce_form_index(cls, v):
        """Accept both int and string form indices (shadow forms use 'shadow_0')."""
        if isinstance(v, str) and v.startswith("shadow_"):
            return v
        try:
            return int(v)
        except (ValueError, TypeError):
            return v


class CrawlerOutput(BaseModel):
    """
    Top-level validated output from crawl_site().

    Enforced at the boundary between crawler.py and the orchestrator/brain.py.
    Any field missing or of wrong type raises ValidationError immediately,
    preventing corrupt data from reaching the AI pipeline.
    """

    url: str
    page_path: str = "/"
    platform: str = "unknown"
    forms_processed: list[CrawledForm] = Field(default_factory=list)
    datalayer_events: list = Field(default_factory=list)
    gtag_events: list = Field(default_factory=list)
    has_phone_links: bool = False
    has_email_links: bool = False

    @field_validator("platform", mode="before")
    @classmethod
    def normalize_platform(cls, v):
        if not v or not isinstance(v, str):
            return "unknown"
        return v.lower().strip()


# ── Discovery Phase Models ─────────────────────────────────────────────────

class DiscoveredFormField(BaseModel):
    """Schema for a single field within a discovered form."""
    name: str = ""
    type: str = ""
    tag: str = ""
    id: str = ""
    label_text: str = ""
    placeholder: str = ""
    required: bool = False
    is_hidden: bool = False
    is_consent: bool = False


class DiscoveredFormButton(BaseModel):
    """Schema for a button within a discovered form."""
    text: str = ""
    type: str = ""
    tag: str = ""
    button_class: str = Field("", alias="class")

    model_config = {"populate_by_name": True}


class DiscoveredForm(BaseModel):
    """Schema for a passively discovered form (no interaction data)."""

    form_index: int | str
    html_attributes: dict = Field(default_factory=dict)
    field_labels: dict[str, str] = Field(default_factory=dict)
    parent_context: list[ParentContext] = Field(default_factory=list)
    form_title: str | None = None
    surrounding_context: list[SurroundingContext] = Field(default_factory=list)
    cf7_form_id: str | None = None
    form_hash: str = ""
    fields: list[DiscoveredFormField] = Field(default_factory=list)
    buttons: list[DiscoveredFormButton] = Field(default_factory=list)
    is_shadow_form: bool = False
    form_id: str | None = None
    form_classes: str | None = None
    form_action: str | None = None
    page_path: str = "/"
    position_on_page: str | None = None

    @field_validator("form_index", mode="before")
    @classmethod
    def coerce_form_index(cls, v):
        """Accept both int and string form indices (shadow forms use 'shadow_0')."""
        if isinstance(v, str) and v.startswith("shadow_"):
            return v
        try:
            return int(v)
        except (ValueError, TypeError):
            return v


class DiscoveryOutput(BaseModel):
    """Top-level output from discover_forms()."""

    url: str
    page_path: str = "/"
    platform: str = "unknown"
    forms_discovered: list[DiscoveredForm] = Field(default_factory=list)
    detected_iframes: list[DetectedIframe] = Field(default_factory=list)
    has_phone_links: bool = False
    has_email_links: bool = False
    has_contact_links: bool = False
    data_layer_events: list = Field(default_factory=list)

    @field_validator("platform", mode="before")
    @classmethod
    def normalize_platform(cls, v):
        if not v or not isinstance(v, str):
            return "unknown"
        return v.lower().strip()

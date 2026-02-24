"""
GTM Recipe Library — Full platform-specific tracking templates.

When a platform is detected in Step 1, the matching recipe is offered to the
user in Signal Review UI (checkbox, checked by default).  If approved, the
recipe provides the complete tracking solution (listener + trigger + GA4 tag +
variables) and AI Steps 2-5 are skipped for that form.

Adding a new recipe = adding a new entry to RECIPES.  No changes needed in
app.py, core_pipeline.py, or brain.py.
"""

from __future__ import annotations

import copy
import json
from typing import Optional

# ── Pydantic import (for GtmTag validation, same as main.py) ──────────────
from pydantic import ValidationError


# ─────────────────────────────────────────────────────────────────────────────
# Recipe Library
# ─────────────────────────────────────────────────────────────────────────────

# ── Form-type → GA4 event name mapping ────────────────────────────────────
FORM_TYPE_EVENT_MAP: dict[str, str] = {
    "newsletter":   "sign_up_newsletter",
    "contact_form": "generate_lead",
    "lead":         "generate_lead",
    "unknown":      "form_submission",
}

RECIPES: dict[str, dict] = {

    # ── Contact Form 7 (WordPress) ──────────────────────────────────────────
    "wordpress_cf7": {
        "name": "Contact Form 7",
        "platform_signals": ["wpcf7"],
        "listener": {
            "tag_name": "Auto - CF7 Form Listener",
            "script": (
                '<script>\n'
                'document.addEventListener("wpcf7mailsent", function(event) {\n'
                '  window.dataLayer = window.dataLayer || [];\n'
                '  window.dataLayer.push({\n'
                '    "event": "cf7submission",\n'
                '    "contactFormId": event.detail.contactFormId,\n'
                '    "response": event.detail.inputs\n'
                '  });\n'
                '});\n'
                '</script>'
            ),
        },
        "plan_item": {
            "event_name": "form_submission",
            "trigger_type": "custom_event",
            "tag_name": "Auto - GA4 Event - form_submission",
            "trigger_name": "Auto - CE - cf7submission",
            "trigger_condition": {"event": "cf7submission"},
            "gtm_payload_keys": ["contactFormId"],
            "payload_schema": {"contactFormId": "string"},
            "semantic_mapping": {"contactFormId": "form_id"},
            "built_ins_to_activate": ["Page URL", "Page Path"],
            "variables_to_create": [],
            "confidence": 1.0,
            "confidence_reason": "Known CF7 recipe \u2014 production-tested GTM template.",
            "failure_risks": [],
            "qa_test_steps": [
                "Submit CF7 form \u2192 verify cf7submission event in dataLayer",
                "Check GA4 tag fires with form_id parameter",
            ],
        },
        "dl_variables": [
            {"name": "DLV - CF7 contactFormId", "dl_key": "contactFormId"},
        ],
    },

    # ── jQuery AJAX Listener (Bounteous / LunaMetrics) ─────────────────────
    "ajax_complete": {
        "name": "jQuery AJAX Listener",
        "platform_signals": ["jquery", "ajaxComplete"],
        "listener": {
            "tag_name": "Auto - AJAX Auto-Event Listener",
            "script": (
                '<script id="gtm-jq-ajax-listen" type="text/javascript">\n'
                '(function() {\n'
                "  'use strict';\n"
                '  var $;\n'
                '  var n = 0;\n'
                '  init();\n'
                '\n'
                '  function init(n) {\n'
                "    if (typeof jQuery !== 'undefined') {\n"
                '      $ = jQuery;\n'
                '      bindToAjax();\n'
                '    } else if (n < 20) {\n'
                '      n++;\n'
                '      setTimeout(init, 500);\n'
                '    }\n'
                '  }\n'
                '\n'
                '  function bindToAjax() {\n'
                "    $(document).bind('ajaxComplete', function(evt, jqXhr, opts) {\n"
                "      var fullUrl = document.createElement('a');\n"
                '      fullUrl.href = opts.url;\n'
                "      var pathname = fullUrl.pathname[0] === '/' ? fullUrl.pathname : '/' + fullUrl.pathname;\n"
                "      var queryString = fullUrl.search[0] === '?' ? fullUrl.search.slice(1) : fullUrl.search;\n"
                "      var queryParameters = objMap(queryString, '&', '=', true);\n"
                "      var headers = objMap(jqXhr.getAllResponseHeaders(), '\\\\n', ':');\n"
                '      dataLayer.push({\n'
                "        'event': 'ajaxComplete',\n"
                "        'attributes': {\n"
                "          'type': opts.type || '',\n"
                "          'url': fullUrl.href || '',\n"
                "          'queryParameters': queryParameters,\n"
                "          'pathname': pathname || '',\n"
                "          'hostname': fullUrl.hostname || '',\n"
                "          'protocol': fullUrl.protocol || '',\n"
                "          'fragment': fullUrl.hash || '',\n"
                "          'statusCode': jqXhr.status || '',\n"
                "          'statusText': jqXhr.statusText || '',\n"
                "          'headers': headers,\n"
                "          'timestamp': evt.timeStamp || '',\n"
                "          'contentType': opts.contentType || '',\n"
                "          'response': (jqXhr.responseJSON || jqXhr.responseXML || jqXhr.responseText || '')\n"
                '        }\n'
                '      });\n'
                '    });\n'
                '  }\n'
                '\n'
                '  function objMap(data, delim, spl, decode) {\n'
                '    var obj = {};\n'
                '    if (!data || !delim || !spl) { return {}; }\n'
                '    var arr = data.split(delim);\n'
                '    for (var i = 0; i < arr.length; i++) {\n'
                '        var item = decode ? decodeURIComponent(arr[i]) : arr[i];\n'
                '        var pair = item.split(spl);\n'
                '        var key = trim_(pair[0]);\n'
                '        var value = trim_(pair[1]);\n'
                '        if (key && value) { obj[key] = value; }\n'
                '    }\n'
                '    return obj;\n'
                '  }\n'
                '\n'
                '  function trim_(str) {\n'
                "    if (str) { return str.replace(/^[\\\\s\\\\uFEFF\\\\xA0]+|[\\\\s\\\\uFEFF\\\\xA0]+$/g, ''); }\n"
                '  }\n'
                '})();\n'
                '/*\n'
                ' * Based on the jQuery AJAX Listener by Bounteous (formerly LunaMetrics)\n'
                ' * Licensed under Creative Commons 4.0 Attribution Public License\n'
                ' */\n'
                '</script>'
            ),
            "extra_params": [
                {"type": "BOOLEAN", "key": "supportDocumentWrite", "value": "false"},
            ],
        },
        "plan_item": {
            "event_name": "form_submission",
            "trigger_type": "custom_event",
            "tag_name": "Auto - GA4 Event - form_submission",
            "trigger_name": "Auto - CE - ajaxComplete",
            "trigger_condition": {"event": "ajaxComplete"},
            "gtm_payload_keys": [],
            "payload_schema": {},
            "semantic_mapping": {},
            "built_ins_to_activate": ["Page URL", "Page Path"],
            "variables_to_create": [],
            "confidence": 0.95,
            "confidence_reason": "Known AJAX listener recipe \u2014 Bounteous/LunaMetrics production-tested GTM template.",
            "failure_risks": [
                "Requires jQuery on the page. Will not fire if site uses vanilla JS AJAX.",
            ],
            "qa_test_steps": [
                "Submit form \u2192 verify ajaxComplete event in dataLayer",
                "Check attributes.statusCode == 200 and attributes.url matches the form endpoint",
                "Verify GA4 tag fires with correct parameters",
            ],
        },
        "dl_variables": [
            {"name": "DLV - attributes.type",            "dl_key": "attributes.type"},
            {"name": "DLV - attributes.url",             "dl_key": "attributes.url"},
            {"name": "DLV - attributes.queryParameters", "dl_key": "attributes.queryParameters"},
            {"name": "DLV - attributes.pathname",        "dl_key": "attributes.pathname"},
            {"name": "DLV - attributes.hostname",        "dl_key": "attributes.hostname"},
            {"name": "DLV - attributes.protocol",        "dl_key": "attributes.protocol"},
            {"name": "DLV - attributes.fragment",        "dl_key": "attributes.fragment"},
            {"name": "DLV - attributes.statusCode",      "dl_key": "attributes.statusCode"},
            {"name": "DLV - attributes.statusText",      "dl_key": "attributes.statusText"},
            {"name": "DLV - attributes.headers",         "dl_key": "attributes.headers"},
            {"name": "DLV - attributes.timestamp",       "dl_key": "attributes.timestamp"},
            {"name": "DLV - attributes.contentType",     "dl_key": "attributes.contentType"},
            {"name": "DLV - attributes.response",        "dl_key": "attributes.response"},
        ],
    },

    # ── HubSpot Embedded Forms ──────────────────────────────────────────────
    "hubspot": {
        "name": "HubSpot Form",
        "platform_signals": ["hs-form", "hs_form_guid", "hubspotforms", "hubspot"],
        "listener": {
            "tag_name": "Auto - HubSpot Form Listener",
            "script": (
                '<script type="text/javascript">\n'
                '  window.addEventListener("message", function(event) {\n'
                '    if(event.data.type === "hsFormCallback" && '
                'event.data.eventName === "onFormSubmitted") {\n'
                '      window.dataLayer = window.dataLayer || [];\n'
                '      window.dataLayer.push({\n'
                '        "event": "hubspot-form-success",\n'
                '        "hs-form-guid": event.data.id\n'
                '      });\n'
                '    }\n'
                '  });\n'
                '</script>'
            ),
        },
        "plan_item": {
            "event_name": "form_submission",
            "trigger_type": "custom_event",
            "tag_name": "Auto - GA4 Event - form_submission",
            "trigger_name": "Auto - CE - hubspot-form-success",
            "trigger_condition": {"event": "hubspot-form-success"},
            "gtm_payload_keys": ["hs-form-guid"],
            "payload_schema": {"hs-form-guid": "string"},
            "semantic_mapping": {"hs-form-guid": "form_id"},
            "built_ins_to_activate": ["Page URL", "Page Path"],
            "variables_to_create": [],
            "confidence": 1.0,
            "confidence_reason": "Known HubSpot recipe \u2014 production-tested GTM template.",
            "failure_risks": [],
            "qa_test_steps": [
                "Submit HubSpot form \u2192 verify hubspot-form-success in dataLayer",
                "Check GA4 tag fires with form_id parameter",
            ],
        },
        "dl_variables": [
            {"name": "DLV - hs-form-guid", "dl_key": "hs-form-guid"},
        ],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_recipe_for_platform(
    platform: str,
    technology_signals: Optional[list[str]] = None,
) -> tuple[str, dict] | None:
    """Return ``(recipe_key, recipe)`` if a known recipe matches *platform*.

    Matching logic (in order):
    1. Direct key match  — ``RECIPES.get(platform)``
    2. Signal overlap     — any *technology_signal* appears in a recipe's
       ``platform_signals`` list.

    Returns ``None`` when no recipe matches.
    """
    # 1. Direct match
    if platform in RECIPES:
        return platform, RECIPES[platform]

    # 2. Signal match
    if technology_signals:
        for key, recipe in RECIPES.items():
            for sig in technology_signals:
                if sig in recipe["platform_signals"]:
                    return key, recipe

    return None


def build_recipe_plan_item(
    recipe_key: str,
    form_index: int,
    cf7_form_id: str | None = None,
    form_type: str | None = None,
) -> dict:
    """Create a full plan item from the recipe template.

    The returned dict is ready to pass to ``add_tag_and_trigger()`` and
    carries a ``use_recipe`` marker so ``compile()`` knows to inject the
    listener tag.

    When *form_type* is provided, the GA4 event name and tag name are
    set contextually (e.g. newsletter → ``sign_up_newsletter``).

    When *cf7_form_id* is provided for CF7 recipes, the trigger condition
    is narrowed to that specific form so multiple CF7 forms on the same
    page get independent triggers.
    """
    recipe = RECIPES[recipe_key]
    item = copy.deepcopy(recipe["plan_item"])
    item["form_index"] = form_index
    item["use_recipe"] = recipe_key

    # ── Contextual event name based on form_type ──
    if form_type and form_type in FORM_TYPE_EVENT_MAP:
        ctx_event = FORM_TYPE_EVENT_MAP[form_type]
        item["event_name"] = ctx_event
        item["tag_name"] = f"Auto - GA4 Event - {ctx_event}"

    # Per-form: add CF7 form ID filter to trigger condition
    if cf7_form_id and recipe_key == "wordpress_cf7":
        item["trigger_condition"]["cf7_form_id"] = cf7_form_id
        item["tag_name"] += f" - CF7#{cf7_form_id}"
        item["trigger_name"] = f"Auto - CE - cf7submission - #{cf7_form_id}"

    return item


def inject_recipe_listener(
    cv: dict,
    account_id: str,
    container_id: str,
    recipe_key: str,
) -> bool:
    """Inject the cHTML listener tag **and** DL variables for *recipe_key*.

    This is called during ``compile()``.  The listener fires on All Pages
    (native GTM trigger ``2147479553``).

    Returns ``True`` if the tag was injected, ``False`` if it already existed
    or the recipe key is unknown.
    """
    recipe = RECIPES.get(recipe_key)
    if not recipe:
        return False

    listener = recipe["listener"]
    tag_name = listener["tag_name"]

    # ── Duplicate guard ──
    existing_tags = [t.get("name") for t in cv.get("tag", [])]
    if tag_name in existing_tags:
        print(f"  ── Recipe listener '{tag_name}' already exists, skipping.")
        return False

    # ── Assign tag ID ──
    from main import get_max_id
    tag_id = str(get_max_id(cv.get("tag", []), "tagId") + 1) + "000"

    tag_params = [
        {"type": "TEMPLATE", "key": "html", "value": listener["script"]},
    ]
    # Append any extra parameters (e.g., supportDocumentWrite)
    tag_params.extend(listener.get("extra_params", []))

    listener_tag = {
        "accountId": account_id,
        "containerId": container_id,
        "tagId": tag_id,
        "name": tag_name,
        "type": "html",
        "parameter": tag_params,
        "firingTriggerId": ["2147479553"],  # All Pages
    }

    # Validate through Pydantic (same pattern as main.py injectors)
    from main import GtmTag
    try:
        validated = GtmTag(**listener_tag).model_dump(exclude_none=True)
        cv.setdefault("tag", []).append(validated)
        print(f"  ++ Injected recipe listener: {tag_name}")
    except ValidationError as e:
        print(f"  !! Failed to inject recipe listener '{tag_name}': {e}")
        return False

    # ── DataLayer Variables ──
    for dlv in recipe.get("dl_variables", []):
        var_name = dlv["name"]
        existing_vars = [v.get("name") for v in cv.get("variable", [])]
        if var_name in existing_vars:
            continue
        var_id = str(get_max_id(cv.get("variable", []), "variableId") + 1)
        cv.setdefault("variable", []).append({
            "accountId": account_id,
            "containerId": container_id,
            "variableId": var_id,
            "name": var_name,
            "type": "v",
            "parameter": [
                {"type": "TEMPLATE", "key": "name", "value": dlv["dl_key"]},
                {"type": "INTEGER", "key": "dataLayerVersion", "value": "2"},
                {"type": "BOOLEAN", "key": "setDefaultValue", "value": "false"},
            ],
        })
        print(f"  ++ Injected recipe variable: {var_name}")

    return True

import argparse
import asyncio
import json
import os
import copy
from crawler import crawl_site
from brain import generate_tracking_plan
from healer import heal_gtm_container
from pydantic import BaseModel, ValidationError, Field
from typing import List, Optional, Literal

# --- Strict GTM Schema Validation Models ---
class GtmParameter(BaseModel):
    type: Literal["TEMPLATE", "BOOLEAN", "INTEGER", "MAP", "LIST"] = Field(..., description="GTM requires strict uppercase primitive types.")
    key: str
    value: str

class GtmFilter(BaseModel):
    type: Literal["EQUALS", "CONTAINS", "STARTS_WITH", "ENDS_WITH", "MATCHES_CSS_SELECTOR", "MATCHES_REGEX"] = Field(..., description="GTM requires strict uppercase filter types.")
    parameter: List[GtmParameter]

class GtmVariable(BaseModel):
    accountId: str
    containerId: str
    variableId: str
    name: str
    type: Literal["v"] = Field(..., description="'v' is the GTM internal code for Data Layer Variable")
    parameter: List[GtmParameter]

class GtmTrigger(BaseModel):
    accountId: str
    containerId: str
    triggerId: str
    name: str
    type: Literal["CUSTOM_EVENT", "FORM_SUBMISSION", "LINK_CLICK", "CLICK", "PAGEVIEW", "DOM_READY", "WINDOW_LOADED", "HTML", "TRIGGER_GROUP", "ELEMENT_VISIBILITY"]
    customEventFilter: Optional[List[GtmFilter]] = None
    filter: Optional[List[GtmFilter]] = None

class GtmTag(BaseModel):
    accountId: str
    containerId: str
    tagId: str
    name: str
    type: str
    parameter: List[GtmParameter]
    firingTriggerId: List[str]
# -----------------------------------------

def extract_gtm_context(gtm_json: dict) -> dict:
    """
    Splits the full GTM JSON export into two lean dicts:
    - 'mechanical_ids': account/container/measurement IDs for the builder (main.py)
    - 'ai_context': only tag names + event names for Gemini (90% fewer tokens)
    """
    container = gtm_json.get("containerVersion", gtm_json)

    # 1. Mechanical IDs — only for builder.py, never sent to AI
    mechanical_ids = {
        "accountId":   container.get("accountId",   ""),
        "containerId": container.get("containerId", ""),
        "measurementId": None,
        "googleTagId":   None,
    }

    # Find GA4 Measurement ID (G-XXXXX) or Google Tag ID (GT-XXXXX)
    for variable in container.get("variable", []):
        if variable.get("type") == "c":  # Constant variable
            for param in variable.get("parameter", []):
                val = str(param.get("value", ""))
                if val.startswith("G-") and not mechanical_ids["measurementId"]:
                    mechanical_ids["measurementId"] = val
                elif val.startswith("GT-") and not mechanical_ids["googleTagId"]:
                    mechanical_ids["googleTagId"] = val

    # Also scan tags for the Measurement ID override
    if not mechanical_ids["measurementId"]:
        for tag in container.get("tag", []):
            for param in tag.get("parameter", []):
                val = str(param.get("value", ""))
                if val.startswith("G-"):
                    mechanical_ids["measurementId"] = val
                    break

    # 2. AI Context — minimal array for Gemini's naming & dedup audit
    ai_context = []
    for tag in container.get("tag", []):
        if tag.get("type") in ("gaaw", "gaawe"):  # GA4 Event tag types
            event_name = None
            for param in tag.get("parameter", []):
                if param.get("key") == "eventName":
                    event_name = param.get("value")
                    break
            if event_name:
                ai_context.append({
                    "tag_name":   tag.get("name"),
                    "event_name": event_name,
                })

    return {
        "mechanical_ids": mechanical_ids,
        "ai_context":     ai_context,
    }

# ── Dependency/Helper Functions for Injectors ──────────────────────
def get_max_id(arr: list, key: str) -> int:
    """Finds the maximum ID in a GTM list (e.g., tags, variables)."""
    if not arr: return 100
    return max([int(x.get(key, 0)) for x in arr if str(x.get(key, '0')).isdigit()] + [0])

def get_unique_name(arr: list, base_name: str) -> str:
    """Ensures a name is unique within a GTM list (avoids import errors)."""
    existing_names = set(x.get("name") for x in arr if "name" in x)
    name = base_name
    counter = 1
    while name in existing_names:
        counter += 1
        name = f"{base_name} ({counter})"
    return name

def auto_create_builtin_variable(cv: dict, account_id: str, container_id: str, var_type: str, var_name: str) -> None:
    """Enables a built-in variable if it doesn't already exist."""
    existing = [v.get("name") for v in cv.get("builtInVariable", [])]
    if var_name not in existing:
        cv.setdefault("builtInVariable", []).append({
            "accountId": account_id,
            "containerId": container_id,
            "type": var_type,
            "name": var_name
        })
        print(f"  + Auto-Enabled Built-In Variable: {var_name}")

def auto_create_dl_variable(cv: dict, account_id: str, container_id: str, var_name: str, dl_key: str, var_type: str = "TEMPLATE") -> str:
    """Creates a regular DataLayer variable if it doesn't already exist."""
    existing = [v.get("name") for v in cv.get("variable", [])]
    if var_name not in existing:
        var_id = str(get_max_id(cv.get("variable", []), "variableId") + 1)
        cv.setdefault("variable", []).append({
            "accountId": account_id,
            "containerId": container_id,
            "variableId": var_id,
            "name": var_name,
            "type": "v",
            "parameter": [
                {"type": var_type,  "key": "name",             "value": dl_key},
                {"type": "INTEGER", "key": "dataLayerVersion", "value": "2"},
                {"type": "BOOLEAN", "key": "setDefaultValue",  "value": "false"}
            ]
        })
        print(f"  + Auto-Created Variable: {var_name} [{dl_key}]")
    return var_name

def create_trigger_group(cv: dict, account_id: str, container_id: str,
                         trigger_ids: list[str], group_name: str) -> str:
    """Creates a GTM Trigger Group requiring ALL listed triggers to fire. Returns group trigger ID."""
    group_id = str(get_max_id(cv.get("trigger", []), "triggerId") + 1)
    conditions = [
        {"type": "MAP", "map": [{"type": "TEMPLATE", "key": "triggerId", "value": str(tid)}]}
        for tid in trigger_ids
    ]
    cv["trigger"].append({
        "accountId": account_id,
        "containerId": container_id,
        "triggerId": group_id,
        "name": get_unique_name(cv["trigger"], group_name),
        "type": "TRIGGER_GROUP",
        "parameter": [{"type": "LIST", "key": "conditions", "list": conditions}]
    })
    print(f"  + Created Trigger Group: {group_name} (triggers: {trigger_ids})")
    return group_id

# ── Standalone Feature Injectors ───────────────────────────────────

def inject_ajax_listener(cv: dict, account_id: str, container_id: str) -> None:
    """Injects the Bounteous AJAX auto-event listener."""
    listener_id = str(get_max_id(cv.get("tag", []), "tagId") + 1)
    
    ajax_tag = {
        "accountId": account_id,
        "containerId": container_id,
        "tagId": f"{listener_id}000",
        "name": "Auto - AJAX Auto-Event Listener",
        "type": "html",
        "parameter": [
            {
                "type": "TEMPLATE",
                "key": "html",
                "value": "<script id=\"gtm-jq-ajax-listen\" type=\"text/javascript\">\n(function() {\n  'use strict';\n  var $;\n  var n = 0;\n  init();\n\n  function init(n) {\n    if (typeof jQuery !== 'undefined') {\n      $ = jQuery;\n      bindToAjax();\n    } else if (n < 20) {\n      n++;\n      setTimeout(init, 500);\n    }\n  }\n\n  function bindToAjax() {\n    $(document).bind('ajaxComplete', function(evt, jqXhr, opts) {\n      var fullUrl = document.createElement('a');\n      fullUrl.href = opts.url;\n      var pathname = fullUrl.pathname[0] === '/' ? fullUrl.pathname : '/' + fullUrl.pathname;\n      var queryString = fullUrl.search[0] === '?' ? fullUrl.search.slice(1) : fullUrl.search;\n      var queryParameters = objMap(queryString, '&', '=', true);\n      var headers = objMap(jqXhr.getAllResponseHeaders(), '\\n', ':');\n      var responseBody = (jqXhr.responseJSON || jqXhr.responseXML || jqXhr.responseText || '');\n      \n      // AutoGTM Deep AJAX Parsing\n      try {\n          if (typeof responseBody === 'string') {\n              var parsed = JSON.parse(responseBody);\n              var strParsed = JSON.stringify(parsed).toLowerCase();\n              if (strParsed.includes('\"error\"') || strParsed.includes('\"status\":0') || strParsed.includes('\"false\"')) {\n                  console.warn(\"AutoGTM: Blocked ajaxComplete event due to error in response.\");\n                  return;\n              }\n          } else if (typeof responseBody === 'object') {\n              var strObj = JSON.stringify(responseBody).toLowerCase();\n              if (strObj.includes('\"error\":') || strObj.includes('\"status\":0') || strObj.includes('\"success\":false')) {\n                  console.warn(\"AutoGTM: Blocked ajaxComplete event due to error in response object.\");\n                  return;\n              }\n          }\n      } catch(e) {}\n\n      dataLayer.push({\n        'event': 'ajaxComplete',\n        'attributes': {\n          'type': opts.type || '',\n          'url': fullUrl.href || '',\n          'queryParameters': queryParameters,\n          'pathname': pathname || '',\n          'hostname': fullUrl.hostname || '',\n          'protocol': fullUrl.protocol || '',\n          'fragment': fullUrl.hash || '',\n          'statusCode': jqXhr.status || '',\n          'statusText': jqXhr.statusText || '',\n          'headers': headers,\n          'timestamp': evt.timeStamp || '',\n          'contentType': opts.contentType || '',\n          'response': responseBody\n        }\n      });\n    });\n  }\n\n  function objMap(data, delim, spl, decode) {\n    var obj = {};\n    if (!data || !delim || !spl) { return {}; }\n    var arr = data.split(delim);\n    for (var i = 0; i < arr.length; i++) {\n        var item = decode ? decodeURIComponent(arr[i]) : arr[i];\n        var pair = item.split(spl);\n        var key = trim_(pair[0]);\n        var value = trim_(pair[1]);\n        if (key && value) {\n            obj[key] = value;\n        }\n    }\n    return obj;\n  }\n\n  function trim_(str) {\n    if (str) {\n      return str.replace(/^[\\s\\uFEFF\\xA0]+|[\\s\\uFEFF\\xA0]+$/g, '');\n    }\n  }\n})();\n</script>"
            },
            {"type": "BOOLEAN", "key": "supportDocumentWrite", "value": "false"}
        ],
        "firingTriggerId": ["2147479553"] # Native All Pages ID
    }
    
    # Avoid duplicate injection
    existing_tags = [t.get("name") for t in cv.get("tag", [])]
    if ajax_tag["name"] not in existing_tags:
        try:
            validated_ajax_tag = GtmTag(**ajax_tag).model_dump(exclude_none=True)
            cv.setdefault("tag", []).append(validated_ajax_tag)
            print("  ++ Injected Bounteous AJAX Auto-Event Listener Tag ++")
        except ValidationError as e:
            print(f"Failed to inject AJAX listener: {e}")

def inject_session_harvester(cv: dict, account_id: str, container_id: str, harvester_keys: list[str]) -> None:
    """Injects the Session Storage Harvester tag to preserve payload across reloads."""
    unique_keys = list(set(harvester_keys))
    if not unique_keys: return
    
    # Check if already injected
    existing_tags = [t.get("name") for t in cv.get("tag", [])]
    if "Auto - Session Storage Harvester" in existing_tags:
        return

    # First, configure the Custom JS variables to read the session storage
    for k in unique_keys:
        cjs_name = f"CJS - Harvester - {k}"
        existing_vars = [v.get("name") for v in cv.get("variable", [])]
        if cjs_name not in existing_vars:
            var_id = str(get_max_id(cv.get("variable", []), "variableId") + 1)
            cjs_script = "function() { try { var data = JSON.parse(sessionStorage.getItem('autogtm_payload') || '{}'); return data['" + k + "'] || undefined; } catch(e) { return undefined; } }"
            cv.setdefault("variable", []).append({
                "accountId": account_id,
                "containerId": container_id,
                "variableId": var_id,
                "name": cjs_name,
                "type": "jsm",
                "parameter": [
                    {"type": "TEMPLATE", "key": "javascript", "value": cjs_script}
                ]
            })
            print(f"  ++ Injected Custom JS Storage Variable for: {k} ++")

    # Inject the actual listener tag
    harvester_id = str(get_max_id(cv.get("tag", []), "tagId") + 1)
    script_str = "<script id=\"gtm-auto-harvester\" type=\"text/javascript\">\n(function() {\n  'use strict';\n  var payloadKeys = {{HARVESTER_KEYS}}; \n\n  function collectData(form) {\n      if(!payloadKeys || payloadKeys.length === 0) return;\n      var dataToStore = {};\n      for(var i=0; i < payloadKeys.length; i++) {\n          var key = payloadKeys[i];\n          var el = form.querySelector('[name=\"' + key + '\"]');\n          if(el) {\n              if (el.type === 'radio' || el.type === 'checkbox') {\n                  var checkedEl = form.querySelector('[name=\"' + key + '\"]:checked');\n                  if (checkedEl) dataToStore[key] = checkedEl.value;\n              } else {\n                  dataToStore[key] = el.value;\n              }\n          }\n      }\n      if (Object.keys(dataToStore).length > 0) {\n          sessionStorage.setItem('autogtm_payload', JSON.stringify(dataToStore));\n      }\n  }\n\n  document.addEventListener('submit', function(e) {\n      if(e.target && e.target.tagName === 'FORM') {\n          collectData(e.target);\n      }\n  });\n})();\n</script>".replace("{{HARVESTER_KEYS}}", json.dumps(unique_keys))
    
    harvester_tag = {
        "accountId": account_id,
        "containerId": container_id,
        "tagId": f"{harvester_id}000",
        "name": "Auto - Session Storage Harvester",
        "type": "html",
        "parameter": [{"type": "TEMPLATE", "key": "html", "value": script_str}],
        "firingTriggerId": ["2147479553"] # All Pages
    }
    try:
        validated_harvester_tag = GtmTag(**harvester_tag).model_dump(exclude_none=True)
        cv.setdefault("tag", []).append(validated_harvester_tag)
        print(f"  ++ Injected Session Storage Harvester for {len(unique_keys)} fields ++")
    except ValidationError as e:
        print(f"Failed to inject Harvester listener: {e}")

def inject_html5_guardian(cv: dict, account_id: str, container_id: str, guardian_selectors: list[str]) -> None:
    """Injects the HTML5 Form Validation Guardian tag to intercept clicks and emit events."""
    if not guardian_selectors: return
    
    existing_tags = [t.get("name") for t in cv.get("tag", [])]
    if "Auto - HTML5 Validation Guardian" in existing_tags:
        return
        
    guardian_id = str(get_max_id(cv.get("tag", []), "tagId") + 1)
    
    script_str = "<script id=\"gtm-auto-guardian\" type=\"text/javascript\">\n(function() {\n  'use strict';\n  var formSelectors = {{FORM_SELECTORS}};\n  document.addEventListener('click', function(e) {\n      if(!e.target) return;\n      var btn = e.target.closest('button[type=\"submit\"], input[type=\"submit\"]');\n      if(btn) {\n          var form = btn.closest('form');\n          if(form) {\n              var matches = formSelectors.some(function(sel) {\n                  return form.matches(sel);\n              });\n              if(matches && form.checkValidity()) {\n                  window.dataLayer = window.dataLayer || [];\n                  window.dataLayer.push({\n                      'event': 'html5_verified_submit',\n                      'form_id': form.id || '',\n                      'form_class': form.className || ''\n                  });\n              }\n          }\n      }\n  });\n})();\n</script>".replace("{{FORM_SELECTORS}}", json.dumps(list(set(guardian_selectors))))
    
    guardian_tag = {
        "accountId": account_id,
        "containerId": container_id,
        "tagId": f"{guardian_id}000",
        "name": "Auto - HTML5 Validation Guardian",
        "type": "html",
        "parameter": [{"type": "TEMPLATE", "key": "html", "value": script_str}],
        "firingTriggerId": ["2147479553"] # All Pages
    }
    
    try:
        validated_guardian = GtmTag(**guardian_tag).model_dump(exclude_none=True)
        cv.setdefault("tag", []).append(validated_guardian)
        print(f"  ++ Injected HTML5 Validation Guardian for selectors: {', '.join(guardian_selectors)} ++")
    except ValidationError as e:
        print(f"Failed to inject HTML5 Guardian: {e}")

def inject_cross_domain_linker(cv: dict, account_id: str, container_id: str, external_domains: list[str]) -> None:
    """Injects a cross-domain linker tag for outbound forms/redirects."""
    if not external_domains: return
    
    existing_tags = [t.get("name") for t in cv.get("tag", [])]
    if "Auto - Cross-Domain Linker" in existing_tags:
        return
        
    linker_id = str(get_max_id(cv.get("tag", []), "tagId") + 1)
    
    linker_tag = {
        "accountId": account_id,
        "containerId": container_id,
        "tagId": f"{linker_id}000",
        "name": "Auto - Cross-Domain Linker",
        "type": "crossDomain",
        "parameter": [
            {
                "type": "LIST",
                "key": "crossDomainAutoLink",
                "list": [{"type": "MAP", "map": [{"type": "TEMPLATE", "key": "crossDomainAutoLinkTarget", "value": d}]} for d in external_domains]
            },
            {"type": "BOOLEAN", "key": "crossDomainDecorateForms", "value": "true"}
        ],
        "firingTriggerId": ["2147479553"] # All Pages
    }
    
    try:
        validated_linker = GtmTag(**linker_tag).model_dump(exclude_none=True)
        cv.setdefault("tag", []).append(validated_linker)
        print(f"  ++ Injected Cross-Domain Linker for domains: {', '.join(external_domains)} ++")
    except ValidationError as e:
        print(f"Failed to inject Cross-Domain Linker: {e}")

def get_inherited_measurement_id(cv: dict) -> str | None:
    """Finds an existing Google Tag or GA4 Config tag and extracts its Measurement ID."""
    for existing_tag in cv.get("tag", []):
        tag_type = existing_tag.get("type", "")
        if tag_type in ["googtag", "gaawc", "google_tag"]:
            for param in existing_tag.get("parameter", []):
                if param.get("key") in ["tagId", "measurementId"]:
                    val = param.get("value", "")
                    if val and (val.startswith("G-") or "{{" in val):
                        print(f"  + Phase 8 Piggybacking: Inheriting global measurement ID/Tag ({val}) from existing Config tag.")
                        return val
    return None

def process_payload_variables(cv: dict, account_id: str, container_id: str, plan_item: dict) -> list[dict]:
    """Infers types, semantically maps keys, and creates GTM Variables for the payload, returning the Event Settings Table mapping."""
    payload_params = []
    payload_keys = plan_item.get("gtm_payload_keys", [])
    payload_schema = plan_item.get("payload_schema") or {}
    semantic_mapping = plan_item.get("semantic_mapping") or {}
    
    # Map Gemini data type → GTM primitive variable parameter type
    TYPE_MAP = {
        "string": "TEMPLATE", "integer": "INTEGER", "number": "INTEGER",
        "boolean": "BOOLEAN",  "array": "TEMPLATE",  "object": "TEMPLATE",
    }
    
    for k in payload_keys:
        inferred_var_name = None
        # 1. First, check if a variable already exists with THIS DL key ('k')
        for v in cv.get("variable", []):
            if v.get("type") == "v":
                is_match = False
                for p in v.get("parameter", []):
                    if p.get("key") == "name" and p.get("value") == k:
                        is_match = True
                        break
                if is_match:
                    inferred_var_name = v.get("name")
                    break
        
        # 2. Check Built-in Variables (enabled)
        if not inferred_var_name:
            built_ins = {b.get("name"): b.get("type") for b in cv.get("builtInVariable", [])}
            # Common mappings: 'url' -> 'Page URL', etc.
            if k == "url" and "Page URL" in built_ins: inferred_var_name = "Page URL"
            if k == "element" and "Click Element" in built_ins: inferred_var_name = "Click Element"
                
        if inferred_var_name:
            print(f"  + Phase 8 Inference: Reusing existing variable '{inferred_var_name}' for key '{k}'")
            final_var_name = inferred_var_name
        else:
            final_var_name = f"DLV - {k}"
            existing_vars_check = [v.get("name") for v in cv.get("variable", [])]
            if final_var_name not in existing_vars_check:
                gtm_type = TYPE_MAP.get(payload_schema.get(k, "string"), "TEMPLATE")
                var_id = str(get_max_id(cv.get("variable", []), "variableId") + 1)
                cv.setdefault("variable", []).append({
                    "accountId": account_id,
                    "containerId": container_id,
                    "variableId": var_id,
                    "name": final_var_name,
                    "type": "v",
                    "parameter": [
                        {"type": gtm_type,  "key": "name",             "value": k},
                        {"type": "INTEGER", "key": "dataLayerVersion", "value": "2"},
                        {"type": "BOOLEAN", "key": "setDefaultValue",  "value": "false"}
                    ]
                })
                print(f"  + Auto-Created Variable [{gtm_type}]: {final_var_name}")
                
        final_key = k
        if k in semantic_mapping:
            final_key = semantic_mapping[k]
            print(f"  + Phase 9 Mapping: site key '{k}' mapped to GA4 param '{final_key}'")
            
        payload_params.append({
            "type": "MAP",
            "map": [
                {"type": "TEMPLATE", "key": "parameter",      "value": final_key},
                {"type": "TEMPLATE", "key": "parameterValue",  "value": f"{{{{{final_var_name}}}}}"}
            ]
        })
        
    return payload_params

# ── Main Event Generation Logic ────────────────────────────────────


def add_tag_and_trigger(gtm_json: dict, plan_item: dict) -> dict:
    """Safely injects Tags and individual event Triggers into a GTM JSON exports format."""
    container = copy.deepcopy(gtm_json)
    
    if "containerVersion" not in container:
        print("Invalid GTM JSON format. Missing containerVersion.")
        return container

    cv = container["containerVersion"]
    cv.setdefault("trigger", [])
    cv.setdefault("tag", [])
    cv.setdefault("variable", [])
    cv.setdefault("builtInVariable", [])
        
    trigger_id = str(get_max_id(cv["trigger"], "triggerId") + 1)
    tag_id = str(get_max_id(cv["tag"], "tagId") + 1)
    
    account_id = cv.get("container", {}).get("accountId", cv.get("accountId", "0"))
    container_id = cv.get("container", {}).get("containerId", cv.get("containerId", "0"))

    event_name = plan_item["event_name"]
    trigger_type = plan_item["trigger_type"]
    condition = plan_item.get("trigger_condition", {})

    # --- Strict Additive Naming Safeguard ---
    # Ensure every single injected element starts with "Auto - "
    if "tag_name" in plan_item and not plan_item["tag_name"].startswith("Auto - "):
        plan_item["tag_name"] = f"Auto - {plan_item['tag_name']}"
    if "trigger_name" in plan_item and not plan_item["trigger_name"].startswith("Auto - "):
        plan_item["trigger_name"] = f"Auto - {plan_item['trigger_name']}"

    # 1. Create Trigger
    trigger_type_map = {
        "custom_event":        "CUSTOM_EVENT",
        "ajax_complete":       "CUSTOM_EVENT",
        "form_submission":     "FORM_SUBMISSION",
        "click_links":         "LINK_CLICK",
        "element_visibility":  "ELEMENT_VISIBILITY",
        "page_view":           "PAGEVIEW",
        "custom_html":         "DOM_READY",
    }
    
    # Use AI-supplied names (from Step 4) if available, otherwise auto-generate
    ai_trigger_name = plan_item.get("trigger_name")
    base_trigger_name = ai_trigger_name or f"Auto - {trigger_type} - {event_name}"
    unique_trigger_name = get_unique_name(cv["trigger"], base_trigger_name)

    new_trigger = {
        "accountId": account_id,
        "containerId": container_id,
        "triggerId": trigger_id,
        "name": unique_trigger_name,
        "type": trigger_type_map.get(trigger_type, "CUSTOM_EVENT")
    }

    if trigger_type == "custom_event":
        dl_event_name = condition.get("event", condition.get("value", ""))
        new_trigger["customEventFilter"] = [
            {
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{_event}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": dl_event_name}
                ]
            }
        ]

        # CF7-specific: filter by contactFormId so each form fires its own event
        cf7_form_id = condition.get("cf7_form_id")
        if cf7_form_id:
            # The wpcf7mailsent JS event pushes contactFormId to dataLayer via GTM listener
            variable_name = "DL - cf7_form_id"
            new_trigger.setdefault("filter", []).append({
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": f"{{{{{variable_name}}}}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": str(cf7_form_id)}
                ]
            })
            # Auto-create the dataLayer variable if missing
            existing_vars = [v.get("name") for v in cv.get("variable", [])]
            if variable_name not in existing_vars:
                var_id = str(get_max_id(cv["variable"], "variableId") + 1)
                cv["variable"].append({
                    "accountId": account_id,
                    "containerId": container_id,
                    "variableId": var_id,
                    "name": variable_name,
                    "type": "v",
                    "parameter": [
                        {"type": "TEMPLATE", "key": "name", "value": "contactFormId"},
                        {"type": "INTEGER",  "key": "dataLayerVersion", "value": "2"},
                        {"type": "BOOLEAN",  "key": "setDefaultValue",  "value": "false"}
                    ]
                })
                print(f"  + Auto-Created CF7 Variable: {variable_name}")

        # Page path filter (fallback when no cf7_form_id)
        elif condition.get("key") == "page_path" and condition.get("value"):
            auto_create_builtin_variable(cv, account_id, container_id, "PAGE_PATH", "Page Path")
            new_trigger.setdefault("filter", []).append({
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{Page Path}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": condition.get("value")}
                ]
            })

        # Generic dataLayer key/value filter
        elif condition.get("key") and condition.get("key") not in ["_event", "event", "page_path", "cf7_form_id"]:
            variable_key = condition.get('key')
            variable_name = f"DataLayer Variable - {variable_key}"
            auto_create_dl_variable(cv, account_id, container_id, variable_name, variable_key)
            
            new_trigger.setdefault("filter", []).append({
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": f"{{{{{variable_name}}}}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": condition.get("value", "")}
                ]
            })
    elif trigger_type == "form_submission":
        # Native GTM form submission trigger (merged: supports both AI and direct formats)
        new_trigger["type"] = "FORM_SUBMISSION"
        new_trigger["parameter"] = [
            {"type": "BOOLEAN", "key": "waitForTags", "value": "false"},
            {"type": "BOOLEAN", "key": "checkValidation", "value": "false"}
        ]

        # ── Detect input format and build filter ──
        # Format A (AI): condition = {"key": "id"|"class"|..., "value": "..."}
        # Format B (direct): condition = {"form_id": "...", "form_class": "..."}
        form_var_map = {
            "class": ("Form Classes", "FORM_CLASSES"),
            "id":    ("Form ID",      "FORM_ID"),
            "target":("Form Target",  "FORM_TARGET"),
            "url":   ("Form URL",     "FORM_URL"),
            "text":  ("Form Text",    "FORM_TEXT"),
            "element":("Form Element","FORM_ELEMENT"),
        }

        if condition.get("key"):
            # Format A: AI-generated with key/value
            key = condition["key"].lower()
            var_name, var_type = form_var_map.get(key, ("Form ID", "FORM_ID"))
            filter_value = condition.get("value", "")
            new_trigger["filter"] = [{
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": f"{{{{{var_name}}}}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": filter_value}
                ]
            }]
            auto_create_builtin_variable(cv, account_id, container_id, var_type, var_name)
            print(f"  + Form Submission trigger (AI format) for {key}: {filter_value!r}")

        elif condition.get("form_id"):
            # Format B: direct form_id
            auto_create_builtin_variable(cv, account_id, container_id, "FORM_ID", "Form ID")
            new_trigger["filter"] = [{
                "type": "CONTAINS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{Form ID}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": condition["form_id"]}
                ]
            }]
            print(f"  + Form Submission trigger for ID: {condition['form_id']!r}")

        elif condition.get("form_class"):
            # Format B: direct form_class
            auto_create_builtin_variable(cv, account_id, container_id, "FORM_CLASSES", "Form Classes")
            new_trigger["filter"] = [{
                "type": "CONTAINS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{Form Classes}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": condition["form_class"]}
                ]
            }]
            print(f"  + Form Submission trigger for Class: {condition['form_class']!r}")

        else:
            # Fallback: page path filter
            auto_create_builtin_variable(cv, account_id, container_id, "PAGE_PATH", "Page Path")
            new_trigger["filter"] = [{
                "type": "CONTAINS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{Page Path}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": condition.get("page_path", "/")}
                ]
            }]
            print(f"  + Form Submission trigger (fallback with Page Path)")

    elif trigger_type == "html5_validation_guardian":
        # The Guardian tag converts the native submit into a Custom Event
        new_trigger["type"] = "CUSTOM_EVENT"
        new_trigger["customEventFilter"] = [
            {
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{_event}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": "html5_verified_submit"}
                ]
            }
        ]
        # Further filter by form ID or class if provided
        key = condition.get('key', 'id')
        val = condition.get('value', '')
        if val:
            var_name = "DataLayer Variable - form_id" if key == "id" else "DataLayer Variable - form_class"
            dl_key = "form_id" if key == "id" else "form_class"
            
            new_trigger.setdefault("filter", []).append({
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": f"{{{{{var_name}}}}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": val}
                ]
            })
            
            # Ensure DLV exists
            existing_vars = [v.get("name") for v in cv.get("variable", [])]
            if var_name not in existing_vars:
                var_id = str(get_max_id(cv["variable"], "variableId") + 1)
                cv["variable"].append({
                    "accountId": account_id,
                    "containerId": container_id,
                    "variableId": var_id,
                    "name": var_name,
                    "type": "v",
                    "parameter": [
                        {"type": "TEMPLATE", "key": "name", "value": dl_key},
                        {"type": "INTEGER",  "key": "dataLayerVersion", "value": "2"},
                        {"type": "BOOLEAN",  "key": "setDefaultValue",  "value": "false"}
                    ]
                })
        print(f"  + Guardian verification trigger created.")
    elif trigger_type == "click_links":
        # Selector can come from condition["selector"] (new) or condition["value"] (legacy)
        selector_val = condition.get("selector") or condition.get("value") or condition.get("filter_value", "")
        new_trigger["filter"] = [
            {
                "type": "CONTAINS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{Click URL}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": selector_val}
                ]
            }
        ]
        
        auto_create_builtin_variable(cv, account_id, container_id, "CLICK_URL", "Click URL")
    elif trigger_type == "ajax_complete":
        new_trigger["customEventFilter"] = [
            {
                "type": "EQUALS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": "{{_event}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": "ajaxComplete"}
                ]
            }
        ]
        # Auto-inject the attributes.url variable if missing
        if condition.get("value"):
            variable_key = "attributes.url"
            variable_name = f"DataLayer Variable - {variable_key}"
            new_trigger.setdefault("filter", [])
            new_trigger["filter"].append({
                "type": "CONTAINS",
                "parameter": [
                    {"type": "TEMPLATE", "key": "arg0", "value": f"{{{{{variable_name}}}}}"},
                    {"type": "TEMPLATE", "key": "arg1", "value": condition.get("value", "")}
                ]
            })
            
            # Check and create variable
            existing_vars = [v.get("name") for v in cv.get("variable", [])]
            if variable_name not in existing_vars:
                var_id = str(get_max_id(cv["variable"], "variableId") + 1)
                new_var = {
                    "accountId": account_id,
                    "containerId": container_id,
                    "variableId": var_id,
                    "name": variable_name,
                    "type": "v", # Data Layer Variable
                    "parameter": [
                        {"type": "TEMPLATE", "key": "name", "value": variable_key},
                        {"type": "INTEGER", "key": "dataLayerVersion", "value": "2"},
                        {"type": "BOOLEAN", "key": "setDefaultValue", "value": "false"}
                    ]
                }
                cv["variable"].append(new_var)
                print(f"  + Auto-Created Variable: {variable_name}")
    elif trigger_type == "element_visibility":
        # Format verified against real GTM export — uses parameter array, NOT direct fields
        selector = condition.get("selector", condition.get("value", ""))
        new_trigger["parameter"] = [
            {"type": "BOOLEAN",  "key": "useOnScreenDuration",   "value": "true"},
            {"type": "INTEGER",  "key": "minimumOnScreenDuration", "value": "2000"},
            {"type": "BOOLEAN",  "key": "useDomChangeListener",  "value": "true"},
            {"type": "TEMPLATE", "key": "firingFrequency",       "value": "ONCE_PER_PAGE"},
            {"type": "TEMPLATE", "key": "selectorType",          "value": "CSS"},
            {"type": "TEMPLATE", "key": "elementSelector",       "value": selector},
            {"type": "TEMPLATE", "key": "onScreenRatio",         "value": "10"}
        ]
        print(f"  + Element Visibility trigger: {selector!r}")

    elif trigger_type == "page_view":
        auto_create_builtin_variable(cv, account_id, container_id, "PAGE_PATH", "Page Path")
        # #11 Regex URL: dynamic redirects use MATCHES_REGEX filter
        if condition.get("page_path_regex"):
            regex_pattern = condition["page_path_regex"]
            new_trigger["filter"] = [{"type": "MATCHES_REGEX", "parameter": [
                {"type": "TEMPLATE", "key": "arg0", "value": "{{Page Path}}"},
                {"type": "TEMPLATE", "key": "arg1", "value": regex_pattern}
            ]}]
            print(f"  + Page View trigger (REGEX) for pattern: {regex_pattern!r}")
        else:
            page_path = condition.get("page_path", condition.get("value", ""))
            new_trigger["filter"] = [{"type": "EQUALS", "parameter": [
                {"type": "TEMPLATE", "key": "arg0", "value": "{{Page Path}}"},
                {"type": "TEMPLATE", "key": "arg1", "value": page_path}
            ]}]
            print(f"  + Page View trigger (EXACT) for path: {page_path!r}")


    # --- Trigger Group Support ---
    # If requires_trigger_group is set and a fallback_method exists,
    # create a secondary trigger and group them together.
    if plan_item.get("requires_trigger_group") and plan_item.get("fallback_method"):
        fb_method = plan_item["fallback_method"]
        fb_cond = plan_item.get("fallback_trigger_condition") or condition

        sec_id = str(get_max_id(cv["trigger"], "triggerId") + 1)
        sec_name = get_unique_name(cv["trigger"], f"Auto - {fb_method} - {event_name} (Secondary)")
        sec_trigger = {
            "accountId": account_id, "containerId": container_id,
            "triggerId": sec_id, "name": sec_name,
            "type": trigger_type_map.get(fb_method, "CUSTOM_EVENT"),
        }

        if fb_method == "element_visibility":
            sel = fb_cond.get("selector", "")
            sec_trigger["parameter"] = [
                {"type": "BOOLEAN", "key": "useOnScreenDuration", "value": "true"},
                {"type": "INTEGER", "key": "minimumOnScreenDuration", "value": "2000"},
                {"type": "BOOLEAN", "key": "useDomChangeListener", "value": "true"},
                {"type": "TEMPLATE", "key": "firingFrequency", "value": "ONCE_PER_PAGE"},
                {"type": "TEMPLATE", "key": "selectorType", "value": "CSS"},
                {"type": "TEMPLATE", "key": "elementSelector", "value": sel},
                {"type": "TEMPLATE", "key": "onScreenRatio", "value": "10"},
            ]
        elif fb_method in ("custom_event", "ajax_complete"):
            evt = fb_cond.get("event", "")
            sec_trigger["customEventFilter"] = [{"type": "EQUALS", "parameter": [
                {"type": "TEMPLATE", "key": "arg0", "value": "{{_event}}"},
                {"type": "TEMPLATE", "key": "arg1", "value": evt},
            ]}]
        elif fb_method == "page_view":
            pp = fb_cond.get("page_path", "")
            sec_trigger["filter"] = [{"type": "EQUALS", "parameter": [
                {"type": "TEMPLATE", "key": "arg0", "value": "{{Page Path}}"},
                {"type": "TEMPLATE", "key": "arg1", "value": pp},
            ]}]

        cv["trigger"].append(sec_trigger)
        group_id = create_trigger_group(
            cv, account_id, container_id,
            [trigger_id, sec_id], f"Auto - Trigger Group - {event_name}"
        )
        trigger_id = group_id  # Tag fires on the group, not individual triggers
        print(f"  + Trigger Group: {event_name} (primary + {fb_method})")

    # --- Phase 8: Config Tag Piggybacking (Inheritance) ---
    inherited_measurement_id = get_inherited_measurement_id(cv)

    # ─── Phase 8+9: Advanced Type Inference + Typed Variable Creation ───
    payload_params = process_payload_variables(cv, account_id, container_id, plan_item)

    # Build GA4 Event tag parameters matching GTM's required schema for gaawe
    if trigger_type == "custom_html":
        # Tag ID, Name logic
        ai_tag_name = plan_item.get("tag_name")
        base_tag_name = ai_tag_name or f"Custom HTML - {event_name}"
        unique_tag_name = get_unique_name(cv["tag"], base_tag_name)
        new_tag = {
            "accountId": account_id,
            "containerId": container_id,
            "tagId": tag_id,
            "name": unique_tag_name,
            "type": "html",
            "parameter": [
                {"type": "TEMPLATE", "key": "html", "value": f"<script>\n{plan_item.get('custom_html_script', '')}\n</script>"}
            ],
            "firingTriggerId": [trigger_id],
            "notes": f"Auto-generated by AutoGTM. {plan_item.get('notes', '')}\n\n" + 
                     (f"🚨 FAILURE RISKS: {', '.join(plan_item.get('failure_risks', []))}\n" if plan_item.get("failure_risks") else "") +
                     (f"🛠️ QA STEPS: {'; '.join(plan_item.get('qa_test_steps', []))}\n" if plan_item.get("qa_test_steps") else ""),
            "tagFiringOption": "ONCE_PER_EVENT"
        }
    else:
        tag_parameters = [
            {"type": "TEMPLATE", "key": "eventName",          "value": event_name},
            {"type": "BOOLEAN",  "key": "sendEcommerceData",  "value": "false"},
            {"type": "LIST",     "key": "eventSettingsTable", "list": payload_params},
            {"type": "BOOLEAN",  "key": "enhancedUserId",     "value": "false"},
            {"type": "BOOLEAN",  "key": "sendUserProvidedData", "value": "true"},
        ]

        # Attach the real GA4 Measurement ID using the correct key for event tags
        if inherited_measurement_id:
            tag_parameters.append({
                "type": "TEMPLATE",
                "key": "measurementIdOverride",   # MUST be measurementIdOverride for gaawe
                "value": inherited_measurement_id
            })

        # Use AI-supplied tag_name (from Step 4) if available, else auto-generate
        ai_tag_name = plan_item.get("tag_name")
        base_tag_name = ai_tag_name or f"GA4 Event - {event_name}"
        unique_tag_name = get_unique_name(cv["tag"], base_tag_name)

        new_trigger["notes"] = f"Auto-generated by AutoGTM. Logic: {plan_item.get('notes', 'No notes provided.')}"

        new_tag = {
            "accountId": account_id,
            "containerId": container_id,
            "tagId": tag_id,
            "name": unique_tag_name,
            "type": "gaawe",
            "parameter": tag_parameters,
            "firingTriggerId": [trigger_id],
            "notes": f"Auto-generated by AutoGTM. {plan_item.get('notes', '')}\n\n" + 
                     (f"🚨 FAILURE RISKS: {', '.join(plan_item.get('failure_risks', []))}\n" if plan_item.get("failure_risks") else "") +
                     (f"🛠️ QA STEPS: {'; '.join(plan_item.get('qa_test_steps', []))}\n" if plan_item.get("qa_test_steps") else "") +
                     (f"🔍 CSS REVIEW: {plan_item['selector_critic']['reasoning']} (Score: {plan_item['selector_critic']['fragility_score']}/10)" if plan_item.get("selector_critic") else ""),
            "tagFiringOption": "ONCE_PER_EVENT"
        }

    cv["trigger"].append(new_trigger)
    cv["tag"].append(new_tag)

    # #Phase 11: Orphaned Variable Bridge (Data Injection Failsafe)
    if plan_item.get("orphaned_bridge"):
        bridge_id = str(len(cv["tag"]) + 1000)
        bridge_html = """<script>
(function() {
  var form = document.querySelector('form'); // Simplified; in prod we'd use closer matching
  if (!form) return;
  form.addEventListener('submit', function() {
    var payload = {};
    var inputs = form.querySelectorAll('input, select, textarea');
    inputs.forEach(function(i) {
      if (i.name || i.id) payload[i.name || i.id] = i.value;
    });
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({
      'event': 'form_data_snapshot',
      'form_payload': payload
    });
  });
})();
</script>"""
        bridge_tag = {
            "accountId": account_id,
            "containerId": container_id,
            "tagId": bridge_id,
            "name": f"AutoGTM - Orphaned Bridge - {event_name}",
            "type": "html",
            "parameter": [
                {"type": "TEMPLATE", "key": "html", "value": bridge_html}
            ],
            "firingTriggerId": [trigger_id], # Fire whenever the main tag fires, or on page view
            "notes": "Failsafe: Scrapes DOM fields if DataLayer is missing info."
        }
        cv["tag"].append(bridge_tag)
        print(f"  + Added Orphaned Bridge tag for {event_name}")

    return container

def export_delta_recipe(compiled_json: dict, base_json: dict) -> dict:
    """
    Extracts ONLY the newly created AutoGTM elements (tags, triggers, variables)
    and any newly activated built-in variables.
    Returns a clean JSON ready for importing (Merge) into GTM without altering existing setups.
    """
    recipe = copy.deepcopy(compiled_json)
    if "containerVersion" not in recipe:
        return recipe
        
    cv = recipe["containerVersion"]
    
    # Diff-based delta: keep only elements NOT present in the base container
    base_cv = base_json.get("containerVersion", base_json)
    for key in ["tag", "trigger", "variable"]:
        if key not in cv:
            continue
        base_names = {item.get("name") for item in base_cv.get(key, []) if item.get("name")}
        cv[key] = [item for item in cv[key] if item.get("name") and item.get("name") not in base_names]

    # Diagnostic: warn on empty delta sections
    delta_counts = {k: len(cv.get(k, [])) for k in ["tag", "trigger", "variable"]}
    if delta_counts["tag"] == 0:
        print("WARNING [delta]: Export contains ZERO tags — AI pipeline may have produced no items.")
    if delta_counts["variable"] == 0 and delta_counts["tag"] > 0:
        print("WARNING [delta]: Tags present but ZERO variables — check variable creation.")
    print(f"  [DELTA] {delta_counts['tag']} tags, {delta_counts['trigger']} triggers, {delta_counts['variable']} variables")
            
    # For builtInVariables, keep ones that were NOT active in the base container.
    base_builtins = set()
    if "containerVersion" in base_json and "builtInVariable" in base_json["containerVersion"]:
        for biv in base_json["containerVersion"]["builtInVariable"]:
            base_builtins.add(biv.get("type"))
            
    if "builtInVariable" in cv:
        cv["builtInVariable"] = [biv for biv in cv["builtInVariable"] if biv.get("type") not in base_builtins]
        
    # Clear out structural arrays we don't generate to avoid overriding / cluttering
    for key in ["customTemplate", "folder", "zone", "client"]:
        if key in cv:
            cv[key] = []

    # Reassign IDs to high range to prevent import conflicts
    recipe = _reassign_delta_ids(recipe)

    # Add traceability metadata (GTM ignores unknown top-level keys)
    recipe["_autogtm_metadata"] = {
        "generated_by": "AutoGTM",
        "delta_tags": len(cv.get("tag", [])),
        "delta_triggers": len(cv.get("trigger", [])),
        "delta_variables": len(cv.get("variable", [])),
        "delta_builtins": len(cv.get("builtInVariable", [])),
    }

    return recipe


DELTA_ID_OFFSET = 9_000_000

def _reassign_delta_ids(delta_json: dict) -> dict:
    """
    Reassigns all IDs in the delta to a high range (9,000,000+)
    to avoid conflicts when importing into a live GTM workspace.
    Updates all cross-references (firingTriggerId, blockingTriggerId, trigger group conditions).
    """
    cv = delta_json.get("containerVersion")
    if not cv:
        return delta_json

    trigger_id_map = {}
    for i, trigger in enumerate(cv.get("trigger", []), start=1):
        old_id = str(trigger.get("triggerId", ""))
        new_id = str(DELTA_ID_OFFSET + i)
        trigger_id_map[old_id] = new_id
        trigger["triggerId"] = new_id

    for i, tag in enumerate(cv.get("tag", []), start=1):
        tag["tagId"] = str(DELTA_ID_OFFSET + 1000 + i)

    for i, var in enumerate(cv.get("variable", []), start=1):
        var["variableId"] = str(DELTA_ID_OFFSET + 2000 + i)

    # Update cross-references: firingTriggerId / blockingTriggerId in tags
    for tag in cv.get("tag", []):
        for ref_key in ("firingTriggerId", "blockingTriggerId"):
            if ref_key in tag:
                tag[ref_key] = [
                    trigger_id_map.get(str(tid), str(tid))
                    for tid in tag[ref_key]
                ]

    # Update trigger group condition references
    for trigger in cv.get("trigger", []):
        if trigger.get("type") == "TRIGGER_GROUP":
            for param in trigger.get("parameter", []):
                if param.get("key") == "conditions":
                    for cond in param.get("list", []):
                        for m in cond.get("map", []):
                            if m.get("key") == "triggerId":
                                old_val = str(m.get("value", ""))
                                m["value"] = trigger_id_map.get(old_val, old_val)

    return delta_json


def main():
    parser = argparse.ArgumentParser(description="AutoGTM Builder")
    parser.add_argument("url", help="The URL to crawl and analyze")
    parser.add_argument("--gtm-json", required=True, help="Path to existing GTM JSON container file")
    parser.add_argument("--output", default="output_gtm.json", help="Path to save the modified GTM JSON")

    args = parser.parse_args()

    print(f"1. Crawling {args.url}...")
    crawler_data = asyncio.run(crawl_site(args.url))
    found_forms = len(crawler_data.get('forms_processed', []))
    print(f"Crawler finished. Found {found_forms} Forms.")

    print("2. Generating Tracking Plan using Gemini...")
    if not os.environ.get("GEMINI_API_KEY"):
        print("WARNING: GEMINI_API_KEY environment variable not set. Will fail.")
    
    try:
        plan = generate_tracking_plan(crawler_data)
        print("Plan generated:")
        print(json.dumps(plan, indent=2))
    except Exception as e:
        print(f"Error during plan generation: {e}")
        return

    print(f"3. Applying Tracking Plan to {args.gtm_json}...")
    try:
        with open(args.gtm_json, 'r') as f:
            gtm_data = json.load(f)
    except Exception as e:
        print(f"Error reading GTM JSON file: {e}")
        return

    updated_gtm = copy.deepcopy(gtm_data)
    
    if "tracking_plan" in plan and isinstance(plan["tracking_plan"], list):
        requires_ajax_listener = False
        for item in plan["tracking_plan"]:
            if item.get("trigger_type") == "ajax_complete":
                requires_ajax_listener = True
            updated_gtm = add_tag_and_trigger(updated_gtm, item)
            print(f"  Added Tag/Trigger for {item['event_name']}")
            
        # Inject specific features (AJAX, Harvester, Guardian, Cross-Domain) modularly
        if requires_ajax_listener:
            cv = updated_gtm.get("containerVersion", {})
            account_id = cv.get("container", {}).get("accountId", "0")
            container_id = cv.get("container", {}).get("containerId", "0")
            inject_ajax_listener(cv, account_id, container_id)

        # Session Harvester Collector for missing variables
        harvester_keys = []
        guardian_selectors = []
        external_domains = []
        
        for item in plan.get("tracking_plan", []):
            ttype = item.get("trigger_type")
            if ttype in ["element_visibility", "page_view", "form_submission"]:
                harvester_keys.extend(item.get("gtm_payload_keys", []))
            elif ttype == "html5_validation_guardian":
                cond = item.get("trigger_condition", {})
                if cond.get("key") == "id" and cond.get("value"):
                    guardian_selectors.append(f"#{cond.get('value')}")
                elif cond.get("key") == "class" and cond.get("value"):
                    guardian_selectors.append(f".{cond.get('value').replace(' ', '.')}")
            elif ttype == "cross_domain_redirect":
                ext = item.get("trigger_condition", {}).get("external_domain")
                if ext and ext not in external_domains:
                    external_domains.append(ext)

        cv = updated_gtm.get("containerVersion", {})
        account_id = cv.get("container", {}).get("accountId", "0")
        container_id = cv.get("container", {}).get("containerId", "0")
        
        inject_session_harvester(cv, account_id, container_id, harvester_keys)
        inject_html5_guardian(cv, account_id, container_id, guardian_selectors)
        inject_cross_domain_linker(cv, account_id, container_id, external_domains)


    else:
        print("No tracking plan generated or invalid format.")

    print(f"4. Saving updated container to {args.output}...")
    try:
        updated_gtm = heal_gtm_container(updated_gtm)
        with open(args.output, 'w') as f:
            json.dump(updated_gtm, f, indent=2)
        print("Success! Modified GTM JSON is ready.")
    except Exception as e:
        print(f"Error saving output JSON: {e}")

if __name__ == "__main__":
    main()

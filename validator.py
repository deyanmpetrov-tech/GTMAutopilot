"""
validator.py — GTM Container Validation
Runs structural and schema checks on a compiled GTM JSON container
and returns a detailed validation report.
"""

from typing import Optional


def validate_gtm_container(gtm: dict) -> dict:
    """
    Validate a GTM container export dict.
    Returns:
        {
            "passed": bool,
            "score": int,          # 0-100
            "checks": [
                {"name": str, "status": "pass"|"fail"|"warn", "detail": str}
            ],
            "errors": [str],
            "warnings": [str],
        }
    """
    checks = []
    errors = []
    warnings = []

    def add(name, status, detail):
        checks.append({"name": name, "status": status, "detail": detail})
        if status == "fail":
            errors.append(f"{name}: {detail}")
        elif status == "warn":
            warnings.append(f"{name}: {detail}")

    # ── 1. Top-level structure ───────────────────────────────────────────────
    if "exportFormatVersion" not in gtm:
        add("Top-level structure", "fail", "Missing exportFormatVersion field")
    elif gtm["exportFormatVersion"] != 2:
        add("Top-level structure", "warn", f"exportFormatVersion is {gtm['exportFormatVersion']}, expected 2")
    else:
        add("Top-level structure", "pass", "exportFormatVersion = 2 ✓")

    cv = gtm.get("containerVersion")
    if not cv:
        add("containerVersion", "fail", "Missing containerVersion block")
        return _build_result(checks, errors, warnings)
    else:
        add("containerVersion", "pass", "containerVersion block present ✓")

    tags      = cv.get("tag", [])
    triggers  = cv.get("trigger", [])
    variables = cv.get("variable", [])
    builtins  = cv.get("builtInVariable", [])

    # ── 2. No duplicate tag IDs ──────────────────────────────────────────────
    tag_ids = [t.get("tagId") for t in tags if t.get("tagId")]
    dup_tags = [tid for tid in set(tag_ids) if tag_ids.count(tid) > 1]
    if dup_tags:
        add("Duplicate tag IDs", "fail", f"Duplicate tagIds: {dup_tags}")
    else:
        add("Duplicate tag IDs", "pass", f"All {len(tags)} tag IDs are unique ✓")

    # ── 3. No duplicate trigger IDs ─────────────────────────────────────────
    trig_ids = [t.get("triggerId") for t in triggers if t.get("triggerId")]
    dup_trigs = [tid for tid in set(trig_ids) if trig_ids.count(tid) > 1]
    if dup_trigs:
        add("Duplicate trigger IDs", "fail", f"Duplicate triggerIds: {dup_trigs}")
    else:
        add("Duplicate trigger IDs", "pass", f"All {len(triggers)} trigger IDs are unique ✓")

    # ── 4. GA4 Event tags have required parameters ───────────────────────────
    gaawe_errors = []
    for tag in tags:
        if tag.get("type") != "gaawe":
            continue
        params = {p.get("key"): p.get("value") for p in tag.get("parameter", [])}
        name = tag.get("name", f"tagId={tag.get('tagId')}")
        if not params.get("eventName"):
            gaawe_errors.append(f"'{name}' missing eventName")
        if "measurementIdOverride" in params and not params["measurementIdOverride"]:
            gaawe_errors.append(f"'{name}' has empty measurementIdOverride")
        # Check for invalid array values in parameters
        for p in tag.get("parameter", []):
            if isinstance(p.get("value"), list):
                gaawe_errors.append(f"'{name}' param '{p.get('key')}' has array value (must use 'list' key)")
    if gaawe_errors:
        add("GA4 Event tag schema", "fail", "; ".join(gaawe_errors))
    else:
        gaawe_count = sum(1 for t in tags if t.get("type") == "gaawe")
        add("GA4 Event tag schema", "pass", f"All {gaawe_count} GA4 Event tags are valid ✓")

    # ── 5. Subscription-specific checks (Phase 12 Extension) ────────────────
    signup_errors = []
    for tag in tags:
        if tag.get("type") != "gaawe":
            continue
        params = {p.get("key"): p.get("value") for p in tag.get("parameter", [])}
        # GA4 sign_up event should ideally have a 'method' parameter
        if params.get("eventName") == "sign_up":
            # Check eventSettingsTable for 'method'
            method_found = False
            for p in tag.get("parameter", []):
                if p.get("key") == "eventSettingsTable":
                    for row in p.get("list", []):
                        row_params = {item.get("key"): item.get("value") for item in row.get("map", [])}
                        if row_params.get("key") == "method":
                            method_found = True
                            break
            if not method_found:
                signup_errors.append(f"'{tag.get('name')}' (sign_up) is missing 'method' parameter")
    
    if signup_errors:
        add("GA4 Sign-up validation", "warn", "; ".join(signup_errors))
    else:
        add("GA4 Sign-up validation", "pass", "All sign_up events have recommended parameters ✓")

    # ── 5. No array 'value' fields (GTM expects strings) ─────────────────────
    import json, re
    raw = json.dumps(cv)
    bad_values = re.findall(r'"value"\s*:\s*\[', raw)
    if bad_values:
        add("Array value fields", "fail",
            f"Found {len(bad_values)} 'value': [...] occurrences — GTM expects strings, use 'list' key for LIST params")
    else:
        add("Array value fields", "pass", "No invalid array 'value' fields ✓")

    # ── 6. Auto-injected tags have valid trigger references ───────────────────
    valid_trig_ids = set(str(t.get("triggerId", "")) for t in triggers)
    dangling = []
    for tag in tags:
        name = tag.get("name", "")
        # Only check our auto-injected tags, not pre-existing ones
        if not (name.startswith("GA4 Event") or name.startswith("Auto -")):
            continue
        for fid in tag.get("firingTriggerId", []):
            if str(fid) not in valid_trig_ids:
                dangling.append(f"Tag '{name}' → triggerId={fid} (not found)")
    if dangling:
        add("Trigger references", "fail", "; ".join(dangling[:3]))
    else:
        auto_count = sum(1 for t in tags if t.get("name","").startswith(("GA4 Event","Auto -")))
        add("Trigger references", "pass", f"All {auto_count} auto-injected tag trigger references are valid ✓")

    # ── 7. All tags have tagFiringOption ─────────────────────────────────────
    missing_firing = [t.get("name") for t in tags if not t.get("tagFiringOption")]
    if missing_firing:
        add("tagFiringOption", "warn",
            f"Tags missing tagFiringOption: {missing_firing[:5]}. GTM may reject these.")
    else:
        add("tagFiringOption", "pass", "All tags have tagFiringOption ✓")

    # ── 8. New tags were actually injected ───────────────────────────────────
    auto_tags = [t for t in tags if "Auto -" in str(t.get("name", "")) or
                 t.get("name", "").startswith("GA4 Event")]
    if auto_tags:
        add("Auto-injected tags", "pass",
            f"{len(auto_tags)} auto-generated tag(s) found: {[t['name'] for t in auto_tags[:3]]}")
    else:
        add("Auto-injected tags", "warn", "No auto-injected tags found — pipeline may not have run correctly")

    # ── 9. Required container metadata ───────────────────────────────────────
    container = cv.get("container", {})
    if container.get("accountId") and container.get("containerId"):
        add("Container metadata", "pass",
            f"accountId={container['accountId']}, containerId={container['containerId']} ✓")
    else:
        add("Container metadata", "warn", "Missing accountId or containerId in container block")

    # ── 10. Overall summary ──────────────────────────────────────────────────
    total = len(tags)
    auto  = len(auto_tags)
    add("Summary", "pass" if not errors else "fail",
        f"Container has {total} tags ({auto} new), {len(triggers)} triggers, {len(variables)} variables")

    return _build_result(checks, errors, warnings)


def _build_result(checks, errors, warnings):
    fail_count = sum(1 for c in checks if c["status"] == "fail")
    warn_count = sum(1 for c in checks if c["status"] == "warn")
    pass_count = sum(1 for c in checks if c["status"] == "pass")
    total = len(checks)
    score = int((pass_count / total) * 100) if total else 0

    return {
        "passed": fail_count == 0,
        "score": score,
        "checks": checks,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "pass": pass_count,
            "warn": warn_count,
            "fail": fail_count,
        }
    }


def validate_delta(delta_json: dict, base_json: dict) -> dict:
    """
    Validate a delta GTM container export against its base container.
    Checks structural integrity specific to delta (merge) imports:
    - Every firingTriggerId in delta tags references a trigger in the delta
    - Every {{Variable}} reference resolves to delta OR base container
    - No orphaned triggers (triggers unreferenced by any tag)
    - Non-empty delta content
    """
    import json as _json
    import re

    checks = []
    errors = []
    warnings = []

    def add(name, status, detail):
        checks.append({"name": name, "status": status, "detail": detail})
        if status == "fail":
            errors.append(f"{name}: {detail}")
        elif status == "warn":
            warnings.append(f"{name}: {detail}")

    cv = delta_json.get("containerVersion")
    if not cv:
        add("Delta structure", "fail", "Missing containerVersion in delta")
        return _build_result(checks, errors, warnings)

    base_cv = base_json.get("containerVersion", base_json)
    delta_tags = cv.get("tag", [])
    delta_triggers = cv.get("trigger", [])
    delta_vars = cv.get("variable", [])

    # ── 1. Trigger reference integrity ────────────────────────────────
    delta_trigger_ids = {str(t.get("triggerId", "")) for t in delta_triggers}
    # Built-in trigger IDs that GTM always has (All Pages, DOM Ready, etc.)
    BUILTIN_TRIGGER_IDS = {"2147479553", "2147479572", "2147479573"}

    broken_refs = []
    for tag in delta_tags:
        tag_name = tag.get("name", "?")
        for fid in tag.get("firingTriggerId", []):
            fid_str = str(fid)
            if fid_str not in delta_trigger_ids and fid_str not in BUILTIN_TRIGGER_IDS:
                broken_refs.append(f"'{tag_name}' → triggerId={fid_str}")

    if broken_refs:
        add("Delta trigger references", "fail",
            f"Tags reference triggers not in delta: {'; '.join(broken_refs[:5])}")
    else:
        add("Delta trigger references", "pass",
            f"All {len(delta_tags)} delta tags have valid trigger references")

    # ── 2. Variable reference integrity ───────────────────────────────
    all_var_names = {v.get("name") for v in delta_vars if v.get("name")}
    all_var_names |= {v.get("name") for v in base_cv.get("variable", []) if v.get("name")}
    all_var_names |= {b.get("name") for b in base_cv.get("builtInVariable", [])}
    all_var_names |= {b.get("name") for b in cv.get("builtInVariable", [])}
    # GTM built-in variable names that are always available
    all_var_names |= {"_event", "Page URL", "Page Path", "Page Hostname",
                      "Referrer", "Event"}

    raw_delta = _json.dumps(cv)
    referenced_vars = set(re.findall(r'\{\{([^}]+)\}\}', raw_delta))
    missing_vars = referenced_vars - all_var_names

    if missing_vars:
        add("Delta variable references", "warn",
            f"Referenced variables not found in delta or base: {sorted(missing_vars)}")
    else:
        add("Delta variable references", "pass",
            f"All {len(referenced_vars)} variable references resolve correctly")

    # ── 3. Orphaned triggers ──────────────────────────────────────────
    referenced_tids = set()
    for tag in delta_tags:
        for fid in tag.get("firingTriggerId", []):
            referenced_tids.add(str(fid))
        for fid in tag.get("blockingTriggerId", []):
            referenced_tids.add(str(fid))

    # Also check trigger group conditions (triggers referenced inside TRIGGER_GROUP)
    for trigger in delta_triggers:
        if trigger.get("type") == "TRIGGER_GROUP":
            for param in trigger.get("parameter", []):
                if param.get("key") == "conditions":
                    for cond in param.get("list", []):
                        for m in cond.get("map", []):
                            if m.get("key") == "triggerId":
                                referenced_tids.add(str(m.get("value")))

    orphaned = [t for t in delta_triggers
                if str(t.get("triggerId", "")) not in referenced_tids]
    if orphaned:
        add("Orphaned triggers", "warn",
            f"{len(orphaned)} unreferenced: {[t.get('name') for t in orphaned[:3]]}")
    else:
        add("Orphaned triggers", "pass",
            f"All {len(delta_triggers)} delta triggers are referenced")

    # ── 4. Non-empty delta check ──────────────────────────────────────
    total_elements = len(delta_tags) + len(delta_triggers) + len(delta_vars)
    if total_elements == 0:
        add("Delta content", "fail", "Delta is completely empty — no tags, triggers, or variables")
    else:
        add("Delta content", "pass",
            f"{len(delta_tags)} tags, {len(delta_triggers)} triggers, {len(delta_vars)} variables")

    return _build_result(checks, errors, warnings)

#!/usr/bin/env python3
"""
debug_delta_logic.py — Diagnostic script for the empty-output bug.

Loads a real GTM container + synthetic shadow newsletter crawler data,
runs the full 5-step AI pipeline with instrumented tracing at every gate,
then compiles → heals → validates → delta-exports.

Asserts final delta JSON has >= 1 tag and >= 1 trigger for newsletter sign_up.

Usage:
    GEMINI_API_KEY=... python debug_delta_logic.py --container /path/to/GTM-XXX.json

Exits 0 on success, 1 on empty output.
"""
import argparse
import copy
import json
import os
import sys
import uuid

# ── Synthetic crawler data: mimics a real shadow newsletter form ──────────

SYNTHETIC_CRAWLER_DATA = {
    "url": "https://areon.bg",
    "page_title": "Арион | Счетоводна кантора",
    "platform": "WordPress",
    "data_layer_events": [],
    "has_contact_links": True,
    "has_phone_links": True,
    "has_email_links": True,
    "forms_processed": [
        {
            "form_index": "shadow_0",
            "is_shadow_form": True,
            "is_successful_submission": True,
            "html_attributes": {
                "id": "wpcf7-f28-o1",
                "class": "wpcf7-form newsletter-form",
                "method": "post",
            },
            "fields": [
                {
                    "name": "your-email",
                    "type": "email",
                    "tag": "input",
                    "id": "",
                    "label_text": "Вашият имейл",
                    "placeholder": "email@example.com",
                    "required": True,
                    "is_hidden": False,
                    "is_consent": False,
                }
            ],
            "buttons": [
                {
                    "text": "Абонирай се",
                    "type": "submit",
                    "tag": "button",
                    "class": "wpcf7-submit newsletter-btn",
                }
            ],
            "visible_labels": ["Вашият имейл", "Абонирай се"],
            "surrounding_text": "Абонирайте се за нашия бюлетин",
            "url": "https://areon.bg",
            "page_path": "/",
            "form_classification": "Newsletter",
            "position_on_page": "footer",
            "trigger_signals": [
                {
                    "type": "page_view",
                    "reason": "Interaction cluster detected outside of standard <form> tag. "
                              "Tracking via element visibility of the container.",
                    "trigger_condition": {"selector": "#wpcf7-f28-o1"},
                }
            ],
        }
    ],
}


def main():
    parser = argparse.ArgumentParser(description="Debug delta logic for empty-output bug.")
    parser.add_argument(
        "--container",
        required=True,
        help="Path to real GTM container JSON (e.g., GTM-KZPFSR9V_areon.json)",
    )
    parser.add_argument(
        "--model",
        default="gemini-2.5-flash",
        help="Gemini model name (default: gemini-2.5-flash)",
    )
    args = parser.parse_args()

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: Set GEMINI_API_KEY environment variable.")
        sys.exit(1)
    os.environ["GEMINI_API_KEY"] = api_key

    # ── Load real container ───────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  DEBUG DELTA LOGIC — Tracing Pipeline Gates")
    print(f"{'='*70}\n")

    with open(args.container, "r", encoding="utf-8") as f:
        gtm_data = json.load(f)

    print(f"[LOAD] Container: {args.container}")
    cv = gtm_data.get("containerVersion", gtm_data)
    tag_count = len(cv.get("tag", []))
    trigger_count = len(cv.get("trigger", []))
    var_count = len(cv.get("variable", []))
    print(f"[LOAD] Existing: {tag_count} tags, {trigger_count} triggers, {var_count} variables")

    # ── Extract GTM context (same as pipeline) ────────────────────────────
    from main import extract_gtm_context

    gtm_context = extract_gtm_context(gtm_data)
    ai_context = gtm_context["ai_context"]
    print(f"\n[CONTEXT] Extracted {len(ai_context)} existing elements for dedup:")
    for el in ai_context:
        ev = el.get("event_name", "")
        print(f"  - {el.get('name', '?')}  event={ev}")

    # ── Prepare crawler data ──────────────────────────────────────────────
    crawler_data = copy.deepcopy(SYNTHETIC_CRAWLER_DATA)
    print(f"\n[CRAWLER] Synthetic forms: {len(crawler_data['forms_processed'])}")
    for fd in crawler_data["forms_processed"]:
        print(f"  - index={fd['form_index']}, shadow={fd.get('is_shadow_form')}, "
              f"success={fd.get('is_successful_submission')}, "
              f"class={fd.get('form_classification', '?')}")

    # ── Run 5-step AI pipeline ────────────────────────────────────────────
    from brain import generate_tracking_plan

    print(f"\n{'='*70}")
    print("  RUNNING 5-STEP AI PIPELINE")
    print(f"{'='*70}\n")

    def trace_log(msg):
        """Log callback that highlights gate decisions."""
        prefix = ""
        if "skipped" in msg.lower():
            prefix = "[GATE:SKIP] "
        elif "shadow form" in msg.lower():
            prefix = "[GATE:SHADOW] "
        elif "bypassing" in msg.lower():
            prefix = "[GATE:BYPASS] "
        elif "auditor returned" in msg.lower():
            prefix = "[GATE:AUDIT] "
        elif "keeping original" in msg.lower():
            prefix = "[GATE:GUARD] "
        print(f"  {prefix}{msg}")

    plan_result = generate_tracking_plan(
        crawler_data=crawler_data,
        model=args.model,
        gtm_data={"ai_context": ai_context},
        log_callback=trace_log,
        include_shadow_forms=True,
        include_iframes=True,
    )

    tracking_plan = plan_result.get("tracking_plan", [])
    pipeline_warnings = plan_result.get("pipeline_warnings", {})

    print(f"\n{'='*70}")
    print(f"  PIPELINE RESULT: {len(tracking_plan)} tags generated")
    print(f"{'='*70}")

    if not tracking_plan:
        print("\n[FAIL] Pipeline produced ZERO tags!")
        print("[FAIL] Check gate decisions above for the cause.")
        if pipeline_warnings:
            print(f"[WARN] Pipeline warnings: {json.dumps(pipeline_warnings, indent=2)}")
        sys.exit(1)

    for item in tracking_plan:
        print(f"  - {item.get('tag_name', '?')}  event={item.get('event_name', '?')}  "
              f"shadow={item.get('is_shadow_form', '?')}  "
              f"confidence={item.get('confidence', '?')}")

    # ── Compile: inject → heal → validate → delta export ──────────────────
    from main import add_tag_and_trigger, export_delta_recipe
    from healer import heal_gtm_container
    from validator import validate_gtm_container

    print(f"\n{'='*70}")
    print("  COMPILE PHASE")
    print(f"{'='*70}\n")

    updated_gtm = copy.deepcopy(gtm_data)
    for item in tracking_plan:
        updated_gtm = add_tag_and_trigger(updated_gtm, item)
        print(f"  [INJECT] {item.get('tag_name', '?')}")

    updated_gtm = heal_gtm_container(updated_gtm)
    print("  [HEAL] Container healed.")

    report = validate_gtm_container(updated_gtm)
    score = report.get("score", "?")
    print(f"  [VALIDATE] Score: {score}/100")

    # ── Delta export ──────────────────────────────────────────────────────
    delta_output = export_delta_recipe(updated_gtm, gtm_data)
    delta_cv = delta_output.get("containerVersion", delta_output)
    delta_tags = delta_cv.get("tag", [])
    delta_triggers = delta_cv.get("trigger", [])
    delta_vars = delta_cv.get("variable", [])

    print(f"\n{'='*70}")
    print(f"  DELTA EXPORT RESULT")
    print(f"{'='*70}")
    print(f"  Tags:     {len(delta_tags)}")
    print(f"  Triggers: {len(delta_triggers)}")
    print(f"  Variables:{len(delta_vars)}")

    if delta_tags:
        for t in delta_tags:
            print(f"    - {t.get('name', '?')} (tagId={t.get('tagId')})")
    if delta_triggers:
        for t in delta_triggers:
            print(f"    - {t.get('name', '?')} (triggerId={t.get('triggerId')})")
    if delta_vars:
        for v in delta_vars:
            print(f"    - {v.get('name', '?')} (variableId={v.get('variableId')})")

    # ── Delta validation ──────────────────────────────────────────────────
    from validator import validate_delta

    delta_report = validate_delta(delta_output, gtm_data)
    print(f"\n  [DELTA VALIDATION] Score: {delta_report.get('score', '?')}/100")
    for check in delta_report.get("checks", []):
        icon = "✅" if check["status"] == "pass" else ("⚠️" if check["status"] == "warn" else "❌")
        print(f"    {icon} {check['name']}: {check['detail']}")

    # ── Metadata check ────────────────────────────────────────────────────
    metadata = delta_output.get("_autogtm_metadata")
    if metadata:
        print(f"\n  [METADATA] {json.dumps(metadata)}")
    else:
        print("\n  [METADATA] ⚠️ Missing _autogtm_metadata key!")

    # ── Save output for inspection ────────────────────────────────────────
    out_path = os.path.join(".debug", "debug_delta_output.json")
    os.makedirs(".debug", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(delta_output, f, indent=2, ensure_ascii=False)
    print(f"\n  [SAVED] {out_path}")

    # ── Final assertion ───────────────────────────────────────────────────
    from main import DELTA_ID_OFFSET

    has_signup_tag = any("sign_up" in t.get("name", "").lower() for t in delta_tags)
    has_trigger = len(delta_triggers) >= 1
    has_variables = len(delta_vars) >= 1
    ids_in_high_range = all(
        int(t.get("tagId", 0)) >= DELTA_ID_OFFSET
        for t in delta_tags if str(t.get("tagId", "")).isdigit()
    ) if delta_tags else True
    delta_valid = delta_report.get("passed", False)

    print(f"\n{'='*70}")
    all_pass = has_signup_tag and has_trigger and has_variables and ids_in_high_range and delta_valid
    if all_pass:
        print("  PASS: Delta output contains sign_up tag + trigger + variables, high-range IDs, validation OK")
        print(f"{'='*70}\n")
        sys.exit(0)
    else:
        reasons = []
        if not has_signup_tag:
            reasons.append("No sign_up tag found in delta export")
        if not has_trigger:
            reasons.append("No triggers found in delta export")
        if not has_variables:
            reasons.append("No variables found in delta export (variable naming bug regression!)")
        if not ids_in_high_range:
            reasons.append(f"Tag IDs not in high range (expected >= {DELTA_ID_OFFSET})")
        if not delta_valid:
            reasons.append(f"Delta validation failed: {'; '.join(delta_report.get('errors', []))}")
        print(f"  FAIL: {'; '.join(reasons)}")
        print(f"{'='*70}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()

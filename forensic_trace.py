import asyncio
import json
import os
from crawler import crawl_site
from models import CrawlerOutput
from brain import _step1_analyze_platform, _step2_validate_signals, PipelineContext
from google import genai

async def run_forensic_trace():
    print("🔬 STARTING FORENSIC TRACE FOR areon.bg")
    url = "https://areon.bg/"
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("❌ ERROR: GEMINI_API_KEY not set.")
        return
    client = genai.Client(api_key=api_key)
    model = "gemini-2.0-flash" # Using stable model from the app.py list

    # --- TRACE 1: Crawler ---
    print("\n📦 TRACE 1: CRAWLER LEVEL")
    raw_data = await crawl_site(url, ignore_cache=True)
    
    newsletter_found = False
    for form in raw_data.get("forms_processed", []):
        context = str(form.get("form_title", "")) + " " + str(form.get("surrounding_context", ""))
        is_newsletter = "бюлетин" in context.lower() or "newsletter" in context.lower()
        if is_newsletter or form.get("cf7_form_id") == "28":
            newsletter_found = True
            print(f"✅ Found Newsletter Form (Index: {form.get('form_index')})")
            print(f"   ↳ Title: {form.get('form_title')}")
            print(f"   ↳ Successful Submission: {form.get('is_successful_submission')}")
            print(f"   ↳ DL Events: {len(form.get('datalayer_events', []))}")
            print(f"   ↳ DL Diff: {form.get('datalayer_diff', {}).get('added_keys')}")
            selected_form = form
            break
    
    if not newsletter_found:
        print("❌ DROP POINT: TRACE 1 (Crawler didn't find the newsletter form)")
        return

    # --- TRACE 2: Data Boundary ---
    print("\n🛡️ TRACE 2: DATA BOUNDARY (PYDANTIC)")
    try:
        validated_data = CrawlerOutput.model_validate(raw_data)
        print("✅ Pydantic Validation Passed.")
    except Exception as e:
        print(f"❌ DROP POINT: TRACE 2 (Pydantic ValidationError: {e})")
        return

    # --- TRACE 3: AI Step 1 (Classification) ---
    print("\n🧠 TRACE 3: AI STEP 1 (CLASSIFICATION)")
    ctx = PipelineContext()
    # Note: _step1_analyze_platform expects raw data structure or validated?
    # brain.py:1325: platform_result = _step1_analyze_platform(client, model, crawler_data, ctx=ctx, log_fn=log_fn)
    platform_result = _step1_analyze_platform(client, model, raw_data, ctx=ctx)
    
    ai_newsletter_form = None
    for f in platform_result.forms:
        if str(f.form_index) == str(selected_form.get('form_index')):
            ai_newsletter_form = f
            print(f"✅ Gemini Classification: {f.form_type}")
            print(f"   ↳ Role: {f.form_role}")
            break
            
    if not ai_newsletter_form:
        print("❌ DROP POINT: TRACE 3 (AI failed to return any analysis for this form index)")
        return
        
    if ai_newsletter_form.form_type != "newsletter":
        print(f"⚠️ Warning: Form misclassified as '{ai_newsletter_form.form_type}'")

    # --- TRACE 4: Orchestrator Filter ---
    print("\n✂️ TRACE 4: ORCHESTRATOR FILTER")
    filter_types = ["newsletter"]
    # Logic from brain.py:1346
    type_map = {f.form_index: f.form_type for f in platform_result.forms}
    is_dropped = type_map.get(selected_form.get('form_index')) not in filter_types
    
    if is_dropped:
        print(f"❌ DROP POINT: TRACE 4 (Filtered out because type was '{ai_newsletter_form.form_type}')")
        return
    else:
        print("✅ Survived 'newsletter' filter.")

    # --- TRACE 5: AI Step 2 (Validation) ---
    print("\n⚖️ TRACE 5: AI STEP 2 (VALIDATION)")
    # _step2_validate_signals(client, model, crawler_data, platform_result, ctx=ctx, log_fn=log_fn)
    validations = _step2_validate_signals(client, model, raw_data, platform_result, ctx=ctx)
    
    valid_form = None
    for v in validations:
        if str(v.form_index) == str(selected_form.get('form_index')):
            valid_form = v
            print(f"✅ Success Signal Validation:")
            print(f"   ↳ Genuine Success: {v.is_genuine_success}")
            print(f"   ↳ Method: {v.best_method}")
            print(f"   ↳ Confidence: {v.method_confidence}")
            if not v.is_genuine_success:
                print(f"   ↳ Rejection Reason: {v.rejection_reason}")
            break
            
    if not valid_form or not valid_form.is_genuine_success:
        print("❌ DROP POINT: TRACE 5 (AI rejected the success signals for this form)")
        return

    print("\n🏁 FINAL RESULT: No drop points found in tracing script. If the plan is empty, check Step 3/4/5.")

if __name__ == "__main__":
    asyncio.run(run_forensic_trace())

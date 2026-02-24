
import asyncio
import json
import os
from google import genai
from crawler import crawl_site
from models import CrawlerOutput
from brain import PipelineContext, _step1_analyze_platform, _step2_validate_signals

async def audit():
    print("🔬 STARTING PIPELINE AUDIT FOR areon.bg")
    
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("❌ ERROR: GEMINI_API_KEY not set.")
        return
    client = genai.Client(api_key=api_key)
    model = "gemini-2.0-flash"

    # --- 1. CRAWLER OUTPUT ---
    print("\n📦 1. CRAWLER OUTPUT INTEGRITY")
    url = "https://areon.bg/"
    # Run a fresh crawl with ignore_cache=True to ensure we use the new submission logic
    raw_data = await crawl_site(url, ignore_cache=True)
    
    forms = raw_data.get("forms_processed", [])
    newsletter = None
    for f in forms:
        text = str(f).lower()
        if "newsletter" in text or "бюлетин" in text or "28" in str(f.get("cf7_form_id")):
            newsletter = f
            break
            
    if not newsletter:
        print("❌ CRITICAL: Newsletter form not found in crawler output!")
        return

    print(f"✅ Found Newsletter Form (Index: {newsletter.get('form_index')})")
    print(f"   ↳ Successful Submission: {newsletter.get('is_successful_submission')}")
    print(f"   ↳ DL Events: {len(newsletter.get('datalayer_events', []))}")
    print(f"   ↳ DL Diff: {newsletter.get('datalayer_diff', {}).get('added_keys', [])}")
    print(f"   ↳ AJAX Status: {newsletter.get('has_successful_ajax')} (Count: {len(newsletter.get('ajax_responses', []))})")

    # --- 2. PYDANTIC BOUNDARY ---
    print("\n🛡️ 2. PYDANTIC BOUNDARY CHECK")
    try:
        # Pass the WHOLE raw_data through the Pydantic validator
        validated_output = CrawlerOutput.model_validate(raw_data)
        print("✅ Pydantic Validation Passed for CrawlerOutput.")
    except Exception as e:
        print(f"❌ PYDANTIC ERROR: {e}")
        return

    # --- 3. STEP 1 (ANALYSIS) ---
    print("\n🧠 3. STEP 1 (AI ANALYSIS)")
    ctx = PipelineContext()
    analysis = _step1_analyze_platform(client, model, raw_data, ctx)
    
    found_in_analysis = False
    print(f"   ↳ AI analysis forms count: {len(analysis.forms)}")
    for f in analysis.forms:
        print(f"   ↳ AI Form index: {f.form_index} (Type: {type(f.form_index)}) | Type: {f.form_type}")
        if str(f.form_type) == "newsletter":
            found_in_analysis = True
            print(f"✅ AI Step 1 classified form #{f.form_index} as 'newsletter'.")
    
    if not found_in_analysis:
        print("❌ DROP POINT: AI Step 1 failed to classify the form as newsletter/conversion.")

    # Check for type mismatch specifically
    if newsletter:
        c_idx = newsletter.get('form_index')
        print(f"\n🔍 TYPE MISMATCH CHECK:")
        print(f"   ↳ Crawler Index: {c_idx} (Type: {type(c_idx)})")
        type_map = {f.form_index: f.form_type for f in analysis.forms}
        print(f"   ↳ Type Map Keys: {list(type_map.keys())} (Types: {[type(k) for k in type_map.keys()]})")
        print(f"   ↳ Match Found: {c_idx in type_map}")
        
    # --- 4. STEP 2 (VALIDATION) ---
    print("\n⚖️ 4. STEP 2 (CONFIDENCE AUDIT)")
    validations = _step2_validate_signals(client, model, raw_data, analysis, ctx)
    
    found_in_validation = False
    for v in validations:
        # Note: Step 2 uses form_index
        if v.form_index == newsletter.get('form_index'):
            found_in_validation = True
            print(f"🔍 Audit for Form #{v.form_index}:")
            print(f"   ↳ Is Genuine Success: {v.is_genuine_success}")
            print(f"   ↳ Rejection Reason: {v.rejection_reason}")
            print(f"   ↳ Best Method: {v.best_method}")
            print(f"   ↳ Confidence: {v.method_confidence}")
            
            if not v.is_genuine_success or v.method_confidence < 0.5:
                print("❌ DROP POINT: AI rejected the success signals or confidence too low.")
    
    if not found_in_validation:
        print("❌ DROP POINT: Form did not even reach Step 2 validation (filtered out?)")

if __name__ == "__main__":
    asyncio.run(audit())

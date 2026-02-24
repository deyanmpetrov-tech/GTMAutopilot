import asyncio
import json
import logging
import os
from crawler import crawl_site

async def debug_advance_edu():
    url = "https://kids.advance-edu.org/#offer"
    print(f"🚀 Starting diagnostic run for: {url}")
    
    # Ensure logs aren't too noisy but informative
    def log_callback(msg):
        print(f"DEBUG: {msg}")

    try:
        # Step 1: Run crawler with ignore_cache=True and session isolation
        # We need a session_id to match the new architecture
        session_id = "debug_advance_edu_rep"
        debug_dir = os.path.join(".debug", session_id)
        
        result = await crawl_site(
            url, 
            log_callback=log_callback, 
            ignore_cache=True, 
            debug_dir=debug_dir,
            session_id=session_id
        )
        
        # Save for Phase 2/3
        with open("debug_crawler_result.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"💾 Results saved to debug_crawler_result.json")
        
        print("\n" + "="*50)
        print("🔍 DIAGNOSTIC SUMMARY")
        print("="*50)
        
        forms = result.get("forms_processed", [])
        print(f"Total Forms Found: {len(forms)}")
        
        for i, form in enumerate(forms):
            print(f"\nFORM #{i} (Index: {form.get('form_index')})")
            print(f"  - ID: {form.get('form_id')}")
            print(f"  - Classes: {form.get('form_classes')}")
            print(f"  - Success: {form.get('is_successful_submission')}")
            print(f"  - Shadow Form: {form.get('is_shadow_form', False)}")
            print(f"  - Attributes: {json.dumps(form.get('html_attributes', {}), indent=2)}")
            print(f"  - DataLayer Events Found: {len(form.get('datalayer_events', []))}")
            for event in form.get('datalayer_events', []):
                print(f"    ↳ Event: {event.get('event')}")
        
        if not forms:
            print("\n❌ NO FORMS DETECTED. Checking if global datalayer_events were captured...")
            print(f"Global DataLayer Events: {len(result.get('datalayer_events', []))}")

    except Exception as e:
        print(f"❌ CRITICAL FAILURE DURING DEBUG: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_advance_edu())

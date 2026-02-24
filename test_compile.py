import json
from main import add_tag_and_trigger

with open('dummy_test_container.json', 'r') as f:
    gtm_data = json.load(f)

plan_item = {
    "event_name": "offer_form_submit",
    "trigger_type": "form_submission",
    "trigger_condition": {
        "key": "class",
        "value": "wpcf7-form"
    }
}

updated = add_tag_and_trigger(gtm_data, plan_item)

with open('output_test.json', 'w') as f:
    json.dump(updated, f, indent=2)

print("Saved output_test.json")

import re

with open("main.py", "r") as f:
    content = f.read()

# We need to insert the logic right before calculating the GA4 tag_parameters
# Target anchor: "tag_parameters = ["
# The new logic will read item.get("gtm_payload_keys", []), create variables if they don't exist, and append them.

injection_code = """    # ─── Deep Payload Extraction: Auto-map DLVs for GA4 ───
    payload_params = []
    payload_keys = item.get("gtm_payload_keys", [])
    if payload_keys:
        for k in payload_keys:
            var_name = f"DLV - {k}"
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
                        {"type": "TEMPLATE", "key": "name", "value": k},
                        {"type": "INTEGER",  "key": "dataLayerVersion", "value": "2"},
                        {"type": "BOOLEAN",  "key": "setDefaultValue",  "value": "false"}
                    ]
                })
                print(f"  + Auto-Created DataLayer Variable: {var_name}")
            
            # Map for GA4 Event Settings Table
            payload_params.append({
                "type": "MAP",
                "map": [
                    {"type": "TEMPLATE", "key": "key", "value": k},
                    {"type": "TEMPLATE", "key": "value", "value": f"{{{{{var_name}}}}}"}
                ]
            })

    # Build GA4 Event tag parameters matching GTM's required schema for gaawe"""

new_content = content.replace("    # Build GA4 Event tag parameters matching GTM's required schema for gaawe", injection_code)

# Now we need to inject the `payload_params` into the `eventSettingsTable`
# Old: {"type": "LIST",     "key": "eventSettingsTable", "list": []},
# New: {"type": "LIST",     "key": "eventSettingsTable", "list": payload_params},

new_content = new_content.replace(
    '{"type": "LIST",     "key": "eventSettingsTable", "list": []}',
    '{"type": "LIST",     "key": "eventSettingsTable", "list": payload_params}'
)


with open("main.py", "w") as f:
    f.write(new_content)

print("Patch applied to main.py successfully.")

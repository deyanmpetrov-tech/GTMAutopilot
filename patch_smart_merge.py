with open("main.py", "r") as f:
    content = f.read()

# 1. Patch Advanced Type Inference for DataLayer variables
old_dl_logic = """    # ─── Deep Payload Extraction: Auto-map DLVs for GA4 ───
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
            })"""

new_dl_logic = """    # ─── Phase 8: Advanced Type Inference (Variable Reuse) ───
    payload_params = []
    payload_keys = item.get("gtm_payload_keys", [])
    if payload_keys:
        for k in payload_keys:
            inferred_var_name = None
            for v in cv.get("variable", []):
                if v.get("type") == "v":
                    for p in v.get("parameter", []):
                        if p.get("key") == "name" and p.get("value") == k:
                            inferred_var_name = v.get("name")
                            break
                if inferred_var_name:
                    break
            
            if inferred_var_name:
                print(f"  + Phase 8 Inference: Safely reusing existing variable '{inferred_var_name}' for key '{k}'")
                final_var_name = inferred_var_name
            else:
                final_var_name = f"DLV - {k}"
                existing_vars_check = [v.get("name") for v in cv.get("variable", [])]
                if final_var_name not in existing_vars_check:
                    var_id = str(get_max_id(cv["variable"], "variableId") + 1)
                    cv["variable"].append({
                        "accountId": account_id,
                        "containerId": container_id,
                        "variableId": var_id,
                        "name": final_var_name,
                        "type": "v",
                        "parameter": [
                            {"type": "TEMPLATE", "key": "name", "value": k},
                            {"type": "INTEGER",  "key": "dataLayerVersion", "value": "2"},
                            {"type": "BOOLEAN",  "key": "setDefaultValue",  "value": "false"}
                        ]
                    })
                    print(f"  + Auto-Created DataLayer Variable: {final_var_name}")
            
            payload_params.append({
                "type": "MAP",
                "map": [
                    {"type": "TEMPLATE", "key": "key", "value": k},
                    {"type": "TEMPLATE", "key": "value", "value": f"{{{{{final_var_name}}}}}"}
                ]
            })"""

if old_dl_logic in content:
    content = content.replace(old_dl_logic, new_dl_logic)
else:
    print("WARNING: Could not find old DL logic block.")

# 2. Patch Config Tag Piggybacking (Lines 406-420)
old_config_logic = """    # GA4 Config tags (googtag) use key='tagId', older gaawc use key='measurementId'
    inherited_measurement_id = None
    for existing_tag in cv.get("tag", []):
        tag_type = existing_tag.get("type", "")
        if tag_type in ["googtag", "gaawc", "google_tag"]:
            for param in existing_tag.get("parameter", []):
                # googtag uses 'tagId', gaawc uses 'measurementId'
                if param.get("key") in ["tagId", "measurementId"]:
                    val = param.get("value", "")
                    if val and val.startswith("G-"):  # Only GA4 measurement IDs
                        inherited_measurement_id = val
                        break
        if inherited_measurement_id:
            break"""

new_config_logic = """    # --- Phase 8: Config Tag Piggybacking (Inheritance) ---
    inherited_measurement_id = None
    for existing_tag in cv.get("tag", []):
        tag_type = existing_tag.get("type", "")
        # Safely extract Measurement ID or Variable Reference (e.g. {{GA4 ID}}) to inherit global settings without redundancy
        if tag_type in ["googtag", "gaawc", "google_tag"]:
            for param in existing_tag.get("parameter", []):
                if param.get("key") in ["tagId", "measurementId"]:
                    val = param.get("value", "")
                    if val and (val.startswith("G-") or "{{" in val):
                        inherited_measurement_id = val
                        print(f"  + Phase 8 Piggybacking: Inheriting global measurement ID/Tag ({val}) from existing Config tag.")
                        break
        if inherited_measurement_id:
            break"""

if old_config_logic in content:
    content = content.replace(old_config_logic, new_config_logic)
else:
    print("WARNING: Could not find old config tag scan logic.")

with open("main.py", "w") as f:
    f.write(content)

print("Smart Container Merging patch applied to main.py successfully!")

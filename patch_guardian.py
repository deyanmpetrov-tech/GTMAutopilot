import json

with open("main.py", "r") as f:
    content = f.read()

GUARDIAN_SCRIPT = """<script id="gtm-auto-guardian" type="text/javascript">
(function() {
  'use strict';
  // List of form IDs or Classes provided by AutoGTM AI
  var formSelectors = {{FORM_SELECTORS}};
  
  document.addEventListener('click', function(e) {
      if(!e.target) return;
      var btn = e.target.closest('button[type="submit"], input[type="submit"]');
      if(btn) {
          var form = btn.closest('form');
          if(form) {
              // Check if this form matches our generated selectors
              var matches = formSelectors.some(function(sel) {
                  return form.matches(sel);
              });
              
              if(matches && form.checkValidity()) {
                  // Only push if the browser's native HTML5 validation passes
                  window.dataLayer = window.dataLayer || [];
                  window.dataLayer.push({
                      'event': 'html5_verified_submit',
                      'form_id': form.id || '',
                      'form_class': form.className || ''
                  });
              }
          }
      }
  });

})();
</script>"""

injection_code = """
        # --- Phase 7: HTML5 Validation Guardian ---
        guardian_selectors = []
        for item in plan.get("tracking_plan", []):
            if item.get("trigger_type") == "html5_validation_guardian":
                cond = item.get("trigger_condition", {})
                if cond.get("key") == "id" and cond.get("value"):
                    guardian_selectors.append(f"#{cond.get('value')}")
                elif cond.get("key") == "class" and cond.get("value"):
                    guardian_selectors.append(f".{cond.get('value').replace(' ', '.')}")

        if guardian_selectors:
            cv = updated_gtm.get("containerVersion", {})
            account_id = cv.get("container", {}).get("accountId", "0")
            container_id = cv.get("container", {}).get("containerId", "0")
            guardian_id = str(max([int(x.get("tagId", 0)) for x in cv.get("tag", []) if str(x.get("tagId", '0')).isdigit()] + [0]) + 1)
            
            script_str = """ + json.dumps(GUARDIAN_SCRIPT) + """.replace("{{FORM_SELECTORS}}", json.dumps(list(set(guardian_selectors))))
            
            guardian_tag = {
                "accountId": account_id,
                "containerId": container_id,
                "tagId": f"{guardian_id}000",
                "name": "Auto - HTML5 Validation Guardian",
                "type": "html",
                "parameter": [
                    {
                        "type": "TEMPLATE",
                        "key": "html",
                        "value": script_str
                    }
                ],
                "firingTriggerId": ["2147479553"] # All Pages
            }
            
            try:
                validated_guardian = GtmTag(**guardian_tag).model_dump(exclude_none=True)
                cv.setdefault("tag", []).append(validated_guardian)
                print(f"  ++ Injected HTML5 Validation Guardian for selectors: {', '.join(guardian_selectors)} ++")
            except ValidationError as e:
                print(f"Failed to inject HTML5 Guardian: {e}")
"""

target = """        # --- Phase 7: Cross-Domain Redirect Stitching ---"""

new_content = content.replace(target, injection_code + "\n" + target)


trigger_injection = """
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
"""

trigger_target = """    elif trigger_type == "click_links":"""
new_content = new_content.replace(trigger_target, trigger_injection + trigger_target)


with open("main.py", "w") as f:
    f.write(new_content)

print("Patch applied to main.py to inject HTML5 Validation Guardian logic.")

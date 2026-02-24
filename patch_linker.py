import re

with open("main.py", "r") as f:
    content = f.read()

# We need to inject the Cross-Domain Linker logic after the harvester logic or the main container modifications.
# A good place is right before the `return container` block in `main()`, specifically where we loop over `plan["tracking_plan"]` to check for `requires_harvester` or `requires_ajax_listener`. We can add another check for `cross_domain_redirect`.

injection_code = """
        # --- Phase 7: Cross-Domain Redirect Stitching ---
        requires_cross_domain = False
        external_domains = []
        for item in plan.get("tracking_plan", []):
            if item.get("trigger_type") == "cross_domain_redirect":
                requires_cross_domain = True
                ext_domain = item.get("trigger_condition", {}).get("external_domain")
                if ext_domain and ext_domain not in external_domains:
                    external_domains.append(ext_domain)

        if requires_cross_domain and external_domains:
            cv = updated_gtm.get("containerVersion", {})
            account_id = cv.get("container", {}).get("accountId", "0")
            container_id = cv.get("container", {}).get("containerId", "0")
            linker_id = str(max([int(x.get("tagId", 0)) for x in cv.get("tag", []) if str(x.get("tagId", '0')).isdigit()] + [0]) + 1)
            
            # Use the "All Pages" trigger to ensure linker works on the form page before redirect
            trigger_id = "2147479553" 
            
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
                        "list": [
                            {
                                "type": "MAP",
                                "map": [
                                    {"type": "TEMPLATE", "key": "crossDomainAutoLinkTarget", "value": d}
                                ]
                            } for d in external_domains
                        ]
                    },
                    {
                        "type": "BOOLEAN",
                        "key": "crossDomainDecorateForms",
                        "value": "true"
                    }
                ],
                "firingTriggerId": [trigger_id]
            }
            
            try:
                validated_linker = GtmTag(**linker_tag).model_dump(exclude_none=True)
                cv.setdefault("tag", []).append(validated_linker)
                print(f"  ++ Injected Cross-Domain Linker for domains: {', '.join(external_domains)} ++")
            except ValidationError as e:
                print(f"Failed to inject Cross-Domain Linker: {e}")
"""

target = """                print(f"Failed to inject Harvester listener: {e}")"""

new_content = content.replace(target, target + injection_code)

with open("main.py", "w") as f:
    f.write(new_content)

print("Patch applied to main.py to inject Cross-Domain Linker logic.")

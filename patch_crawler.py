with open("crawler.py", "r") as f:
    lines = f.readlines()

out = []
in_form_block = False

for i, line in enumerate(lines):
    if "async def crawl_site(" in line:
        out.append(line.replace("log_callback=None)", "log_callback=None, consent_only=False)"))
        continue
        
    if "# 1. Contact links" in line:
        out.append("        if not consent_only:\n")
        in_form_block = True
        
    if in_form_block and "result[\"datalayer_events\"] = datalayer_events" in line:
        in_form_block = False
        
    if in_form_block:
        out.append("    " + line if line.strip() else "\n")
    else:
        out.append(line)

with open("crawler.py", "w") as f:
    f.writelines(out)

print("Saved crawler.py patch")

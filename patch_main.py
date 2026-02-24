import re

with open('main.py', 'r') as f:
    content = f.read()

# Replace all try..except Pydantic dumps with direct appends
content = re.sub(
    r'try:\s*cv\["variable"\]\.append\(GtmVariable\(\*\*new_var\)\.model_dump\(exclude_none=True\)\)\s*print\(f"  \+ Auto-Created Variable: \{variable_name\}"\)\s*except ValidationError as e:\s*print\(f"Failed to create variable \{variable_name\}: \{e\}"\)',
    r'cv["variable"].append(new_var)\n                print(f"  + Auto-Created Variable: {variable_name}")',
    content
)

content = re.sub(
    r'try:\s*cv\["trigger"\]\.append\(GtmTrigger\(\*\*new_trigger\)\.model_dump\(exclude_none=True\)\)\s*except ValidationError as e:\s*print\(f"Failed to create Trigger: \{e\}"\)',
    r'cv["trigger"].append(new_trigger)',
    content
)

content = re.sub(
    r'try:\s*cv\["tag"\]\.append\(GtmTag\(\*\*new_tag\)\.model_dump\(exclude_none=True\)\)\s*except ValidationError as e:\s*print\(f"Failed to create Tag: \{e\}"\)',
    r'new_tag["tagFiringOption"] = "ONCE_PER_EVENT"\n    cv["tag"].append(new_tag)',
    content
)

content = re.sub(
    r'try:\s*cv\["tag"\]\.append\(GtmTag\(\*\*new_listener_tag\)\.model_dump\(exclude_none=True\)\)\s*except ValidationError as e:\s*print\(f"Failed to create Auto-Event Listener Tag: \{e\}"\)',
    r'new_listener_tag["tagFiringOption"] = "ONCE_PER_PAGE"\n            cv["tag"].append(new_listener_tag)',
    content
)

with open('main.py', 'w') as f:
    f.write(content)
print("main.py patched successfully")

import os
def patch_app():
    with open('app.py', 'r') as f:
        content = f.read()
    
    # Temporarily hardcode the dummy_gtm.json if no file is uploaded so the browser can click Generate
    if "# --- TEST PATCH ---" not in content:
        patched = content.replace(
            "gtm_file = st.file_uploader(\"Upload base GTM Container (JSON)\", type=\"json\")",
            "gtm_file = st.file_uploader(\"Upload base GTM Container (JSON)\", type=\"json\")\n        # --- TEST PATCH ---\n        if not gtm_file:\n            with open('dummy_gtm.json', 'rb') as dummy:\n                import io\n                gtm_file = io.BytesIO(dummy.read())\n                gtm_file.name = 'dummy_gtm.json'\n        # ------------------"
        )
        with open('app.py', 'w') as f:
            f.write(patched)
        print("Patched app.py to auto-load dummy_gtm.json if no file is uploaded.")
    else:
        print("Already patched.")

if __name__ == "__main__":
    patch_app()

import os

with open("app.py", "r") as f:
    lines = f.readlines()

out = []
builder_lines = []

in_main = False
for i, line in enumerate(lines):
    if "─── Main Layout ───" in line:
        in_main = True
        out.append(line)
        out.append("tab_builder, tab_validator = st.tabs(['🏷️ AutoGTM Builder', '🛡️ Consent Validator'])\n\n")
        out.append("with tab_builder:\n")
        continue
        
    if in_main:
        # Indent the builder lines
        out.append("    " + line if line.strip() else "\n")
    else:
        out.append(line)

# Append the validator tab logic
validator_code = """
with tab_validator:
    col_v1, col_v2 = st.columns([1, 1])
    
    with col_v1:
        st.header("1. Validation Target")
        val_url = st.text_input(
            "Website URL",
            value=_cfg.get("url", ""),
            placeholder="https://example.com",
            key="validator_url"
        )
        val_button = st.button("🛡️ Run Consent Validation", type="primary", disabled=not val_url)
        
    with col_v2:
        st.header("2. Validation Log")
        val_log_ph = st.empty()
        
    if val_button:
        save_config(gemini_key, val_url, selected_model)
        
        v_log_lines = []
        def vlog(msg: str, icon: str = "ℹ️"):
            v_log_lines.append(f"{icon} {msg}")
            val_log_ph.markdown("\\n\\n".join(v_log_lines))
            
        vlog(f"**Analyzing Consent Mode** on `{val_url}`", "🔄")
        with st.spinner("Intercepting payload and chronologies..."):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            def vcrawl_log(msg):
                vlog(msg, "   ↳")

            v_data = loop.run_until_complete(crawl_site(val_url, log_callback=vcrawl_log, consent_only=True))
            
        consent = v_data.get("consent_mode_report", {})
        
        st.divider()
        if consent.get("status") == "NOT_DETECTED":
            st.error("❌ **No Consent Mode detected on this website!**")
        else:
            status_color = "🟢" if consent["status"].startswith("VALID") else "🔴"
            version_text = f" (v{consent.get('version', '?')})" if consent.get("version") else ""
            st.subheader(f"🛡️ Consent Mode: {status_color} {consent['status']}{version_text}")
            
            if consent.get("errors"):
                for err in consent["errors"]:
                    st.error(f"**Error:** {err}")
            if consent.get("warnings"):
                for warn in consent["warnings"]:
                    st.warning(f"**Warning:** {warn}")
                    
            if consent["status"].startswith("VALID"):
                st.success("✅ **Consent Mode v2 is installed correctly!** The sequence and payload schema comply with Google Ads policies.")
"""
out.append(validator_code)

with open("app.py", "w") as f:
    f.writelines(out)

print("Saved app.py refactored")

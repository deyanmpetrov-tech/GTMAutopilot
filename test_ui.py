import asyncio
from playwright.async_api import async_playwright

async def run_test():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print("Opening Streamlit UI at http://localhost:8501/...")
        await page.goto("http://localhost:8501/")
        
        print("Waiting for UI to load...")
        # Since standard get_by_label on streamlit sometimes fails on text inputs if the label is disjoint, 
        # we can just use locator with aria-label or just rely on get_by_label
        await page.wait_for_selector('input[aria-label="Target URL"]', timeout=10000)
        await page.fill('input[aria-label="Target URL"]', "http://localhost:3000/")
        
        print("Uploading GTM Container...")
        file_input = page.locator('input[type="file"]')
        await file_input.set_input_files("/Users/deyanpetrov/Documents/GTMAutopilot/AutoGTM_Enhanced_Container.json")
        
        print("Wait for upload to register...")
        await page.wait_for_timeout(2000)
        
        print("Clicking Start Button...")
        await page.get_by_role("button", name="🚀 Analyze & Generate").click()
        
        print("Waiting for generation process (up to 120s)...")
        # In Streamlit, Download buttons are links with a download attribute
        try:
            await page.wait_for_selector('a[download]', timeout=60000)
            print("Process complete! Taking screenshot...")
            await page.screenshot(path="e2e_ui_success.png", full_page=True)
            print("Screenshot saved to e2e_ui_success.png")
            
            # Click download
            async with page.expect_download() as download_info:
                await page.click('a[download]')
            download = await download_info.value
            await download.save_as("downloaded_gtm_test.json")
            print(f"Downloaded generated file to {download.suggested_filename}")
        except Exception as e:
            print(f"Test failed or timed out: {e}")
            await page.screenshot(path="e2e_ui_failure.png", full_page=True)
            print("Screenshot saved to e2e_ui_failure.png")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_test())


import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://developers.google.com/earth-engine/datasets/catalog/GOOGLE_DYNAMICWORLD_V1")
        
        # Click the "Open in Code Editor" button
        await page.click('a:has-text("Open in Code Editor")')
        
        # Wait for the new page to open
        await page.wait_for_timeout(5000)

        # Get the new page
        pages = browser.contexts[0].pages
        page = pages[-1]

        await page.screenshot(path="code_editor.png")
        with open("code_editor.html", "w") as f:
            f.write(await page.content())
            
        await browser.close()

asyncio.run(main())

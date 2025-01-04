import asyncio
from playwright.async_api import async_playwright
from time import time

async def click_visible_button(page, text: str):
    """
    Clicks the first visible button containing the specified text.
    """
    try:
        print(f"Clicking button with text: {text}")
        await page.click(f"text='{text}'")
        print("Button clicked successfully.")
    except Exception as e:
        print(f"Error clicking button: {e}")

async def scrape_play_store_html(app_id: str, scroll_timeout: int = 5) -> str:
    """
    Scrapes the Google Play Store for a given app.
    Uses explicit mouse scrolling to fetch and save the HTML structure.
    Saves only the last HTML after scrolling is completed.
    """
    base_url = f"https://play.google.com/store/apps/details?id={app_id}&hl=en"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Visible browser for debugging
        page = await browser.new_page()

        try:
            print("Opening app page...")
            await page.goto(base_url)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(2)  # Asynchronous sleep

            # Click "See all reviews" button
            print("Clicking 'See all reviews' button...")
            await click_visible_button(page, "See all reviews")
            await asyncio.sleep(2)

            # Click the sort dropdown and select "Newest"
            print("Applying filters to sort by 'Newest'...")
            await click_visible_button(page, "Most relevant")
            await asyncio.sleep(1)
            await click_visible_button(page, "Newest")
            await asyncio.sleep(1)

            # Initialize variables for scrolling
            print("Scrolling with mouse wheel and collecting HTML...")
            start_time = time()
            last_html = ""

            while True:
                # Check if the timeout has been reached
                if time() - start_time > scroll_timeout:
                    print("Scroll timeout reached. Stopping scrolling.")
                    break

                # Perform explicit mouse scroll
                await page.mouse.wheel(0, 10000)  # Scroll down by 1000 pixels
                await asyncio.sleep(0.1)  # Minimal delay for smooth scrolling

                # Get the current HTML
                current_html = await page.content()

                # Detect if more content is loading
                if current_html != last_html:
                    last_html = current_html  # Update with the latest content
                else:
                    print("No more content to load. Stopping scrolling.")
                    break

            print("Finished scraping and saved the last HTML.")
            return last_html

        except Exception as e:
            print(f"Error during scraping: {str(e)}")
        finally:
            await browser.close()

# Example of how to run the asynchronous function
# if __name__ == "__main__":
#     app_id = "com.application.zomato"  # Replace with a real app ID
#     result = asyncio.run(scrape_play_store_html(app_id))
#     print(result)

from selenium import webdriver
import time
import sys
import pandas as pd
import random

# Load the hyperlinks from a text file
def load_links(file_path, url_column = "query url"):
    """
    Reads a text file containing hyperlinks (one per line) and returns a list of links.

    Args:
        file_path (str): Path to a csv file containing links, one per line
        url_column (str): The column name in the CSV file that contains the URLs

    Returns:
        list: A list of non-empty, stripped hyperlinks.
    """
    df = pd.read_csv(file_path, dtype=str)
    links = [link.strip() for link in df[url_column].dropna() if link.strip()]
    return links


# Open each link in a new window
def open_links_in_new_windows(links):
    """
    Opens each link in a new browser window using Selenium WebDriver.

    Args:
        links (list): List of hyperlinks to open.

    Behavior:
        - Opens the first link in the main window.
        - Opens each subsequent link in a new tab/window.
        - Waits for user input before closing all windows.
    """
    driver = webdriver.Chrome()  # Change to webdriver.Firefox() or others if needed

    try:
        # Open each link in a new window
        for i, link in enumerate(links):
            if i == 0:
                driver.get(link)  # Open first link in the main window
            else:
                driver.execute_script(f"window.open('{link}', '_blank');")  # Open each additional link in a new window
            
            # Add a short delay to avoid overwhelming the browser
            time.sleep(5+random.uniform(0,3))
        
        # Switch between windows to ensure they're loaded
        for i in range(len(links)):
            driver.switch_to.window(driver.window_handles[i])
            time.sleep(0.2)  # Optional delay for viewing each window before switching
    finally:
        input("Press Enter to close all windows and quit...")  # Wait for user input to close windows
        driver.quit()

def main():
    """
    Main entry point for the script.

    Reads command-line arguments to get the file path, loads links, and opens them in browser windows.
    """
    args = sys.argv[1:]
    if len(args) >= 1:
        file_path = args[0]
    else:
        raise TypeError

    links = load_links(file_path)
    print("Opening links in batches of 10...")
    num_links = len(links)
    num_batches = (num_links + 9) // 10  # Calculate number of batches of 20
    for batch in range(num_batches):
        batch_links = links[batch*10:(batch+1)*10]
        print(f"Opening batch {batch + 1} with {len(batch_links)} links...")
        open_links_in_new_windows(batch_links)
        print("Press Enter to continue to the next batch...")
        input()

if __name__ == "__main__":
    main()

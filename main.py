import os
import time
import random
import traceback
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ===== Dispider SDK =====
from dispider import get_next_task, submit_task_result, report_task_failure, report_needs_manual_intervention

# ===== 硬编码账号密码 =====
DISPIDER_USER = "2950174609@qq.com"
DISPIDER_PASS = "Jixiaorui060104@"

# ===== 结果列定义 =====
RESULT_COLUMNS = [
    "type", "ticker", "user_name", "post_date", "content", "comment", "retweet", "like", "view"
]

class StockTweetsScraper:
    def __init__(self, headless=True, proxy=None):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        if proxy:
            chrome_options.add_argument(f"--proxy-server={proxy}")

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)
        self.logged_in = False

    def login(self):
        if self.logged_in:
            return
        self.driver.get("https://x.com/login")
        try:
            username_input = self.wait.until(EC.presence_of_element_located((By.NAME, "text")))
            username_input.send_keys(USERNAME)
            username_input.send_keys("\n")
            time.sleep(2)

            password_input = self.wait.until(EC.presence_of_element_located((By.NAME, "password")))
            password_input.send_keys(PASSWORD)
            password_input.send_keys("\n")
            time.sleep(5)

            if "home" in self.driver.current_url:
                self.logged_in = True
            else:
                raise Exception("Login failed")
        except Exception as e:
            raise Exception(f"Login error: {e}")

    def search_stock(self, ticker, since_date, max_tweets=1):
        url = f"https://x.com/search?q=%24{ticker}%20since%3A{since_date}&src=typed_query&f=live"
        self.driver.get(url)
        time.sleep(3)
        tweets = self.driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
        results = []
        for tweet in tweets[:max_tweets]:
            try:
                user = tweet.find_element(By.XPATH, ".//div[@data-testid='User-Name']").text
                content = tweet.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
                stats = {k: 0 for k in ["comment", "retweet", "like", "view"]}
                for stat_key, testid in zip(stats.keys(), ["reply", "retweet", "like", "view"]):
                    try:
                        stats[stat_key] = int(tweet.find_element(By.XPATH, f".//div[@data-testid='{testid}']").text or 0)
                    except:
                        pass
                results.append({
                    "type": "post",
                    "ticker": ticker,
                    "user_name": user,
                    "post_date": since_date,
                    "content": content,
                    **stats
                })
            except Exception:
                continue
        return results

    def scrape_replies(self, tweet_url, max_replies=1):
        self.driver.get(tweet_url)
        time.sleep(3)
        replies = self.driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
        results = []
        for reply in replies[1:max_replies+1]:
            try:
                user = reply.find_element(By.XPATH, ".//div[@data-testid='User-Name']").text
                content = reply.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
                results.append({
                    "type": "reply",
                    "ticker": "-",
                    "user_name": user,
                    "post_date": "-",
                    "content": content,
                    "comment": 0,
                    "retweet": 0,
                    "like": 0,
                    "view": 0
                })
            except Exception:
                continue
        return results

    def close(self):
        self.driver.quit()

def run_worker():
    scraper = StockTweetsScraper(headless=True)
    try:
        scraper.login()
        while True:
            task = get_next_task()
            if not task:
                time.sleep(5)
                continue
            try:
                data = task.data
                results = []
                if data.get("scrape_replies") and data.get("tweet_url"):
                    results = scraper.scrape_replies(data["tweet_url"], data.get("max_replies", 1))
                elif data.get("stock"):
                    results = scraper.search_stock(data["stock"], data.get("since_date", "2025-01-01"), data.get("max_tweets", 1))
                if results:
                    first = results[0]
                    filtered = {col: first.get(col, None) for col in RESULT_COLUMNS}
                    submit_task_result(task.id, filtered)
                else:
                    report_task_failure(task.id, "No results found")
            except Exception as e:
                if "captcha" in str(e).lower():
                    report_needs_manual_intervention(task.id, "Captcha or block detected")
                else:
                    report_task_failure(task.id, str(e))
    finally:
        scraper.close()

if __name__ == "__main__":
    run_worker()

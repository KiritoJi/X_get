"""
Twitter股票标签($TSLA等)爬虫 — 终极版（一次性解析互动数据）
"""

# 导入dispider客户端
from dispider import Dispider

# 配置你的账号密码
DISPIDER_USER = "2950174609@qq.com"
DISPIDER_PASS = "Jixiaorui060104@"

# 初始化客户端
# 客户端会自动处理登录和 Token 管理
client = Dispider(username=DISPIDER_USER, password=DISPIDER_PASS)

import os
import time
import random
import json
import logging
import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException
)
import re

# ========================= 日志配置 =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_tweets_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("StockTweetsScraper")


class StockTweetsScraper:
    """Twitter股票标签爬虫类"""

    def __init__(self, headless=False, use_proxy=False, data_dir="data"):
        self.headless = headless
        self.use_proxy = use_proxy
        self.data_dir = data_dir
        self.driver = None
        Path(data_dir).mkdir(exist_ok=True)

        # 用户代理池（随机切换防止被封）
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
        ]

    def setup_driver(self):
        """配置并启动Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
        if self.headless:
            chrome_options.add_argument("--headless")
        if self.use_proxy:
            PROXY = "http://your-proxy-address:port"
            chrome_options.add_argument(f'--proxy-server={PROXY}')
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            return driver
        except WebDriverException as e:
            logger.error(f"启动WebDriver出错: {e}")
            raise

    def _login_and_keep_session(self):
        """✅ 一次登录保持Session"""
        logger.info("请在60秒内完成Twitter登录...")
        self.driver.get("https://twitter.com/login")
        time.sleep(60)
        logger.info("假设已完成登录，将继续执行")

    def _search_stock_after_login(self, stock_symbol, since_date):
        """✅ 登录后若跳转到首页，自动搜索股票"""
        logger.info("检测到跳转首页，自动执行搜索...")
        try:
            search_box = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[data-testid='SearchBox_Search_Input']"))
            )
            search_box.clear()
            search_box.send_keys(f"${stock_symbol} since:{since_date}")
            search_box.send_keys(u'\ue007')
            time.sleep(5)
        except:
            logger.error("自动搜索股票时出错")

    def _parse_count(self, count_str):
        """解析计数字符串为整数（支持K、M）"""
        if not count_str or count_str == "":
            return 0
        count_str = count_str.replace(",", "").strip()
        if "K" in count_str:
            return int(float(count_str.replace("K", "")) * 1000)
        elif "M" in count_str:
            return int(float(count_str.replace("M", "")) * 1000000)
        else:
            try:
                return int(count_str)
            except:
                return 0

    def _get_interaction_counts(self, tweet):
        """
        ✅ 从 aria-label 一次性提取互动数据
        返回：comment, retweet, like, view
        """
        comment = retweet = like = view = 0
        try:
            group = tweet.find_element(By.XPATH, ".//div[@role='group'][@aria-label]")
            aria = group.get_attribute("aria-label") or ""
            # 用正则匹配数字
            replies_match = re.search(r"([\d,]+)\s+repl", aria)
            reposts_match = re.search(r"([\d,]+)\s+repost", aria)
            likes_match = re.search(r"([\d,]+)\s+like", aria)
            views_match = re.search(r"([\d,]+)\s+view", aria)

            comment = self._parse_count(replies_match.group(1)) if replies_match else 0
            retweet = self._parse_count(reposts_match.group(1)) if reposts_match else 0
            like = self._parse_count(likes_match.group(1)) if likes_match else 0
            view = self._parse_count(views_match.group(1)) if views_match else 0
        except:
            pass
        return comment, retweet, like, view

    def scrape_stock_tweets(self, stock_symbol, max_tweets=100, since_date="2025-01-01"):
        """✅ 抓取推文列表"""
        try:
            self.driver = self.setup_driver()
            self._login_and_keep_session()
            search_url = f"https://twitter.com/search?q=%24{stock_symbol}%20since%3A{since_date}&src=typed_query&f=live"
            self.driver.get(search_url)
            time.sleep(5)
            if "home" in self.driver.current_url:
                self._search_stock_after_login(stock_symbol, since_date)

            tweets_data = []
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            while len(tweets_data) < max_tweets:
                tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, "article[data-testid='tweet']")
                for tweet in tweet_elements:
                    if len(tweets_data) >= max_tweets:
                        break
                    try:
                        # ✅ 基础字段
                        user_name = tweet.find_element(By.CSS_SELECTOR, "div[data-testid='User-Name'] span").text
                        content = tweet.find_element(By.CSS_SELECTOR, "div[data-testid='tweetText']").text \
                            if tweet.find_elements(By.CSS_SELECTOR, "div[data-testid='tweetText']") else ""
                        post_date = tweet.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime")
                        # ✅ 互动数据（一次性解析）
                        comment, retweet, like, view = self._get_interaction_counts(tweet)
                        # ✅ 真实推文链接
                        tweet_url = tweet.find_element(By.CSS_SELECTOR, "a[href*='/status/']").get_attribute("href")

                        tweet_data = {
                            "type": "tweet",
                            "ticker": stock_symbol,
                            "user_name": user_name,
                            "post_date": post_date,
                            "content": content,
                            "comment": comment,
                            "retweet": retweet,
                            "like": like,
                            "view": view,
                            "tweet_url": tweet_url  # ✅ 临时保留，用于抓取回复
                        }
                        tweets_data.append(tweet_data)
                        logger.info(f"已抓取 {len(tweets_data)} 条推文")
                    except Exception as e:
                        logger.error(f"处理推文时出错: {e}")
                        continue

                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(2, 4))
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            return pd.DataFrame(tweets_data)
        except Exception as e:
            logger.error(f"抓取推文时出错: {e}")
            return pd.DataFrame()

    def scrape_tweet_replies(self, tweet_url, stock_symbol, max_replies=50):
        """✅ 抓取单条推文下的回复"""
        try:
            self.driver.get(tweet_url)
            time.sleep(5)
            replies_data = []
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            while len(replies_data) < max_replies:
                reply_elements = self.driver.find_elements(By.CSS_SELECTOR, "article[data-testid='tweet']")
                for reply in reply_elements:
                    if len(replies_data) >= max_replies:
                        break
                    try:
                        user_name = reply.find_element(By.CSS_SELECTOR, "div[data-testid='User-Name'] span").text
                        content = reply.find_element(By.CSS_SELECTOR, "div[data-testid='tweetText']").text \
                        if reply.find_elements(By.CSS_SELECTOR, "div[data-testid='tweetText']") else ""
                        post_date = reply.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime")
                        # ✅ 一次性解析互动数据
                        comment, retweet, like, view = self._get_interaction_counts(reply)

                        reply_data = {
                            "type": "reply",
                            "ticker": stock_symbol,
                            "user_name": user_name,
                            "post_date": post_date,
                            "content": content,
                            "comment": comment,
                            "retweet": retweet,
                            "like": like,
                            "view": view
                        }
                        replies_data.append(reply_data)
                        logger.info(f"已抓取 {len(replies_data)} 条回复")
                    except Exception:
                        continue

                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            return pd.DataFrame(replies_data)
        except Exception as e:
            logger.error(f"抓取回复时出错: {e}")
            return pd.DataFrame()

    def save_all_to_one_json(self, all_data, export_dir):
        """✅ 所有数据一次保存到一个JSON文件（最终去掉 tweet_url）"""
        try:
            for d in all_data:
                d.pop("tweet_url", None)
            Path(export_dir).mkdir(parents=True, exist_ok=True)
            file_path = os.path.join(export_dir, f"all_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            logger.info(f"已保存所有数据至 {file_path}")
        except Exception as e:
            logger.error(f"保存数据时出错: {e}")


def parse_arguments():
    parser = argparse.ArgumentParser(description='Twitter股票标签爬虫')
    parser.add_argument('--stock', type=str, required=True, help='股票标签，如TSLA')
    parser.add_argument('--max_tweets', type=int, default=5)
    parser.add_argument('--since_date', type=str, default='2025-01-01')
    parser.add_argument('--max_replies', type=int, default=3)
    parser.add_argument('--headless', action='store_true')
    parser.add_argument('--use_proxy', action='store_true')
    parser.add_argument('--data_dir', type=str, default='data')
    parser.add_argument('--scrape_replies', action='store_true')
    return parser.parse_args()


def main():
    args = parse_arguments()
    scraper = StockTweetsScraper(
        headless=args.headless,
        use_proxy=args.use_proxy,
        data_dir=args.data_dir
    )

    export_dir = os.path.join(scraper.data_dir, "exports", datetime.now().strftime("%Y%m%d_%H%M%S"))
    Path(export_dir).mkdir(parents=True, exist_ok=True)

    all_data = []
    tweets_df = scraper.scrape_stock_tweets(
        stock_symbol=args.stock,
        max_tweets=args.max_tweets,
        since_date=args.since_date
    )
    all_data.extend(tweets_df.to_dict(orient="records"))

    if not tweets_df.empty and args.scrape_replies:
        for _, row in tweets_df.iterrows():
            tweet_url = row["tweet_url"]  # ✅ 使用真实推文链接
            replies_df = scraper.scrape_tweet_replies(
                tweet_url=tweet_url, stock_symbol=args.stock, max_replies=args.max_replies
            )
            all_data.extend(replies_df.to_dict(orient="records"))

    scraper.save_all_to_one_json(all_data, export_dir)

    if scraper.driver:
        scraper.driver.quit()


if __name__ == "__main__":
    main()

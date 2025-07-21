"""
Twitter股票标签($TSLA等)爬虫
专门用于抓取Twitter/X上包含特定股票标签的推文及评论
"""

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
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    StaleElementReferenceException,
    WebDriverException
)

# 配置日志
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
        """
        初始化爬虫
        
        参数:
            headless: 是否使用无头模式
            use_proxy: 是否使用代理
            data_dir: 数据保存目录
        """
        self.headless = headless
        self.use_proxy = use_proxy
        self.data_dir = data_dir
        self.driver = None
        
        # 创建数据目录
        Path(data_dir).mkdir(exist_ok=True)
        
        # 用户代理列表
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
        ]
    
    def setup_driver(self):
        """配置Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # 随机选择用户代理
        user_agent = random.choice(self.user_agents)
        chrome_options.add_argument(f'--user-agent={user_agent}')
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # 如果使用代理
        if self.use_proxy:
            # 这里应该替换为实际的代理地址
            PROXY = "http://your-proxy-address:port"
            chrome_options.add_argument(f'--proxy-server={PROXY}')
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            return driver
        except WebDriverException as e:
            logger.error(f"设置WebDriver时出错: {e}")
            raise
    
    def _extract_tweet_id(self, tweet_url):
        """从推文URL提取推文ID"""
        try:
            return tweet_url.split("/")[-1]
        except:
            return None
    
    def _parse_count(self, count_str):
        """解析互动计数字符串为整数"""
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
    
    def _extract_media_links(self, tweet_element):
        """提取推文中的媒体链接"""
        media_links = []
        try:
            # 查找图片
            images = tweet_element.find_elements(By.CSS_SELECTOR, "img[src*='pbs.twimg.com/media']")
            for img in images:
                src = img.get_attribute("src")
                if src and "profile" not in src and src not in media_links:
                    media_links.append({"url": src, "type": "image"})
            
            # 查找视频
            videos = tweet_element.find_elements(By.CSS_SELECTOR, "video")
            for video in videos:
                src = video.get_attribute("src")
                if src and src not in [m["url"] for m in media_links]:
                    media_links.append({"url": src, "type": "video"})
                    
            # 查找视频缩略图
            video_thumbs = tweet_element.find_elements(By.CSS_SELECTOR, "div[data-testid='videoPlayer']")
            for _ in video_thumbs:
                # 视频播放器存在，但可能无法直接获取URL
                media_links.append({"url": "video_player_detected", "type": "video"})
                
        except Exception as e:
            logger.warning(f"提取媒体链接时出错: {e}")
            
        return media_links
    
    def scrape_stock_tweets(self, stock_symbol, max_tweets=100, since_date="2025-01-01"):
        """
        抓取包含特定股票标签的推文
        
        参数:
            stock_symbol: 股票标签，如 'TSLA'
            max_tweets: 要抓取的最大推文数量
            since_date: 起始日期，格式为 'YYYY-MM-DD'
            
        返回:
            包含推文数据的DataFrame
        """
        try:
            self.driver = self.setup_driver()
            
            # 构建搜索URL
            search_url = f"https://twitter.com/search?q=%24{stock_symbol}%20since%3A{since_date}&src=typed_query&f=live"
            
            logger.info(f"正在访问: {search_url}")
            self.driver.get(search_url)

            # 等待页面加载（比如60秒，给你手动登录时间）
            time.sleep(60)
            
            tweets_data = []
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            retry_count = 0
            max_retries = 3
            
            while len(tweets_data) < max_tweets and retry_count < max_retries:
                # 等待推文加载
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "article[data-testid='tweet']"))
                    )
                except TimeoutException:
                    logger.warning("等待推文元素超时")
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error("达到最大重试次数，退出")
                        break
                    continue
                    
                # 获取当前页面上的所有推文
                tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, "article[data-testid='tweet']")
                
                for tweet in tweet_elements:
                    if len(tweets_data) >= max_tweets:
                        break
                        
                    try:
                        # 提取用户信息
                        user_element = tweet.find_element(By.CSS_SELECTOR, "div[data-testid='User-Name']")
                        username = user_element.find_element(By.CSS_SELECTOR, "span").text
                        handle = user_element.find_elements(By.CSS_SELECTOR, "span")[1].text
                        
                        # 提取推文内容
                        try:
                            content_element = tweet.find_element(By.CSS_SELECTOR, "div[data-testid='tweetText']")
                            content = content_element.text
                        except NoSuchElementException:
                            content = ""
                        
                        # 提取时间戳
                        timestamp = tweet.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime")
                        
                        # 提取互动数据
                        try:
                            reply_count = tweet.find_element(By.CSS_SELECTOR, "div[data-testid='reply']").text
                        except NoSuchElementException:
                            reply_count = "0"
                            
                        try:
                            retweet_count = tweet.find_element(By.CSS_SELECTOR, "div[data-testid='retweet']").text
                        except NoSuchElementException:
                            retweet_count = "0"
                            
                        try:
                            like_count = tweet.find_element(By.CSS_SELECTOR, "div[data-testid='like']").text
                        except NoSuchElementException:
                            like_count = "0"
                        
                        # 提取推文链接
                        tweet_link = user_element.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                        tweet_id = self._extract_tweet_id(tweet_link)
                        
                        # 提取媒体链接
                        media_links = self._extract_media_links(tweet)
                        
                        # 存储数据
                        tweet_data = {
                            "tweet_id": tweet_id,
                            "username": username,
                            "handle": handle,
                            "content": content,
                            "timestamp": timestamp,
                            "replies": self._parse_count(reply_count),
                            "retweets": self._parse_count(retweet_count),
                            "likes": self._parse_count(like_count),
                            "tweet_url": tweet_link,
                            "stock_symbol": stock_symbol,
                            "media_urls": [m["url"] for m in media_links],
                            "media_types": [m["type"] for m in media_links]
                        }
                        
                        tweets_data.append(tweet_data)
                        logger.info(f"已抓取 {len(tweets_data)} 条推文")
                        
                    except StaleElementReferenceException:
                        logger.warning("元素已过时，跳过此推文")
                        continue
                    except Exception as e:
                        logger.error(f"处理推文时出错: {e}")
                        continue
                
                # 随机延迟，模拟人类操作
                time.sleep(random.uniform(2, 5))  # 每次循环间隔2~5秒
                
                # 滚动到页面底部加载更多推文
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(2, 4))  # 滚动后再等2~4秒
                
                # 检查是否已滚动到底部
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    # 尝试再次滚动
                    time.sleep(2)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        logger.info("已到达页面底部，无法加载更多推文")
                        break
                
                last_height = new_height
                retry_count = 0  # 重置重试计数
            
            # 创建DataFrame
            df = pd.DataFrame(tweets_data)
            return df
            
        except Exception as e:
            logger.error(f"抓取推文时出错: {e}")
            return pd.DataFrame()
            
        finally:
            if self.driver:
                self.driver.quit()
    
    def scrape_tweet_replies(self, tweet_url, max_replies=50):
        """
        抓取特定推文的回复
        
        参数:
            tweet_url: 推文URL
            max_replies: 要抓取的最大回复数量
            
        返回:
            包含回复数据的DataFrame
        """
        try:
            self.driver = self.setup_driver()
            
            logger.info(f"正在访问推文: {tweet_url}")
            self.driver.get(tweet_url)
            
            # 等待页面加载
            time.sleep(5)
            
            # 提取推文ID
            tweet_id = self._extract_tweet_id(tweet_url)
            if not tweet_id:
                logger.error("无法提取推文ID")
                return pd.DataFrame()
            
            # 等待回复加载
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "article[data-testid='tweet']"))
                )
            except TimeoutException:
                logger.warning("等待推文元素超时")
                return pd.DataFrame()
                
            # 获取原始推文，跳过它
            original_tweet = self.driver.find_element(By.CSS_SELECTOR, "article[data-testid='tweet']")
            
            # 滚动到原始推文下方
            self.driver.execute_script("arguments[0].scrollIntoView(true);", original_tweet)
            time.sleep(2)
            
            replies_data = []
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            retry_count = 0
            max_retries = 3
            
            while len(replies_data) < max_replies and retry_count < max_retries:
                # 获取当前页面上的所有推文（回复）
                reply_elements = self.driver.find_elements(By.CSS_SELECTOR, "article[data-testid='tweet']")
                
                # 跳过第一个元素（原始推文）
                for reply in reply_elements[1:]:
                    if len(replies_data) >= max_replies:
                        break
                        
                    try:
                        # 提取用户信息
                        user_element = reply.find_element(By.CSS_SELECTOR, "div[data-testid='User-Name']")
                        username = user_element.find_element(By.CSS_SELECTOR, "span").text
                        handle = user_element.find_elements(By.CSS_SELECTOR, "span")[1].text
                        
                        # 提取回复内容
                        try:
                            content_element = reply.find_element(By.CSS_SELECTOR, "div[data-testid='tweetText']")
                            content = content_element.text
                        except NoSuchElementException:
                            content = ""
                        
                        # 提取时间戳
                        timestamp = reply.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime")
                        
                        # 提取回复链接
                        reply_link = user_element.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                        reply_id = self._extract_tweet_id(reply_link)
                        
                        # 提取互动数据
                        try:
                            reply_count = reply.find_element(By.CSS_SELECTOR, "div[data-testid='reply']").text
                        except NoSuchElementException:
                            reply_count = "0"
                            
                        try:
                            retweet_count = reply.find_element(By.CSS_SELECTOR, "div[data-testid='retweet']").text
                        except NoSuchElementException:
                            retweet_count = "0"
                            
                        try:
                            like_count = reply.find_element(By.CSS_SELECTOR, "div[data-testid='like']").text
                        except NoSuchElementException:
                            like_count = "0"
                        
                        # 提取媒体链接
                        media_links = self._extract_media_links(reply)
                        
                        # 存储数据
                        reply_data = {
                            "tweet_id": tweet_id,
                            "reply_id": reply_id,
                            "username": username,
                            "handle": handle,
                            "content": content,
                            "timestamp": timestamp,
                            "replies": self._parse_count(reply_count),
                            "retweets": self._parse_count(retweet_count),
                            "likes": self._parse_count(like_count),
                            "reply_url": reply_link,
                            "media_urls": [m["url"] for m in media_links],
                            "media_types": [m["type"] for m in media_links]
                        }
                        
                        replies_data.append(reply_data)
                        logger.info(f"已抓取 {len(replies_data)} 条回复")
                        
                    except StaleElementReferenceException:
                        logger.warning("元素已过时，跳过此回复")
                        continue
                    except Exception as e:
                        logger.error(f"处理回复时出错: {e}")
                        continue
                
                # 随机延迟，模拟人类行为
                time.sleep(random.uniform(1, 3))
                
                # 滚动到页面底部加载更多回复
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # 检查是否已滚动到底部
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    # 尝试再次滚动
                    time.sleep(2)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        logger.info("已到达页面底部，无法加载更多回复")
                        break
                
                last_height = new_height
                retry_count = 0  # 重置重试计数
            
            # 创建DataFrame
            df = pd.DataFrame(replies_data)
            return df
            
        except Exception as e:
            logger.error(f"抓取回复时出错: {e}")
            return pd.DataFrame()
            
        finally:
            if self.driver:
                self.driver.quit()
    
    def save_to_csv(self, df, filename):
        """
        保存DataFrame到CSV文件
        
        参数:
            df: DataFrame
            filename: 文件名
            
        返回:
            保存的文件路径
        """
        try:
            # 创建导出目录
            export_dir = os.path.join(self.data_dir, "exports")
            Path(export_dir).mkdir(exist_ok=True)
            
            # 构建文件路径
            file_path = os.path.join(export_dir, filename)
            
            # 保存数据
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            logger.info(f"数据已保存至 {file_path}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"保存数据时出错: {e}")
            return None
    
    def save_to_json(self, df, filename):
        """
        保存DataFrame到JSON文件
        
        参数:
            df: DataFrame
            filename: 文件名
            
        返回:
            保存的文件路径
        """
        try:
            # 创建导出目录
            export_dir = os.path.join(self.data_dir, "exports")
            Path(export_dir).mkdir(exist_ok=True)
            
            # 构建文件路径
            file_path = os.path.join(export_dir, filename)
            
            # 转换为JSON并保存
            json_data = df.to_json(orient='records', force_ascii=False, indent=4)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_data)
            
            logger.info(f"数据已保存至 {file_path}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"保存数据时出错: {e}")
            return None

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Twitter股票标签爬虫')
    
    parser.add_argument('--stock', type=str, required=True, help='股票标签，如TSLA')
    parser.add_argument('--max_tweets', type=int, default=100, help='要抓取的最大推文数量')
    parser.add_argument('--since_date', type=str, default='2025-01-01', help='起始日期，格式为YYYY-MM-DD')
    parser.add_argument('--max_replies', type=int, default=20, help='每条推文要抓取的最大回复数量')
    parser.add_argument('--headless', action='store_true', help='是否使用无头模式')
    parser.add_argument('--use_proxy', action='store_true', help='是否使用代理')
    parser.add_argument('--data_dir', type=str, default='data', help='数据保存目录')
    parser.add_argument('--format', type=str, choices=['csv', 'json', 'both'], default='csv', help='数据保存格式')
    parser.add_argument('--scrape_replies', action='store_true', help='是否抓取回复')
    
    return parser.parse_args()

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()
    
    # 创建爬虫实例
    scraper = StockTweetsScraper(
        headless=args.headless,
        use_proxy=args.use_proxy,
        data_dir=args.data_dir
    )
    
    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 抓取推文
    logger.info(f"开始抓取关于 ${args.stock} 的推文")
    tweets_df = scraper.scrape_stock_tweets(
        stock_symbol=args.stock,
        max_tweets=args.max_tweets,
        since_date=args.since_date
    )
    
    # 保存推文数据
    if not tweets_df.empty:
        if args.format in ['csv', 'both']:
            tweets_filename = f"{args.stock}_tweets_{timestamp}.csv"
            scraper.save_to_csv(tweets_df, tweets_filename)
            
        if args.format in ['json', 'both']:
            tweets_filename = f"{args.stock}_tweets_{timestamp}.json"
            scraper.save_to_json(tweets_df, tweets_filename)
        
        logger.info(f"成功抓取 {len(tweets_df)} 条关于 ${args.stock} 的推文")
        
        # 如果需要抓取回复
        if args.scrape_replies:
            # 获取前N条推文的URL
            top_tweets = min(10, len(tweets_df))  # 最多抓取前10条推文的回复
            tweet_urls = tweets_df.head(top_tweets)['tweet_url'].tolist()
            
            all_replies = []
            
            # 抓取每条推文的回复
            for i, url in enumerate(tweet_urls):
                logger.info(f"开始抓取第 {i+1}/{len(tweet_urls)} 条推文的回复")
                
                replies_df = scraper.scrape_tweet_replies(
                    tweet_url=url,
                    max_replies=args.max_replies
                )
                
                if not replies_df.empty:
                    all_replies.append(replies_df)
                    logger.info(f"成功抓取 {len(replies_df)} 条回复")
                else:
                    logger.warning(f"未找到回复或抓取失败")
                
                # 随机延迟，避免频繁请求
                time.sleep(random.uniform(3, 6))
            
            # 合并所有回复数据
            if all_replies:
                all_replies_df = pd.concat(all_replies, ignore_index=True)
                
                # 保存回复数据
                if args.format in ['csv', 'both']:
                    replies_filename = f"{args.stock}_replies_{timestamp}.csv"
                    scraper.save_to_csv(all_replies_df, replies_filename)
                    
                if args.format in ['json', 'both']:
                    replies_filename = f"{args.stock}_replies_{timestamp}.json"
                    scraper.save_to_json(all_replies_df, replies_filename)
                
                logger.info(f"成功抓取并保存 {len(all_replies_df)} 条回复")
            else:
                logger.warning("未找到任何回复或所有抓取均失败")
    else:
        logger.error(f"未找到关于 ${args.stock} 的推文或抓取失败")

if __name__ == "__main__":
    main()
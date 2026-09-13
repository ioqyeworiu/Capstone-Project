from time import sleep
import threading
import queue
import psutil

from base_crawler import BaseCrawler, logger
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv(override=True)

DB_WORKERS = 4
BATCH_SIZE = 20
BATCH_TIMEOUT = 2.0
CLEAR_CACHE_EVERY_N_PAGES = 20
RESTART_DRIVER_EVERY_N_PAGES = 50      # restart cứng theo số trang
RESTART_DRIVER_RAM_MB = 2500            # hoặc restart sớm hơn nếu RAM vượt ngưỡng này

def standalize_url(url: str):
    return url.split("?")[0]

def get_driver_ram_mb(driver):
    """Tổng RAM thực tế (USS - Unique Set Size, không đếm trùng shared memory)
    của process Edge chính + toàn bộ process con (renderer, gpu...)"""
    try:
        pid = driver.service.process.pid
        proc = psutil.Process(pid)
        total = proc.memory_full_info().uss
        for child in proc.children(recursive=True):
            try:
                total += child.memory_full_info().uss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return total / (1024 * 1024)
    except Exception:
        logger.exception("fail to get driver RAM")
        return None

def producer_crawl(crawler: BaseCrawler, url_seed: str, url_queue: queue.Queue, db_queue: queue.Queue):
    visited = set()
    url_queue.put(url_seed)
    db_queue.put(url_seed)
    visited.add(url_seed)

    pages_since_clear = 0
    pages_since_restart = 0

    while not url_queue.empty():
        url = url_queue.get()
        try:
            crawler.driver.get(url)
        except (TimeoutException, WebDriverException):
            logger.exception(f"Failed to load {url}")
            url_queue.task_done()
            continue

        sleep(0.03)

        other_urls_elements = crawler.driver.find_elements(By.XPATH, "//a[contains(@class, 'product-item')]")
        other_urls = [el.get_attribute("href") for el in other_urls_elements]

        for href in other_urls:
            if not href:
                continue
            if href.startswith("https://tka.tiki.vn"):
                try:
                    crawler.driver.get(href)
                    sleep(0.03)
                except (TimeoutException, WebDriverException):
                    logger.exception(f"Failed to resolve redirect {href}")
                    continue
                new_url = standalize_url(crawler.driver.current_url)
            else:
                new_url = standalize_url(href)

            if new_url in visited:
                continue

            try:
                url_queue.put_nowait(new_url)
            except queue.Full:
                logger.warning("url_queue full")
            try:
                db_queue.put_nowait(new_url)
                visited.add(new_url)
            except queue.Full:
                logger.warning("db_queue full")

        pages_since_clear += 1
        pages_since_restart += 1

        # dọn nhẹ định kỳ (giữ nguyên như cũ)
        if pages_since_clear >= CLEAR_CACHE_EVERY_N_PAGES:
            try:
                crawler.driver.get("about:blank")
                crawler.driver.execute_cdp_cmd("Network.clearBrowserCache", {})
                crawler.driver.execute_cdp_cmd("Memory.forciblyPurgeJavaScriptMemory", {})
                logger.info("Cleared browser cache and purged JS memory")
            except Exception:
                logger.exception("Failed to clear cache/memory via CDP")
            pages_since_clear = 0

        # kiểm tra RAM, restart hẳn driver nếu cần
        ram_mb = get_driver_ram_mb(crawler.driver)
        need_restart = pages_since_restart >= RESTART_DRIVER_EVERY_N_PAGES
        if ram_mb is not None and ram_mb > RESTART_DRIVER_RAM_MB:
            need_restart = True

        if need_restart:
            logger.info(f"Restarting driver | RAM before restart: {ram_mb:.0f} MB" if ram_mb else "Restarting driver")
            crawler.restart_driver()
            pages_since_restart = 0

        logger.info(f"url_queue size: {url_queue.qsize()} | visited: {len(visited)} | RAM: {ram_mb:.0f}MB" if ram_mb else f"url_queue size: {url_queue.qsize()} | visited: {len(visited)}")

def consumer_write_db(engine, db_queue: queue.Queue, worker_id: int):
    while True:
        batch = []
        stop = False

        # chờ item đầu tiên (block tối đa BATCH_TIMEOUT)
        try:
            item = db_queue.get(timeout=BATCH_TIMEOUT)
        except queue.Empty:
            continue

        if item is None:
            db_queue.task_done()
            break
        batch.append(item)
        db_queue.task_done()

        # sau đó rút thêm không chờ, cho tới khi đủ BATCH_SIZE hoặc hết hàng đang có sẵn
        while len(batch) < BATCH_SIZE:
            try:
                item = db_queue.get_nowait()
            except queue.Empty:
                break
            if item is None:
                stop = True
                db_queue.task_done()
                break
            batch.append(item)
            db_queue.task_done()

        _flush_batch(engine, batch, worker_id)
        if stop:
            break


def _flush_batch(engine, batch, worker_id):
    if not batch:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO public.crawl_state(url)
                    SELECT unnest(:urls)
                    ON CONFLICT DO NOTHING
                """),
                {"urls": batch},
            )
        logger.info(f"[db-worker-{worker_id}] inserted {len(batch)} urls")
    except SQLAlchemyError:
        logger.exception(f"[db-worker-{worker_id}] insert batch failed")

class DiscoveryCrawler(BaseCrawler):
    def run(self, *args) -> None:
        url_seed = standalize_url(args[0])
        engine = create_engine(os.getenv("POSTGRES_URL"), pool_size=10, max_overflow=10, pool_timeout=10)

        url_queue: queue.Queue = queue.Queue(maxsize=1000)
        db_queue: queue.Queue = queue.Queue(maxsize=1000)

        consumer_threads = []
        for i in range(DB_WORKERS):
            t = threading.Thread(target=consumer_write_db, args=(engine, db_queue, i), daemon=True)
            t.start()
            consumer_threads.append(t)

        try:
            producer_crawl(self, url_seed, url_queue, db_queue)   # truyền self (crawler), không phải self.driver
        finally:
            for _ in consumer_threads:
                db_queue.put(None)
            db_queue.join()
            for t in consumer_threads:
                t.join(timeout=10)

if __name__ == "__main__":
    crawler = DiscoveryCrawler(
        driver_path=r"C:\Users\pmqua\Downloads\edgedriver_win64\msedgedriver.exe",
        user_data_dir=r"C:\Users\pmqua\Downloads\selenium_user_data",
        cleanup_user_data_dir=True,
        headless=True,
        profile_name="discovery"
    )
    crawler.start()
    try:
        crawler.run(
            "https://tiki.vn/cay-lan-bui-quan-ao-ghe-sofa-ga-giuong-long-cho-meo-noi-that-nha-cua-sieu-sach-henrysa-p276598029.html"
        )
    finally:
        crawler.stop()
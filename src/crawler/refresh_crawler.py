from confluent_kafka import Producer
from selenium.webdriver.common.by import By
from base_crawler import BaseCrawler, logger
from sqlalchemy import create_engine, text
from functools import partial
from threading import Thread
from multiprocessing import Pool, Process
import threading
import psutil
import os
from dotenv import load_dotenv
from time import sleep, monotonic
from uuid import uuid4

load_dotenv(override=True)

CACHE_CLEAR_INTERVAL = 25         # cứ mỗi 25 URL thì dọn cache (nếu chưa tới lúc restart)
RESTART_INTERVAL = 100             # cứ mỗi 50 URL thì restart driver, bất kể RAM
MAX_DRIVER_MEMORY_MB = 2000       # hoặc RAM vượt ngưỡng này thì restart sớm hơn
DEFAULT_BATCH_SIZE = 500
CRAWL_SLEEP_AFTER_LOAD = 0.03     # đợi sau khi driver.get() trước khi lấy page_source
EMPTY_BATCH_SLEEP = 3             # nghỉ khi không còn URL nào cần crawl
POLL_INTERVAL = 0.1               # timeout cho producer.poll() trong poll thread
KAFKA_TOPIC = 'product_data_changes'

MAX_RESIZE_ITERATIONS = 6      # tránh resize vô hạn nếu trang lỗi/vô cực
RESIZE_BUFFER_PX = 300         # dư ra để chắc chắn phần cuối trang cũng "nhìn thấy"
DEFAULT_WINDOW_HEIGHT = 1080   # kích thước mặc định, quay lại sau khi crawl xong

def create_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": "35.209.29.2:9092",
        "client.id": f"refresh-crawler-{os.getpid()}-{uuid4().hex[:8]}",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "compression.type": "snappy",
        "queue.buffering.max.messages": 500_000,
        "queue.buffering.max.kbytes": 1_048_576,
    })

def get_url(db_engine, batch_size):
    with db_engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE public.crawl_state
                SET next_crawled_at = NOW() + crawl_interval*INTERVAL '1 minute',
                last_crawled_at = NOW()
                WHERE id IN (
                    SELECT id FROM public.crawl_state
                    WHERE status = 'ACTIVE'
                    AND next_crawled_at <= NOW()
                    ORDER BY next_crawled_at ASC
                    LIMIT :batch_size
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, url;
            """),
            {"batch_size": batch_size},
        )
        rows = result.fetchall()
    return rows


def delivery_report(err, msg, url_id):
    if err is not None:
        logger.error(f"URL ID {url_id}: delivery failed: {err}")
    else:
        logger.info(f"URL ID {url_id}: delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def get_driver_memory_mb(driver) -> float:
    """
    Tính tổng RSS (MB) của cả cây tiến trình driver: msedgedriver.exe
    (driver.service.process) + toàn bộ msedge.exe con của nó. Đây là con số
    phản ánh đúng RAM mà browser đang thực sự chiếm, không phải RAM của
    tiến trình Python chính.
    """
    if driver is None:
        return 0.0
    try:
        driver_pid = driver.service.process.pid
    except Exception:
        return 0.0

    try:
        parent = psutil.Process(driver_pid)
    except psutil.NoSuchProcess:
        return 0.0

    total = 0
    try:
        total += parent.memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    try:
        for child in parent.children(recursive=True):
            try:
                total += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except psutil.NoSuchProcess:
        pass

    return total / (1024 * 1024)


def clear_driver_cache(driver) -> None:
    """
    Dọn cache, cookies, local/session storage của driver hiện tại thông qua
    Chrome DevTools Protocol (CDP) - hoạt động với cả Edge (Chromium-based).
    Nhẹ hơn nhiều so với restart hẳn driver.

    Lưu ý: chỉ dọn được HTTP/disk cache + cookies (toàn cục) và
    localStorage/sessionStorage của ORIGIN đang mở (không xoá được storage
    của mọi domain đã từng ghé qua trong session). RAM do tab/DOM/JS heap
    chiếm giữ thì CDP không có cách giải phóng trực tiếp - phần đó chỉ có
    restart driver mới xử lý được triệt để.
    """
    if driver is None:
        return

    try:
        driver.execute_cdp_cmd("Network.clearBrowserCache", {})
        driver.execute_cdp_cmd("Network.clearBrowserCookies", {})
        logger.info("Đã dọn browser cache + cookies (CDP)")
    except Exception:
        logger.exception("error when clearing cache/cookies via CDP")

    try:
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    except Exception:
        logger.exception("error when clearing localStorage/sessionStorage")


def maintain_driver_health(crawler, urls_since_restart: int) -> int:
    """
    Gọi sau mỗi URL đã crawl. Trả về giá trị counter mới (0 nếu vừa restart
    thành công, giữ nguyên nếu chưa tới mốc kiểm tra hoặc restart thất bại).

    Chỉ thực sự kiểm tra/hành động tại các mốc bội số của
    CACHE_CLEAR_INTERVAL (25), để tránh overhead quét tiến trình (psutil)
    sau mỗi URL:
        - Mốc 25, 75, 125...            -> dọn cache (nếu RAM chưa vượt ngưỡng)
        - Mốc 50, 100, 150... hoặc bất
          kỳ mốc 25 nào mà RAM đã vượt   -> restart hẳn driver
    """
    if urls_since_restart % CACHE_CLEAR_INTERVAL != 0:
        return urls_since_restart

    mem_mb = get_driver_memory_mb(crawler.driver)
    logger.info(
        f"Health check: crawled {urls_since_restart} URL with restart before "
        f"Current RAM: {mem_mb:.1f} MB"
    )

    need_restart = urls_since_restart >= RESTART_INTERVAL or mem_mb > MAX_DRIVER_MEMORY_MB

    if need_restart:
        reason = (
            f"Reached {urls_since_restart} URL with restart before"
            if urls_since_restart >= RESTART_INTERVAL
            else f"RAM exceeded ({mem_mb:.1f} MB > {MAX_DRIVER_MEMORY_MB} MB)"
        )
        logger.warning(f"Restart driver do {reason}")
        try:
            crawler.restart_driver()
        except Exception:
            logger.exception("Restart driver failed, retry later")
            return urls_since_restart  # giữ nguyên counter, thử lại ở checkpoint kế tiếp
        return 0  # reset counter sau khi restart thành công

    logger.info(f"Clear cache (after {urls_since_restart} URL)")
    clear_driver_cache(crawler.driver)
    return urls_since_restart


# --------------------------------------------------------------------------- #
# Crawler chính
# --------------------------------------------------------------------------- #
class RefreshCrawler(BaseCrawler):
    def __init__(self, kafka_producer, db_engine, **kwargs):
        super().__init__(**kwargs)
        self.producer = kafka_producer
        self.db_engine = db_engine

    def _poll_loop(self, stop_event):
        # Chạy liên tục trong suốt vòng đời crawler, không chờ queue đầy
        # mới poll -> callback được xử lý gần như ngay khi broker ack.
        while not stop_event.is_set():
            try:
                self.producer.poll(POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Error occurred while polling Kafka: {e}")

    def _crawl_one(self, url_id, url) -> str | None:
        try:
            t0 = monotonic()
            self.driver.get(url)
            t1 = monotonic()
            while True:
                # Cuộn xuống từng đoạn ngắn 700 pixel
                sleep(0.05) # Dừng lại để chờ Tiki gọi API và render HTML
                self.driver.execute_script("window.scrollBy(0, 700);")
                
                # Kiểm tra tọa độ hiện tại so với chiều cao tối đa của trang
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                current_position = self.driver.execute_script("return window.pageYOffset + window.innerHeight")
                
                # Dừng lại khi cuộn chạm đáy
                if current_position >= new_height:
                    break
            html = self.driver.page_source
            t2 = monotonic()
    
            logger.info(f"Finished crawling URL {url_id} in {t2 - t0:.2f} seconds (load: {t1 - t0:.2f}s, scroll: {t2 - t1:.2f}s)")
            return html
        
        except Exception as e:
            logger.error(f"Error occurred while fetching URL {url}: {e}")
            return None
    
    def _send_to_kafka(self, url_id, html: str) -> None:
        html_bytes = html.encode('utf-8')
        cb = partial(delivery_report, url_id=url_id)
        try:
            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=str(url_id).encode('utf-8'),
                value=html_bytes,
                callback=cb,
            )
        except BufferError:
            logger.warning(f"Local producer queue is full, waiting for space (URL ID {url_id})")
            self.producer.poll(0.5)
            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=str(url_id).encode('utf-8'),
                value=html_bytes,
                callback=cb,
            )

    def run(self, *args) -> None:
        stop_event = threading.Event()
        poll_thread = Thread(target=self._poll_loop, args=(stop_event,), daemon=True)
        poll_thread.start()

        urls_since_restart = 0  # biến cục bộ, chỉ sống trong vòng đời run()

        try:
            while True:
                rows = get_url(self.db_engine, batch_size=args[0] if args else DEFAULT_BATCH_SIZE)
                if not rows:
                    logger.info("No product to refresh, sleeping for 3 seconds...")
                    sleep(EMPTY_BATCH_SLEEP)
                    continue

                for row in rows:
                    url_id, url = row[0], row[1]

                    html = self._crawl_one(url_id, url)

                    urls_since_restart += 1
                    urls_since_restart = maintain_driver_health(self, urls_since_restart)

                    if html is None:
                        continue

                    self._send_to_kafka(url_id, html)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, stopping crawler...")
        finally:
            logger.info("Flushing producer queue and stopping crawler...")
            stop_event.set()
            poll_thread.join(timeout=5)
            remaining = self.producer.flush(timeout=30)
            if remaining > 0:
                logger.warning("remaining %d not sent", remaining)

def run_refresh_crawler(batch_size, profile_name):
    db_engine = create_engine(os.getenv("POSTGRES_URL"), pool_pre_ping=True)
    kafka_producer = create_kafka_producer()
    crawler = RefreshCrawler(
        kafka_producer=kafka_producer,
        db_engine=db_engine,
        headless=True,
        driver_path=r"C:\Users\pmqua\Downloads\edgedriver_win64\msedgedriver.exe",
        user_data_dir=r"C:\Users\pmqua\Downloads\selenium_user_data",
        cleanup_user_data_dir=True,
        profile_name=profile_name,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.37"
    )
    crawler.start()
    try:
        crawler.run(batch_size)
    finally:
        crawler.stop()

if __name__ == "__main__":
    p1 = Process(target=run_refresh_crawler, args=(DEFAULT_BATCH_SIZE, "refresh1"))
    p2 = Process(target=run_refresh_crawler, args=(DEFAULT_BATCH_SIZE, "refresh2"))
    p3 = Process(target=run_refresh_crawler, args=(DEFAULT_BATCH_SIZE, "refresh3"))
    # p4 = Process(target=run_refresh_crawler, args=(DEFAULT_BATCH_SIZE, "refresh4"))

    p1.start()
    p2.start()
    p3.start()
    # p4.start()
    try:
        p1.join()
        p2.join()
        p3.join()
        # p4.join()
    except KeyboardInterrupt:
        logger.info("Main process interrupted, waiting for children to shut down...")
        p1.join()
        p2.join()
        p3.join()
        # p4.join()
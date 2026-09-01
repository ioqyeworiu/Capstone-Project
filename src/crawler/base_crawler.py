"""
base_crawler.py
----------------
Lop Crawler co so dung Selenium (Edge), co vong doi ro rang:
    - start()   : khoi tao driver (tu don rac tien trinh/lock con sot truoc khi mo moi)
    - run()     : logic crawl (override o lop con)
    - stop()    : dung driver va don dep TOAN BO tai nguyen, KE CA khi driver bi treo

Dam bao don dep that su sach (khong con tien trinh msedge.exe / msedgedriver.exe mo cot)
ngay ca khi:
    - driver.quit() bi treo hoac loi (vi du dang load trang khi bi ngat)
    - Chuong trinh bi Ctrl+C (KeyboardInterrupt / SIGINT) hoac bi kill (SIGTERM)
    - Co exception xay ra trong luc crawl
    - Dung `with BaseCrawler(...) as crawler:` (context manager)

Cach lam sach "trong sach" (giong doc2) gom 3 lop:
    1. Sau khi goi driver.quit(), kill thang toan bo cay tien trinh con cua
       msedgedriver.exe theo PID (khong cho no co co hoi treo).
    2. Quet toan bo tien trinh msedge*/msedgedriver* con dang tro toi dung
       user-data-dir cua phien nay va kill not (phong truong hop quit() khong
       kip tao driver, hoac tien trinh mo coi tu lan chay truoc do crash).
    3. Xoa cac file lock (SingletonLock, SingletonCookie, SingletonSocket) trong
       profile de lan sau khoi dong khong bi Edge tuong profile dang duoc dung.

Cach dung:
    class MyCrawler(BaseCrawler):
        def run(self):
            self.driver.get("https://example.com")
            print(self.driver.title)

    with MyCrawler(headless=True) as c:
        c.run()

Yeu cau cai them: pip install psutil
"""

import atexit
import logging
import os
import shutil
import signal
import tempfile
from abc import ABC
from pathlib import Path
from typing import Optional

import psutil
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.common.exceptions import WebDriverException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("BaseCrawler")


class BaseCrawler(ABC):
    """
    Lop crawler co so. Ke thua va override phuong thuc `run()` de viet logic crawl rieng.
    """

    def __init__(
        self,
        headless: bool = True,
        driver_path: Optional[str] = None,
        user_data_dir: Optional[str] = None,
        profile_name: str = "profile",
        cleanup_user_data_dir: bool = False,
        window_size: str = "1920,1080",
        user_agent: Optional[str] = None,
        extra_options: Optional[list] = None,
        page_load_timeout: int = 30,
        implicit_wait: int = 5,
    ):
        """
        user_data_dir:
            Thu muc GOC do ban tu chi dinh va SO HUU. Crawler KHONG BAO GIO xoa
            thu muc nay, du cleanup_user_data_dir=True. Neu de None, crawler se
            tu tao mot thu muc TAM (o dau do trong he thong) va XOA sach khi stop().
        profile_name:
            Ten thu muc con (nam BEN TRONG user_data_dir) ma crawler tu tao ra de
            luu profile Edge that su (--user-data-dir thuc te tro vao day). Dung
            chung profile_name giua cac lan chay -> session/cookie duoc giu lai.
        cleanup_user_data_dir:
            Chi ap dung khi ban TU chi dinh user_data_dir. Mac dinh False (giu lai
            thu muc con `profile_name` de dung lai lan sau). Dat True neu muon xoa
            RIENG thu muc con do sau khi stop() - thu muc GOC user_data_dir van
            luon duoc giu nguyen.
        """
        self.headless = headless
        self.driver_path = driver_path
        self.user_data_dir = user_data_dir
        self.profile_name = profile_name
        self._cleanup_user_data_dir = cleanup_user_data_dir
        self._is_temp_profile = user_data_dir is None

        self.window_size = window_size
        self.user_agent = user_agent
        self.extra_options = extra_options or []
        self.page_load_timeout = page_load_timeout
        self.implicit_wait = implicit_wait

        self.driver: Optional[webdriver.Edge] = None
        self._profile_dir: Optional[str] = None
        self._is_running: bool = False
        self._stopped: bool = False

        # Dam bao cleanup du chuong trinh thoat bat thuong
        atexit.register(self.stop)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # ------------------------------------------------------------------ #
    # Vong doi chinh
    # ------------------------------------------------------------------ #
    
    def start(self) -> None:
        """Khoi tao Selenium WebDriver (Edge)."""
        if self._is_running:
            logger.warning("Crawler is already running, skipping start().")
            return

        # Xac dinh thu muc profile se dung TRUOC khi mo driver, de co the don
        # rac tien trinh/lock con sot tu lan chay truoc (neu dung chung profile).
        self._resolve_profile_dir()
        self._cleanup_orphan_processes()

        logger.info("Initializing driver...")
        options = self._build_options()

        try:
            if self.driver_path:
                service = Service(executable_path=self.driver_path)
                self.driver = webdriver.Edge(service=service, options=options)
            else:
                self.driver = webdriver.Edge(options=options)

            self.driver.set_page_load_timeout(self.page_load_timeout)
            self.driver.implicitly_wait(self.implicit_wait)
            self._is_running = True
            self._stopped = False
            logger.info("Driver Init done. Crawler is running.")
        except WebDriverException as e:
            logger.error(f"Cannot initialize driver: {e}")
            self.stop()
            raise

    def restart_driver(self):
        logger.info("Restarting driver...")
        self.stop()
        self.start()
        logger.info("Driver restarted successfully.")

    def run(self, *args) -> None:
        """
        Logic crawl chinh. BAT BUOC override o lop con.
        Vi du:
            self.driver.get("https://example.com")
        """
        raise NotImplementedError("Subclass must implement the run() method.")

    def stop(self) -> None:
        """Dung driver va don dep TOAN BO tai nguyen (idempotent - goi nhieu lan van an toan)."""
        if self._stopped:
            return
        self._stopped = True

        driver_pid = None
        if self.driver is not None:
            logger.info("Dang dong driver...")
            try:
                driver_pid = self.driver.service.process.pid
            except Exception:
                driver_pid = None
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Error occurred while quitting driver (ignoring, will kill process): {e}")
            finally:
                self.driver = None

        if driver_pid:
            self._kill_process_tree(driver_pid)

        self._cleanup_orphan_processes()
        self._remove_lock_files()
        self._cleanup_profile_dir()

        self._is_running = False
        logger.info("Cleanup done. Crawler stoped safely.")

    # ------------------------------------------------------------------ #
    # Ho tro noi bo
    # ------------------------------------------------------------------ #
    def _resolve_profile_dir(self) -> None:
        if self.user_data_dir:
            # Thu muc GOC: chi tao neu chua co, KHONG BAO GIO bi xoa boi crawler.
            root = Path(self.user_data_dir)
            root.mkdir(parents=True, exist_ok=True)

            # Profile THAT SU nam trong 1 thu muc con do crawler tu quan ly ->
            # day moi la thu muc co the bi xoa khi cleanup, thu muc goc luon an toan.
            profile_dir = root / self.profile_name
            profile_dir.mkdir(parents=True, exist_ok=True)
            self._profile_dir = str(profile_dir.resolve())
        else:
            self._profile_dir = str(Path(tempfile.mkdtemp(prefix="crawler_profile_")).resolve())

    def _build_options(self) -> Options:
        options = Options()

        if self.headless:
            options.add_argument("--headless=new")

        options.add_argument(f"--user-data-dir={self._profile_dir}")
        options.add_argument(f"--window-size={self.window_size}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        # options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-notifications")
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_argument("--media-cache-size=1")

        if self.user_agent:
            options.add_argument(f"--user-agent={self.user_agent}")

        for opt in self.extra_options:
            options.add_argument(opt)

        return options

    def _kill_process_tree(self, pid: int) -> None:
        """Kill toan bo tien trinh con (msedge.exe, msedgedriver.exe) cua 1 PID goc."""
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return
        try:
            children = parent.children(recursive=True)
        except psutil.NoSuchProcess:
            children = []
        for child in children:
            try:
                child.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        try:
            parent.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    def _cleanup_orphan_processes(self) -> None:
        """
        Kill moi tien trinh msedge.exe / msedgedriver.exe con dang dung dung
        profile nay (bat ke cua phien nao) - phong khi quit()/kill theo PID
        khong xu ly het, hoac lan chay truoc bi crash giua chung.
        """
        if not self._profile_dir:
            return

        target = os.path.normcase(os.path.abspath(self._profile_dir))
        killed = 0
        for proc in psutil.process_iter(["name", "cmdline"]):
            try:
                name = (proc.info["name"] or "").lower()
                if "msedge" not in name:
                    continue
                cmdline = proc.info["cmdline"] or []
                cmdline_str = os.path.normcase(" ".join(cmdline))
                if target in cmdline_str:
                    proc.kill()
                    killed += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        if killed:
            logger.info(f"Killed {killed} orphan processes (profile: {self._profile_dir}).")

    def _remove_lock_files(self) -> None:
        if not self._profile_dir:
            return
        for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            lock_path = Path(self._profile_dir) / lock_name
            try:
                if lock_path.exists():
                    lock_path.unlink()
            except Exception:
                pass

    def _cleanup_profile_dir(self) -> None:
        """
        CHI xoa thu muc profile ma chinh crawler tao ra:
            - Truong hop tam (khong truyen user_data_dir): xoa thu muc tam.
            - Truong hop truyen user_data_dir: xoa (neu duoc yeu cau) thu muc con
              `profile_name` NAM BEN TRONG user_data_dir. Thu muc GOC user_data_dir
              khong bao gio bi dong den o day.
        """
        if not self._profile_dir or not Path(self._profile_dir).exists():
            self._profile_dir = None
            return

        should_delete = self._is_temp_profile or self._cleanup_user_data_dir

        if should_delete:
            try:
                shutil.rmtree(self._profile_dir, ignore_errors=True)
                logger.info(f"Deleted profile directory (not deleting root directory): {self._profile_dir}")
            except Exception as e:
                logger.warning(f"Cannot delete profile directory: {e}")
        else:
            logger.info(f"Retaining profile directory: {self._profile_dir}")

        self._profile_dir = None

    def _handle_signal(self, signum, frame):
        logger.info(f"received signal {signum}, stopping crawler safely...")
        self.stop()
        raise SystemExit(0)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
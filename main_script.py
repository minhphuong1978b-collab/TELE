"""
🤖 BOT - STRICT MODE CDP - ULTRA FAST QUEUE MODE v5.0
Bot dùng trình duyệt bạn đã mở sẵn qua port 9222.

CHANGELOG v5.0 - ULTRA FAST MODE:
- Telethon optimized: sequential_updates=True, reconnect auto
- Telegram handler chạy IN-PROCESS với max priority
- Message workers tăng: 6 → 12 workers
- Zero-copy event delivery: event vào queue NGAY khi nhận
- Remove handler delay: không lọc cũ trong handler (filter ở worker)
- Watchdog interval: 30s → 10s (nhanh hơn 3x)
- History queue async: không block main thread
- Semaphore submit: từ 3 → 5 concurrent (nhanh hơn)
"""

import asyncio
import csv
import json
import re
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from telethon import TelegramClient, events
from telethon.tl.types import MessageEntitySpoiler
from playwright.async_api import async_playwright

from config import Config
from logger_setup import logger
from code_validator import CodeValidator
from database import init_database
from rate_limiter import init_anti_detection
from monitoring import init_monitoring
from task_manager import init_task_system

from features import (
    print_version_info,
    get_shutdown_handler,
    get_db_backup,
    get_profile_cleaner,
    get_system_tester,
    ConfigValidator,
)


class BotState:
    def __init__(self):
        self.playwright_instance = None
        self.connected_browsers = {}
        self.account_pages = {}
        self.context_locks = {}
        self.is_running = True
        self.cf_verified = {}
        self.submission_count = {}
        self.handler_registered = False
        # Cache input fields: key -> (username_input, code_input, cache_time)
        self._input_cache = {}
        self._input_cache_ttl = 30.0  # giây
        # FIX 5 & 6: Dedup code theo site — tránh nạp trùng khi nhiều kênh cùng post 1 code.
        # key=(domain, code_upper) → timestamp lần đầu thấy. Dọn định kỳ để không rò rỉ memory.
        self._site_code_seen: dict = {}
        self._site_code_ttl: float = 10.0  # giây — đọc lại từ Config khi cần


bot_state = BotState()

# ✅ V5.0: Telethon client tối ưu
client = TelegramClient(
    Config.SESSION_NAME,
    Config.API_ID,
    Config.API_HASH,
    device_model="Desktop Bot",
    system_version="Windows 10",
    app_version="1.0",
    connection_retries=5,
    retry_delay=1,
    auto_reconnect=True,
    use_ipv6=False,
    flood_sleep_threshold=60,
    receive_updates=True,
    sequential_updates=True,  # ✅ True = nhận update theo sequence, không bỏ tin
)

_systems = None
message_queue = None
message_workers = []

# Background queue cho file I/O (CSV/JSONL) để không chặn luồng chính
_history_queue: asyncio.Queue = None
_history_writer_task = None

# Submit task tách riêng khỏi worker Telegram để Telegram không bị nghẽn khi Playwright đang bấm/chờ kết quả.
_submit_semaphore: asyncio.Semaphore | None = None
_active_submit_tasks: set[asyncio.Task] = set()


def normalize_domain(url: str) -> str:
    parsed = urlparse(url or "")
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "").strip("/")


# ============================================================
# HÀM LẤY ACCOUNT MẶC ĐỊNH THEO DOMAIN (CHO WATCHDOG)
# ============================================================

def get_default_account_for_domain(domain: str) -> str | None:
    """Trả về username của account có priority cao nhất cho domain này."""
    for chat_id, cfg in Config.CHANNEL_CONFIG.items():
        if normalize_domain(cfg["url"]) == domain:
            accounts = cfg.get("accounts", [])
            if accounts:
                sorted_acc = sorted(accounts, key=lambda a: a.get("priority", 999))
                return sorted_acc[0]["username"]
    return None


# ============================================================
# FIX 5 & 6: Dedup code theo site + dọn memory định kỳ
# ============================================================

def _prune_site_code_seen():
    """FIX 6: Dọn các entry hết TTL trong _site_code_seen để tránh rò rỉ memory."""
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    expired = [k for k, ts in bot_state._site_code_seen.items() if now - ts > ttl]
    for k in expired:
        del bot_state._site_code_seen[k]
    if expired:
        logger.debug(f"🧹 Đã dọn {len(expired)} entry hết hạn khỏi site_code_seen")


def is_site_code_duplicate(domain: str, code: str) -> bool:
    """
    FIX 5: Trả True nếu code này đã được submit cho domain trong TTL giây gần đây.
    Tự dọn entry hết hạn trước khi kiểm tra để tránh rò rỉ memory.
    """
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    _prune_site_code_seen()
    key = (domain, code.upper())
    seen_at = bot_state._site_code_seen.get(key)
    if seen_at is not None and now - seen_at < ttl:
        return True
    # Ghi nhận lần đầu thấy
    bot_state._site_code_seen[key] = now
    return False


# ============================================================
# 📒 LỊCH SỬ CODE / DAILY MAINTENANCE LOG
# ============================================================
CODE_HISTORY_DIR = Path("logs/code_history")
CODE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _write_history_row(row: dict):
    """Ghi 1 dòng vào CSV + JSONL — chạy trong background worker."""
    try:
        fieldnames = [
            "time", "event_type", "channel", "site", "account", "code",
            "source", "status", "telegram_delay", "submit_elapsed",
            "message", "screenshot",
        ]
        csv_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        jsonl_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.jsonl"

        write_header = not csv_path.exists()
        with csv_path.open("a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(row)

        with jsonl_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug(f"⚠️ Không ghi được code history: {e}")


async def _history_writer_loop():
    """Worker chạy nền, xử lý queue ghi lịch sử."""
    global _history_queue
    while True:
        try:
            row = await _history_queue.get()
            if row is None:
                break
            # Ghi file trong thread pool để không block event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _write_history_row, row)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"⚠️ history_writer_loop lỗi: {e}")
        finally:
            try:
                _history_queue.task_done()
            except Exception:
                pass


def start_history_writer():
    """Khởi động background writer task."""
    global _history_queue, _history_writer_task
    _history_queue = asyncio.Queue(maxsize=2000)
    _history_writer_task = asyncio.create_task(_history_writer_loop())
    logger.info("✅ Background history writer đã khởi động")


def get_submit_semaphore() -> asyncio.Semaphore:
    """✅ V5.0: Semaphore tăng từ 3 → 5 để submit nhanh hơn (nhưng vẫn an toàn)."""
    global _submit_semaphore
    if _submit_semaphore is None:
        limit = max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS", 5)))
        _submit_semaphore = asyncio.Semaphore(limit)
    return _submit_semaphore


async def submit_code_limited(user: str, code: str, target_url: str, systems: dict):
    """Chạy submit trong semaphore riêng, không chặn message worker Telegram."""
    sem = get_submit_semaphore()
    async with sem:
        # ✅ FIX MIN_DELAY: Áp dụng MIN_DELAY_BETWEEN_SUBMITS thực sự (trước đây config này bị bỏ qua)
        delay = float(getattr(Config, "MIN_DELAY_BETWEEN_SUBMITS", 0.8))
        if delay > 0:
            await asyncio.sleep(delay)
        return await submit_code_safe(user, code, target_url, systems)


def track_submit_task(task: asyncio.Task, label: str = ""):
    """Giữ reference task và log exception để task nền không bị im lặng."""
    _active_submit_tasks.add(task)

    def _done(t: asyncio.Task):
        _active_submit_tasks.discard(t)
        try:
            result = t.result()
            if isinstance(result, dict):
                ok = "✅" if result.get("success") else "⚠️"
                logger.info(f"{ok} [SUBMIT TASK DONE] {label} | {result.get('message', '')}")
        except asyncio.CancelledError:
            logger.debug(f"🛑 [SUBMIT TASK CANCELLED] {label}")
        except Exception as e:
            logger.error(f"❌ [SUBMIT TASK ERROR] {label}: {e}")

    task.add_done_callback(_done)
    return task


def append_code_history(
    event_type: str,
    code: str = "",
    target_url: str = "",
    account: str = "",
    channel: str = "",
    source: str = "",
    status: str = "",
    telegram_delay=None,
    submit_elapsed=None,
    message: str = "",
    screenshot: str = "",
):
    """
    Enqueue lịch sử code — KHÔNG block luồng chính.
    Ghi file thực tế được thực hiện bởi background worker.
    """
    try:
        row = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "channel": channel or "",
            "site": normalize_domain(target_url),
            "account": account or "",
            "code": str(code or ""),
            "source": source or "",
            "status": status or "",
            "telegram_delay": "" if telegram_delay is None else f"{float(telegram_delay):.2f}",
            "submit_elapsed": "" if submit_elapsed is None else f"{float(submit_elapsed):.2f}",
            "message": str(message or "").replace("\n", " ")[:300],
            "screenshot": str(screenshot or ""),
        }

        if _history_queue is not None:
            try:
                _history_queue.put_nowait(row)
            except asyncio.QueueFull:
                logger.debug("⚠️ History queue đầy, bỏ qua 1 dòng log")
        else:
            # Fallback nếu chưa khởi động writer
            _write_history_row(row)

        return row
    except Exception as e:
        logger.debug(f"⚠️ Không enqueue được code history: {e}")
        return None


def build_daily_summary():
    """Tạo file tổng kết cuối ngày từ lịch sử RESULT."""
    try:
        csv_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        if not csv_path.exists():
            logger.info("📒 Chưa có lịch sử code hôm nay để tổng kết")
            return None

        summary = {}
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("event_type") != "RESULT":
                    continue
                key = (row.get("site", ""), row.get("account", ""))
                if key not in summary:
                    summary[key] = {"SUCCESS": 0, "FAILED": 0, "UNKNOWN": 0}
                status = row.get("status") or "UNKNOWN"
                if status not in summary[key]:
                    summary[key][status] = 0
                summary[key][status] += 1

        out_path = CODE_HISTORY_DIR / f"daily_summary_{_today_str()}.csv"
        with out_path.open("w", newline="", encoding="utf-8-sig") as f:
            fieldnames = ["date", "site", "account", "success", "failed", "unknown", "total"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for (site, account), counts in sorted(summary.items()):
                success = counts.get("SUCCESS", 0)
                failed = counts.get("FAILED", 0)
                unknown = counts.get("UNKNOWN", 0)
                writer.writerow({
                    "date": _today_str(),
                    "site": site,
                    "account": account,
                    "success": success,
                    "failed": failed,
                    "unknown": unknown,
                    "total": success + failed + unknown,
                })

        logger.info(f"📒 Đã tạo báo cáo cuối ngày: {out_path}")
        return str(out_path)
    except Exception as e:
        logger.warning(f"⚠️ Không tạo được daily summary: {e}")
        return None


def measure_telegram_delay(event) -> float | None:
    """Đo độ trễ thật giữa thời gian Telegram ghi nhận và thời điểm hiện tại."""
    try:
        msg_time = event.message.date
        if msg_time.tzinfo is None:
            msg_time = msg_time.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        delay = (now_utc - msg_time).total_seconds()
        return delay
    except Exception:
        return None


def log_telegram_delay(event):
    """Đo và log delay — giữ lại để tương thích, dùng trong process_telegram_message."""
    delay = measure_telegram_delay(event)
    if delay is not None:
        logger.warning(
            f"⏱️ Delay Telegram (tính từ lúc nhận): {delay:.2f}s "
            f"| msg_time_utc={event.message.date.strftime('%H:%M:%S')}"
        )
    else:
        logger.warning("⚠️ Không đo được Delay Telegram")
    return delay


def page_key(user: str, target_url: str) -> str:
    return f"{user.lower()}|{normalize_domain(target_url)}"


def get_user_port(user: str) -> int:
    for port, users_list in getattr(Config, "CDP_CONNECTIONS", {}).items():
        if user in users_list:
            return int(port)
    return 9222


def build_unique_account_targets():
    """
    CHẾ ĐỘ 1 TAB / KÊNH:
    Mỗi domain chỉ mở đúng 1 tab duy nhất (dùng account priority cao nhất để lấy port).
    Trả về danh sách theo domain, không lặp lại cùng domain.
    """
    items = []
    seen_domains = set()

    sorted_channels = sorted(
        Config.CHANNEL_CONFIG.items(),
        key=lambda item: item[1].get("priority", 999),
    )

    for chat_id, channel_config in sorted_channels:
        target_url = channel_config["url"]
        domain = normalize_domain(target_url)

        # Mỗi domain chỉ tạo 1 tab duy nhất
        if domain in seen_domains:
            continue
        seen_domains.add(domain)

        accounts = channel_config.get("accounts", [])
        if not accounts:
            continue

        # Dùng account đầu tiên (priority cao nhất) để xác định port CDP
        first_account = sorted(accounts, key=lambda a: a.get("priority", 999))[0]
        port = get_user_port(first_account["username"])

        items.append(
            {
                "chat_id": chat_id,
                "channel_name": channel_config.get("name", ""),
                "target_url": target_url,
                "domain": domain,
                "port": port,
                "accounts": sorted(accounts, key=lambda a: a.get("priority", 999)),
            }
        )

    return items


async def verify_telegram_session():
    logger.info("\n" + "=" * 70)
    logger.info("🔐 XÁC MINH TELEGRAM SESSION...")

    try:
        me = await client.get_me()
        logger.info("✅ SESSION HỢP LỆ!")
        logger.info(f"   👤 Username: @{me.username}")
        logger.info(f"   🆔 User ID: {me.id}")
        return True
    except Exception as e:
        logger.error(f"❌ SESSION LỖI: {e}")
        return False


async def verify_channels_and_get_ids():
    logger.info("\n" + "=" * 70)
    logger.info("📡 XÁC MINH CHANNELS...")

    valid_channels = {}
    my_dialogs = {dialog.id: dialog async for dialog in client.iter_dialogs()}

    for chat_id, channel_config in Config.CHANNEL_CONFIG.items():
        if chat_id in my_dialogs:
            logger.info(f"✅ HỢP LỆ: {channel_config['name']}")
            valid_channels[chat_id] = channel_config
        else:
            logger.warning(f"❌ CHƯA THAM GIA: {channel_config['name']}")

    return valid_channels


async def init_systems():
    print_version_info()

    # ✅ FIX CONFIG-VALIDATE: Luôn chạy validate lúc khởi động, không phụ thuộc RUN_SYSTEM_TEST_ON_START
    if not ConfigValidator.validate_all():
        logger.critical("❌ Config không hợp lệ! Kiểm tra lại config và chạy lại bot.")
        raise SystemExit(1)

    db = init_database(Config.DATABASE_PATH)
    anti_det = init_anti_detection()
    health_mon, alert_mgr, perf_mon = init_monitoring()
    task_q, sched = init_task_system(max_concurrent=Config.MAX_CONCURRENT_SUBMITS)

    bot_state.playwright_instance = await async_playwright().start()
    get_shutdown_handler().setup(bot_state)

    # Khởi động background history writer ngay khi init
    start_history_writer()

    return {
        "db": db,
        "anti_detection": anti_det,
        "performance_monitor": perf_mon,
    }


async def safe_is_visible(element) -> bool:
    try:
        return await element.is_visible()
    except Exception:
        return False


# ============================================================
# ⚡ INPUT FIELDS — CÓ CACHE ĐỂ TRÁNH SCAN LẠI MỖI LẦN
# ============================================================

def _invalidate_input_cache(key: str):
    """Xóa cache input fields cho một page key."""
    bot_state._input_cache.pop(key, None)


async def find_input_fields(page, cache_key: str = None):
    """
    Tìm input fields. Nếu có cache_key thì cache kết quả lại TTL giây.
    Cache chỉ dùng cho submit (không dùng khi preload để tránh stale).
    """
    now = time.time()

    if cache_key:
        cached = bot_state._input_cache.get(cache_key)
        if cached:
            username_input, code_input, cache_time = cached
            if now - cache_time < bot_state._input_cache_ttl:
                # Kiểm tra nhanh xem element còn attached không
                try:
                    if code_input:
                        await code_input.is_visible()
                    return username_input, code_input
                except Exception:
                    # Element stale, xóa cache
                    _invalidate_input_cache(cache_key)

    username_input = None
    code_input = None

    username_selectors = [
        "input[placeholder*='tài' i]",
        "input[placeholder*='user' i]",
        "input[placeholder*='login' i]",
        "input[name='username']",
        "input[name='user']",
        "input[id='username']",
        "input[id='user']",
        "input[type='text']",
    ]

    code_selectors = [
        "input[placeholder*='code' i]",
        "input[placeholder*='mã' i]",
        "input[placeholder*='gift' i]",
        "input[name='code']",
        "input[name='giftcode']",
        "input[id='code']",
        "input[id='giftcode']",
    ]

    try:
        for selector in username_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    username_input = element
                    break
            except Exception:
                pass

        for selector in code_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    code_input = element
                    break
            except Exception:
                pass

        if not username_input or not code_input:
            inputs = await page.query_selector_all(
                "input:not([type='hidden']):not([type='checkbox']):not([type='radio']):not([type='submit'])"
            )

            visible_inputs = []
            for input_element in inputs:
                if await safe_is_visible(input_element):
                    visible_inputs.append(input_element)

            if len(visible_inputs) >= 2:
                if not username_input:
                    username_input = visible_inputs[0]
                if not code_input:
                    code_input = visible_inputs[1]

            elif len(visible_inputs) == 1:
                if not code_input:
                    code_input = visible_inputs[0]

    except Exception as e:
        logger.debug(f"⚠️ Lỗi tìm input fields: {e}")

    # Lưu cache nếu tìm thấy code_input
    if cache_key and code_input:
        bot_state._input_cache[cache_key] = (username_input, code_input, now)

    return username_input, code_input


async def get_input_value(input_element) -> str:
    try:
        value = await input_element.input_value(timeout=1000)
        return value.strip()
    except Exception:
        return ""


async def click_submit_fast(page) -> bool:
    """
    Bấm submit nhanh nhất có thể.
    Ưu tiên JS click (nhanh hơn Playwright click), fallback Enter.
    """
    fast_selectors = [
        ".submit-btn",
        "img.submit-btn",
        ".submit-button-container .submit-btn",
        ".submit-button-container img",
        "[class*='submit' i]",
        "[class*='check' i]",
        "button[type='submit']",
        "input[type='submit']",
        "button:has-text('Nạp')",
        "button:has-text('NHẬN')",
        "button:has-text('Gửi')",
        "button:has-text('Submit')",
        "button:has-text('Redeem')",
        "button",
    ]

    for selector in fast_selectors:
        try:
            element = await page.query_selector(selector)
            if element and await safe_is_visible(element):
                # Dùng JS click để nhanh hơn, tránh overhead scroll/focus
                await page.evaluate("el => el.click()", element)
                return True
        except Exception:
            pass

    try:
        await page.keyboard.press("Enter")
        return True
    except Exception:
        return False


async def take_result_screenshot(page, user: str, code: str, target_url: str, status: str) -> str:
    """Chụp ảnh sau khi đã bấm nạp, chỉ dùng cho UNKNOWN/FAILED nếu bật config."""
    if not bool(getattr(Config, "SCREENSHOT_ON_UNKNOWN", False)):
        return ""

    try:
        shot_dir = Path("logs/screenshots")
        shot_dir.mkdir(parents=True, exist_ok=True)
        safe_domain = normalize_domain(target_url).replace(".", "_").replace("/", "_")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = shot_dir / f"{safe_domain}_{user}_{code}_{status}_{ts}.png"
        await page.screenshot(path=str(path), full_page=False)
        return str(path)
    except Exception as e:
        logger.debug(f"⚠️ Không chụp được screenshot kết quả: {e}")
        return ""


async def fill_username_if_needed(page, user: str, cache_key: str = None) -> bool:
    username_input, _ = await find_input_fields(page, cache_key=cache_key)

    if not username_input:
        return False

    current_value = await get_input_value(username_input)

    if current_value.lower() == user.lower():
        return True

    if current_value == "":
        try:
            await username_input.fill(user)
            return True
        except Exception:
            return False

    return False


async def connect_to_cdp_port(port: int):
    if port in bot_state.connected_browsers:
        return bot_state.connected_browsers[port]

    logger.info(f"🖥️ Đang kết nối trình duyệt CDP port {port}...")
    cdp_url = f"http://127.0.0.1:{port}"

    browser = await bot_state.playwright_instance.chromium.connect_over_cdp(cdp_url)
    bot_state.connected_browsers[port] = browser

    logger.info(f"✅ Đã kết nối CDP port {port}")
    return browser


async def _setup_page_performance(page, label: str = ""):
    """
    Tối ưu tốc độ cho tab nhập code:
    - Block ảnh, font, media, websocket tracking, analytics (không cần thiết để nhập code).
    - Giảm tải CPU/RAM đáng kể khi mở nhiều tab.
    """
    # Các domain tracking/ads vô dụng → abort thẳng để giảm network + CPU
    _BLOCK_DOMAINS = (
        "google-analytics", "googletagmanager", "doubleclick",
        "facebook.net", "fbcdn.net", "hotjar", "clarity.ms",
        "crisp.chat", "intercom", "tawk.to", "pusher",
        "sentry.io", "newrelic", "datadog-browser",
    )
    _BLOCK_TYPES = ("media", "ping")
    _KEEP_KEYWORDS = ("tangqua", "giftcode", "code", "reward", "inputcode", "uy88", "mmoo")

    async def _handle_route(route):
        req = route.request
        url = req.url.lower()
        rtype = req.resource_type
        # Luôn cho phép Cloudflare/Turnstile
        if "challenges.cloudflare.com" in url or "cloudflare" in url:
            await route.continue_()
            return

        # Block tracking domains ngay lập tức
        if any(d in url for d in _BLOCK_DOMAINS):
            await route.abort()
            return

        # Block resource types nặng (trừ những URL quan trọng)
        if rtype in _BLOCK_TYPES and not any(k in url for k in _KEEP_KEYWORDS):
            await route.abort()
            return
        await route.continue_()

    try:
        await page.route("**/*", _handle_route)
        # Ẩn dấu hiệu automation để Cloudflare Turnstile không nhận ra bot
        await page.add_init_script("""
            // Xóa navigator.webdriver - dấu hiệu rõ nhất của automation
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            // Ẩn các biến automation của Chrome/Edge CDP
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            // Giả lập plugins như trình duyệt thật
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en-US', 'en'] });
        """)
        # Tắt animation CSS để giảm CPU render
        await page.add_init_script("""
            document.addEventListener('DOMContentLoaded', () => {
                const style = document.createElement('style');
                style.textContent = '*, *::before, *::after { animation-duration: 0.001ms !important; transition-duration: 0.001ms !important; }';
                document.head.appendChild(style);
            });
        """)
        logger.debug(f"⚡ [{label}] Đã bật chặn ảnh/font/media/tracking để giảm lag tab")
    except Exception as e:
        logger.debug(f"⚠️ [{label}] Không setup page performance: {e}")


async def _wake_tab_for_submit(page, label: str = ""):
    """
    Đánh thức tab trước khi submit để tránh throttling của Chrome:
    - Chrome throttle JS và setTimeout trên tab nền (background tab).
    - bring_to_front() + JS visibility trick giúp tab hoạt động bình thường.
    """
    try:
        await page.bring_to_front()
        # Override visibilityState để Chrome không throttle tab này
        await page.evaluate("""
            Object.defineProperty(document, 'visibilityState', {
                get: () => 'visible', configurable: true
            });
            Object.defineProperty(document, 'hidden', {
                get: () => false, configurable: true
            });
            document.dispatchEvent(new Event('visibilitychange'));
        """)
    except Exception:
        try:
            await page.bring_to_front()
        except Exception:
            pass


# ============================================================
# 🐕 WATCHDOG TỰ ĐỘNG ĐIỀN USERNAME KHI TAB BỊ F5
# ============================================================

async def auto_fill_usernames_watchdog():
    """
    ✅ V5.0: Watchdog interval tăng từ 30s → 10s (nhanh hơn 3x).
    Kiểm tra tất cả các tab đang quản lý.
    - Chỉ điền username khi ô trống hoàn toàn.
    - Mỗi domain chỉ được điền tối đa 1 lần trong 5 phút (tránh spam khi user liên tục xóa).
    """
    CHECK_INTERVAL = 10  # ✅ Giảm từ 30 xuống 10 (nhanh hơn 3x)
    last_filled_time = {}  # domain_key -> timestamp

    while bot_state.is_running:
        await asyncio.sleep(CHECK_INTERVAL)
        if not bot_state.account_pages:
            continue

        for domain_key, page in list(bot_state.account_pages.items()):
            try:
                if page.is_closed():
                    last_filled_time.pop(domain_key, None)
                    continue

                username_input, _ = await find_input_fields(page, cache_key=None)
                if not username_input:
                    continue

                current_value = await get_input_value(username_input)

                # Nếu ô đã có username → bỏ qua, xóa dấu hiệu đã điền
                if current_value.strip():
                    last_filled_time.pop(domain_key, None)
                    continue

                # Ô đang trống → cần điền, nhưng kiểm tra cooldown
                now = time.time()
                last_filled = last_filled_time.get(domain_key)
                if last_filled and (now - last_filled) < 300:  # 5 phút
                    continue  # Đã điền gần đây, bỏ qua

                default_user = get_default_account_for_domain(domain_key)
                if not default_user:
                    continue

                # Điền username
                await page.evaluate(
                    "(el, val) => { el.value = val; el.dispatchEvent(new Event('input', {bubbles:true})); el.dispatchEvent(new Event('change', {bubbles:true})); }",
                    username_input, default_user
                )
                logger.info(f"🔄 [{domain_key}] Tự động điền username '{default_user}' (phát hiện ô trống)")
                last_filled_time[domain_key] = now

            except Exception as e:
                logger.debug(f"⚠️ Lỗi watchdog fill username cho {domain_key}: {e}")


async def open_new_tab_for_account(context, user: str, target_url: str):
    """
    Tự mở tab mới khi account chưa có tab đúng web.
    Sau khi mở, bot thử điền username nếu tìm thấy ô tài khoản.
    """
    domain = normalize_domain(target_url)
    page = await context.new_page()

    # Tối ưu tab ngay khi tạo: block ảnh/font/media để tải nhanh hơn
    await _setup_page_performance(page, label=f"{user}|{domain}")

    try:
        logger.info(f"🆕 [{user} | {domain}] Chưa có tab sẵn, bot tự mở tab mới...")
        await page.goto(
            target_url,
            wait_until="domcontentloaded",
            timeout=Config.PAGE_LOAD_TIMEOUT,
        )
    except Exception as e:
        logger.warning(f"⚠️ [{user} | {domain}] Mở tab mới nhưng tải trang lỗi: {e}")

    try:
        await asyncio.sleep(float(getattr(Config, "AUTO_OPEN_TAB_WAIT", 1.0)))
    except Exception:
        await asyncio.sleep(1)

    try:
        await page.bring_to_front()
    except Exception:
        pass

    # ✅ KHÔNG tự điền username — người dùng tự đăng nhập thủ công
    logger.info(f"🖥️ [{user} | {domain}] Tab đã mở. Hãy tự đăng nhập trên cửa sổ Edge.")

    return page


async def find_best_page_for_account(context, user: str, target_url: str, assigned_pages: set):
    target_domain = normalize_domain(target_url)
    auto_open = bool(getattr(Config, "AUTO_OPEN_MISSING_TABS", True))

    matching_pages = []

    for page in context.pages:
        try:
            page_url = page.url.lower()
            if target_domain in page_url and page not in assigned_pages:
                matching_pages.append(page)
        except Exception:
            pass

    # 1. Ưu tiên tab đã có đúng username.
    for page in matching_pages:
        username_input, _ = await find_input_fields(page)
        if not username_input:
            continue

        current_value = await get_input_value(username_input)

        if current_value.lower() == user.lower():
            return page, "Tab đã điền đúng username"

    # 2. Nếu có tab đúng domain và ô username trống thì tự điền.
    for page in matching_pages:
        username_input, _ = await find_input_fields(page)
        if not username_input:
            continue

        current_value = await get_input_value(username_input)

        if current_value == "":
            try:
                await username_input.fill(user)
                return page, "Tab username trống, bot đã tự điền"
            except Exception:
                continue

    # 3. Nếu có tab đúng domain nhưng không tìm thấy ô username.
    for page in matching_pages:
        username_input, _ = await find_input_fields(page)

        if not username_input:
            try:
                await page.bring_to_front()
            except Exception:
                pass

            return page, "Tab đúng domain nhưng không tìm thấy ô username"

    # 4. Nếu không có tab phù hợp, tự mở tab mới.
    if auto_open:
        try:
            page = await open_new_tab_for_account(context, user, target_url)
            return page, "Bot tự mở tab mới và thử điền username"
        except Exception as e:
            return None, f"Không mở được tab mới: {e}"

    return None, "Không có tab đúng domain và AUTO_OPEN_MISSING_TABS đang tắt"

async def preload_browsers_and_accounts():
    logger.info("\n" + "=" * 70)
    logger.info("🔄 ĐANG MỞ TAB — CHẾ ĐỘ 1 TAB / KÊNH (DOMAIN)...")

    account_targets = build_unique_account_targets()
    assigned_pages = set()

    if not account_targets:
        logger.error("❌ Không có kênh nào trong CHANNEL_CONFIG")
        return

    for item in account_targets:
        target_url = item["target_url"]
        domain = item["domain"]
        port = item["port"]
        accounts = item["accounts"]
        # Key theo domain (không phải user|domain) vì chỉ có 1 tab cho cả domain
        key = domain

        try:
            browser = await connect_to_cdp_port(port)

            if not browser.contexts:
                logger.error(f"❌ Port {port} không có browser context")
                continue

            context = browser.contexts[0]

            # Tìm tab đã mở đúng domain, hoặc tự mở tab mới
            page = None
            reason = ""

            for p in context.pages:
                try:
                    if domain in p.url.lower() and p not in assigned_pages:
                        page = p
                        reason = "Tìm thấy tab đã mở sẵn"
                        break
                except Exception:
                    pass

            if not page:
                if bool(getattr(Config, "AUTO_OPEN_MISSING_TABS", True)):
                    logger.info(f"🆕 [{domain}] Chưa có tab sẵn, bot tự mở tab mới...")
                    page = await context.new_page()
                    await _setup_page_performance(page, label=domain)
                    try:
                        await page.goto(
                            target_url,
                            wait_until="domcontentloaded",
                            timeout=Config.PAGE_LOAD_TIMEOUT,
                        )
                        await asyncio.sleep(float(getattr(Config, "AUTO_OPEN_TAB_WAIT", 1.0)))
                    except Exception as e:
                        logger.warning(f"⚠️ [{domain}] Mở tab mới nhưng tải trang lỗi: {e}")
                    reason = "Bot tự mở tab mới"
                else:
                    logger.error(
                        f"❌ [{domain}] Không có tab sẵn và AUTO_OPEN_MISSING_TABS đang tắt. "
                        f"Hãy mở thủ công: {target_url}"
                    )
                    continue

            assigned_pages.add(page)

            # Lưu tab theo key = domain
            bot_state.account_pages[key] = page
            bot_state.context_locks[key] = asyncio.Lock()
            bot_state.cf_verified[key] = True
            bot_state.submission_count[key] = 0

            await _setup_page_performance(page, label=domain)

            try:
                await page.bring_to_front()
            except Exception:
                pass

            _, code_input = await find_input_fields(page)
            first_user = accounts[0]["username"] if accounts else ""

            if code_input:
                logger.info(
                    f"✅ [{domain}] {reason}. Sẵn sàng nhập code. "
                    f"Tài khoản theo thứ tự: {[a['username'] for a in accounts]}"
                )
            else:
                logger.warning(
                    f"⚠️ [{domain}] {reason} nhưng chưa thấy ô nhập code. "
                    f"Kiểm tra login/Cloudflare trên tab này."
                )

        except Exception as e:
            logger.error(f"❌ Lỗi thiết lập tab [{domain}]: {e}")

    total = len(bot_state.account_pages)
    logger.info(f"✅ Tổng số tab đã mở: {total} tab cho {total} domain")
    logger.info("=" * 70 + "\n")


# ============================================================
# 🔍 BỘ TRÍCH XUẤT CODE — SPOILER / MARKER / STRICT FILTER
# ============================================================

def validate_candidate(code: str, target_url: str, source: str = "normal"):
    """
    Gọi CodeValidator theo kiểu tương thích:
    - Nếu code_validator.py mới hỗ trợ source thì truyền source.
    - Nếu đang là bản cũ thì tự fallback để không bị TypeError.
    """
    try:
        return CodeValidator.validate_code(code, target_url, source=source)
    except TypeError:
        return CodeValidator.validate_code(code, target_url)


def get_filter_group_name(target_url: str) -> str:
    group_name, _ = CodeValidator.get_filter_group(target_url)
    return group_name


# Các nhóm cần chặn block/fallback để tránh bắt rác (Q88CODE, Q88DANGNHAP...)
# Nhóm chặn block/fallback toàn bài để tránh bắt rác
# new88 KHÔNG nằm đây vì code NEW88 nằm thẳng dạng 2 cột plain text, không dùng spoiler
_STRICT_GROUPS = {"multi_site_strict", "qq88"}


def is_multi_site_strict(target_url: str) -> bool:
    """Chỉ kiểm tra đúng nhóm multi_site_strict (giữ lại để tương thích)."""
    return get_filter_group_name(target_url) == "multi_site_strict"


def is_strict_group(target_url: str) -> bool:
    """True nếu URL thuộc bất kỳ nhóm nào cần lọc gắt (multi_site_strict + qq88)."""
    return get_filter_group_name(target_url) in _STRICT_GROUPS


def looks_like_marker_code(token: str) -> bool:
    clean = CodeValidator.clean_code(token)
    return (
        6 <= len(clean) <= 10
        and clean == clean.upper()
        and any(c.isalpha() for c in clean)
        and any(c.isdigit() for c in clean)
        and not clean.isdigit()
    )


def unique_keep_order(items):
    seen = set()
    result = []

    for item in items:
        clean = CodeValidator.clean_code(item)
        if not clean:
            continue

        upper = clean.upper()

        if upper not in seen:
            seen.add(upper)
            result.append(clean)

    return result


def remove_noise_from_text(text: str) -> str:
    cleaned = text or ""

    cleaned = re.sub(r"https?://\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"www\.\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b[a-zA-Z0-9.-]+\.(com|net|org|vn|app|info)\b", " ", cleaned, flags=re.IGNORECASE)

    cleaned = cleaned.replace("：", ":")
    cleaned = cleaned.replace("|", " ")
    cleaned = cleaned.replace("•", " ")

    return cleaned


def line_has_code_marker(line: str) -> bool:
    upper = line.upper()

    markers = [
        "NHẬN CODE NGAY",
        "NHAN CODE NGAY",
        "NHẬN CODE",
        "NHAN CODE",
        "NHẬP CODE",
        "NHAP CODE",
        "PHÁT CODE",
        "PHAT CODE",
        "CODE FREE",
    ]

    return any(marker in upper for marker in markers)


def line_is_noise(line: str) -> bool:
    upper = line.upper().strip()

    if not upper:
        return True

    noise_keywords = [
        "HTTP",
        "WWW",
        ".COM",
        "FACEBOOK",
        "TELEGRAM",
        "TIKTOK",
        "ZALO",
        "CSKH",
        "BOT",
        "CHECK LINK",
        "LINK",
        "ĐĂNG KÝ",
        "DANG KY",
        "TRUY CẬP",
        "TRUY CAP",
        "THÔNG TIN",
        "THONG TIN",
        "THÔNG TIN LIÊN HỆ",
        "THONG TIN LIEN HE",
        "TRANG CHỦ",
        "TRANG CHU",
        "CHÍNH THỨC",
        "CHINH THUC",
        "CẢNH BÁO",
        "CANH BAO",
        "GIẢ MẠO",
        "GIA MAO",
        "VIEW POST",
        "COMMENT",
        "HASHTAG",
        "HOTLINE",
    ]

    return any(keyword in upper for keyword in noise_keywords)


def extract_tokens_from_line(line: str):
    special_chars = re.escape(getattr(Config, "SPECIAL_CODE_CHARS_30", ""))
    min_len = getattr(Config, "CODE_MIN_LENGTH", 6)
    max_len = getattr(Config, "CODE_MAX_LENGTH", 15)
    max_raw_len = max_len + 30

    pattern = rf"[A-Za-z0-9{special_chars}]{{{min_len},{max_raw_len}}}"
    tokens = []

    for candidate in re.findall(pattern, line or ""):
        clean = CodeValidator.clean_code(candidate)
        if min_len <= len(clean) <= max_len:
            tokens.append(candidate)

    return tokens


def token_score(token: str, target_url: str) -> int:
    validation = validate_candidate(token, target_url)

    if not validation["valid"]:
        return -100

    clean = validation["clean_code"]
    score = 0

    has_lower = any(c.islower() for c in clean)
    has_upper = any(c.isupper() for c in clean)
    has_digit = any(c.isdigit() for c in clean)

    if has_upper and not has_lower:
        score += 35

    if has_lower and has_upper:
        score += 20

    if has_digit:
        score += 25

    if 6 <= len(clean) <= 10:
        score += 20

    if validation.get("confidence", 0) >= 0.9:
        score += 20

    if CodeValidator.detect_site_identity(clean):
        score += 30

    return score


def extract_spoiler_codes(event, target_url: str):
    """
    Nếu code nằm trong spoiler/làm mờ:
    - Không bắt buộc dấu đặc biệt (source="spoiler").
    - Xử lý spoiler text có nhiều dòng hoặc nhiều code cách nhau bằng khoảng trắng.
    - Ví dụ LLwin: "❤BA3Z71❤G5AMJC❤P2L3R4❤9XQU6❤" → bắt được 4 code.
    """
    codes = []

    if not event.message.entities:
        return codes

    try:
        for entity, entity_text in event.message.get_entities_text():
            if not isinstance(entity, MessageEntitySpoiler):
                continue

            spoiler_text = (entity_text or "").strip()
            if not spoiler_text:
                continue

            # Tách từng dòng trong spoiler (có thể có nhiều code multi-line)
            spoiler_lines = spoiler_text.splitlines() if "\n" in spoiler_text else [spoiler_text]

            for spoiler_line in spoiler_lines:
                spoiler_line = spoiler_line.strip()
                if not spoiler_line:
                    continue

                tokens = extract_tokens_from_line(spoiler_line)
                if not tokens:
                    # Fallback: cả dòng là 1 token
                    tokens = [spoiler_line]

                for token in tokens:
                    validation = validate_candidate(token, target_url, source="spoiler")

                    if validation["valid"]:
                        codes.append(validation["clean_code"])
                        logger.info(
                            f"🔒 Spoiler code: {token} -> {validation['clean_code']} "
                            f"| group={validation.get('filter_group')} "
                            f"| special={validation.get('special_count')}"
                        )
                    else:
                        logger.debug(f"🔒 Bỏ spoiler token {token}: {validation['reason']}")

    except Exception as e:
        logger.warning(f"⚠️ Lỗi đọc spoiler/làm mờ: {e}")

    return unique_keep_order(codes)


def extract_marker_near_codes(text: str, target_url: str):
    """
    Nếu code nằm gần dòng NHẬN CODE NGAY:
    cho phép code 6 ký tự viết hoa + có số.
    Chỉ scan 3 dòng gần marker với nhóm strict để tránh bắt rác.
    """
    cleaned_text = remove_noise_from_text(text)
    lines = [line.strip() for line in cleaned_text.splitlines()]
    codes = []

    # QQ88 cũng dùng strict scan (3 dòng, chỉ bắt code dạng UPPER+digit)
    strict_group = is_strict_group(target_url)
    scan_limit = 3 if strict_group else 8

    for index, line in enumerate(lines):
        if not line_has_code_marker(line):
            continue

        scan_lines = []

        if line:
            scan_lines.append(line)

        for offset in range(1, scan_limit + 1):
            if index + offset < len(lines):
                scan_lines.append(lines[index + offset])

        for scan_line in scan_lines:
            if line_is_noise(scan_line):
                continue

            tokens = extract_tokens_from_line(scan_line)

            if strict_group:
                tokens.extend(re.findall(r"\b[A-Z0-9]{6,10}\b", scan_line))

            for token in tokens:
                clean = CodeValidator.clean_code(token)

                if strict_group:
                    if not looks_like_marker_code(clean):
                        continue

                    validation = validate_candidate(clean, target_url, source="marker")

                    if validation["valid"]:
                        codes.append(validation["clean_code"])
                        logger.info(f"🎯 Marker strict code: {token} -> {validation['clean_code']}")
                    else:
                        logger.info(f"🎯 Bỏ marker token {token}: {validation['reason']}")

                else:
                    validation = validate_candidate(token, target_url, source="marker")
                    if validation["valid"]:
                        codes.append(validation["clean_code"])

    return unique_keep_order(codes)


def extract_code_block_codes(text: str, target_url: str):
    # QQ88 cũng chặn block toàn bài để tránh Q88CODE, Q88DANGNHAP, Q88NOHU...
    if is_strict_group(target_url):
        return []

    cleaned_text = remove_noise_from_text(text)
    lines = [line.strip() for line in cleaned_text.splitlines()]
    codes = []

    consecutive_code_lines = []

    for line in lines:
        if line_is_noise(line):
            if len(consecutive_code_lines) >= 2:
                break
            continue

        tokens = extract_tokens_from_line(line)
        valid_tokens = []

        for token in tokens:
            validation = validate_candidate(token, target_url)
            if validation["valid"]:
                valid_tokens.append(validation["clean_code"])

        if valid_tokens:
            consecutive_code_lines.append(valid_tokens)
        else:
            if len(consecutive_code_lines) >= 2:
                break

    for token_group in consecutive_code_lines:
        codes.extend(token_group)

    return unique_keep_order(codes)


def _line_is_pure_noise_or_emoji(line: str) -> bool:
    """Trả True nếu dòng chỉ toàn emoji/ký tự đặc biệt, không có chữ/số thực."""
    stripped = re.sub(r"[^\w]", "", line, flags=re.UNICODE)
    return len(stripped) < 2


def extract_new88_block_codes(text: str, target_url: str):
    """
    NEW88 đặc thù: code nằm thẳng trong tin nhắn dạng 2 cột plain text:
        VHU9PFBB   zTnC4Q7M
        zeaKwkqP   YcPj4PZf
        yS6RSTsC   YrHPb5oo
    - Không dùng spoiler, không có marker NHẬN CODE.
    - Bỏ qua dòng trống và dòng chỉ có emoji (dòng đệm giữa các code).
    - Dừng chỉ khi gặp dòng NOISE thật sự (link, quảng cáo, hashtag).
    - Dùng source="spoiler" để bypass yêu cầu dấu đặc biệt (NEW88 code thuần chữ+số).
    """
    if get_filter_group_name(target_url) != "new88":
        return []

    cleaned_text = remove_noise_from_text(text)
    lines = [line.strip() for line in cleaned_text.splitlines()]
    codes = []
    consecutive_no_code = 0
    MAX_NO_CODE_LINES = 5  # Cho phép tối đa 5 dòng liên tiếp không có code trước khi dừng

    for line in lines:
        if not line or _line_is_pure_noise_or_emoji(line):
            # Dòng trống / emoji thuần → bỏ qua, KHÔNG dừng luôn
            consecutive_no_code += 1
            if consecutive_no_code >= MAX_NO_CODE_LINES:
                break
            continue

        if line_is_noise(line):
            # Dòng quảng cáo/link thật sự → dừng để tránh bắt rác bên dưới
            break

        tokens = extract_tokens_from_line(line)
        found_in_line = 0
        for token in tokens:
            # source="spoiler" để bỏ qua yêu cầu dấu đặc biệt
            # NEW88 code là mix chữ hoa/thường/số, không có dấu đặc biệt
            validation = validate_candidate(token, target_url, source="spoiler")
            if validation["valid"]:
                codes.append(validation["clean_code"])
                found_in_line += 1

        if found_in_line > 0:
            consecutive_no_code = 0
        else:
            consecutive_no_code += 1
            if consecutive_no_code >= MAX_NO_CODE_LINES:
                break

    result = unique_keep_order(codes)
    if result:
        logger.info(f"🎯 [new88] Block 2 cột plain text: {result}")
    return result


def extract_fallback_codes(text: str, target_url: str):
    """
    Ngoài vùng marker:
    nhóm strict không fallback toàn bài để tránh bắt rác.
    """
    # QQ88 cũng không fallback toàn bài để tránh Q88CODE, Q88DANGNHAP, Q88NOHU...
    if is_strict_group(target_url):
        logger.info("🛡️ Bỏ fallback toàn bài cho nhóm strict/qq88 để tránh bắt rác")
        return []

    cleaned_text = remove_noise_from_text(text)
    tokens = extract_tokens_from_line(cleaned_text)
    scored = []

    for token in tokens:
        score = token_score(token, target_url)
        if score > 0:
            scored.append((score, token))

    scored.sort(key=lambda item: item[0], reverse=True)

    return unique_keep_order([token for score, token in scored])


def _get_filter_group_config(target_url: str) -> dict:
    """Lấy dict config của filter group tương ứng với URL."""
    group_name = get_filter_group_name(target_url)
    return Config.CODE_FILTER_GROUPS.get(group_name, Config.CODE_FILTER_GROUPS.get("default", {}))


def extract_codes_from_message(event, raw_text: str, target_url: str):
    group_cfg = _get_filter_group_config(target_url)
    group_name = get_filter_group_name(target_url)
    prefer_spoiler = group_cfg.get("prefer_spoiler", True)
    allow_fallback = group_cfg.get("allow_fallback", True)
    is_new88 = (group_name == "new88")

    # --- BƯỚC 1: Spoiler (nếu group bật prefer_spoiler VÀ không phải new88) ---
    # new88 không dùng spoiler → bỏ qua để không lãng phí và tránh nhầm
    # qq88, multi_site_strict, llwin... đều dùng spoiler
    if prefer_spoiler and not is_new88:
        spoiler_codes = extract_spoiler_codes(event, target_url)
        if spoiler_codes:
            logger.info(f"🎯 [{group_name}] Ưu tiên code trong spoiler/làm mờ: {spoiler_codes}")
            return spoiler_codes

    # --- BƯỚC 2: Marker (NHẬN CODE NGAY / CODE FREE ...) ---
    marker_codes = extract_marker_near_codes(raw_text, target_url)
    if marker_codes:
        logger.info(f"🎯 [{group_name}] Ưu tiên code gần marker NHẬN CODE: {marker_codes}")
        return marker_codes

    # --- BƯỚC 3a: NEW88 block 2 cột plain text (đặc thù riêng) ---
    # NEW88 không dùng spoiler, không có marker, code nằm thẳng dạng 2 cột
    new88_codes = extract_new88_block_codes(raw_text, target_url)
    if new88_codes:
        return new88_codes

    # --- BƯỚC 3b: Block scan thông thường (chỉ nhóm không strict) ---
    # qq88, multi_site_strict → is_strict_group=True → trả []
    block_codes = extract_code_block_codes(raw_text, target_url)
    if block_codes:
        logger.info(f"🎯 [{group_name}] Bắt code theo block nhiều dòng: {block_codes}")
        return block_codes

    # --- BƯỚC 4: Fallback toàn bài (chỉ nhóm allow_fallback=True) ---
    # new88 allow_fallback=False trong config → không fallback
    # qq88  allow_fallback=False → tương tự
    if not allow_fallback:
        logger.info(f"🛡️ [{group_name}] Bỏ fallback toàn bài (allow_fallback=False trong config)")
        return []

    fallback_codes = extract_fallback_codes(raw_text, target_url)
    if fallback_codes:
        logger.info(f"🎯 [{group_name}] Fallback code đã lọc gắt: {fallback_codes}")
    return fallback_codes


# ============================================================
# ⚡ RESULT DETECTION — SONG SONG THAY VÌ TUẦN TỰ
# ============================================================

async def _fetch_element_text(page, selector: str) -> str:
    """Lấy text của tất cả element khớp selector, trả về string ghép."""
    try:
        elements = await page.query_selector_all(selector)
        texts = []
        for element in elements:
            try:
                text = await element.inner_text(timeout=300)
                if text and text.strip():
                    texts.append(text.strip())
            except Exception:
                pass
        return " ".join(texts)
    except Exception:
        return ""


async def detect_result_text(page) -> str:
    """
    Detect result text SONG SONG (asyncio.gather) thay vì quét tuần tự.
    Nhanh hơn đáng kể khi có nhiều popup/toast.
    """
    result_selectors = [
        ".swal2-html-container",
        ".swal2-title",
        ".modal-body",
        ".alert",
        ".message",
        "[class*='success']",
        "[class*='error']",
        "[class*='notice']",
        "[class*='toast']",
    ]

    tasks = [_fetch_element_text(page, sel) for sel in result_selectors]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    combined = ""
    for r in results:
        if isinstance(r, str) and r:
            combined += r + " "

    return combined.strip()


async def submit_code_safe(user: str, code: str, target_url: str, systems: dict):
    start_time = time.time()
    db = systems["db"]
    perf_mon = systems["performance_monitor"]
    # KEY theo domain — 1 tab cho toàn bộ domain, không phụ thuộc vào user
    domain = normalize_domain(target_url)
    key = domain

    if key not in bot_state.context_locks:
        logger.warning(f"⏭️ [{user} | {domain}] Chưa có tab được gán cho domain này, bỏ qua code {code}")
        append_code_history(
            event_type="SKIPPED",
            code=code,
            target_url=target_url,
            account=user,
            status="SKIPPED",
            message="Chưa có tab được gán",
        )
        return {"success": False, "message": "Chưa có tab được gán"}

    try:
        async with bot_state.context_locks[key]:
            page = bot_state.account_pages.get(key)

            if not page:
                logger.warning(f"⏭️ [{user} | {domain}] Không tìm thấy page trong bộ nhớ")
                append_code_history(
                    event_type="SKIPPED",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="SKIPPED",
                    message="Không tìm thấy page",
                )
                return {"success": False, "message": "Không tìm thấy page"}

            # Kiểm tra và reload tab nếu bị lỗi / trang trắng
            try:
                page_url = page.url
                page_ok = bool(page_url) and page_url != "about:blank"
            except Exception:
                page_ok = False
            if not page_ok:
                logger.warning(
                    f"🔄 [{user} | {domain}] Tab bị lỗi/trắng (url={page_url!r}), "
                    f"đang reload về {target_url}..."
                )
                try:
                    await page.goto(
                        target_url,
                        wait_until="domcontentloaded",
                        timeout=Config.PAGE_LOAD_TIMEOUT,
                    )
                    await asyncio.sleep(float(getattr(Config, "AUTO_OPEN_TAB_WAIT", 1.0)))
                    _invalidate_input_cache(key)
                    bot_state.account_pages[key] = page
                    logger.info(f"✅ [{domain}] Đã reload tab thành công")
                except Exception as reload_err:
                    logger.error(f"❌ [{domain}] Không reload được tab: {reload_err}")
                    append_code_history(
                        event_type="ERROR",
                        code=code,
                        target_url=target_url,
                        account=user,
                        status="ERROR",
                        message=f"Tab chết, reload thất bại: {reload_err}",
                    )
                    return {"success": False, "message": f"Tab chết, reload thất bại: {reload_err}"}

            # Đánh thức tab trước khi nhập để tránh Chrome throttle tab nền
            await _wake_tab_for_submit(page, label=f"{user}|{domain}")

            # Luôn xóa cache input trước mỗi lần submit để đảm bảo điền đúng username mới
            _invalidate_input_cache(key)
            username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                # Thử 1 lần nữa
                _invalidate_input_cache(key)
                username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                logger.warning(f"❌ [{user} | {domain}] Không tìm thấy ô nhập code")
                append_code_history(
                    event_type="ERROR",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="ERROR",
                    message="Không tìm thấy ô nhập code",
                )
                return {"success": False, "message": "Không tìm thấy ô nhập code"}

            try:
                # Điền username của account hiện tại vào ô tài khoản (quan trọng khi đổi account)
                if username_input:
                    await page.evaluate(
                        "(el, val) => { el.value = val; el.dispatchEvent(new Event('input', {bubbles:true})); el.dispatchEvent(new Event('change', {bubbles:true})); }",
                        username_input, user
                    )
                await page.evaluate(
                    "(el, val) => { el.focus(); el.value = ''; el.value = val; el.dispatchEvent(new Event('input', {bubbles:true})); el.dispatchEvent(new Event('change', {bubbles:true})); }",
                    code_input, code
                )
            except Exception as e:
                # Stale element — xóa cache và thử lại 1 lần
                _invalidate_input_cache(key)
                logger.warning(f"❌ [{user} | {domain}] Lỗi nhập form (stale?), thử lại: {e}")
                try:
                    username_input, code_input = await find_input_fields(page, cache_key=key)
                    if code_input:
                        if username_input:
                            await page.evaluate(
                                "(el, val) => { el.value = val; el.dispatchEvent(new Event('input', {bubbles:true})); el.dispatchEvent(new Event('change', {bubbles:true})); }",
                                username_input, user
                            )
                        await page.evaluate(
                            "(el, val) => { el.focus(); el.value = ''; el.value = val; el.dispatchEvent(new Event('input', {bubbles:true})); el.dispatchEvent(new Event('change', {bubbles:true})); }",
                            code_input, code
                        )
                    else:
                        raise RuntimeError("Không tìm thấy ô nhập code sau retry")
                except Exception as e2:
                    logger.warning(f"❌ [{user} | {domain}] Lỗi nhập form: {e2}")
                    append_code_history(
                        event_type="ERROR",
                        code=code,
                        target_url=target_url,
                        account=user,
                        status="ERROR",
                        message=f"Lỗi nhập form: {e2}",
                    )
                    return {"success": False, "message": str(e2)}

            clicked = await click_submit_fast(page)
            if not clicked:
                logger.warning(f"❌ [{user} | {domain}] Không bấm được submit/Enter")
                append_code_history(
                    event_type="ERROR",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="ERROR",
                    message="Không bấm được submit/Enter",
                )
                return {"success": False, "message": "Không bấm được submit/Enter"}

            click_elapsed = time.time() - start_time
            logger.info(f"🚀 [{user} | {domain}] ĐÃ BẤM NẠP code {code} sau {click_elapsed:.2f}s")
            append_code_history(
                event_type="SUBMIT_CLICKED",
                code=code,
                target_url=target_url,
                account=user,
                status="PENDING",
                submit_elapsed=click_elapsed,
                message="Đã bấm nạp",
            )

            # Đọc RESULT_WAIT trực tiếp từ Config (đã parse từ .env đúng cách)
            result_wait_ms = Config.RESULT_WAIT
            await asyncio.sleep(result_wait_ms / 1000.0)

            # Detect result song song
            result_text = await detect_result_text(page)

            elapsed = time.time() - start_time
            result_upper = result_text.upper()

            success_keywords = ["THÀNH CÔNG", "SUCCESS", "CỘNG", "NHẬN THÀNH CÔNG", "OK"]
            failed_keywords = ["SAI", "LỖI", "ĐÃ SỬ", "HẾT HẠN", "KHÔNG", "FAILED", "ERROR"]
            # Từ khóa xác nhận có điểm/xu được cộng vào tài khoản
            point_keywords = ["ĐIỂM", "XU", "COIN", "POINT", "CỘNG", "+", "VND", "K ", "THƯỞNG", "BONUS"]

            is_success = any(keyword in result_upper for keyword in success_keywords)
            is_failed = any(keyword in result_upper for keyword in failed_keywords)
            has_points = any(keyword in result_upper for keyword in point_keywords)

            if is_success and not is_failed:
                logger.info(
                    f"✅ [{user} | {domain}] NẠP THÀNH CÔNG ({elapsed:.2f}s) "
                    f"| có_điểm={has_points} | MSG: {result_text[:80]}"
                )
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, db.record_submission, code, user, target_url, "SUCCESS", result_text[:100])
                bot_state.submission_count[key] += 1
                perf_mon.record_task("submit_code", elapsed, True)
                append_code_history(
                    event_type="RESULT",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="SUCCESS",
                    submit_elapsed=elapsed,
                    message=result_text[:100],
                )
                return {"success": True, "has_points": has_points, "message": result_text[:100]}

            if len(result_text.strip()) < 5:
                screenshot = await take_result_screenshot(page, user, code, target_url, "UNKNOWN")
                logger.warning(
                    f"⚠️ [{user} | {domain}] ĐÃ BẤM NẠP ({elapsed:.2f}s) "
                    f"nhưng KHÔNG THẤY KẾT QUẢ RÕ RÀNG"
                )
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, db.record_submission, code, user, target_url, "UNKNOWN", "Không thấy popup rõ ràng")
                perf_mon.record_task("submit_code", elapsed, False)
                append_code_history(
                    event_type="RESULT",
                    code=code,
                    target_url=target_url,
                    account=user,
                    status="UNKNOWN",
                    submit_elapsed=elapsed,
                    message="Không thấy popup rõ ràng",
                    screenshot=screenshot,
                )
                return {"success": False, "message": "Không thấy popup rõ ràng"}

            screenshot = await take_result_screenshot(page, user, code, target_url, "FAILED")
            logger.warning(f"❌ [{user} | {domain}] THẤT BẠI ({elapsed:.2f}s) - MSG: {result_text[:80]}")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, db.record_submission, code, user, target_url, "FAILED", result_text[:100])
            perf_mon.record_task("submit_code", elapsed, False)
            append_code_history(
                event_type="RESULT",
                code=code,
                target_url=target_url,
                account=user,
                status="FAILED",
                submit_elapsed=elapsed,
                message=result_text[:100],
                screenshot=screenshot,
            )
            return {"success": False, "message": result_text[:100]}

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"❌ [{user} | {domain}] Lỗi submit: {e}")
        perf_mon.record_task("submit_code", elapsed, False)
        append_code_history(
            event_type="ERROR",
            code=code,
            target_url=target_url,
            account=user,
            status="ERROR",
            submit_elapsed=elapsed,
            message=str(e),
        )
        return {"success": False, "message": str(e)}


async def _submit_sequential_for_channel(
    code: str,
    available_accounts: list,
    target_url: str,
    channel_name: str,
    domain: str,
):
    """
    Nhập code TUẦN TỰ từng tài khoản — ĐỔI TÀI KHOẢN CHỈ KHI CÓ ĐIỂM:

    - Thành công + có điểm → chuyển sang tài khoản tiếp theo (code còn hiệu lực)
    - Thành công nhưng không rõ điểm → dừng (an toàn, tránh nhập trùng)
    - Thất bại (sai/hết hạn/đã dùng) → dừng ngay (code vô hiệu, thử tiếp vô nghĩa)

    Mỗi kênh chỉ dùng 1 tab. Hàm chạy trong nền, không chặn Telegram worker.
    """
    for idx, account in enumerate(available_accounts):
        user = account["username"]
        is_last = (idx == len(available_accounts) - 1)

        logger.info(
            f"🔄 [SEQUENTIAL] [{domain}] Tài khoản {idx+1}/{len(available_accounts)}: [{user}] "
            f"→ nhập code {code}"
        )

        result = await submit_code_limited(user, code, target_url, _systems)

        success = result.get("success", False) if result else False
        has_points = result.get("has_points", False) if result else False
        msg = result.get("message", "?") if result else "?"

        if success and has_points:
            if is_last:
                logger.info(
                    f"✅ [SEQUENTIAL] [{domain}] [{user}] THÀNH CÔNG + CÓ ĐIỂM. "
                    f"Đã hết danh sách tài khoản."
                )
                return
            logger.info(
                f"✅ [SEQUENTIAL] [{domain}] [{user}] THÀNH CÔNG + CÓ ĐIỂM → "
                f"chuyển sang tài khoản tiếp theo để nhập tiếp."
            )
            continue  # Tiếp tục — thử account kế tiếp

        if success and not has_points:
            logger.warning(
                f"⚠️ [SEQUENTIAL] [{domain}] [{user}] THÀNH CÔNG nhưng không xác nhận được điểm. "
                f"Dừng để tránh nhập trùng. MSG: {msg}"
            )
            return

        # Thất bại rõ ràng
        logger.warning(
            f"❌ [SEQUENTIAL] [{domain}] [{user}] THẤT BẠI → dừng, không thử tài khoản tiếp. "
            f"MSG: {msg}"
        )
        return

    logger.info(f"✅ [SEQUENTIAL] [{domain}] Đã nhập code {code} cho toàn bộ {len(available_accounts)} tài khoản.")


async def process_telegram_message(event):
    if not _systems:
        return

    channel_config = Config.CHANNEL_CONFIG.get(event.chat_id)

    if not channel_config:
        return

    target_url = channel_config["url"]
    accounts = channel_config["accounts"]
    raw_text = event.message.text or ""

    logger.info(f"\n👀 Vừa có message từ: {channel_config['name']}")

    # Dùng delay đã đo từ handler (trước khi vào queue) để không bị cộng thêm queue lag.
    # Chỉ log lại tại đây, KHÔNG lọc lại (đã lọc ở handler rồi).
    telegram_delay = getattr(event, "_pre_measured_delay", None)
    if telegram_delay is not None:
        logger.warning(
            f"⏱️ Delay Telegram (đo từ handler): {telegram_delay:.2f}s "
            f"| msg_time_utc={event.message.date.strftime('%H:%M:%S')}"
        )
    else:
        # Fallback: đo lại nếu không có (ví dụ: tin được replay thủ công)
        telegram_delay = measure_telegram_delay(event)
        if telegram_delay is not None:
            logger.warning(f"⏱️ Delay Telegram (đo lại): {telegram_delay:.2f}s")

    # extract + validate trong một bước (bỏ double-validate)
    raw_candidates = extract_codes_from_message(event, raw_text, target_url)
    final_codes = unique_keep_order(raw_candidates)

    if not final_codes:
        logger.info("⏭️ Không có code hợp lệ để nạp")
        return

    logger.info(f"📋 Codes chuẩn để nạp: {final_codes}")

    for code in final_codes:
        append_code_history(
            event_type="DETECTED",
            code=code,
            target_url=target_url,
            channel=channel_config.get("name", ""),
            source="telegram",
            status="PENDING",
            telegram_delay=telegram_delay,
        )

    # Kiểm tra tab của domain này đã sẵn sàng chưa (1 tab cho toàn bộ domain)
    domain = normalize_domain(target_url)
    domain_key = domain  # key trong account_pages là domain, không phải user|domain

    if domain_key not in bot_state.account_pages:
        logger.warning(f"⚠️ [{domain}] Tab chưa được mở/gán. Bỏ qua toàn bộ message này.")
        return

    # Lấy danh sách tài khoản theo thứ tự priority từ channel_config
    available_accounts = sorted(accounts, key=lambda a: a.get("priority", 999))
    logger.info(
        f"📋 [{domain}] Sẽ thử lần lượt {len(available_accounts)} tài khoản: "
        f"{[a['username'] for a in available_accounts]}"
    )

    if not available_accounts:
        logger.warning("⚠️ Không có tài khoản nào được cấu hình cho kênh này")
        return

    scheduled = 0

    for code in final_codes:
        # ✅ Dedup code theo site trong vòng SITE_CODE_DEDUP_TTL giây
        # Ngăn trường hợp 2 kênh cùng site post code trùng → bot nạp 2 lần bị lỗi "đã sử dụng"
        if is_site_code_duplicate(domain, code):
            logger.warning(
                f"⏭️ [DEDUP] Code {code} cho site '{domain}' đã được gửi gần đây — bỏ qua"
            )
            append_code_history(
                event_type="SKIPPED",
                code=code,
                target_url=target_url,
                channel=channel_config.get("name", ""),
                status="SKIPPED",
                message="Dedup: code đã nạp cho site này trong vòng TTL",
            )
            continue

        # ✅ SEQUENTIAL MODE: Mỗi code chạy 1 task nền riêng,
        # bên trong task đó thử lần lượt từng tài khoản đến khi thành công.
        # Không submit song parallel nhiều tài khoản cùng lúc cho cùng 1 code.
        task = asyncio.create_task(
            _submit_sequential_for_channel(
                code=code,
                available_accounts=available_accounts,
                target_url=target_url,
                channel_name=channel_config.get("name", ""),
                domain=domain,
            )
        )
        track_submit_task(task, label=f"sequential|{domain}|{code}")
        scheduled += 1

    if scheduled:
        logger.warning(
            f"⚡ Đã khởi động {scheduled} task nhập code tuần tự cho kênh '{channel_config['name']}'. "
            f"Mỗi code sẽ thử lần lượt {len(available_accounts)} tài khoản, dừng khi thành công."
        )
    else:
        logger.warning("⚠️ Có code nhưng không có task nào được chạy (tất cả đã bị dedup hoặc không có tab)")


# ✅ V5.0: Tăng message workers từ 6 → 12 (gấp đôi)
async def message_worker(worker_id: int):
    global message_queue

    logger.info(f"👷 Message worker #{worker_id} đã khởi động")
    max_delay = float(getattr(Config, "MAX_TELEGRAM_DELAY_SECONDS", 8.0))

    while bot_state.is_running:
        try:
            # Dùng timeout để không block mãi khi is_running thay đổi
            try:
                event = await asyncio.wait_for(message_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Worker #{worker_id} lỗi khi lấy queue: {e}")
            await asyncio.sleep(0.1)
            continue

        try:
            # Handler đã lọc tin cũ trước khi vào queue. Worker chỉ log queue lag, không bỏ nhầm tin do Playwright bận.
            received_at = getattr(event, "_handler_received_at", None)
            if received_at is not None:
                queue_lag = time.perf_counter() - received_at
                logger.warning(f"📦 [WORKER #{worker_id}] Queue lag: {queue_lag:.3f}s")
            else:
                queue_lag = None

            current_delay = measure_telegram_delay(event)
            if current_delay is not None:
                logger.debug(f"⏱️ [WORKER #{worker_id}] Current telegram delay: {current_delay:.2f}s")

            await process_telegram_message(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"❌ Worker #{worker_id} xử lý message lỗi: {e}")
        finally:
            try:
                message_queue.task_done()
            except Exception:
                pass


def start_message_workers():
    global message_queue, message_workers

    if message_queue is None:
        message_queue = asyncio.Queue(
            maxsize=int(getattr(Config, "MESSAGE_QUEUE_MAXSIZE", 500))
        )

    if message_workers:
        return

    # ✅ V5.0: Tăng workers từ 6 → 12 (gấp đôi, xử lý nhiều tin cùng lúc)
    worker_count = int(getattr(Config, "MESSAGE_WORKERS", 12))

    for worker_id in range(1, worker_count + 1):
        message_workers.append(asyncio.create_task(message_worker(worker_id)))

    logger.info(
        f"🚀 Đã bật message queue: maxsize={getattr(Config, 'MESSAGE_QUEUE_MAXSIZE', 500)}, "
        f"workers={worker_count}"
    )


async def setup_telegram_handler():
    if bot_state.handler_registered:
        return

    channel_ids = list(Config.CHANNEL_CONFIG.keys())
    start_message_workers()

    max_delay = float(getattr(Config, "MAX_TELEGRAM_DELAY_SECONDS", 8.0))

    @client.on(events.NewMessage(chats=channel_ids))
    async def handler(event):
        # ✅ V5.0: Đây là log NHẬN TIN THẬT SỰ IN-PROCESS. Nếu dòng này hiện nhanh thì Telegram không chậm.
        received_at = time.perf_counter()
        delay = measure_telegram_delay(event)
        event._handler_received_at = received_at
        event._pre_measured_delay = delay

        if delay is not None:
            logger.warning(
                f"📥 [TELEGRAM HANDLER] Nhận update ngay | chat_id={event.chat_id} | "
                f"server_delay={delay:.2f}s"
            )
        else:
            logger.warning(f"📥 [TELEGRAM HANDLER] Nhận update ngay | chat_id={event.chat_id}")

        # Lọc tin cũ ngay tại handler. Không để worker cộng thêm queue lag rồi bỏ nhầm.
        if delay is not None and delay > max_delay:
            logger.warning(
                f"⏭️ [HANDLER] Bỏ tin quá cũ TRƯỚC KHI VÀO QUEUE: "
                f"{delay:.2f}s > {max_delay:.2f}s"
            )
            return

        try:
            message_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("⚠️ Message queue đầy, bỏ qua message mới để tránh nghẽn")
    bot_state.handler_registered = True
    logger.info("✅ Telegram handlers setup xong!\n")

async def cleanup_browsers():
    for port, browser in bot_state.connected_browsers.items():
        try:
            await browser.close()
            logger.info(f"✅ Đã ngắt kết nối CDP port {port}")
        except Exception:
            pass


async def main():
    global _systems

    try:
        logger.info("\n" + "=" * 70)
        logger.info("🚀 KHỞI ĐỘNG BOT STRICT MODE - LỌC CODE CHUẨN HƠN")
        logger.info("=" * 70)
        logger.info("✨ V5.0 ULTRA FAST MODE: Sequential Updates + 12 Workers + 10s Watchdog")
        logger.info("=" * 70)

        _systems = await init_systems()

        logger.info("\n📨 Kết nối Telegram...")
        try:
            await client.start(catch_up=bool(getattr(Config, "TELEGRAM_CATCH_UP", False)))
        except TypeError:
            await client.start()

        if not await verify_telegram_session():
            return

        valid_channels = await verify_channels_and_get_ids()

        if not valid_channels:
            logger.error("❌ Không có channel hợp lệ. Kiểm tra lại CHANNEL_CONFIG hoặc quyền Telegram.")
            return

        await setup_telegram_handler()

        if bool(getattr(Config, "RUN_SYSTEM_TEST_ON_START", False)):
            tester = get_system_tester(client)
            await tester.test_all(_systems["db"])

        await preload_browsers_and_accounts()

        # ✅ CHỜ NGƯỜI DÙNG TỰ ĐĂNG NHẬP THỦ CÔNG
        # Edge đã mở và ở đầu màn hình — người dùng tự thao tác xong rồi bấm Enter
        logger.info("=" * 70)
        logger.info("🖐️  Edge đã mở ở đầu màn hình.")
        logger.info("👉  Hãy tự đăng nhập / vào đúng trang trên Edge.")
        logger.info("⏎   Sau khi xong, quay lại đây bấm ENTER để bot bắt đầu chạy.")
        logger.info("=" * 70)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, input, "\n>>> Bấm ENTER khi đã sẵn sàng: ")
        logger.info("✅ Bắt đầu lắng nghe code...")

        logger.info("=" * 70)
        logger.info("✅ BOT SẴN SÀNG! ĐANG LẮNG NGHE MÃ CODE...")
        logger.info("=" * 70)

        async def delayed_background_maintenance():
            try:
                await asyncio.sleep(int(getattr(Config, "BACKGROUND_MAINTENANCE_DELAY", 300)))
                asyncio.create_task(get_db_backup().schedule_daily_backup(Config.DATABASE_PATH, bot_state))
                asyncio.create_task(
                    get_profile_cleaner().schedule_daily_cleanup(
                        Path(Config.BROWSER_PROFILE_DIR) / "browser_profiles",
                        bot_state,
                    )
                )
            except Exception as e:
                logger.warning(f"⚠️ Không khởi động được background maintenance: {e}")

        asyncio.create_task(delayed_background_maintenance())

        # ✅ WATCHDOG: Tự động kiểm tra và click lại Cloudflare mỗi 30s
        async def cloudflare_watchdog():
            """
            Watchdog quét định kỳ — nếu phát hiện tab nào đang bị Cloudflare chặn
            thì bring_to_front tab đó lên để người dùng tự click, không tự click, không log thừa.
            """
            CF_DETECT_SELECTORS = [
                "iframe[src*='challenges.cloudflare.com']",
                "iframe[src*='turnstile']",
                "iframe[title*='Cloudflare']",
                "iframe[title*='cloudflare']",
                "iframe[src*='hcaptcha.com']",
                ".cf-turnstile",
                "[class*='turnstile']",
                "[data-sitekey]",
            ]

            # Watchdog ngẫu nhiên 5-10 phút để Cloudflare không detect pattern cố định
            while bot_state.is_running:
                try:
                    interval = random.uniform(300.0, 600.0)
                    logger.info(f"⏱️ Watchdog quét lại sau {interval/60:.1f} phút")
                    await asyncio.sleep(interval)
                    if not bot_state.account_pages:
                        continue
                    for key, page in list(bot_state.account_pages.items()):
                        # Kiểm tra tab còn sống
                        try:
                            current_url = page.url
                        except Exception:
                            continue

                        # Phát hiện Cloudflare
                        try:
                            cf_found = (
                                "challenges.cloudflare.com" in current_url
                                or "/cdn-cgi/challenge-platform" in current_url
                            )
                            if not cf_found:
                                for sel in CF_DETECT_SELECTORS:
                                    try:
                                        el = await page.query_selector(sel)
                                        if el and await el.is_visible():
                                            cf_found = True
                                            break
                                    except Exception:
                                        continue

                            if cf_found:
                                # Chỉ in 1 dòng và đưa tab lên trước để người dùng click
                                logger.warning(f"⚠️ Cloudflare trên [{key}] — hãy click xác minh!")
                                try:
                                    await page.bring_to_front()
                                except Exception:
                                    pass
                        except Exception:
                            continue
                except asyncio.CancelledError:
                    break
                except Exception:
                    pass

        asyncio.create_task(cloudflare_watchdog())
        # ✅ WATCHDOG TỰ ĐỘNG ĐIỀN USERNAME — V5.0: Interval 10s (nhanh hơn 3x)
        asyncio.create_task(auto_fill_usernames_watchdog())

        # ✅ FIX HEARTBEAT: Log 1 dòng mỗi 5 phút để xác nhận bot đang chạy bình thường
        async def heartbeat_loop():
            interval = float(getattr(Config, "HEARTBEAT_INTERVAL", 300.0))  # mặc định 5 phút
            logger.info(f"💓 Heartbeat khởi động, log mỗi {interval:.0f}s")
            while bot_state.is_running:
                try:
                    await asyncio.sleep(interval)
                    if bot_state.is_running:
                        pages_ready = len(bot_state.account_pages)
                        pending_submits = len(_active_submit_tasks)
                        logger.info(
                            f"💓 [HEARTBEAT] Bot đang chạy | "
                            f"tabs_ready={pages_ready} | "
                            f"active_submits={pending_submits} | "
                            f"tg_connected={client.is_connected()}"
                        )
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug(f"⚠️ Heartbeat lỗi: {e}")

        asyncio.create_task(heartbeat_loop())

        # ✅ FIX 1: Reconnect loop — tự khởi động lại khi mất mạng / DC timeout
        _reconnect_delay = float(getattr(Config, "RECONNECT_DELAY_SECONDS", 5.0))
        _reconnect_max = float(getattr(Config, "RECONNECT_MAX_DELAY_SECONDS", 60.0))
        _reconnect_backoff = 1.0
        while bot_state.is_running:
            try:
                if not client.is_connected():
                    logger.warning("🔄 Telegram mất kết nối, đang kết nối lại...")
                    await client.connect()
                await client.run_until_disconnected()
                # run_until_disconnected() trả về bình thường → bot_state.is_running đã False
                break
            except (ConnectionError, OSError, asyncio.TimeoutError) as conn_err:
                if not bot_state.is_running:
                    break
                wait = min(_reconnect_delay * _reconnect_backoff, _reconnect_max)
                logger.warning(
                    f"⚠️ Telegram mất kết nối ({conn_err}), thử lại sau {wait:.0f}s..."
                )
                await asyncio.sleep(wait)
                _reconnect_backoff = min(_reconnect_backoff * 2, 12)
            except Exception as loop_err:
                if not bot_state.is_running:
                    break
                logger.error(f"❌ Lỗi vòng lặp Telegram: {loop_err}")
                await asyncio.sleep(_reconnect_delay)

    except Exception as e:
        logger.critical(f"❌ Lỗi critical: {e}")

    finally:
        logger.info("\n🛑 Dừng bot...")
        bot_state.is_running = False

        # Flush history queue trước khi tắt
        if _history_queue is not None:
            try:
                await asyncio.wait_for(_history_queue.join(), timeout=5.0)
            except Exception:
                pass
            if _history_writer_task:
                _history_writer_task.cancel()
                try:
                    await _history_writer_task
                except asyncio.CancelledError:
                    pass

        if _active_submit_tasks:
            logger.info(f"⏳ Đang chờ {len(_active_submit_tasks)} submit task nền kết thúc...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(_active_submit_tasks), return_exceptions=True),
                    timeout=8.0,
                )
            except Exception:
                for t in list(_active_submit_tasks):
                    t.cancel()

        for worker in message_workers:
            worker.cancel()

        await cleanup_browsers()

        build_daily_summary()

        if bot_state.playwright_instance:
            await bot_state.playwright_instance.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Bot đã dừng (Ctrl+C)")
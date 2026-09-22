"""Bound Hub SDK traffic and retain headroom for other account activity."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging
import re
import threading
import time
from urllib.parse import urlsplit

from huggingface_hub import constants, set_client_factory
from huggingface_hub.utils._http import default_client_factory

LOG = logging.getLogger("gdelt-hub-traffic")


def reset_delay(headers, now):
    delays = []
    retry_after = headers.get("retry-after", "")
    if retry_after.isdigit():
        delays.append(int(retry_after))
    elif retry_after:
        try:
            delays.append((parsedate_to_datetime(retry_after) - now).total_seconds())
        except (ValueError, TypeError, OverflowError):
            pass
    delays.extend(int(value) for value in re.findall(
        r"(?:^|;)\s*t\s*=\s*(\d+)", headers.get("ratelimit", "")))
    return max(1, max(delays, default=300)) + 1


def rate_limit_delay(exc, now):
    response = getattr(exc, "response", None)
    if response is None or response.status_code != 429:
        return None
    return reset_delay(response.headers, now)


class HubRequestLimiter:
    """One request per second across SDK threads; stop before reported exhaustion.

    Apply to the Hub host only, leaving bulk CDN/Xet transfers unconstrained.
    Counting resolver requests too is conservative and keeps the API request
    rate comfortably below the observed 1,000-per-five-minute allowance.
    """
    def __init__(self, clock=time.monotonic, sleep=time.sleep):
        self.clock, self.sleep = clock, sleep
        self.host = urlsplit(constants.ENDPOINT).hostname
        self.lock = threading.Lock()
        self.next_request_at = 0.0

    def request(self, request):
        if request.url.host != self.host:
            return
        with self.lock:
            while (delay := self.next_request_at - self.clock()) > 0:
                self.sleep(min(delay, 30))
            self.next_request_at = self.clock() + 1.0

    def response(self, response):
        if response.request.url.host != self.host:
            return
        remaining = [int(value) for value in re.findall(
            r"(?:^|;)\s*r\s*=\s*(\d+)", response.headers.get("ratelimit", ""))]
        if response.status_code != 429 and not any(value <= 100 for value in remaining):
            return
        delay = reset_delay(response.headers, datetime.now(timezone.utc))
        with self.lock:
            self.next_request_at = max(self.next_request_at, self.clock() + delay)
        LOG.warning("hub_cooldown http_status=%s remaining=%s seconds=%s",
                    response.status_code, min(remaining, default=None), delay)


_install_lock = threading.Lock()
_installed = False


def install_hub_request_limiter():
    global _installed
    with _install_lock:
        if _installed:
            return
        limiter = HubRequestLimiter()
        def client_factory():
            # Preserve the pinned SDK's default TLS, redirects and request hook.
            client = default_client_factory()
            client.event_hooks["request"].insert(0, limiter.request)
            client.event_hooks["response"].append(limiter.response)
            return client
        set_client_factory(client_factory)
        _installed = True

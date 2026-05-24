"""Tests for TelegramAdapter send-path health tracking after reconnect storms.

After sustained Bad Gateway / TimedOut reconnect cycles, the PTB httpx client
can enter a wedged state where send_message returns a valid Message but the
message never arrives.  The _send_path_degraded flag gates a post-send getMe()
probe that catches the wedge and surfaces SendResult(success=False).
"""
import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import PlatformConfig


def _ensure_telegram_mock():
    if "telegram" in sys.modules and hasattr(sys.modules["telegram"], "__file__"):
        return

    telegram_mod = MagicMock()
    telegram_mod.ext.ContextTypes.DEFAULT_TYPE = type(None)
    telegram_mod.constants.ParseMode.MARKDOWN_V2 = "MarkdownV2"
    telegram_mod.constants.ChatType.GROUP = "group"
    telegram_mod.constants.ChatType.SUPERGROUP = "supergroup"
    telegram_mod.constants.ChatType.CHANNEL = "channel"
    telegram_mod.constants.ChatType.PRIVATE = "private"

    telegram_mod.error.NetworkError = type("NetworkError", (OSError,), {})
    telegram_mod.error.TimedOut = type("TimedOut", (OSError,), {})
    telegram_mod.error.BadRequest = type("BadRequest", (Exception,), {})

    for name in ("telegram", "telegram.ext", "telegram.constants", "telegram.request"):
        sys.modules.setdefault(name, telegram_mod)
    sys.modules.setdefault("telegram.error", telegram_mod.error)


_ensure_telegram_mock()

from gateway.platforms.telegram import TelegramAdapter  # noqa: E402


def _make_adapter() -> TelegramAdapter:
    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="***"))
    # Wire up a mock bot so send() doesn't bail on "Not connected"
    adapter._bot = MagicMock()
    adapter._bot.get_me = AsyncMock(return_value=MagicMock())
    adapter._bot.send_message = AsyncMock(return_value=MagicMock(message_id=42))
    return adapter


# ---------------------------------------------------------------------------
# _send_path_degraded flag
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_path_healthy_by_default():
    """send() should not probe getMe() when the adapter is healthy."""
    adapter = _make_adapter()
    assert adapter._send_path_degraded is False

    result = await adapter.send("123", "hello world")

    assert result.success is True
    assert result.message_id == "42"
    # getMe() should NOT have been called — no probe needed
    adapter._bot.get_me.assert_not_called()


@pytest.mark.asyncio
async def test_send_path_degraded_probe_passes_clears_flag():
    """When degraded, send() probes getMe() and clears the flag on success."""
    adapter = _make_adapter()
    adapter._send_path_degraded = True

    result = await adapter.send("123", "hello")

    assert result.success is True
    adapter._bot.get_me.assert_awaited_once()
    assert adapter._send_path_degraded is False


@pytest.mark.asyncio
async def test_send_path_degraded_probe_fails_returns_failure():
    """When degraded and getMe() probe fails, send() returns success=False
    so callers (cron live-adapter) fall through to standalone delivery."""
    adapter = _make_adapter()
    adapter._send_path_degraded = True
    adapter._bot.get_me = AsyncMock(side_effect=OSError("connection reset"))

    result = await adapter.send("123", "hello")

    assert result.success is False
    assert "send_path_degraded" in result.error
    assert result.retryable is True
    # Flag stays True so the next send also probes
    assert adapter._send_path_degraded is True


@pytest.mark.asyncio
async def test_reconnect_storm_sets_degraded_flag(monkeypatch):
    """_handle_polling_network_error should set _send_path_degraded=True."""
    adapter = _make_adapter()
    # Wire up a mock app with updater for the reconnect path
    adapter._app = MagicMock()
    adapter._app.updater = MagicMock()
    adapter._app.updater.running = False
    adapter._app.updater.stop = AsyncMock()
    adapter._app.updater.start_polling = AsyncMock()
    adapter._polling_error_callback_ref = AsyncMock()

    import gateway.platforms.telegram as tg_mod
    monkeypatch.setattr(tg_mod, "Update", MagicMock(ALL_TYPES=[]))

    # Simulate a single reconnect error (attempt 1/10)
    await adapter._handle_polling_network_error(OSError("Bad Gateway"))

    assert adapter._send_path_degraded is True


@pytest.mark.asyncio
async def test_verify_polling_clears_degraded_flag():
    """_verify_polling_after_reconnect should clear the degraded flag
    when the getMe() heartbeat probe passes."""
    adapter = _make_adapter()
    adapter._send_path_degraded = True

    # Wire up a mock app with running updater
    adapter._app = MagicMock()
    adapter._app.updater = MagicMock()
    adapter._app.updater.running = True
    adapter._app.bot = MagicMock()
    adapter._app.bot.get_me = AsyncMock(return_value=MagicMock())

    # Speed up: patch the heartbeat delay to 0
    with patch("gateway.platforms.telegram.asyncio.sleep", new_callable=AsyncMock):
        await adapter._verify_polling_after_reconnect()

    assert adapter._send_path_degraded is False

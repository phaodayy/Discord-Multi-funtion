"""Quản lý bot cho dashboard: class Bot và BotManager.

Module này là phiên bản tái cấu trúc của ``self-bot.py`` dành cho web
dashboard. Mọi thao tác I/O đều dùng ``app_logger`` từ ``core.logger``.

Ghi chú: ``self-bot.py`` vẫn được giữ nguyên; module này thay thế lớp điều
khiển bằng terminal (control_loop) bằng API cho dashboard.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import os
import random
import shutil
import time
from pathlib import Path
from typing import Any, Callable

import discord

from core.logger import app_logger

BotState = dict[str, Any]
StateListener = Callable[[BotState], Any]

# Gốc project (thư mục chứa core/) để đường dẫn tương đối luôn ổn định
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _resolve_tokens_path(filepath: str) -> Path:
    """Đổi đường dẫn file token thành tuyệt đối, neo theo thư mục project."""
    path = Path(filepath)
    if path.is_absolute():
        return path
    return _PROJECT_ROOT / path


# -----------------------------------------------------------------------------
# MONKEY PATCH: Fix discord.py-self compatibility with new Discord API
# (giữ nguyên từ self-bot.py, chỉ thay print bằng logging)
# -----------------------------------------------------------------------------
def _apply_discord_hotfix() -> None:
    """Patch FriendFlags để tránh crash khởi động với discord.py-self."""

    def _patched_friend_flags_from_dict(cls: type, data: dict[str, Any] | None) -> Any:
        if data is None:
            return cls.all()
        try:
            return cls(
                all=data.get("all", True),
                mutual_guilds=data.get("mutual_guilds", True),
                mutual_friends=data.get("mutual_friends", True),
            )
        except Exception:
            return cls.all()

    try:
        target_flags = getattr(discord.enums, "FriendFlags", None)
        if target_flags is None:
            target_flags = getattr(discord.flags, "FriendFlags", None)

        if target_flags is None:
            app_logger.warning("Không tìm thấy FriendFlags để hotfix; bot có thể crash khi start.")
            return

        target_flags._from_dict = classmethod(_patched_friend_flags_from_dict)
        app_logger.info("Đã áp dụng hotfix FriendFlags cho discord.py-self.")
    except Exception as exc:
        app_logger.warning("Không thể áp dụng hotfix discord.py-self: %s", exc)


_apply_discord_hotfix()


def _invoke_callback(callback: Callable[..., Any], *args: Any) -> None:
    """Gọi callback đồng bộ/async an toàn, không làm hỏng luồng gọi."""
    try:
        result = callback(*args)
    except Exception:
        app_logger.exception(
            "Callback %s gặp lỗi", getattr(callback, "__qualname__", callback)
        )
        return

    if not inspect.isawaitable(result):
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        if inspect.iscoroutine(result):
            result.close()
        app_logger.warning("Không có event loop đang chạy để thực thi callback async.")
        return

    task = loop.create_task(result)  # type: ignore[arg-type]
    task.add_done_callback(_on_async_callback_done)


def _on_async_callback_done(task: "asyncio.Task[Any]") -> None:
    """Ghi nhận lỗi từ callback async mà không gây warning 'never retrieved'."""
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        app_logger.error("Callback async gặp lỗi: %s", exc)


class Bot(discord.Client):
    """Client riêng cho từng token, quản lý kết nối voice và state cho API."""

    # ID duy nhất được phép dùng lệnh chat đồng loạt; để trống OWNER_ID trong .env là tắt
    OWNER_ID: int | None = int(os.getenv("OWNER_ID", "0").strip() or 0) or None

    def __init__(self, token: str, channel_id: int) -> None:
        super().__init__()
        self.token_str: str = token
        self.target_channel_id: int = channel_id
        self.vc: discord.VoiceClient | None = None
        self.is_connected: bool = False
        self.is_streaming: bool = False
        self.moved_to_channel: int | None = None
        self.is_leader: bool = False
        self.error: str | None = None
        self.ready_at: float | None = None
        self.on_state_change: Callable[[BotState], Any] | None = None

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------
    @property
    def token_masked(self) -> str:
        """Trả token đã che, dùng để hiển thị trên dashboard."""
        return f"{self.token_str[:6]}..."

    @property
    def bot_id(self) -> str:
        """ID duy nhất cho dashboard: Discord user ID, hoặc hash token khi chưa login."""
        if self.user is not None:
            return str(self.user.id)
        digest = hashlib.sha256(self.token_str.encode("utf-8")).hexdigest()[:16]
        return f"tok-{digest}"

    @property
    def is_in_voice(self) -> bool:
        """Bot có đang ở trong một voice channel hay không."""
        return self._current_voice_channel() is not None

    @property
    def latency_ms(self) -> int:
        """Độ trễ gateway (ms), trả -1 khi chưa có số đo hợp lệ."""
        latency = self.latency
        if latency is None or not math.isfinite(latency):
            return -1
        return int(round(latency * 1000))

    @property
    def uptime_seconds(self) -> float:
        """Thời gian bot đã ready (giây), 0.0 nếu chưa từng ready."""
        if self.ready_at is None:
            return 0.0
        return round(time.time() - self.ready_at, 1)

    def _current_voice_channel(self) -> discord.abc.GuildChannel | None:
        """Tìm voice channel hiện tại của bot trong các guild đã tham gia."""
        for guild in self.guilds:
            me = guild.me
            if me is None:
                continue
            voice = me.voice
            if voice is not None and voice.channel is not None:
                return voice.channel
        return None

    def to_dict(self) -> BotState:
        """Trả toàn bộ state của bot theo schema dùng cho dashboard/API."""
        voice_channel = self._current_voice_channel()

        is_mute = False
        is_deaf = False
        is_video = False
        if voice_channel is not None:
            me = voice_channel.guild.me
            state = me.voice if me is not None else None
            if state is not None:
                is_mute = bool(state.self_mute)
                is_deaf = bool(state.self_deaf)
                is_video = bool(state.self_video)

        info_channel = voice_channel or self.get_channel(self.target_channel_id)
        guild = getattr(info_channel, "guild", None)

        user = self.user
        if user is None:
            user_info: dict[str, Any] = {
                "id": None,
                "username": None,
                "display_name": None,
                "avatar_url": None,
            }
        else:
            display_avatar = getattr(user, "display_avatar", None)
            user_info = {
                "id": user.id,
                "username": user.name,
                "display_name": getattr(user, "display_name", user.name),
                "avatar_url": str(display_avatar.url) if display_avatar else None,
            }

        return {
            "id": self.bot_id,
            "token_masked": self.token_masked,
            "ready": self.is_ready(),
            "is_in_voice": voice_channel is not None,
            "target_channel_id": self.target_channel_id,
            "channel_name": getattr(info_channel, "name", None),
            "guild_name": getattr(guild, "name", None),
            "is_mute": is_mute,
            "is_deaf": is_deaf,
            "is_video": is_video,
            "is_streaming": self.is_streaming,
            "is_leader": self.is_leader,
            "error": self.error,
            "latency_ms": self.latency_ms,
            "uptime_seconds": self.uptime_seconds,
            "user": user_info,
        }

    def notify_state_change(self) -> None:
        """Thông báo state mới cho callback (nếu có)."""
        if self.on_state_change is not None:
            _invoke_callback(self.on_state_change, self.to_dict())

    # ------------------------------------------------------------------
    # Discord events
    # ------------------------------------------------------------------
    async def on_ready(self) -> None:
        """Sự kiện khi bot login thành công."""
        self.ready_at = time.time()
        self.error = None
        app_logger.info(
            "Đã đăng nhập: %s (%s...)", self.user, self.token_str[:6]
        )
        # Chưa gán kênh (chỉ đăng nhập để chọn kênh sau) thì không join
        if self.target_channel_id:
            await self.join_vc()
        self.notify_state_change()

    async def on_message(self, message: discord.Message) -> None:
        """Lắng nghe lệnh chat đặc biệt ``<!nội dung>`` từ owner."""
        if self.OWNER_ID is None or self.user is None or message.author.id == self.user.id:
            return
        if message.author.id != self.OWNER_ID:
            return

        content = message.content.strip()
        if not (content.startswith("<!") and content.endswith(">")):
            return

        msg_to_say = content[2:-1]
        if not msg_to_say:
            return

        try:
            # Delay ngẫu nhiên để tránh bot spam cùng 1 tích tắc
            await asyncio.sleep(random.uniform(0.5, 2.5))
            await message.channel.send(msg_to_say)
        except Exception as exc:
            app_logger.warning("Không thể echo tin nhắn (%s): %s", self.user, exc)

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        """Theo dõi auto-room và cập nhật state cho dashboard."""
        if self.user is None or member.id != self.user.id:
            return

        if (
            self.is_leader
            and before.channel is not None
            and after.channel is not None
            and before.channel.id != after.channel.id
            and before.channel.id == self.target_channel_id
        ):
            self.moved_to_channel = after.channel.id
            app_logger.info(
                "LEADER %s được move sang room: %s (%s)",
                self.user,
                after.channel.name,
                after.channel.id,
            )

        self.is_connected = after.channel is not None
        if after.channel is None and self.is_streaming:
            # Rời voice thì stream cũng kết thúc
            self.is_streaming = False
        self.notify_state_change()

    # ------------------------------------------------------------------
    # Hành động
    # ------------------------------------------------------------------
    async def join_vc(self, channel_id: int | None = None) -> bool:
        """Join voice channel, trả về True nếu thành công.

        Nếu ``channel_id`` được truyền, cập nhật ``target_channel_id`` trước.
        """
        if channel_id is not None:
            self.target_channel_id = channel_id

        max_retries = 3
        for attempt in range(max_retries):
            try:
                channel = self.get_channel(self.target_channel_id)
                fetch_error = ""
                if channel is None:
                    # Thử fetch channel nếu không có trong cache
                    try:
                        channel = await self.fetch_channel(self.target_channel_id)
                    except discord.Forbidden:
                        fetch_error = "tài khoản chưa vào server này hoặc không có quyền xem kênh"
                    except discord.NotFound:
                        fetch_error = "ID không tồn tại (có thể bạn đã dán ID server thay vì ID kênh)"
                    except Exception as exc:
                        fetch_error = str(exc)

                if channel is None:
                    self.error = f"Không tìm thấy channel {self.target_channel_id}"
                    if fetch_error:
                        self.error += f": {fetch_error}"
                    app_logger.error("%s (%s)", self.error, self.user)
                    self.notify_state_change()
                    return False

                if not isinstance(
                    channel, (discord.VoiceChannel, discord.StageChannel)
                ):
                    self.error = f"ID {self.target_channel_id} không phải voice channel"
                    app_logger.warning("%s (%s)", self.error, self.user)
                    self.notify_state_change()
                    return False

                guild = channel.guild
                # Join bằng change_voice_state (nhẹ hơn, ít lỗi 4006 với selfbot)
                await guild.change_voice_state(
                    channel=channel, self_mute=False, self_deaf=False
                )

                # Polling chờ state cập nhật (tối đa 10s)
                for _ in range(20):
                    await asyncio.sleep(0.5)
                    me = guild.me
                    if (
                        me is not None
                        and me.voice is not None
                        and me.voice.channel is not None
                        and me.voice.channel.id == channel.id
                    ):
                        self.is_connected = True
                        self.error = None
                        app_logger.info("Đã vào %s | %s", channel.name, self.user)
                        self.notify_state_change()
                        return True

                app_logger.warning(
                    "Đã gửi tín hiệu join nhưng state chưa cập nhật (%s)", self.user
                )
            except Exception as exc:
                self.error = str(exc)
                app_logger.error(
                    "Lỗi join VC (%s) lần %d/%d: %s",
                    self.user,
                    attempt + 1,
                    max_retries,
                    exc,
                )
                await asyncio.sleep(2)

        self.notify_state_change()
        return False

    async def toggle_state(
        self,
        mute: bool | None = None,
        deaf: bool | None = None,
        video: bool | None = None,
    ) -> bool:
        """Bật/tắt Mic/Deaf/Video trên voice channel bot đang hoạt động."""
        try:
            # Ưu tiên voice channel đang active (bot có thể đã bị move khỏi lobby)
            channel = self._current_voice_channel()
            if channel is None:
                channel = self.get_channel(self.target_channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(self.target_channel_id)
                except Exception as exc:
                    app_logger.debug(
                        "Không fetch được channel %s: %s", self.target_channel_id, exc
                    )

            if channel is None:
                self.error = f"Không tìm thấy channel {self.target_channel_id}"
                app_logger.warning("%s (không thể toggle)", self.error)
                self.notify_state_change()
                return False

            guild = channel.guild
            me = guild.me
            if me is None or me.voice is None or me.voice.channel is None:
                self.error = "Chưa tham gia voice nên không thể toggle"
                app_logger.warning(
                    "%s đã ready nhưng chưa ở trong voice.", self.user
                )
                self.notify_state_change()
                return False

            current = me.voice
            new_mute = mute if mute is not None else current.self_mute
            new_deaf = deaf if deaf is not None else current.self_deaf
            new_video = video if video is not None else current.self_video

            await guild.change_voice_state(
                channel=current.channel,
                self_mute=new_mute,
                self_deaf=new_deaf,
                self_video=new_video,
            )
            self.error = None
            app_logger.info(
                "Cập nhật %s: Mute=%s, Deaf=%s, Cam=%s",
                self.user,
                new_mute,
                new_deaf,
                new_video,
            )
            self.notify_state_change()
            return True
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Toggle thất bại cho %s: %s", self.user, exc)
            self.notify_state_change()
            return False

    async def start_stream(self) -> bool:
        """Bật stream (Go Live) bằng gateway Opcode 18 (STREAM_CREATE)."""
        try:
            channel = self._current_voice_channel()
            if channel is None:
                raise RuntimeError("Bot chưa tham gia voice channel")

            ws = self.ws
            if ws is None or getattr(ws, "closed", False):
                raise RuntimeError("WebSocket chưa kết nối")

            guild = channel.guild
            await ws.send_as_json(
                {
                    "op": 18,
                    "d": {
                        "type": "guild",
                        "guild_id": str(guild.id),
                        "channel_id": str(channel.id),
                        "preferred_region": None,
                    },
                }
            )
            self.is_streaming = True
            self.error = None
            app_logger.info("Đã bật stream cho %s tại %s", self.user, channel.name)
            self.notify_state_change()
            return True
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Bật stream thất bại cho %s: %s", self.user, exc)
            self.notify_state_change()
            return False

    async def stop_stream(self) -> bool:
        """Tắt stream bằng gateway Opcode 19 (STREAM_DELETE)."""
        try:
            channel = self._current_voice_channel()
            if channel is None:
                raise RuntimeError("Bot chưa tham gia voice channel")

            ws = self.ws
            if ws is None or getattr(ws, "closed", False):
                raise RuntimeError("WebSocket chưa kết nối")

            guild = channel.guild
            user = self.user
            if user is None:
                raise RuntimeError("Bot chưa đăng nhập")

            await ws.send_as_json(
                {
                    "op": 19,
                    "d": {
                        "stream_key": f"guild:{guild.id}:{channel.id}:{user.id}",
                    },
                }
            )
            self.is_streaming = False
            self.error = None
            app_logger.info("Đã tắt stream cho %s", self.user)
            self.notify_state_change()
            return True
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Tắt stream thất bại cho %s: %s", self.user, exc)
            self.notify_state_change()
            return False

    async def rename(self, new_nick: str | None) -> bool:
        """Đổi nickname; ``None`` hoặc ``"reset"`` để trả về tên mặc định."""
        if new_nick is None or new_nick.lower() == "reset":
            target_nick: str | None = None
        else:
            target_nick = new_nick

        try:
            channel = self._current_voice_channel() or self.get_channel(
                self.target_channel_id
            )
            if channel is None:
                try:
                    channel = await self.fetch_channel(self.target_channel_id)
                except Exception as exc:
                    app_logger.debug(
                        "Không fetch được channel %s: %s", self.target_channel_id, exc
                    )

            if channel is None:
                self.error = "Không tìm thấy guild để đổi tên"
                app_logger.warning("%s (%s)", self.error, self.user)
                self.notify_state_change()
                return False

            guild = channel.guild
            me = guild.me
            if me is None:
                self.error = "Không tìm thấy thành viên trong guild"
                app_logger.warning("%s (%s)", self.error, self.user)
                self.notify_state_change()
                return False

            await me.edit(nick=target_nick)
            self.error = None
            app_logger.info(
                "Đã đổi tên %s → %s", self.user, target_nick or "(mặc định)"
            )
            self.notify_state_change()
            return True
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Đổi tên thất bại cho %s: %s", self.user, exc)
            self.notify_state_change()
            return False

    async def send_message(self, channel_id: int, content: str) -> bool:
        """Gửi tin nhắn tới text channel; trả True nếu thành công."""
        try:
            channel = self.get_channel(channel_id)
            if channel is None:
                channel = await self.fetch_channel(channel_id)
            await channel.send(content)
            self.error = None
            app_logger.info("Đã gửi tin nhắn qua %s tới kênh %s", self.user, channel_id)
            return True
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Gửi tin nhắn qua %s thất bại: %s", self.user, exc)
            self.notify_state_change()
            return False


class BotManager:
    """Quản lý tập trung các bot cho web dashboard."""

    def __init__(self, tokens_file: str = "tokens.txt") -> None:
        self.bots: list[Bot] = []
        self.tokens: list[str] = []
        self.channel_ids: list[int] = []  # Hỗ trợ nhiều channel
        self.tokens_file: str = tokens_file
        self.state_listeners: list[StateListener] = []
        self.active_mode: str = "idle"  # "idle" / "normal" / "auto_room"
        self.start_timestamp: float = time.time()

    # ------------------------------------------------------------------
    # Listeners
    # ------------------------------------------------------------------
    def add_state_listener(self, callback: StateListener) -> None:
        """Đăng ký callback nhận state dict mỗi khi có bot thay đổi."""
        if callback not in self.state_listeners:
            self.state_listeners.append(callback)

    def remove_state_listener(self, callback: StateListener) -> None:
        """Gỡ callback đã đăng ký."""
        if callback in self.state_listeners:
            self.state_listeners.remove(callback)

    def _dispatch_state(self, state: BotState) -> None:
        """Chuyển state của bot tới toàn bộ listener."""
        for listener in list(self.state_listeners):
            _invoke_callback(listener, state)

    def _create_bot(
        self, token: str, channel_id: int, is_leader: bool = False
    ) -> Bot:
        """Tạo bot, gắn callback state và thêm vào danh sách quản lý."""
        bot = Bot(token, channel_id)
        bot.is_leader = is_leader
        bot.on_state_change = self._dispatch_state
        self.bots.append(bot)
        return bot

    def _find_bot(self, bot_id: int | str) -> Bot | None:
        """Tìm bot theo ID duy nhất (user ID/hash token), token đầy đủ hoặc token đã che."""
        key = str(bot_id)
        for bot in self.bots:
            if key == bot.bot_id or key == bot.token_str or key == bot.token_masked:
                return bot
        return None

    # ------------------------------------------------------------------
    # Token
    # ------------------------------------------------------------------
    def load_tokens(self, filepath: str | None = None) -> None:
        """Đọc token từ file; thiếu file thì log lỗi và để danh sách rỗng."""
        path = _resolve_tokens_path(filepath or self.tokens_file)
        if not path.exists():
            app_logger.error("Không tìm thấy file token: %s", path)
            self.tokens = []
            return

        lines = path.read_text(encoding="utf-8").splitlines()
        self.tokens = [line.strip() for line in lines if line.strip()]
        app_logger.info("Đã nạp %d token từ %s", len(self.tokens), path)

    def save_tokens(self, tokens: list[str]) -> None:
        """Ghi danh sách token vào file và cập nhật bộ nhớ."""
        cleaned = [token.strip() for token in tokens if token.strip()]
        path = _resolve_tokens_path(self.tokens_file)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Sao lưu nội dung cũ trước khi ghi đè để tránh mất dữ liệu
        if path.exists():
            backup = path.with_name(path.name + ".bak")
            try:
                shutil.copy2(path, backup)
            except OSError as exc:
                app_logger.warning("Không tạo được backup %s: %s", backup, exc)

        content = "\n".join(cleaned)
        if content:
            content += "\n"
        path.write_text(content, encoding="utf-8")
        self.tokens = cleaned
        app_logger.info("Đã lưu %d token vào %s", len(cleaned), path)

    # ------------------------------------------------------------------
    # Khởi động
    # ------------------------------------------------------------------
    async def safe_start_bot(self, bot: Bot, token: str) -> None:
        """Wrapper chạy bot an toàn, bắt lỗi login và cập nhật state."""
        try:
            await bot.start(token)
        except discord.LoginFailure:
            bot.error = "Token không hợp lệ hoặc đã hết hạn"
            app_logger.error(
                "Đăng nhập thất bại: token %s... không hợp lệ.", token[:6]
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.error = str(exc)
            app_logger.exception("Lỗi khi chạy bot %s...: %s", token[:6], exc)
        finally:
            bot.notify_state_change()

    async def start_normal(
        self, channel_ids: list[int] | None = None, delay: float = 5.0
    ) -> None:
        """Chế độ thường: chia đều bot vào các voice channel (round-robin).

        Không có channel nào thì chỉ đăng nhập; chọn kênh sau bằng ``move_bots``.
        """
        self.channel_ids = list(channel_ids or [])

        if not self.tokens:
            app_logger.error("Chưa có token nào; hãy gọi load_tokens() trước.")
            return

        self.active_mode = "normal"
        self.start_timestamp = time.time()

        num_channels = len(self.channel_ids)
        for i, token in enumerate(self.tokens):
            if len(token) < 5:
                continue

            assigned_channel = self.channel_ids[i % num_channels] if num_channels else 0
            bot = self._create_bot(token, assigned_channel)
            app_logger.info("Bot %d → Channel %s", i + 1, assigned_channel or "(chưa chọn)")

            asyncio.create_task(self.safe_start_bot(bot, token))

            if i < len(self.tokens) - 1 and delay > 0:
                app_logger.info(
                    "Chờ %.1fs trước lần login tiếp theo... (%d/%d)",
                    delay,
                    i + 1,
                    len(self.tokens),
                )
                await asyncio.sleep(delay)

    async def start_auto_room(
        self, lobby_id: int, leader_count: int = 5, delay: float = 5.0
    ) -> None:
        """Auto-Room: leader join lobby, đợi bị move, rồi phân phối phần còn lại."""
        total = len(self.tokens)
        if total < leader_count:
            app_logger.error(
                "Cần ít nhất %d token; hiện có %d.", leader_count, total
            )
            return

        self.active_mode = "auto_room"
        self.start_timestamp = time.time()

        # --- Bước 1: Gửi leader vào lobby ---
        app_logger.info("BƯỚC 1: Gửi %d leader vào lobby %s...", leader_count, lobby_id)
        leaders: list[Bot] = []
        for i in range(leader_count):
            token = self.tokens[i]
            if len(token) < 5:
                continue
            bot = self._create_bot(token, lobby_id, is_leader=True)
            leaders.append(bot)
            app_logger.info("Leader %d/%d → Lobby %s", i + 1, leader_count, lobby_id)
            asyncio.create_task(self.safe_start_bot(bot, token))
            if i < leader_count - 1 and delay > 0:
                app_logger.info(
                    "Chờ %.1fs... (%d/%d)", delay, i + 1, leader_count
                )
                await asyncio.sleep(delay)

        # --- Bước 2: Đợi leader được server bot move sang room mới ---
        app_logger.info("BƯỚC 2: Đang đợi server bot tạo room và move leader...")
        app_logger.info("(Timeout: 3 phút)")

        timeout_seconds = 180
        new_channel_id: int | None = None

        for tick in range(timeout_seconds * 2):
            await asyncio.sleep(0.5)
            for leader in leaders:
                if leader.moved_to_channel:
                    new_channel_id = leader.moved_to_channel
                    break
            if new_channel_id:
                break
            if tick > 0 and tick % 20 == 0:
                app_logger.info("... đã đợi %ds", tick // 2)

        if new_channel_id is None:
            app_logger.error("TIMEOUT! Không phát hiện room mới sau 3 phút.")
            app_logger.error("Hãy đảm bảo server bot có auto-room khi join lobby.")
            return

        app_logger.info("Phát hiện room mới: %s", new_channel_id)

        # Đợi thêm 5s để tất cả leader đã được move
        app_logger.info("Đợi thêm 5s để ổn định...")
        await asyncio.sleep(5)

        new_rooms: list[int] = list(
            {
                leader.moved_to_channel
                for leader in leaders
                if leader.moved_to_channel
            }
        )
        app_logger.info("Tổng room mới phát hiện: %d → %s", len(new_rooms), new_rooms)

        # --- Bước 3: Phân phối token còn lại vào các room mới ---
        remaining_tokens = self.tokens[leader_count:]
        if not remaining_tokens:
            app_logger.warning("Không còn token nào để phân phối.")
            return

        num_rooms = len(new_rooms)
        app_logger.info(
            "BƯỚC 3: Phân phối %d token còn lại vào %d room...",
            len(remaining_tokens),
            num_rooms,
        )

        for i, token in enumerate(remaining_tokens):
            if len(token) < 5:
                continue
            assigned_room = new_rooms[i % num_rooms]
            bot = self._create_bot(token, assigned_room)
            app_logger.info(
                "Bot %d → Room %s", leader_count + i + 1, assigned_room
            )
            asyncio.create_task(self.safe_start_bot(bot, token))
            if i < len(remaining_tokens) - 1 and delay > 0:
                app_logger.info(
                    "Chờ %.1fs... (%d/%d)",
                    delay,
                    leader_count + i + 1,
                    total,
                )
                await asyncio.sleep(delay)

        app_logger.info("HOÀN TẤT! Đã phân phối tất cả %d token.", total)

    async def sync_running_bots(self, delay: float = 5.0) -> dict[str, int]:
        """Khớp bot đang chạy với danh sách token: token mới thì đăng nhập, token bị xoá thì đăng xuất.

        Chỉ làm khi hệ thống đang chạy; token mới vào cùng kênh với các bot hiện có.
        """
        if not self.bots:
            return {"added": 0, "removed": 0}

        # Chọn kênh đích trước khi đóng bot bị xoá, ưu tiên kênh bot đang ngồi
        target: int | None = None
        for bot in self.bots:
            current = bot._current_voice_channel()
            if current is not None:
                target = current.id
                break
        if target is None:
            target = self.bots[0].target_channel_id or (self.channel_ids[0] if self.channel_ids else None)

        wanted = set(self.tokens)
        removed = [bot for bot in self.bots if bot.token_str not in wanted]
        if removed:
            await asyncio.gather(*(bot.close() for bot in removed), return_exceptions=True)
            for bot in removed:
                self.bots.remove(bot)
                app_logger.info("Đã đăng xuất token bị xoá: %s", bot.token_masked)

        running = {bot.token_str for bot in self.bots}
        new_tokens = [token for token in self.tokens if token not in running and len(token) >= 5]
        if new_tokens and target is not None:
            asyncio.create_task(self._start_tokens(new_tokens, target, delay))
        elif new_tokens:
            app_logger.warning("Có %d token mới nhưng chưa biết kênh để vào.", len(new_tokens))

        for bot in self.bots:
            bot.notify_state_change()
        return {"added": len(new_tokens) if target is not None else 0, "removed": len(removed)}

    async def _start_tokens(self, tokens: list[str], channel_id: int, delay: float) -> None:
        """Đăng nhập lần lượt các token vào một kênh, nghỉ ``delay`` giây giữa mỗi lần."""
        for index, token in enumerate(tokens):
            bot = self._create_bot(token, channel_id)
            app_logger.info("Token mới %s → Channel %s", bot.token_masked, channel_id)
            asyncio.create_task(self.safe_start_bot(bot, token))
            bot.notify_state_change()
            if index < len(tokens) - 1 and delay > 0:
                await asyncio.sleep(delay)

    async def stop_all(self) -> None:
        """Đóng toàn bộ bot và đưa hệ thống về trạng thái idle."""
        app_logger.info("Đang dừng %d bot...", len(self.bots))
        if self.bots:
            await asyncio.gather(
                *(bot.close() for bot in self.bots), return_exceptions=True
            )
        self.bots.clear()
        self.active_mode = "idle"
        app_logger.info("Đã dừng toàn bộ bot.")

    # ------------------------------------------------------------------
    # Điều khiển
    # ------------------------------------------------------------------
    async def toggle_all(
        self,
        mute_toggle: bool = False,
        cam_toggle: bool = False,
        deaf_toggle: bool = False,
    ) -> int:
        """Toggle state cho toàn bộ bot đã ready; trả số bot thành công."""
        ready_bots = [bot for bot in self.bots if bot.is_ready()]
        app_logger.info("Tìm thấy %d bot ready; bắt đầu toggle...", len(ready_bots))

        if not ready_bots:
            app_logger.warning("Không có bot nào ở trạng thái ready.")
            return 0

        results = await asyncio.gather(
            *(
                bot.toggle_state(
                    mute=mute_toggle, deaf=deaf_toggle, video=cam_toggle
                )
                for bot in ready_bots
            )
        )
        count = sum(1 for result in results if result)
        app_logger.info("Đã toggle thành công %d/%d bot.", count, len(ready_bots))
        return count

    async def toggle_single(
        self,
        bot_id: int | str,
        mute: bool | None = None,
        deaf: bool | None = None,
        video: bool | None = None,
    ) -> bool:
        """Toggle state cho một bot theo ID/token; trả về kết quả."""
        bot = self._find_bot(bot_id)
        if bot is None:
            app_logger.warning("Không tìm thấy bot: %s", bot_id)
            return False
        return await bot.toggle_state(mute=mute, deaf=deaf, video=video)

    async def stream_single(self, bot_id: int | str, enable: bool) -> bool:
        """Bật/tắt stream cho một bot theo ID; trả về kết quả."""
        bot = self._find_bot(bot_id)
        if bot is None:
            app_logger.warning("Không tìm thấy bot: %s", bot_id)
            return False
        if not bot.is_ready():
            app_logger.warning("Bot chưa ready: %s", bot_id)
            return False
        return await (bot.start_stream() if enable else bot.stop_stream())

    async def stream_all(self, enable: bool) -> int:
        """Bật/tắt stream cho toàn bộ bot ready đang ở trong voice."""
        targets = [bot for bot in self.bots if bot.is_ready() and bot.is_in_voice]
        count = 0
        for bot in targets:
            if await (bot.start_stream() if enable else bot.stop_stream()):
                count += 1

        app_logger.info(
            "%s stream: %d/%d bot", "Bật" if enable else "Tắt", count, len(targets)
        )
        return count

    async def spam_reaction(self, channel_id: int, message_id: int, emoji: str) -> int:
        """Cho toàn bộ bot ready thả reaction; trả số lần thành công."""
        ready_bots = [bot for bot in self.bots if bot.is_ready()]
        app_logger.info(
            "Tìm thấy %d bot ready; bắt đầu thả %s...", len(ready_bots), emoji
        )

        count = 0
        for bot in ready_bots:
            try:
                # Dùng HTTP request trực tiếp để nhanh hơn
                await bot.http.add_reaction(channel_id, message_id, emoji)
                count += 1
                await asyncio.sleep(0.1)
            except Exception as exc:
                app_logger.error("Bot %s thả reaction thất bại: %s", bot.user, exc)

        app_logger.info("Đã thả reaction %d/%d lần.", count, len(ready_bots))
        return count

    async def rename_all(self, new_name: str) -> int:
        """Đổi nickname cho toàn bộ bot ready; trả số bot thành công."""
        ready_bots = [bot for bot in self.bots if bot.is_ready()]
        label = "mặc định" if new_name.lower() == "reset" else new_name
        app_logger.info("Đang đổi tên %d bot → '%s'...", len(ready_bots), label)

        count = 0
        for bot in ready_bots:
            if await bot.rename(new_name):
                count += 1
            # Delay an toàn
            await asyncio.sleep(1.0)

        app_logger.info("Đổi tên thành công %d/%d bot.", count, len(ready_bots))
        return count

    async def rename_single(self, bot_id: int | str, name: str) -> bool:
        """Đổi nickname cho một bot theo ID/token; trả về kết quả."""
        bot = self._find_bot(bot_id)
        if bot is None:
            app_logger.warning("Không tìm thấy bot: %s", bot_id)
            return False
        return await bot.rename(name)

    async def move_bots(
        self, channel_id: int, bot_ids: list[int | str] | None = None
    ) -> int:
        """Di chuyển bot ready tới channel; trả số bot di chuyển thành công."""
        if bot_ids is None:
            targets = list(self.bots)
        else:
            targets = [
                bot
                for bot in (self._find_bot(bot_id) for bot_id in bot_ids)
                if bot is not None
            ]

        moved = 0
        for bot in targets:
            if not bot.is_ready():
                app_logger.warning("Bỏ qua %s: bot chưa ready.", bot.token_masked)
                continue
            if await bot.join_vc(channel_id):
                moved += 1

        app_logger.info(
            "Đã di chuyển %d/%d bot tới channel %s", moved, len(targets), channel_id
        )
        return moved

    async def broadcast_message(self, channel_id: int, content: str) -> int:
        """Gửi tin nhắn bằng toàn bộ bot ready; trả số lần gửi thành công."""
        sent = 0
        for bot in self.bots:
            if not bot.is_ready():
                continue
            try:
                channel = bot.get_channel(channel_id)
                if channel is None:
                    channel = await bot.fetch_channel(channel_id)
                await channel.send(content)
                sent += 1
                # Delay nhỏ để tránh rate limit
                await asyncio.sleep(0.1)
            except Exception as exc:
                app_logger.error(
                    "Gửi tin nhắn thất bại qua %s: %s", bot.token_masked, exc
                )

        app_logger.info(
            "Đã gửi tin nhắn bằng %d bot tới channel %s", sent, channel_id
        )
        return sent

    def get_available_voice_channels(self, bot_id: int | str) -> list[dict[str, Any]]:
        """Liệt kê voice/stage channel trong các guild mà bot đang tham gia."""
        bot = self._find_bot(bot_id)
        if bot is None:
            app_logger.warning("Không tìm thấy bot: %s", bot_id)
            return []

        return self._voice_channels_of(bot)

    @staticmethod
    def _voice_channels_of(bot: Bot) -> list[dict[str, Any]]:
        """Voice/stage channel trong các guild của một bot (ID dạng chuỗi cho JS)."""
        channels: list[dict[str, Any]] = []
        for guild in bot.guilds:
            try:
                for channel in list(guild.voice_channels) + list(
                    guild.stage_channels
                ):
                    channels.append(
                        {
                            "id": str(channel.id),
                            "name": channel.name,
                            "guild_id": str(guild.id),
                            "guild_name": guild.name,
                            "user_limit": channel.user_limit,
                            "member_count": len(channel.members),
                        }
                    )
            except Exception as exc:
                app_logger.warning(
                    "Không lấy được danh sách kênh của guild %s: %s", guild.id, exc
                )
        return channels

    def get_available_text_channels(self, bot_id: int | str) -> list[dict[str, Any]]:
        """Liệt kê text channel mà bot gửi được tin nhắn, sắp theo server rồi vị trí kênh."""
        bot = self._find_bot(bot_id)
        if bot is None:
            app_logger.warning("Không tìm thấy bot: %s", bot_id)
            return []

        channels: list[dict[str, Any]] = []
        for guild in sorted(bot.guilds, key=lambda item: item.name.lower()):
            try:
                me = guild.me
                for channel in guild.text_channels:
                    if me is not None and not channel.permissions_for(me).send_messages:
                        continue
                    channels.append(
                        {
                            "id": str(channel.id),
                            "name": channel.name,
                            "guild_id": str(guild.id),
                            "guild_name": guild.name,
                            "category": channel.category.name if channel.category else None,
                        }
                    )
            except Exception as exc:
                app_logger.warning(
                    "Không lấy được text channel của guild %s: %s", guild.id, exc
                )
        return channels

    def get_voice_channel_members(self, channel_id: int) -> dict[str, Any] | None:
        """Người đang ở trong một voice channel, nhìn qua bất kỳ bot ready nào thấy kênh đó."""
        own_ids = self._own_user_ids()
        for bot in self.bots:
            if not bot.is_ready():
                continue
            channel = bot.get_channel(channel_id)
            if isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
                return self._voice_channel_detail(channel, own_ids)
        return None

    def get_guild_voice_overview(self, guild_id: int) -> dict[str, Any] | None:
        """Mọi voice channel của một server kèm người đang ở trong, theo thứ tự kênh trên Discord."""
        own_ids = self._own_user_ids()
        for bot in self.bots:
            if not bot.is_ready():
                continue
            guild = bot.get_guild(guild_id)
            if guild is None:
                continue
            channels = list(guild.voice_channels) + list(guild.stage_channels)
            return {
                "id": str(guild.id),
                "name": guild.name,
                "accounts": self._guild_accounts(guild_id),
                "channels": [self._voice_channel_detail(channel, own_ids) for channel in channels],
            }
        return None

    def _guild_accounts(self, guild_id: int) -> list[dict[str, Any]]:
        """Các tài khoản token đang online và là thành viên của server."""
        accounts: list[dict[str, Any]] = []
        for bot in self.bots:
            if not bot.is_ready() or bot.user is None or bot.get_guild(guild_id) is None:
                continue
            current = bot._current_voice_channel()
            in_this_guild = current is not None and current.guild.id == guild_id
            avatar = getattr(bot.user, "display_avatar", None)
            accounts.append(
                {
                    "id": bot.bot_id,
                    "name": getattr(bot.user, "display_name", bot.user.name),
                    "username": bot.user.name,
                    "avatar_url": str(avatar.url) if avatar else None,
                    "voice_channel_id": str(current.id) if in_this_guild else None,
                    "voice_channel_name": current.name if in_this_guild else None,
                }
            )
        return accounts

    def _own_user_ids(self) -> set[int]:
        """User ID của các tài khoản token đang chạy."""
        return {bot.user.id for bot in self.bots if bot.user is not None}

    @staticmethod
    def _voice_channel_detail(
        channel: discord.VoiceChannel | discord.StageChannel, own_ids: set[int]
    ) -> dict[str, Any]:
        """Thông tin một voice channel và người đang ở trong (ID dạng chuỗi cho JS)."""
        members: list[dict[str, Any]] = []
        for member in channel.members:
            voice = member.voice
            avatar = getattr(member, "display_avatar", None)
            members.append(
                {
                    "id": str(member.id),
                    "name": member.name,
                    "display_name": member.display_name,
                    "avatar_url": str(avatar.url) if avatar else None,
                    "is_own": member.id in own_ids,
                    "mute": bool(voice and (voice.self_mute or voice.mute)),
                    "deaf": bool(voice and (voice.self_deaf or voice.deaf)),
                    "video": bool(voice and voice.self_video),
                    "stream": bool(voice and getattr(voice, "self_stream", False)),
                }
            )
        # Bot của mình xếp cuối để người thật hiện trước
        members.sort(key=lambda item: (item["is_own"], item["display_name"].lower()))
        return {
            "id": str(channel.id),
            "name": channel.name,
            "guild_name": channel.guild.name,
            "user_limit": channel.user_limit,
            "members": members,
        }

    def get_all_voice_channels(self) -> list[dict[str, Any]]:
        """Gộp voice channel mà ít nhất một bot ready nhìn thấy, kèm số bot thấy/đang ở đó."""
        merged: dict[str, dict[str, Any]] = {}
        for bot in self.bots:
            if not bot.is_ready():
                continue
            current = bot._current_voice_channel()
            current_id = str(current.id) if current is not None else None
            for channel in self._voice_channels_of(bot):
                entry = merged.setdefault(
                    channel["id"], {**channel, "bots_visible": 0, "bots_here": 0}
                )
                entry["bots_visible"] += 1
                if channel["id"] == current_id:
                    entry["bots_here"] += 1
        return sorted(
            merged.values(), key=lambda item: (item["guild_name"].lower(), item["name"].lower())
        )

    async def chat_single(self, bot_id: int | str, channel_id: int, message: str) -> bool:
        """Gửi tin nhắn bằng một bot tới text channel chỉ định."""
        bot = self._find_bot(bot_id)
        if bot is None:
            app_logger.warning("Không tìm thấy bot: %s", bot_id)
            return False
        return await bot.send_message(channel_id, message)

    async def chat_all(
        self, channel_id: int, message: str, delay: float = 1.0
    ) -> int:
        """Gửi tin nhắn bằng toàn bộ bot ready, nghỉ ``delay`` giây giữa mỗi bot."""
        ready_bots = [bot for bot in self.bots if bot.is_ready()]
        sent = 0
        for bot in ready_bots:
            if await bot.send_message(channel_id, message):
                sent += 1
            if delay > 0:
                await asyncio.sleep(delay)

        app_logger.info(
            "Chat all: %d/%d bot gửi thành công tới kênh %s",
            sent,
            len(ready_bots),
            channel_id,
        )
        return sent

    # ------------------------------------------------------------------
    # Tổng quan
    # ------------------------------------------------------------------
    def get_summary(self) -> dict[str, Any]:
        """Trả tổng quan hệ thống cho API dashboard."""
        ready_bots = sum(1 for bot in self.bots if bot.is_ready())
        bots_in_voice = sum(1 for bot in self.bots if bot.is_in_voice)
        leaders = sum(1 for bot in self.bots if bot.is_leader)

        return {
            "active_mode": self.active_mode,
            "start_timestamp": self.start_timestamp,
            "uptime_seconds": round(time.time() - self.start_timestamp, 1),
            "tokens_file": self.tokens_file,
            "total_tokens": len(self.tokens),
            "total_bots": len(self.bots),
            "ready_bots": ready_bots,
            "bots_in_voice": bots_in_voice,
            "leaders": leaders,
            "channel_ids": list(self.channel_ids),
            "bots": [bot.to_dict() for bot in self.bots],
        }


bot_manager = BotManager()

__all__ = ["Bot", "BotManager", "BotState", "StateListener", "bot_manager"]

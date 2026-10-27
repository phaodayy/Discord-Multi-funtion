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
        # Tự nhận lời mời kết bạn (có thể tắt bằng env AUTO_ACCEPT_FRIENDS=0)
        self.auto_accept_friends: bool = os.getenv("AUTO_ACCEPT_FRIENDS", "1") != "0"

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
        # Quét các lời mời kết bạn còn tồn — nhận hết nếu bật auto-accept
        if self.auto_accept_friends:
            asyncio.create_task(self.sweep_friend_requests())
        self.notify_state_change()

    async def _accept_incoming(self, rel: Any) -> bool:
        """Nhận một lời mời kết bạn, né được anti-spam 80013."""
        # Thử (1) accept thẳng
        try:
            await rel.accept()
            return True
        except Exception as exc:
            if "80013" not in str(exc):
                raise
        # (2) Mở DM với người gửi — Discord coi là đã "biết" nhau
        try:
            await rel.user.create_dm()
            await asyncio.sleep(0.3)
            await rel.accept()
            return True
        except Exception as exc:
            if "80013" not in str(exc):
                app_logger.warning("%s dm+accept lỗi: %s", self.user, exc)
                return False
        # (3) Gửi friend request ngược — nếu Discord cho qua thì khớp outgoing
        try:
            await self.send_friend_request(rel.user)
            return True
        except Exception as exc:
            app_logger.warning(
                "%s send_friend_request fallback lỗi: %s", self.user, exc
            )
            return False

    async def on_relationship_add(self, relationship: Any) -> None:
        """Tự nhận lời mời kết bạn khi có người gửi tới."""
        if not self.auto_accept_friends:
            return
        try:
            if int(getattr(relationship.type, "value", 0)) == 3:  # incoming_request
                await asyncio.sleep(random.uniform(0.4, 1.8))
                if await self._accept_incoming(relationship):
                    app_logger.info(
                        "%s đã accept kết bạn từ %s", self.user, relationship.user
                    )
        except Exception as exc:
            app_logger.warning("%s accept kết bạn lỗi: %s", self.user, exc)

    async def sweep_friend_requests(self) -> int:
        """Quét và accept toàn bộ lời mời kết bạn đang chờ. Trả số đã accept."""
        count = 0
        # Thử fetch mới để tránh cache rỗng; fallback về self.relationships
        try:
            relationships = await self.fetch_relationships()
        except Exception:
            try:
                relationships = list(self.relationships)
            except Exception as exc:
                app_logger.warning("%s không đọc được relationships: %s", self.user, exc)
                return 0
        incoming = [r for r in relationships if int(getattr(r.type, "value", 0)) == 3]
        app_logger.info(
            "%s có %d/%d relationship, %d incoming request",
            self.user, len(incoming), len(relationships), len(incoming),
        )
        for rel in incoming:
            try:
                if await self._accept_incoming(rel):
                    count += 1
                    app_logger.info("%s đã accept kết bạn từ %s", self.user, rel.user)
                await asyncio.sleep(random.uniform(0.5, 1.5))
            except Exception as exc:
                app_logger.warning("%s accept kết bạn lỗi: %s", self.user, exc)
        return count

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

    async def oauth2_authorize(
        self,
        client_id: int,
        scopes: list[str],
        redirect_uri: str | None = None,
        state: str | None = None,
        response_type: str = "code",
    ) -> dict[str, Any]:
        """Authorize OAuth2 app bằng token của chính acc này; trả location có code."""
        try:
            res = await self.http.authorize_oauth2(
                application_id=client_id,
                scopes=scopes,
                response_type=response_type,
                redirect_uri=redirect_uri,
                state=state,
            )
            location = res.get("location") if isinstance(res, dict) else None
            app_logger.info("Bot %s authorize OAuth client_id=%s", self.user, client_id)
            return {"ok": True, "location": location}
        except Exception as exc:
            app_logger.error("Bot %s OAuth authorize lỗi: %s", self.user, exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def leave_guild(self, guild_id: int) -> bool:
        """Rời server (guild) nhưng vẫn giữ nguyên đăng nhập."""
        try:
            guild = self.get_guild(guild_id)
            if guild is None:
                return True  # Không ở trong server đó, coi như xong
            await guild.leave()
            self.error = None
            app_logger.info("%s đã rời server %s", self.user, guild.name)
            self.notify_state_change()
            return True
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Rời server thất bại cho %s: %s", self.user, exc)
            self.notify_state_change()
            return False

    async def leave_vc(self) -> bool:
        """Rời voice channel hiện tại nhưng giữ nguyên đăng nhập."""
        try:
            channel = self._current_voice_channel()
            if channel is None:
                self.error = None
                return True  # Đã không ở trong voice, coi như xong
            await channel.guild.change_voice_state(channel=None)
            self.error = None
            app_logger.info("%s đã rời voice channel %s", self.user, channel.name)
            self.notify_state_change()
            return True
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Rời voice thất bại cho %s: %s", self.user, exc)
            self.notify_state_change()
            return False

    async def edit_profile(
        self,
        display_name: str | None = None,
        bio: str | None = None,
        accent_color: int | None = None,
        avatar_bytes: bytes | None = None,
        avatar_clear: bool = False,
        banner_bytes: bytes | None = None,
        banner_clear: bool = False,
    ) -> dict[str, Any]:
        """Sửa profile toàn cục: display_name / bio / accent / avatar / banner."""
        changes: dict[str, Any] = {}
        if display_name is not None:
            changes["global_name"] = display_name or None
        if bio is not None:
            changes["bio"] = bio
        if accent_color is not None:
            try:
                changes["accent_colour"] = discord.Colour(int(accent_color))
            except Exception as exc:
                raise ValueError(f"accent_color không hợp lệ: {exc}") from exc
        if avatar_clear:
            changes["avatar"] = None
        elif avatar_bytes is not None:
            changes["avatar"] = avatar_bytes
        if banner_clear:
            changes["banner"] = None
        elif banner_bytes is not None:
            changes["banner"] = banner_bytes
        if not changes:
            return {"ok": True, "changed": []}
        try:
            await self.user.edit(**changes)
            self.error = None
            app_logger.info("Đã sửa profile %s: %s", self.user, list(changes.keys()))
            self.notify_state_change()
            return {"ok": True, "changed": list(changes.keys())}
        except Exception as exc:
            self.error = str(exc)
            app_logger.error("Sửa profile thất bại cho %s: %s", self.user, exc)
            self.notify_state_change()
            return {"ok": False, "error": str(exc)[:300]}

    async def rename(self, new_nick: str | None) -> bool:
        """Đổi display name toàn cục của tài khoản; ``None`` hoặc ``"reset"`` để xoá."""
        if new_nick is None or new_nick.lower() == "reset":
            target: str | None = None
        else:
            target = new_nick

        try:
            # Đổi global_name — hiện ở mọi server, không cần đang trong guild nào
            await self.user.edit(global_name=target)
            self.error = None
            app_logger.info(
                "Đã đổi tên %s → %s", self.user, target or "(mặc định)"
            )
            # Nếu đang trong voice channel, cập nhật thêm nickname của guild đó
            channel = self._current_voice_channel()
            if channel is not None and channel.guild.me is not None:
                try:
                    await channel.guild.me.edit(nick=target)
                except Exception as exc:
                    app_logger.debug("Không đổi nick guild %s: %s", channel.guild.id, exc)
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
        # Trạng thái chiến dịch (bấm/chọn lặp nhiều vòng)
        # Nhiều chiến dịch có thể chạy song song; mỗi cái có id riêng.
        self._campaigns: dict[str, dict[str, Any]] = {}

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

    async def delete_token(self, bot_id: int | str) -> bool:
        """Xoá token của một tài khoản: dừng bot, bỏ token khỏi file."""
        bot = self._find_bot(bot_id)
        token_to_remove = bot.token_str if bot else None
        if bot is not None:
            try:
                await bot.close()
            except Exception as exc:
                app_logger.warning("Không đóng được bot %s: %s", bot_id, exc)
            if bot in self.bots:
                self.bots.remove(bot)
        if token_to_remove is None:
            # Có thể bot_id là chính token hoặc đoạn token
            key = str(bot_id)
            token_to_remove = next((t for t in self.tokens if t == key or t.startswith(key.rstrip(".")[:6])), None)
        if token_to_remove is None:
            return False
        new_tokens = [t for t in self.tokens if t != token_to_remove]
        if len(new_tokens) == len(self.tokens):
            return False
        self.save_tokens(new_tokens)
        self._dispatch_state({"bot_id": str(bot_id), "state": "removed"})
        return True

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
        self, channel_ids: list[int] | None = None, delay: float = 0.0
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
        self, lobby_id: int, leader_count: int = 5, delay: float = 0.0
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

    async def sync_running_bots(self, delay: float = 0.0) -> dict[str, int]:
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

    async def edit_profile(self, bot_id: int | str, **fields: Any) -> dict[str, Any]:
        """Sửa profile của một bot (display_name, bio, accent_color, avatar, banner)."""
        bot = self._find_bot(bot_id)
        if bot is None or not bot.is_ready():
            return {"ok": False, "error": "offline"}
        return await bot.edit_profile(**fields)

    async def oauth2_verify(
        self, bot_ids: list[int | str], oauth_url: str
    ) -> dict[str, Any]:
        """Tự authorize OAuth2 cho các acc; dùng khi shop/community có nút Xác minh.

        ``oauth_url`` là URL dán từ nút Xác minh (dạng
        ``https://discord.com/oauth2/authorize?client_id=...&scope=...&...``).
        Sau khi authorize, tool tự gọi ``location`` để worker shop nhận code.
        """
        import urllib.parse as _urlp
        import aiohttp

        parsed = _urlp.urlparse(oauth_url)
        qs = _urlp.parse_qs(parsed.query)
        try:
            client_id = int(qs.get("client_id", [None])[0] or 0)
        except (TypeError, ValueError):
            client_id = 0
        if not client_id:
            return {"success": 0, "total": len(bot_ids), "error": "missing_client_id", "results": []}
        scope = (qs.get("scope", [""])[0] or "identify").split()
        redirect_uri = qs.get("redirect_uri", [None])[0]
        state = qs.get("state", [None])[0]
        response_type = qs.get("response_type", ["code"])[0]

        async def run_one(bid: int | str) -> dict[str, Any]:
            bot = self._find_bot(bid)
            if bot is None or not bot.is_ready():
                return {"bot_id": str(bid), "ok": False, "error": "offline"}
            auth = await bot.oauth2_authorize(client_id, scope, redirect_uri, state, response_type)
            if not auth.get("ok"):
                return {"bot_id": str(bid), **auth}
            location = auth.get("location")
            if not location:
                return {"bot_id": str(bid), "ok": True, "callback": None}
            # Gọi callback của worker để nó cấp role
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(location, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        status = resp.status
                        text = await resp.text()
                return {"bot_id": str(bid), "ok": True, "callback_status": status, "callback_snippet": text[:120]}
            except Exception as exc:
                return {"bot_id": str(bid), "ok": True, "callback_error": str(exc)[:200]}

        results = await asyncio.gather(*(run_one(bid) for bid in bot_ids))
        return {
            "success": sum(1 for r in results if r.get("ok")),
            "total": len(bot_ids),
            "client_id": client_id,
            "scope": scope,
            "results": list(results),
        }

    async def leave_server(
        self, guild_id: int, bot_ids: list[int | str] | None = None
    ) -> dict[str, Any]:
        """Cho các tài khoản chỉ định (hoặc tất cả acc đang ở trong server) rời server."""
        if bot_ids is None:
            targets = [bot for bot in self.bots if bot.is_ready() and bot.get_guild(guild_id) is not None]
        else:
            targets = [
                bot for bot in (self._find_bot(bid) for bid in bot_ids)
                if bot is not None and bot.is_ready() and bot.get_guild(guild_id) is not None
            ]
        if not targets:
            return {"left": 0, "total": 0}
        results = await asyncio.gather(*(bot.leave_guild(guild_id) for bot in targets))
        left = sum(1 for ok in results if ok)
        app_logger.info("Đã cho %d/%d bot rời server %s", left, len(targets), guild_id)
        return {"left": left, "total": len(targets)}

    async def sweep_friend_requests(
        self, bot_ids: list[int | str] | None = None
    ) -> dict[str, Any]:
        """Nhận hết lời mời kết bạn đang chờ cho các acc chỉ định (hoặc tất cả)."""
        if bot_ids is None:
            targets = [bot for bot in self.bots if bot.is_ready()]
        else:
            targets = [
                bot for bot in (self._find_bot(bid) for bid in bot_ids)
                if bot is not None and bot.is_ready()
            ]
        if not targets:
            return {"accepted": 0, "bots": 0}
        counts = await asyncio.gather(
            *(bot.sweep_friend_requests() for bot in targets), return_exceptions=True
        )
        total = sum(c for c in counts if isinstance(c, int))
        app_logger.info(
            "Sweep kết bạn: accept %d lời mời trên %d acc", total, len(targets)
        )
        return {"accepted": total, "bots": len(targets)}

    def set_auto_accept_friends(self, enabled: bool) -> int:
        """Bật/tắt tự accept kết bạn cho mọi bot."""
        for bot in self.bots:
            bot.auto_accept_friends = bool(enabled)
        return len(self.bots)

    async def leave_voice(self, bot_ids: list[int | str] | None = None) -> int:
        """Rời voice channel cho một hoặc toàn bộ bot ready; giữ nguyên đăng nhập."""
        if bot_ids is None:
            targets = [bot for bot in self.bots if bot.is_ready() and bot.is_in_voice]
        else:
            targets = [
                bot
                for bot in (self._find_bot(bid) for bid in bot_ids)
                if bot is not None and bot.is_ready() and bot.is_in_voice
            ]
        if not targets:
            return 0
        results = await asyncio.gather(*(bot.leave_vc() for bot in targets))
        left = sum(1 for ok in results if ok)
        app_logger.info("Đã cho %d/%d bot rời voice", left, len(targets))
        return left

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
                channels.extend(self._guild_message_channels(guild))
            except Exception as exc:
                app_logger.warning(
                    "Không lấy được text channel của guild %s: %s", guild.id, exc
                )
        return channels

    @staticmethod
    def _guild_message_channels(guild: discord.Guild) -> list[dict[str, Any]]:
        """Mọi nơi gửi được tin trong server, theo thứ tự thanh bên Discord.

        Gồm kênh text, kênh thông báo, diễn đàn (và thread trong đó), khung chat
        của kênh voice/stage và các thread đang mở. Kênh không xem được thì bỏ;
        kênh chỉ đọc vẫn liệt kê để khớp với Discord.
        """
        me = guild.me

        def entry(channel: Any, kind: str, can_send: bool, parent: Any = None) -> dict[str, Any]:
            category = getattr(parent or channel, "category", None)
            return {
                "id": str(channel.id),
                "name": channel.name,
                "guild_id": str(guild.id),
                "guild_name": guild.name,
                "category": category.name if category else None,
                "kind": kind,
                "can_send": can_send,
                "parent_id": str(parent.id) if parent is not None else None,
            }

        def perms_of(channel: Any) -> Any:
            return channel.permissions_for(me) if me is not None else None

        # Thread đang mở, gom theo kênh cha để xếp ngay dưới kênh đó
        threads_by_parent: dict[int, list[Any]] = {}
        for thread in getattr(guild, "threads", []):
            threads_by_parent.setdefault(thread.parent_id, []).append(thread)

        def thread_entries(parent: Any) -> list[dict[str, Any]]:
            items: list[dict[str, Any]] = []
            for thread in sorted(threads_by_parent.get(parent.id, []), key=lambda item: item.name.lower()):
                perms = perms_of(thread)
                if perms is not None and not perms.view_channel:
                    continue
                can_send = perms is None or bool(getattr(perms, "send_messages_in_threads", perms.send_messages))
                items.append(entry(thread, "thread", can_send, parent))
            return items

        def sort_key(channel: Any) -> tuple[int, int, int]:
            category = getattr(channel, "category", None)
            # Kênh voice luôn nằm dưới kênh text trong cùng danh mục, như Discord hiển thị
            is_voice = isinstance(channel, (discord.VoiceChannel, discord.StageChannel))
            return (category.position if category else -1, int(is_voice), channel.position)

        items: list[dict[str, Any]] = []
        for channel in sorted(guild.channels, key=sort_key):
            if isinstance(channel, discord.CategoryChannel):
                continue
            perms = perms_of(channel)
            if perms is not None and not perms.view_channel:
                continue
            can_send = perms is None or bool(perms.send_messages)
            if isinstance(channel, discord.ForumChannel):
                # Gửi vào diễn đàn nghĩa là tạo bài mới; dashboard chỉ gửi vào thread có sẵn
                items.append(entry(channel, "forum", False))
                items.extend(thread_entries(channel))
            elif isinstance(channel, discord.TextChannel):
                items.append(entry(channel, "news" if channel.is_news() else "text", can_send))
                items.extend(thread_entries(channel))
            elif isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
                items.append(entry(channel, "voice", can_send))
        return items

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

    def get_guilds(self) -> list[dict[str, Any]]:
        """Các server mà ít nhất một tài khoản online đang tham gia, kèm tài khoản trong đó."""
        merged: dict[int, dict[str, Any]] = {}
        for bot in self.bots:
            if not bot.is_ready() or bot.user is None:
                continue
            current = bot._current_voice_channel()
            for guild in bot.guilds:
                entry = merged.setdefault(
                    guild.id, {"id": str(guild.id), "name": guild.name, "accounts": [], "bots_here": 0}
                )
                entry["accounts"].append({"id": bot.bot_id, "name": getattr(bot.user, "display_name", bot.user.name)})
                if current is not None and current.guild.id == guild.id:
                    entry["bots_here"] += 1
        return sorted(merged.values(), key=lambda item: item["name"].lower())

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
    # Component (nút / select menu) trong kênh chỉ đọc
    # ------------------------------------------------------------------
    def get_available_read_only_channels(self, bot_id: int | str) -> list[dict[str, Any]]:
        """Kênh tài khoản xem được nhưng không gửi tin (ví dụ kênh thông báo, kênh shop bot)."""
        bot = self._find_bot(bot_id)
        if bot is None:
            return []
        channels: list[dict[str, Any]] = []
        for guild in sorted(bot.guilds, key=lambda item: item.name.lower()):
            try:
                for entry in self._guild_message_channels(guild):
                    if entry.get("can_send") is False and entry.get("kind") != "forum":
                        channels.append(entry)
            except Exception as exc:
                app_logger.warning("Không lấy được kênh chỉ đọc của guild %s: %s", guild.id, exc)
        return channels

    async def fetch_channel_components(
        self, bot_id: int | str, channel_id: int, limit: int = 15
    ) -> list[dict[str, Any]]:
        """Tin nhắn gần đây trong kênh có button / select menu, kèm thông tin component."""
        bot = self._find_bot(bot_id)
        if bot is None:
            return []
        channel = bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(channel_id)
            except Exception as exc:
                app_logger.warning("Không mở được kênh %s: %s", channel_id, exc)
                return []

        results: list[dict[str, Any]] = []
        try:
            async for message in channel.history(limit=max(1, min(50, int(limit)))):
                rows = self._serialize_components(message.components)
                if not rows:
                    continue
                author = getattr(message, "author", None)
                author_name = getattr(author, "display_name", None) or getattr(author, "name", "")
                results.append(
                    {
                        "message_id": str(message.id),
                        "channel_id": str(channel.id),
                        "author": author_name,
                        "author_bot": bool(getattr(author, "bot", False)),
                        "content": (message.content or "")[:500],
                        "created_at": message.created_at.isoformat() if message.created_at else None,
                        "components": rows,
                    }
                )
        except discord.Forbidden:
            app_logger.warning("Không có quyền đọc lịch sử kênh %s", channel_id)
            return []
        except Exception as exc:
            app_logger.warning("Lấy tin nhắn kênh %s thất bại: %s", channel_id, exc)
            return []
        return results

    @staticmethod
    def _serialize_components(components: Any) -> list[dict[str, Any]]:
        """Rút gọn ActionRow / Button / SelectMenu thành dạng JSON đơn giản."""
        rows: list[dict[str, Any]] = []
        for row in components or []:
            children: list[dict[str, Any]] = []
            for child in getattr(row, "children", []):
                if isinstance(child, discord.Button):
                    children.append(
                        {
                            "type": "button",
                            "custom_id": child.custom_id or "",
                            "label": child.label or "",
                            "style": int(getattr(child.style, "value", 0) or 0),
                            "disabled": bool(child.disabled),
                            "url": child.url or "",
                            "emoji": str(child.emoji) if child.emoji else "",
                        }
                    )
                elif isinstance(child, discord.SelectMenu):
                    options = []
                    for opt in getattr(child, "options", []):
                        options.append(
                            {
                                "label": opt.label,
                                "value": opt.value,
                                "description": opt.description or "",
                                "emoji": str(opt.emoji) if opt.emoji else "",
                                "default": bool(opt.default),
                            }
                        )
                    children.append(
                        {
                            "type": "select",
                            "custom_id": child.custom_id or "",
                            "placeholder": child.placeholder or "",
                            "min_values": int(child.min_values),
                            "max_values": int(child.max_values),
                            "disabled": bool(child.disabled),
                            "options": options,
                        }
                    )
            if children:
                rows.append({"children": children})
        return rows

    async def interact_component(
        self,
        bot_ids: list[int | str],
        channel_id: int,
        message_id: int,
        custom_id: str,
        values: list[str] | None = None,
    ) -> dict[str, Any]:
        """Mỗi tài khoản bấm nút / chọn option cho component chỉ định; chạy song song."""

        async def run_one(bid: int | str) -> dict[str, Any]:
            bot = self._find_bot(bid)
            if bot is None or not bot.is_ready():
                return {"bot_id": str(bid), "ok": False, "error": "offline"}
            try:
                channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                message = await channel.fetch_message(message_id)
                target = self._find_component(message.components, custom_id)
                if target is None:
                    return {"bot_id": str(bid), "ok": False, "error": "component_not_found"}
                if isinstance(target, discord.Button):
                    await target.click()
                elif isinstance(target, discord.SelectMenu):
                    chosen = [opt for opt in target.options if opt.value in (values or [])]
                    if not chosen:
                        return {"bot_id": str(bid), "ok": False, "error": "no_option"}
                    await target.choose(*chosen)
                app_logger.info(
                    "Bot %s thao tác component %s ở kênh %s (values=%s)",
                    bot.user, custom_id, channel_id, values,
                )
                return {"bot_id": str(bid), "ok": True}
            except Exception as exc:
                app_logger.error("Bot %s thao tác component thất bại: %s", bid, exc)
                return {"bot_id": str(bid), "ok": False, "error": str(exc)[:200]}

        results = await asyncio.gather(*(run_one(bid) for bid in bot_ids))
        success = sum(1 for r in results if r.get("ok"))
        return {"success": success, "total": len(bot_ids), "results": list(results)}

    async def _run_campaign_once(
        self,
        bot_ids: list[int | str],
        steps: list[dict[str, Any]],
        wait_between: float,
        cancel: asyncio.Event,
    ) -> list[dict[str, Any]]:
        """Một vòng chiến dịch."""
        step_results: list[dict[str, Any]] = []
        for index, step in enumerate(steps):
            if cancel.is_set():
                break
            try:
                channel_id = int(step["channel_id"])
                message_id = int(step["message_id"])
            except (KeyError, ValueError, TypeError):
                step_results.append({"step": index, "ok": 0, "total": len(bot_ids), "error": "bad_ids"})
                continue
            custom_id = str(step.get("custom_id") or "")
            values = step.get("values") or []
            result = await self.interact_component(
                bot_ids, channel_id, message_id, custom_id, values
            )
            step_results.append({"step": index, **result})
            if wait_between > 0 and index < len(steps) - 1 and not cancel.is_set():
                await asyncio.sleep(wait_between)
        return step_results

    async def join_server(
        self, bot_ids: list[int | str], invite: str
    ) -> dict[str, Any]:
        """Dán link mời / mã mời → các tài khoản chỉ định cùng vào server (song song)."""
        code = (invite or "").strip()
        if not code:
            return {"success": 0, "total": 0, "results": []}
        # Tách code từ link discord.gg/xxx hoặc discord.com/invite/xxx
        for prefix in ("discord.gg/", "discord.com/invite/", "discordapp.com/invite/"):
            idx = code.lower().find(prefix)
            if idx >= 0:
                code = code[idx + len(prefix) :]
                break
        code = code.split("?")[0].split("/")[0].strip()

        async def run_one(bid: int | str) -> dict[str, Any]:
            bot = self._find_bot(bid)
            if bot is None or not bot.is_ready():
                return {"bot_id": str(bid), "ok": False, "error": "offline"}
            try:
                invite_obj = await bot.accept_invite(code)
                guild = getattr(invite_obj, "guild", None)
                app_logger.info(
                    "Bot %s đã vào server %s qua mã mời %s",
                    bot.user, getattr(guild, "name", "?"), code,
                )
                return {
                    "bot_id": str(bid),
                    "ok": True,
                    "guild_id": str(guild.id) if guild else None,
                    "guild_name": getattr(guild, "name", None),
                }
            except discord.HTTPException as exc:
                # Discord thường bắt captcha hoặc chặn khi tự động vào server
                msg = str(exc)
                app_logger.warning("Bot %s không vào được server (%s): %s", bot.user, code, msg)
                return {"bot_id": str(bid), "ok": False, "error": msg[:200]}
            except Exception as exc:
                app_logger.error("Bot %s join server lỗi: %s", bid, exc)
                return {"bot_id": str(bid), "ok": False, "error": str(exc)[:200]}

        results = await asyncio.gather(*(run_one(bid) for bid in bot_ids))
        return {
            "success": sum(1 for r in results if r.get("ok")),
            "total": len(bot_ids),
            "results": list(results),
            "code": code,
        }

    def start_campaign(
        self,
        bot_ids: list[int | str],
        steps: list[dict[str, Any]],
        wait_between: float = 1.0,
        loops: int = 1,
        wait_between_loops: float = 2.0,
        name: str | None = None,
    ) -> str:
        """Khởi động một chiến dịch mới ở nền. Trả campaign_id duy nhất.

        Nhiều chiến dịch có thể chạy song song — mỗi cái có id và trạng thái riêng.
        """
        campaign_id = hashlib.sha1(f"{time.time()}-{random.random()}".encode()).hexdigest()[:12]
        cancel = asyncio.Event()
        info: dict[str, Any] = {
            "id": campaign_id,
            "name": (name or "").strip() or f"Chiến dịch {campaign_id[:6]}",
            "started_at": time.time(),
            "loops_target": loops,
            "loops_done": 0,
            "steps_count": len(steps),
            "bots_count": len(bot_ids),
            "last_step_results": [],
            "running": True,
            "cancelled": False,
        }
        task = asyncio.create_task(
            self._run_campaign_loop(
                campaign_id, bot_ids, steps, wait_between, loops, wait_between_loops, cancel, info
            )
        )
        self._campaigns[campaign_id] = {"task": task, "cancel": cancel, "info": info}
        app_logger.info("Khởi động chiến dịch %s (name=%s)", campaign_id, info["name"])
        return campaign_id

    async def _run_campaign_loop(
        self,
        campaign_id: str,
        bot_ids: list[int | str],
        steps: list[dict[str, Any]],
        wait_between: float,
        loops: int,
        wait_between_loops: float,
        cancel: asyncio.Event,
        info: dict[str, Any],
    ) -> None:
        """Vòng lặp của một chiến dịch; cập nhật thẳng vào ``info``."""
        try:
            index = 0
            while True:
                if cancel.is_set():
                    break
                step_results = await self._run_campaign_once(bot_ids, steps, wait_between, cancel)
                index += 1
                info["loops_done"] = index
                info["last_step_results"] = step_results
                app_logger.info(
                    "[%s] xong vòng %d (mục tiêu %s)",
                    info["name"], index, "∞" if loops == 0 else loops,
                )
                if loops and index >= loops:
                    break
                if cancel.is_set():
                    break
                if wait_between_loops > 0:
                    waited = 0.0
                    while waited < wait_between_loops and not cancel.is_set():
                        await asyncio.sleep(min(0.5, wait_between_loops - waited))
                        waited += 0.5
        finally:
            info["running"] = False
            info["cancelled"] = cancel.is_set()
            info["ended_at"] = time.time()

    def stop_campaign(self, campaign_id: str | None = None) -> int:
        """Dừng một chiến dịch (theo id) hoặc tất cả nếu không truyền id. Trả số đã dừng."""
        if campaign_id:
            entry = self._campaigns.get(campaign_id)
            if not entry or not entry["info"].get("running"):
                return 0
            entry["cancel"].set()
            app_logger.info("Yêu cầu dừng chiến dịch %s", campaign_id)
            return 1
        count = 0
        for entry in self._campaigns.values():
            if entry["info"].get("running"):
                entry["cancel"].set()
                count += 1
        if count:
            app_logger.info("Yêu cầu dừng %d chiến dịch", count)
        return count

    def campaign_status(self, campaign_id: str | None = None) -> dict[str, Any]:
        """Trả trạng thái một chiến dịch hoặc tất cả."""
        if campaign_id:
            entry = self._campaigns.get(campaign_id)
            return dict(entry["info"]) if entry else {"running": False}
        campaigns = [dict(entry["info"]) for entry in self._campaigns.values()]
        # Sắp xếp: đang chạy trước, mới nhất trên cùng
        campaigns.sort(key=lambda c: (not c.get("running"), -c.get("started_at", 0)))
        running = sum(1 for c in campaigns if c.get("running"))
        return {"running": running > 0, "running_count": running, "campaigns": campaigns}

    def cleanup_finished_campaigns(self, keep_recent: int = 20) -> int:
        """Dọn các chiến dịch đã kết thúc, giữ ``keep_recent`` cái mới nhất."""
        finished = [
            (cid, entry) for cid, entry in self._campaigns.items()
            if not entry["info"].get("running")
        ]
        finished.sort(key=lambda x: x[1]["info"].get("ended_at", 0), reverse=True)
        to_remove = finished[keep_recent:]
        for cid, _ in to_remove:
            self._campaigns.pop(cid, None)
        return len(to_remove)

    @staticmethod
    def _find_component(components: Any, custom_id: str) -> Any:
        """Tìm Button / SelectMenu theo custom_id trong các ActionRow."""
        for row in components or []:
            for child in getattr(row, "children", []):
                if getattr(child, "custom_id", None) == custom_id:
                    return child
        return None

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

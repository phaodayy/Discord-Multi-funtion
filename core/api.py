"""FastAPI router cung cấp REST API cho dashboard."""

from __future__ import annotations

import asyncio
import secrets
from typing import Any, Literal

from fastapi import APIRouter, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config import config
from core.auth import create_token
from core.engine import bot_manager
from core.logger import app_logger, log_handler
from core.tunnel import tunnel_manager

router: APIRouter = APIRouter(prefix="/api")


# -----------------------------------------------------------------------------
# Request models
# -----------------------------------------------------------------------------
class LoginBody(BaseModel):
    """Body cho endpoint đăng nhập."""

    password: str


class StartBody(BaseModel):
    """Body khởi động bot ở chế độ normal hoặc auto_room."""

    mode: Literal["normal", "auto_room"]
    channel_ids: list[str] = Field(default_factory=list)
    delay: float = 0.0
    leader_count: int = 5


class ToggleBody(BaseModel):
    """Body bật/tắt mic, deaf, video hoặc stream cho một hoặc toàn bộ bot."""

    bot_id: str | None = None
    mute: bool | None = None
    deaf: bool | None = None
    video: bool | None = None
    stream: bool | None = None


class RenameBody(BaseModel):
    """Body đổi nickname cho một hoặc toàn bộ bot."""

    bot_id: str | None = None
    name: str


class MoveBody(BaseModel):
    """Body di chuyển bot tới voice channel khác."""

    target_channel_id: str
    bot_ids: list[str] | None = None


class LeaveVoiceBody(BaseModel):
    """Body cho bot rời voice channel (giữ nguyên đăng nhập)."""

    bot_ids: list[str] | None = None


class LeaveServerBody(BaseModel):
    """Body cho bot rời server (guild)."""

    guild_id: str
    bot_ids: list[str] | None = None


class ReactionBody(BaseModel):
    """Body thả reaction vào một tin nhắn."""

    channel_id: str
    message_id: str
    emoji: str


class TokensBody(BaseModel):
    """Body cập nhật danh sách token."""

    tokens: list[str]


class ChatSingleBody(BaseModel):
    """Body gửi tin nhắn bằng một bot."""

    bot_id: str
    channel_id: str
    message: str


class ChatAllBody(BaseModel):
    """Body gửi tin nhắn bằng toàn bộ bot ready."""

    channel_id: str
    message: str
    delay: float = 1.0


class JoinServerBody(BaseModel):
    """Body cho các tài khoản vào server qua link mời."""

    invite: str
    bot_ids: list[str] = Field(default_factory=list)


class ComponentInteractBody(BaseModel):
    """Body cho tài khoản bấm nút / chọn option trong kênh chỉ đọc."""

    bot_ids: list[str]
    channel_id: str
    message_id: str
    custom_id: str
    values: list[str] = Field(default_factory=list)


class CampaignStep(BaseModel):
    """Một bước trong chiến dịch: bấm nút hoặc chọn option."""

    channel_id: str
    message_id: str
    custom_id: str
    values: list[str] = Field(default_factory=list)


class CampaignBody(BaseModel):
    """Body chạy chiến dịch gồm nhiều bước nối nhau; có thể lặp nhiều vòng."""

    bot_ids: list[str]
    steps: list[CampaignStep]
    wait_between: float = 1.0
    loops: int = 1  # 0 = lặp vô hạn, dừng bằng /campaign/stop
    wait_between_loops: float = 2.0


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _parse_int(value: str, field_name: str) -> int:
    """Đổi chuỗi ID sang int, trả lỗi 400 nếu không hợp lệ."""
    try:
        return int(value)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400, detail=f"{field_name} không hợp lệ: {value}"
        )


def _mask_token(token: str) -> str:
    """Che token để hiển thị an toàn trên dashboard."""
    return f"{token[:6]}..."


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@router.post("/login", response_model=None)
async def login(body: LoginBody) -> dict[str, Any] | JSONResponse:
    """Kiểm tra mật khẩu và trả JWT cho dashboard."""
    if not config.password or not secrets.compare_digest(body.password, config.password):
        app_logger.warning("Đăng nhập thất bại từ dashboard.")
        return JSONResponse(status_code=401, content={"error": "Sai mật khẩu"})

    token = create_token()
    expires_in = config.jwt_expire_hours * 3600
    app_logger.info("Dashboard đăng nhập thành công.")
    return {"token": token, "expires_in": expires_in}


@router.get("/status")
async def get_status() -> dict[str, Any]:
    """Tổng quan trạng thái bot và tunnel."""
    return {**bot_manager.get_summary(), **tunnel_manager.get_status()}


@router.post("/start")
async def start(body: StartBody) -> dict[str, Any]:
    """Khởi động bot ở chế độ normal hoặc auto_room (chạy nền)."""
    channel_ids = [_parse_int(cid, "channel_id") for cid in body.channel_ids]
    # Normal mode cho phép không có kênh: chỉ đăng nhập, chọn kênh sau trên dashboard
    if not channel_ids and body.mode != "normal":
        raise HTTPException(status_code=400, detail="Cần ít nhất một channel ID")

    # START lại khi đang chạy: dừng bot cũ để không đăng nhập trùng một token
    if bot_manager.bots:
        await bot_manager.stop_all()

    if body.mode == "normal":
        asyncio.create_task(bot_manager.start_normal(channel_ids, body.delay))
        message = (
            f"Đã bắt đầu chế độ normal với {len(channel_ids)} channel"
        )
    else:
        lobby_id = channel_ids[0]
        asyncio.create_task(
            bot_manager.start_auto_room(lobby_id, body.leader_count, body.delay)
        )
        message = (
            f"Đã bắt đầu chế độ auto_room tại lobby {lobby_id} "
            f"với {body.leader_count} leader"
        )

    app_logger.info(message)
    return {"success": True, "message": message}


@router.post("/stop")
async def stop() -> dict[str, Any]:
    """Dừng toàn bộ bot."""
    await bot_manager.stop_all()
    return {"success": True, "message": "Đã dừng tất cả bots"}


@router.post("/toggle")
async def toggle(body: ToggleBody) -> dict[str, Any]:
    """Toggle mic/deaf/video hoặc stream cho một bot hoặc toàn bộ bot."""
    if body.stream is not None:
        if body.bot_id is not None:
            success = await bot_manager.stream_single(body.bot_id, body.stream)
            return {"success": True, "affected": 1 if success else 0}
        affected = await bot_manager.stream_all(body.stream)
        return {"success": True, "affected": affected}

    if body.bot_id is not None:
        success = await bot_manager.toggle_single(
            body.bot_id, mute=body.mute, deaf=body.deaf, video=body.video
        )
        return {"success": True, "affected": 1 if success else 0}

    affected = await bot_manager.toggle_all(
        mute_toggle=bool(body.mute),
        cam_toggle=bool(body.video),
        deaf_toggle=bool(body.deaf),
    )
    return {"success": True, "affected": affected}


@router.post("/rename")
async def rename(body: RenameBody) -> dict[str, Any]:
    """Đổi nickname cho một bot hoặc toàn bộ bot."""
    if body.bot_id is not None:
        success = await bot_manager.rename_single(body.bot_id, body.name)
        return {"success": True, "affected": 1 if success else 0}

    affected = await bot_manager.rename_all(body.name)
    return {"success": True, "affected": affected}


@router.post("/leave-voice")
async def leave_voice(body: LeaveVoiceBody) -> dict[str, Any]:
    """Cho bot rời voice channel hiện tại; không truyền ``bot_ids`` nghĩa là tất cả."""
    left = await bot_manager.leave_voice(body.bot_ids)
    return {"success": True, "left": left}


@router.post("/leave-server")
async def leave_server(body: LeaveServerBody) -> dict[str, Any]:
    """Cho bot rời server (guild); không truyền ``bot_ids`` nghĩa là tất cả acc đang ở trong server."""
    guild_id = _parse_int(body.guild_id, "guild_id")
    result = await bot_manager.leave_server(guild_id, body.bot_ids)
    return {"success": True, **result}


@router.post("/move")
async def move(body: MoveBody) -> dict[str, Any]:
    """Di chuyển bot tới voice channel chỉ định (join ngay và cập nhật state)."""
    target_channel_id = _parse_int(body.target_channel_id, "target_channel_id")
    moved = await bot_manager.move_bots(target_channel_id, body.bot_ids)
    return {"success": True, "moved": moved}


@router.get("/voice-channels")
async def get_all_voice_channels() -> dict[str, Any]:
    """Liệt kê voice/stage channel mà các bot đang online nhìn thấy."""
    return {"channels": bot_manager.get_all_voice_channels()}


@router.get("/voice-channels/{channel_id}/members")
async def get_voice_channel_members(channel_id: str) -> dict[str, Any]:
    """Danh sách người đang ở trong một voice channel."""
    detail = bot_manager.get_voice_channel_members(_parse_int(channel_id, "channel_id"))
    if detail is None:
        raise HTTPException(status_code=404, detail="Không bot nào thấy kênh này")
    return detail


@router.get("/guilds")
async def get_guilds() -> dict[str, Any]:
    """Server mà các tài khoản online đang tham gia."""
    return {"guilds": bot_manager.get_guilds()}


@router.get("/guilds/{guild_id}/voice")
async def get_guild_voice(guild_id: str) -> dict[str, Any]:
    """Các voice channel của một server kèm người đang ở trong."""
    overview = bot_manager.get_guild_voice_overview(_parse_int(guild_id, "guild_id"))
    if overview is None:
        raise HTTPException(status_code=404, detail="Không bot nào ở trong server này")
    return overview


@router.get("/bots/{bot_id}/text-channels")
async def get_bot_text_channels(bot_id: str) -> dict[str, Any]:
    """Liệt kê text channel mà một bot gửi được tin nhắn."""
    return {"channels": bot_manager.get_available_text_channels(bot_id)}


@router.get("/bots/{bot_id}/voice-channels")
async def get_bot_voice_channels(bot_id: str) -> dict[str, Any]:
    """Liệt kê voice/stage channel khả dụng cho một bot."""
    channels = bot_manager.get_available_voice_channels(bot_id)
    return {"channels": channels}


@router.post("/chat/single")
async def chat_single(body: ChatSingleBody) -> dict[str, Any]:
    """Gửi tin nhắn bằng một bot tới text channel chỉ định."""
    message = body.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Tin nhắn không được để trống")
    channel_id = _parse_int(body.channel_id, "channel_id")
    sent = await bot_manager.chat_single(body.bot_id, channel_id, message)
    return {"success": True, "sent": 1 if sent else 0}


@router.post("/chat/all")
async def chat_all(body: ChatAllBody) -> dict[str, Any]:
    """Gửi tin nhắn bằng toàn bộ bot ready với delay giữa mỗi bot."""
    message = body.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Tin nhắn không được để trống")
    channel_id = _parse_int(body.channel_id, "channel_id")
    delay = max(0.0, body.delay)
    sent = await bot_manager.chat_all(channel_id, message, delay)
    return {"success": True, "sent": sent}


@router.post("/reaction")
async def reaction(body: ReactionBody) -> dict[str, Any]:
    """Cho toàn bộ bot thả reaction vào tin nhắn chỉ định."""
    channel_id = _parse_int(body.channel_id, "channel_id")
    message_id = _parse_int(body.message_id, "message_id")
    reacted = await bot_manager.spam_reaction(channel_id, message_id, body.emoji)
    return {"success": True, "reacted": reacted}


@router.post("/join-server")
async def join_server(body: JoinServerBody) -> dict[str, Any]:
    """Dán link mời; các tài khoản chỉ định cùng vào server."""
    invite = (body.invite or "").strip()
    if not invite:
        raise HTTPException(status_code=400, detail="Thiếu link mời")
    bot_ids = body.bot_ids
    if not bot_ids:
        # Mặc định: tất cả tài khoản đang online
        bot_ids = [bot.bot_id for bot in bot_manager.bots if bot.is_ready()]
    if not bot_ids:
        raise HTTPException(status_code=400, detail="Không có tài khoản online nào")
    result = await bot_manager.join_server(bot_ids, invite)
    return {"success": True, **result}


@router.get("/bots/{bot_id}/readonly-channels")
async def get_readonly_channels(bot_id: str) -> dict[str, Any]:
    """Kênh chỉ đọc mà một tài khoản xem được (shop bot, thông báo, kênh khoá chat)."""
    return {"channels": bot_manager.get_available_read_only_channels(bot_id)}


@router.get("/bots/{bot_id}/channels/{channel_id}/components")
async def get_channel_components(bot_id: str, channel_id: str, limit: int = 15) -> dict[str, Any]:
    """Tin nhắn gần đây trong kênh có button hoặc select menu."""
    messages = await bot_manager.fetch_channel_components(
        bot_id, _parse_int(channel_id, "channel_id"), limit=limit
    )
    return {"messages": messages}


@router.post("/components/interact")
async def interact_component(body: ComponentInteractBody) -> dict[str, Any]:
    """Cho các tài khoản chỉ định bấm nút hoặc chọn option trong một tin nhắn."""
    if not body.bot_ids:
        raise HTTPException(status_code=400, detail="Cần chọn ít nhất một tài khoản")
    if not body.custom_id:
        raise HTTPException(status_code=400, detail="Thiếu custom_id của component")
    channel_id = _parse_int(body.channel_id, "channel_id")
    message_id = _parse_int(body.message_id, "message_id")
    result = await bot_manager.interact_component(
        body.bot_ids,
        channel_id,
        message_id,
        body.custom_id,
        body.values,
    )
    return {"success": True, **result}


@router.post("/components/campaign")
async def run_campaign(body: CampaignBody) -> dict[str, Any]:
    """Chạy chiến dịch (nhiều bước, nhiều vòng); chạy nền và trả ngay."""
    if not body.bot_ids:
        raise HTTPException(status_code=400, detail="Cần chọn ít nhất một tài khoản")
    if not body.steps:
        raise HTTPException(status_code=400, detail="Chiến dịch chưa có bước nào")
    if bot_manager._campaign_running:
        raise HTTPException(status_code=409, detail="Đang có một chiến dịch chạy; hãy dừng trước")
    steps = [step.model_dump() for step in body.steps]
    bot_manager._campaign_task = asyncio.create_task(
        bot_manager.run_campaign(
            body.bot_ids,
            steps,
            max(0.0, body.wait_between),
            max(0, int(body.loops)),
            max(0.0, body.wait_between_loops),
        )
    )
    return {"success": True, "started": True, "loops": body.loops}


@router.post("/components/campaign/stop")
async def stop_campaign() -> dict[str, Any]:
    """Dừng chiến dịch đang chạy."""
    stopped = bot_manager.stop_campaign()
    return {"success": True, "stopped": stopped}


@router.get("/components/campaign/status")
async def campaign_status() -> dict[str, Any]:
    """Trạng thái chiến dịch: đang chạy, số vòng đã xong."""
    return bot_manager.campaign_status()


@router.get("/tokens")
async def get_tokens() -> dict[str, Any]:
    """Trả danh sách token kèm bản đã che."""
    tokens = list(bot_manager.tokens)
    return {
        "count": len(tokens),
        "tokens": [
            {"index": index, "masked": _mask_token(token), "full": token}
            for index, token in enumerate(tokens)
        ],
    }


@router.post("/profile/{bot_id}")
async def edit_profile(
    bot_id: str,
    display_name: str | None = Form(default=None),
    bio: str | None = Form(default=None),
    accent_color: str | None = Form(default=None),
    avatar_clear: str | None = Form(default=None),
    banner_clear: str | None = Form(default=None),
    avatar: UploadFile | None = None,
    banner: UploadFile | None = None,
) -> dict[str, Any]:
    """Sửa profile tài khoản (display_name/bio/accent_color/avatar/banner).

    Dạng multipart/form-data; mọi trường tuỳ chọn.
    """
    fields: dict[str, Any] = {}
    if display_name is not None:
        fields["display_name"] = display_name.strip() or None
    if bio is not None:
        fields["bio"] = bio
    if accent_color:
        try:
            value = int(accent_color.lstrip("#"), 16)
            fields["accent_color"] = value
        except ValueError:
            raise HTTPException(status_code=400, detail="accent_color không hợp lệ")
    if avatar_clear == "1":
        fields["avatar_clear"] = True
    elif avatar is not None:
        data = await avatar.read()
        if data:
            fields["avatar_bytes"] = data
    if banner_clear == "1":
        fields["banner_clear"] = True
    elif banner is not None:
        data = await banner.read()
        if data:
            fields["banner_bytes"] = data

    result = await bot_manager.edit_profile(bot_id, **fields)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "edit_failed")
    return {"success": True, **result}


@router.delete("/tokens/{bot_id}")
async def delete_token(bot_id: str) -> dict[str, Any]:
    """Xoá một tài khoản (dừng bot + bỏ token khỏi file)."""
    ok = await bot_manager.delete_token(bot_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")
    return {"success": True, "count": len(bot_manager.tokens)}


@router.put("/tokens")
async def update_tokens(body: TokensBody) -> dict[str, Any]:
    """Lưu danh sách token mới vào file."""
    bot_manager.save_tokens(body.tokens)
    # Đang chạy thì áp dụng ngay, không bắt người dùng phải START lại
    synced = await bot_manager.sync_running_bots()
    return {"success": True, "count": len(bot_manager.tokens), **synced}


@router.get("/logs")
async def get_logs() -> dict[str, Any]:
    """Trả các log gần đây đang được buffer."""
    return {"logs": log_handler.get_recent_logs()}


@router.delete("/logs")
async def delete_logs() -> dict[str, Any]:
    """Xoá toàn bộ log đang buffer."""
    log_handler.clear_logs()
    return {"success": True}

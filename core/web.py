"""Phục vụ HTML dashboard và WebSocket realtime."""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from core.auth import verify_token
from core.engine import bot_manager
from core.logger import app_logger, log_handler
from core.tunnel import tunnel_manager

router: APIRouter = APIRouter()

AUTH_TIMEOUT_SECONDS = 10.0
MESSAGE_QUEUE_SIZE = 1000


# -----------------------------------------------------------------------------
# WebSocket
# -----------------------------------------------------------------------------
async def _reject(websocket: WebSocket) -> None:
    """Gửi lỗi Unauthorized rồi đóng kết nối."""
    with contextlib.suppress(Exception):
        await websocket.send_json({"type": "error", "message": "Unauthorized"})
    with contextlib.suppress(Exception):
        await websocket.close(code=1008)


def _enqueue(queue: asyncio.Queue[dict[str, Any]], message: dict[str, Any]) -> None:
    """Thêm message vào queue, bỏ qua nếu queue đầy."""
    try:
        queue.put_nowait(message)
    except asyncio.QueueFull:
        app_logger.debug("Queue WebSocket đầy; bỏ qua %s", message.get("type"))


async def _writer_loop(
    websocket: WebSocket, queue: asyncio.Queue[dict[str, Any]]
) -> None:
    """Gửi message từ queue tới client theo đúng thứ tự."""
    while True:
        message = await queue.get()
        try:
            await websocket.send_json(message)
        except Exception:
            return


async def _stream_events(websocket: WebSocket) -> None:
    """Đăng ký listener và đẩy state/log realtime cho tới khi client ngắt."""
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=MESSAGE_QUEUE_SIZE)

    async def on_state(_state: dict[str, Any]) -> None:
        _enqueue(queue, {"type": "state_update", "data": bot_manager.get_summary()})

    async def on_log(entry: dict[str, str]) -> None:
        _enqueue(queue, {"type": "log", "data": entry})

    bot_manager.add_state_listener(on_state)
    log_handler.add_listener(on_log)
    writer_task = asyncio.create_task(_writer_loop(websocket, queue))

    try:
        await websocket.send_json(
            {"type": "state_update", "data": bot_manager.get_summary()}
        )
        await websocket.send_json(
            {"type": "logs_history", "data": log_handler.get_recent_logs()}
        )
        await websocket.send_json(
            {"type": "tunnel_status", "data": tunnel_manager.get_status()}
        )

        while True:
            await websocket.receive()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        app_logger.warning("WebSocket session kết thúc: %s", exc)
    finally:
        writer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await writer_task
        bot_manager.remove_state_listener(on_state)
        log_handler.remove_listener(on_log)


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """Xác thực JWT qua message đầu tiên rồi mở luồng realtime."""
    await websocket.accept()

    try:
        raw = await asyncio.wait_for(
            websocket.receive_json(), timeout=AUTH_TIMEOUT_SECONDS
        )
    except WebSocketDisconnect:
        return
    except (asyncio.TimeoutError, ValueError, json.JSONDecodeError):
        await _reject(websocket)
        return

    token = (
        raw.get("token")
        if isinstance(raw, dict) and raw.get("type") == "auth"
        else None
    )
    if not isinstance(token, str) or verify_token(token) is None:
        await _reject(websocket)
        return

    # Handler log cần event loop để đẩy log từ mọi thread tới dashboard
    log_handler.set_loop(asyncio.get_running_loop())
    app_logger.info("Dashboard kết nối WebSocket.")
    await _stream_events(websocket)


# -----------------------------------------------------------------------------
# Dashboard HTML
# -----------------------------------------------------------------------------
@router.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    """Trả toàn bộ HTML dashboard dưới dạng single-page app."""
    return HTMLResponse(content=get_dashboard_html())


# Alias để dashboard.py đăng ký route trực tiếp
dashboard_route = dashboard


def get_dashboard_html() -> str:
    """Trả HTML/CSS/JS inline của dashboard (không phụ thuộc bên ngoài)."""
    return _DASHBOARD_HTML


_DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>kwishtt</title>
<style>
:root{
  --bg:#1e1f22; --surface:#2b2d31; --surface-2:#313338; --surface-3:#383a40;
  --border:#3f4147; --border-strong:#4e5058;
  --text:#f2f3f5; --text-muted:#b5bac1; --text-faint:#949ba4;
  --accent:#5865f2; --accent-hover:#4752c4; --accent-soft:rgba(88,101,242,.16);
  --green:#23a55a; --yellow:#f0b232; --red:#f23f43; --gray:#80848e;
  --radius:10px; --radius-lg:14px;
  --shadow:0 12px 32px rgba(0,0,0,.5);
  --mono:"Cascadia Code","Fira Code",Consolas,"Courier New",monospace;
}
*{box-sizing:border-box;}
html,body{margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:"Segoe UI",Roboto,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5;}
.hidden{display:none !important;}
.mono{font-family:var(--mono);}
h2{margin:0;font-size:15px;font-weight:600;}
h3{margin:0 0 8px;font-size:16px;}
a{color:var(--accent);}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:4px;}
::selection{background:var(--accent-soft);}
@media (prefers-reduced-motion:reduce){
  *{transition:none !important;animation:none !important;}
}

/* ---------- Icons ---------- */
.ico{display:inline-flex;width:16px;height:16px;flex:0 0 auto;}
.ico svg{width:100%;height:100%;display:block;}

/* ---------- Login ---------- */
.login-screen{position:fixed;inset:0;z-index:100;background:var(--bg);display:flex;align-items:center;justify-content:center;padding:20px;}
.login-card{position:relative;width:100%;max-width:400px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:32px 28px;text-align:center;box-shadow:var(--shadow);}
.login-logo{width:56px;height:56px;margin:0 auto 14px;border-radius:16px;background:var(--accent-soft);border:1px solid var(--accent);display:flex;align-items:center;justify-content:center;color:var(--accent);}
.login-logo .ico{width:28px;height:28px;}
.login-card h1{font-size:20px;margin-bottom:6px;}
.login-sub{color:var(--text-muted);font-size:13px;margin:0 0 22px;}
.login-card .lang-switch{position:absolute;top:14px;right:14px;}
.password-field{position:relative;margin-bottom:6px;}
.password-field input{width:100%;padding-right:44px !important;}
.password-field .icon-btn{position:absolute;right:4px;top:50%;transform:translateY(-50%);}
.login-error{color:var(--red);min-height:18px;font-size:12px;margin:8px 0;}

/* ---------- Buttons & inputs ---------- */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:38px;padding:8px 14px;border-radius:8px;border:1px solid var(--border);background:var(--surface-2);color:var(--text);font:inherit;font-size:13px;font-weight:600;cursor:pointer;transition:background .15s,border-color .15s,color .15s,transform .05s;}
.btn:hover{border-color:var(--border-strong);background:var(--surface-3);}
.btn:active{transform:translateY(1px);}
.btn:disabled{opacity:.5;cursor:not-allowed;}
.btn-block{width:100%;}
.btn-sm{min-height:32px;padding:5px 10px;font-size:12px;}
.btn-primary{background:var(--accent);border-color:var(--accent);color:#fff;}
.btn-primary:hover{background:var(--accent-hover);border-color:var(--accent-hover);}
.btn-success{background:var(--green);border-color:var(--green);color:#fff;}
.btn-success:hover{filter:brightness(1.08);background:var(--green);}
.btn-danger{background:var(--red);border-color:var(--red);color:#fff;}
.btn-danger:hover{filter:brightness(1.08);background:var(--red);}
.btn-ghost{background:transparent;border-color:transparent;color:var(--text-muted);}
.btn-ghost:hover{background:var(--surface-3);border-color:transparent;color:var(--text);}
.icon-btn{display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border-radius:8px;border:1px solid transparent;background:transparent;color:var(--text-muted);cursor:pointer;transition:background .15s,color .15s;}
.icon-btn:hover:not(:disabled){background:var(--surface-3);color:var(--text);}
.icon-btn:disabled{opacity:.45;cursor:not-allowed;}
input[type="text"],input[type="password"],input[type="number"],select,textarea{background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:9px 11px;font:inherit;font-size:13px;outline:none;transition:border-color .15s;}
input:focus,select:focus,textarea:focus{border-color:var(--accent);}
input::placeholder,textarea::placeholder{color:var(--text-faint);}
label{display:flex;flex-direction:column;gap:5px;color:var(--text-muted);font-size:12px;font-weight:600;}

/* ---------- Layout ---------- */
.container{max-width:1440px;margin:0 auto;padding:16px;display:grid;gap:16px;}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:16px;}
.panel-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:12px;}
.count{color:var(--text-faint);font-size:12px;font-weight:600;background:var(--bg);border:1px solid var(--border);border-radius:999px;padding:2px 10px;}

/* ---------- Header ---------- */
.header{display:flex;align-items:center;gap:14px;flex-wrap:wrap;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:10px 16px;}
.logo{display:flex;align-items:center;gap:9px;font-size:16px;font-weight:700;letter-spacing:.3px;}
.logo-mark{width:30px;height:30px;border-radius:9px;background:var(--accent-soft);border:1px solid var(--accent);color:var(--accent);display:flex;align-items:center;justify-content:center;}
.logo-mark .ico{width:17px;height:17px;}
.pill{display:inline-flex;align-items:center;gap:6px;background:var(--bg);border:1px solid var(--border);border-radius:999px;padding:4px 12px;color:var(--text-muted);font-size:12px;}
.pill b{color:var(--text);font-weight:600;font-variant-numeric:tabular-nums;}
.header-right{margin-left:auto;display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
.tunnel{display:flex;align-items:center;gap:6px;background:var(--bg);border:1px solid var(--border);border-radius:999px;padding:3px 4px 3px 12px;max-width:100%;}
.tunnel-url{color:var(--accent);font-family:var(--mono);font-size:12px;max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-decoration:none;}
.tunnel-url:hover{text-decoration:underline;}

/* ---------- Language ---------- */
.lang-switch{display:flex;gap:4px;background:var(--bg);border:1px solid var(--border);border-radius:999px;padding:3px;}
.lang-btn{display:inline-flex;align-items:center;gap:6px;background:transparent;border:none;border-radius:999px;padding:4px 10px;color:var(--text-muted);font:inherit;font-size:12px;font-weight:600;cursor:pointer;transition:background .15s,color .15s;}
.lang-btn:hover{color:var(--text);}
.lang-btn.active{background:var(--accent-soft);color:var(--accent);}
.lang-btn .flag{width:18px;height:12px;border-radius:2px;display:block;flex:0 0 auto;}

/* ---------- Stat cards ---------- */
.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;}
.stat-card{display:flex;align-items:center;gap:12px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:14px 16px;}
.stat-ico{width:38px;height:38px;border-radius:10px;background:var(--bg);border:1px solid var(--border);display:flex;align-items:center;justify-content:center;color:var(--text-muted);}
.stat-ico .ico{width:18px;height:18px;}
.stat-value{font-size:22px;font-weight:700;line-height:1.2;font-variant-numeric:tabular-nums;}
.stat-label{color:var(--text-muted);font-size:12px;font-weight:600;}
.stat-card.online .stat-ico{color:var(--green);border-color:var(--green);background:rgba(35,165,90,.12);}
.stat-card.voice .stat-ico{color:var(--accent);border-color:var(--accent);background:var(--accent-soft);}
.stat-card.errors .stat-ico{color:var(--red);border-color:var(--red);background:rgba(242,63,67,.12);}
.stat-card.errors.has-errors .stat-value{color:var(--red);}

/* ---------- App shell ---------- */
.app{display:grid;grid-template-columns:220px minmax(0,1fr);min-height:100vh;}
.sidenav{position:sticky;top:0;height:100vh;display:flex;flex-direction:column;gap:18px;background:var(--surface);border-right:1px solid var(--border);padding:16px 12px;}
.sidenav .logo{padding:2px 6px;}
.nav-list{display:flex;flex-direction:column;gap:4px;}
.nav-item{display:flex;align-items:center;gap:10px;width:100%;padding:10px 12px;border-radius:8px;border:none;background:transparent;color:var(--text-muted);font:inherit;font-size:14px;font-weight:600;text-align:left;cursor:pointer;transition:background .15s,color .15s;}
.nav-item:hover{background:var(--surface-3);color:var(--text);}
.nav-item.active{background:var(--accent-soft);color:var(--text);}
.nav-item.active .ico{color:var(--accent);}
.nav-badge{margin-left:auto;min-width:22px;padding:1px 7px;border-radius:999px;background:var(--bg);border:1px solid var(--border);color:var(--text-faint);font-size:11px;text-align:center;font-variant-numeric:tabular-nums;}
.nav-badge.empty-badge{background:rgba(240,178,50,.12);border-color:var(--yellow);color:var(--yellow);}
.nav-foot{margin-top:auto;display:flex;flex-direction:column;align-items:flex-start;gap:10px;}
.content{min-width:0;padding:16px;display:flex;flex-direction:column;gap:16px;max-width:1680px;width:100%;}
.view{display:flex;flex-direction:column;gap:16px;}
.view-title{margin:0;font-size:18px;font-weight:700;}
.two-col{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;align-items:start;}
.hint{color:var(--text-faint);font-size:12px;margin:-4px 0 10px;}
.run-dot{width:8px;height:8px;border-radius:50%;background:var(--gray);flex:0 0 auto;}
.run-dot.running{background:var(--green);box-shadow:0 0 0 3px rgba(35,165,90,.2);}
@media (max-width:1000px){
  .two-col{grid-template-columns:1fr;}
}

/* ---------- Quick join ---------- */
.sub-label{color:var(--text-muted);font-size:12px;font-weight:600;margin:14px 0 8px;}
.chip-row{display:flex;flex-wrap:wrap;gap:8px;}
.chip{display:inline-flex;align-items:center;gap:6px;max-width:100%;padding:6px 6px 6px 12px;border-radius:999px;border:1px solid var(--border);background:var(--surface-2);color:var(--text);font:inherit;font-size:12.5px;cursor:pointer;transition:border-color .15s,background .15s;}
.chip:hover{border-color:var(--accent);background:var(--surface-3);}
.chip-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.chip-sub{color:var(--text-faint);}
.chip-x{display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:50%;color:var(--text-faint);}
.chip-x:hover{background:var(--surface);color:var(--text);}
.chip-x .ico{width:12px;height:12px;}
.qj-groups{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px;}
.qj-group{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:10px;}
.qj-guild{font-size:12px;font-weight:700;color:var(--text-muted);margin:0 0 8px 2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.qj-channel{display:flex;align-items:center;gap:8px;width:100%;padding:8px 10px;margin-top:4px;border-radius:8px;border:1px solid transparent;background:transparent;color:var(--text);font:inherit;font-size:13px;text-align:left;cursor:pointer;transition:background .15s,border-color .15s;}
.qj-channel:hover{background:var(--surface-3);border-color:var(--border-strong);}
.qj-channel .ico{width:14px;height:14px;color:var(--text-faint);}
.qj-channel .name{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.qj-channel .meta{color:var(--text-faint);font-size:11.5px;font-variant-numeric:tabular-nums;}
.qj-channel.here{background:rgba(35,165,90,.1);border-color:rgba(35,165,90,.45);}
.qj-channel.here .ico{color:var(--green);}
.qj-here{color:var(--green);font-size:11px;font-weight:700;}
.qj-channel:disabled{opacity:.6;cursor:wait;}
.qj-detail-title{flex:1;min-width:0;}
.qj-detail-title .name{font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.qj-detail-title .sub{color:var(--text-faint);font-size:12px;}
.member-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:8px;}
.member{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:8px;background:var(--bg);border:1px solid var(--border);min-width:0;}
.member .avatar{width:32px;height:32px;}
.member-info{flex:1;min-width:0;}
.member-name{font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.member-sub{color:var(--text-faint);font-size:11.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.member-own{color:var(--accent);font-weight:700;}
.member-icons{display:flex;gap:4px;color:var(--text-faint);}
.member-icons .ico{width:14px;height:14px;}
.member-icons .off{color:var(--red);}
.member-icons .live{color:var(--green);}
.qj-channel.selected{background:var(--accent-soft);border-color:var(--accent);}
.qj-channel.selected .ico{color:var(--accent);}
.qj-cat{color:var(--text-faint);font-size:10.5px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;margin:10px 0 2px 4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.sc-list-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;}
.sc-list-head .sub-label{margin-bottom:8px;}
.sc-list-head input{width:220px;max-width:100%;padding:6px 10px;}
.sc-channels{max-height:340px;overflow-y:auto;padding-right:2px;}
#sc-message{width:100%;height:90px;resize:vertical;}
.sc-actions{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-top:10px;}
.sc-actions .hint{margin:0;}

/* ---------- Onboarding ---------- */
.onboard{display:flex;align-items:center;gap:16px;border-color:var(--yellow);}
.onboard-ico{width:28px !important;height:28px !important;color:var(--yellow);}
.onboard-body{flex:1;min-width:0;}
.onboard-body p{margin:4px 0 0;}

/* ---------- Voice panel ---------- */
.start-card{border-color:var(--accent);}
.start-card .onboard-ico{color:var(--accent);}
.vp-top{display:flex;align-items:flex-end;gap:10px;}
.vp-server{flex:1;max-width:420px;}
#gx-guild{width:100%;font-size:14px;padding:10px 12px;}
.vp-cols{display:grid;grid-template-columns:minmax(260px,2fr) minmax(0,3fr);gap:20px;margin-top:16px;align-items:start;}
.vp-accounts,.vp-channels{min-width:0;}
.vp-toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-top:10px;padding-top:10px;border-top:1px solid var(--border);}
.vp-toolbar .hint{margin:0;}
.vp-tools{display:flex;align-items:center;gap:4px;}
.tool{width:34px;height:34px;border-radius:8px;border:1px solid var(--border);background:var(--surface-2);color:var(--text-muted);display:inline-flex;align-items:center;justify-content:center;cursor:pointer;transition:background .15s,border-color .15s,color .15s;}
.tool:hover:not(:disabled){border-color:var(--border-strong);color:var(--text);}
.tool:disabled{opacity:.4;cursor:not-allowed;}
.tool.on{background:var(--accent-soft);border-color:var(--accent);color:var(--accent);}
.tool.mixed{border-color:var(--yellow);color:var(--yellow);}
.tool .ico{width:15px;height:15px;}
.vp-tools .btn{margin-left:6px;}
.vp-link{margin-top:12px;}
.vp-link input{font-size:13px;padding:9px 11px;}
.gx-acc-sub.err{color:var(--red);}
.gx-account .member-icons{display:flex;gap:3px;color:var(--text-faint);}
.gx-account .member-icons .ico{width:13px;height:13px;}
@media (max-width:900px){
  .vp-cols{grid-template-columns:1fr;}
}
.gx-accounts{display:flex;flex-direction:column;gap:4px;}
.gx-account{display:flex;align-items:center;gap:10px;width:100%;padding:7px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface-2);color:var(--text);font:inherit;text-align:left;cursor:pointer;transition:border-color .15s,background .15s;}
.gx-account:hover{border-color:var(--border-strong);}
.gx-account.checked{border-color:var(--accent);background:var(--accent-soft);}
.gx-check{width:16px;height:16px;border-radius:4px;border:1.5px solid var(--border-strong);display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;color:#fff;}
.gx-account.checked .gx-check{background:var(--accent);border-color:var(--accent);}
.gx-check .ico{width:11px;height:11px;}
.gx-account .avatar{width:26px;height:26px;}
.gx-acc-info{flex:1;min-width:0;}
.gx-acc-name{font-size:13px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.gx-acc-sub{font-size:11.5px;color:var(--text-faint);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.gx-acc-sub.in{color:var(--green);}
.gx-acc-actions{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-top:8px;}
.gx-acc-actions .hint{margin:0;}
.gx-channels{display:flex;flex-direction:column;gap:8px;}
.gx-channel{border:1px solid var(--border);border-radius:var(--radius);background:var(--surface-2);}
.gx-channel.here{border-color:rgba(35,165,90,.55);background:rgba(35,165,90,.07);}
.gx-head{display:flex;align-items:center;gap:8px;padding:8px 8px 8px 10px;}
.gx-head > .ico{width:15px;height:15px;color:var(--text-faint);}
.gx-channel.here .gx-head > .ico{color:var(--green);}
.gx-name{flex:1;min-width:0;font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.gx-count{color:var(--text-faint);font-size:11.5px;font-variant-numeric:tabular-nums;}
.gx-head .btn{min-height:28px;padding:3px 10px;font-size:12px;}
.gx-members{display:flex;flex-direction:column;gap:2px;padding:0 8px 8px 30px;}
.gx-member{display:flex;align-items:center;gap:8px;min-width:0;padding:3px 4px;border-radius:6px;}
.gx-member .avatar{width:22px;height:22px;}
.gx-member .avatar-fallback .ico{width:12px;height:12px;}
.gx-member-name{flex:1;min-width:0;font-size:12.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.gx-member.own .gx-member-name{color:var(--accent);font-weight:600;}
.gx-tag{font-size:10px;font-weight:700;color:var(--accent);background:var(--accent-soft);border-radius:999px;padding:1px 6px;margin-left:4px;}
.gx-member .member-icons .ico{width:13px;height:13px;}
/* ---------- Join ---------- */
.join-row{display:flex;gap:10px;}
.join-row input{flex:1;min-width:0;font-size:14px;padding:11px 12px;}
.join-row .btn{min-width:96px;}

/* ---------- Controls ---------- */
.control-row{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;margin-bottom:10px;}
.control-row:last-child{margin-bottom:0;}
.control-row .grow{flex:1;min-width:150px;}
.control-row input,.control-row select{width:100%;}
.mode-row{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
.bulk-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;}
.bulk-grid .btn{min-height:36px;font-size:12px;}

/* ---------- Log console ---------- */
.log-console{height:calc(100vh - 190px);min-height:300px;overflow-y:auto;background:#0f1014;border:1px solid var(--border);border-radius:var(--radius);padding:10px;font-family:var(--mono);font-size:12px;line-height:1.55;}
.log-line{white-space:pre-wrap;word-break:break-word;}
.log-info{color:var(--text);}
.log-warning{color:var(--yellow);}
.log-error{color:var(--red);}
.log-debug{color:var(--text-faint);}

/* ---------- Token manager ---------- */
#token-textarea{width:100%;height:320px;resize:vertical;font-family:var(--mono);font-size:12px;margin:4px 0 12px;}
.token-actions{display:flex;align-items:center;justify-content:space-between;gap:10px;}
.token-actions .hint{margin:0;}

/* ---------- Channel picker & chat ---------- */
.channel-list{display:grid;gap:6px;max-height:320px;overflow-y:auto;margin:4px 0 16px;}
.channel-item{display:flex;flex-direction:column;gap:2px;width:100%;text-align:left;background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:10px 12px;color:var(--text);font:inherit;font-size:13px;cursor:pointer;transition:border-color .15s,background .15s;}
.channel-item:hover{border-color:var(--accent);background:var(--surface-3);}
.channel-meta{color:var(--text-faint);font-size:11.5px;}
.muted{color:var(--text-muted);font-size:13px;}
.modal label{margin-bottom:12px;}
.modal label textarea,.modal label input{width:100%;}
#chatall-message{width:100%;height:70px;resize:vertical;font-size:13px;}

/* ---------- Message reaction ---------- */
.reaction-emoji{display:flex;align-items:flex-end;gap:10px;margin-bottom:10px;}
.reaction-emoji label{flex:1;min-width:0;}
.emoji-preview{width:54px;height:42px;display:flex;align-items:center;justify-content:center;background:var(--bg);border:1px solid var(--border);border-radius:8px;font-size:20px;overflow:hidden;white-space:nowrap;flex:0 0 auto;}
.emoji-picker{display:grid;grid-template-columns:repeat(8,1fr);gap:6px;background:var(--bg);border:1px solid var(--border);border-radius:10px;padding:10px;margin-bottom:10px;max-height:190px;overflow-y:auto;}
.emoji-option{display:flex;align-items:center;justify-content:center;font-size:20px;line-height:1;height:36px;background:transparent;border:1px solid transparent;border-radius:8px;cursor:pointer;}
.emoji-option:hover,.emoji-option:focus-visible{background:var(--surface-3);border-color:var(--border-strong);}
.spinner{width:14px;height:14px;border-radius:50%;border:2px solid rgba(255,255,255,.35);border-top-color:#fff;animation:spin .7s linear infinite;display:inline-block;flex:0 0 auto;}
@keyframes spin{to{transform:rotate(360deg);}}
input.invalid{border-color:var(--red) !important;}

/* ---------- Toasts ---------- */
.toast-container{position:fixed;top:16px;right:16px;z-index:300;display:flex;flex-direction:column;gap:8px;width:min(360px,calc(100vw - 32px));}
.toast{display:flex;align-items:flex-start;gap:10px;background:var(--surface);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:10px;padding:12px 14px;box-shadow:var(--shadow);font-size:13px;cursor:pointer;animation:toast-in .18s ease-out;}
.toast.success{border-left-color:var(--green);}
.toast.error{border-left-color:var(--red);}
.toast .ico{width:16px;height:16px;margin-top:1px;}
.toast.success .ico{color:var(--green);}
.toast.error .ico{color:var(--red);}
.toast.info .ico{color:var(--accent);}
@keyframes toast-in{from{opacity:0;transform:translateY(-8px);}to{opacity:1;transform:none;}}

/* ---------- Modal ---------- */
.modal-backdrop{position:fixed;inset:0;z-index:200;background:rgba(0,0,0,.65);display:flex;align-items:center;justify-content:center;padding:20px;}
.modal{width:100%;max-width:420px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:20px;box-shadow:var(--shadow);}
.modal p{color:var(--text-muted);font-size:13px;margin:0 0 18px;}
.modal-actions{display:flex;justify-content:flex-end;gap:10px;}

/* ---------- Responsive ---------- */
@media (max-width:860px){
  .app{grid-template-columns:minmax(0,1fr);}
  .sidenav{min-width:0;position:sticky;top:0;z-index:50;height:auto;flex-direction:row;flex-wrap:wrap;align-items:center;gap:8px;padding:8px 10px;border-right:none;border-bottom:1px solid var(--border);}
  .nav-list{flex-direction:row;overflow-x:auto;order:3;width:100%;}
  .nav-item{width:auto;white-space:nowrap;padding:8px 10px;}
  .nav-foot{margin:0 0 0 auto;flex-direction:row;align-items:center;}
  .content{padding:10px;gap:12px;}
  .header{gap:10px;}
  .header-right{margin-left:0;width:100%;}
  .tunnel-url{max-width:150px;}
  .stats-grid{grid-template-columns:repeat(2,1fr);}
  .bots-grid{grid-template-columns:1fr;}
  .toggle-btn{min-height:44px;}
  .btn{min-height:44px;}
  .bulk-grid{grid-template-columns:repeat(2,1fr);}
  .emoji-picker{grid-template-columns:repeat(6,1fr);}
  .reaction-emoji{flex-wrap:wrap;}
  .toast-container{top:auto;bottom:16px;right:10px;left:10px;width:auto;}
}
</style>
</head>
<body>

<div id="toast-container" class="toast-container" aria-live="polite" aria-atomic="false"></div>

<div id="confirm-modal" class="modal-backdrop hidden" role="dialog" aria-modal="true" aria-labelledby="confirm-title" aria-describedby="confirm-message">
  <div class="modal">
    <h3 id="confirm-title">Confirm action</h3>
    <p id="confirm-message"></p>
    <div class="modal-actions">
      <button id="confirm-cancel" class="btn" type="button">Cancel</button>
      <button id="confirm-ok" class="btn btn-danger" type="button">Confirm</button>
    </div>
  </div>
</div>

<div id="login-screen" class="login-screen">
  <div class="login-card">
    <div class="lang-switch">
      <button class="lang-btn" data-lang="en" type="button" aria-pressed="false" data-i18n-title="lang_english">
        <span class="flag" data-flag="en"></span><span>EN</span>
      </button>
      <button class="lang-btn" data-lang="vi" type="button" aria-pressed="false" data-i18n-title="lang_vietnamese">
        <span class="flag" data-flag="vi"></span><span>VI</span>
      </button>
    </div>
    <div class="login-logo"><span class="ico" data-icon="bot"></span></div>
    <h1>kwishtt</h1>
    <p class="login-sub" data-i18n="login_sub">Sign in to manage your voice system</p>
    <div class="password-field">
      <input id="password-input" type="password" data-i18n-ph="ph_password" placeholder="Dashboard password" autocomplete="current-password" aria-label="Password">
      <button id="password-toggle" class="icon-btn" type="button" data-i18n-title="title_show_password" aria-label="Show password">
        <span class="ico" data-icon="eye"></span>
      </button>
    </div>
    <div class="login-error" id="login-error" role="alert"></div>
    <button id="login-btn" class="btn btn-primary btn-block" type="button" data-i18n="btn_login">Login</button>
  </div>
</div>

<div id="dashboard" class="hidden">
  <div class="app">

    <nav class="sidenav" aria-label="Navigation">
      <span class="logo"><span class="logo-mark"><span class="ico" data-icon="bot"></span></span>kwishtt</span>
      <div class="nav-list">
        <button class="nav-item" type="button" data-view="voice">
          <span class="ico" data-icon="volume"></span><span data-i18n="nav_voice">Voice</span>
        </button>
        <button class="nav-item" type="button" data-view="messages">
          <span class="ico" data-icon="message"></span><span data-i18n="nav_messages">Messages</span>
        </button>
        <button class="nav-item" type="button" data-view="tokens">
          <span class="ico" data-icon="key"></span><span data-i18n="nav_tokens">Tokens</span>
          <span class="nav-badge" id="nav-token-count">0</span>
        </button>
        <button class="nav-item" type="button" data-view="advanced">
          <span class="ico" data-icon="settings"></span><span data-i18n="nav_advanced">Advanced</span>
        </button>
      </div>
      <div class="nav-foot">
        <div class="lang-switch">
          <button class="lang-btn" data-lang="en" type="button" aria-pressed="false" data-i18n-title="lang_english">
            <span class="flag" data-flag="en"></span><span>EN</span>
          </button>
          <button class="lang-btn" data-lang="vi" type="button" aria-pressed="false" data-i18n-title="lang_vietnamese">
            <span class="flag" data-flag="vi"></span><span>VI</span>
          </button>
        </div>
        <button id="logout-btn" class="btn btn-ghost btn-sm" type="button">
          <span class="ico" data-icon="logout"></span><span data-i18n="btn_logout">Logout</span>
        </button>
      </div>
    </nav>

    <div class="content">
      <header class="header">
        <h1 class="view-title" id="view-title">Voice</h1>
        <span class="pill"><span class="run-dot" id="run-dot"></span><b id="stat-mode">—</b><span id="uptime-wrap" class="hidden"> · <b id="stat-uptime">00:00:00</b></span></span>
        <span class="pill hidden" id="voice-pill"><span class="ico" data-icon="volume"></span><b id="voice-pill-text"></b></span>
        <div class="header-right">
          <span class="tunnel hidden" id="tunnel-wrap">
            <span class="ico" data-icon="globe" title="Tunnel"></span>
            <a id="tunnel-url" class="tunnel-url" href="#" target="_blank" rel="noopener">-</a>
            <button id="copy-tunnel" class="icon-btn" type="button" disabled data-i18n-title="title_copy_tunnel" aria-label="Copy tunnel URL">
              <span class="ico" data-icon="copy"></span>
            </button>
          </span>
          <button id="stop-btn" class="btn btn-danger btn-sm hidden" type="button">
            <span class="ico" data-icon="stop"></span><span data-i18n="btn_stop_all">Stop all</span>
          </button>
        </div>
      </header>

      <!-- ===== Voice (trang chính): chọn server → tài khoản → kênh ===== -->
      <section class="view" data-view="voice">

        <section class="panel onboard hidden" id="onboard">
          <span class="ico onboard-ico" data-icon="key"></span>
          <div class="onboard-body">
            <h2 data-i18n="onboard_title">Add a token to get started</h2>
            <p class="muted" data-i18n="onboard_desc">Each token is one Discord account. Paste them on the Tokens page, then come back here to pick a channel.</p>
          </div>
          <button class="btn btn-primary" type="button" data-goto="tokens" data-i18n="btn_go_tokens">Add tokens</button>
        </section>

        <section class="panel onboard start-card hidden" id="start-card">
          <span class="ico onboard-ico" data-icon="volume"></span>
          <div class="onboard-body">
            <h2 data-i18n="start_title">Accounts are offline</h2>
            <p class="muted" data-i18n="start_desc">Log the accounts in first. Then pick a server, the accounts and a channel.</p>
          </div>
          <button id="start-all-btn" class="btn btn-primary" type="button">
            <span class="ico" data-icon="play"></span><span data-i18n="btn_start_all">Log in all accounts</span>
          </button>
        </section>

        <section class="panel voice-panel hidden" id="voice-panel">
          <div class="vp-top">
            <label class="vp-server"><span data-i18n="gx_step_server">Server</span>
              <select id="gx-guild"></select>
            </label>
            <button id="qj-refresh" class="icon-btn" type="button" data-i18n-title="btn_refresh" aria-label="Refresh">
              <span class="ico" data-icon="refresh"></span>
            </button>
          </div>
          <div class="vp-cols">
            <div class="vp-accounts">
              <div class="panel-head">
                <h2 data-i18n="accounts_title">Accounts</h2>
                <button class="link-btn" id="gx-select-all" type="button" data-i18n="gx_select_all">Select all</button>
              </div>
              <div id="gx-accounts" class="gx-accounts"></div>
              <div class="vp-toolbar">
                <span class="hint" id="gx-selected-hint"></span>
                <div class="vp-tools">
                  <button class="tool" type="button" data-sel-field="mute" data-i18n-title="all_mute" aria-label="Mute"><span class="ico" data-icon="micOff"></span></button>
                  <button class="tool" type="button" data-sel-field="deaf" data-i18n-title="all_deaf" aria-label="Deafen"><span class="ico" data-icon="deafOff"></span></button>
                  <button class="tool" type="button" data-sel-field="video" data-i18n-title="all_video" aria-label="Camera"><span class="ico" data-icon="cam"></span></button>
                  <button class="tool" type="button" data-sel-field="stream" data-i18n-title="all_stream" aria-label="Stream"><span class="ico" data-icon="screen"></span></button>
                  <button id="gx-chat" class="btn btn-sm" type="button" disabled>
                    <span class="ico" data-icon="message"></span><span data-i18n="btn_chat">Chat</span>
                  </button>
                </div>
              </div>
            </div>
            <div class="vp-channels">
              <div class="panel-head"><h2 data-i18n="vp_channels_title">Voice channels</h2></div>
              <div id="qj-channels" class="gx-channels"></div>
              <div class="join-row vp-link">
                <input id="qj-input" type="text" autocomplete="off" data-i18n-ph="ph_quick_link" placeholder="Paste a channel link or ID" aria-label="Channel link">
                <button id="qj-join" class="btn" type="button"><span data-i18n="btn_enter">Join</span></button>
              </div>
            </div>
          </div>
        </section>
      </section>

      <!-- ===== Messages ===== -->
      <section class="view hidden" data-view="messages">
        <section class="panel">
          <div class="panel-head">
            <h2 data-i18n="sc_title">Send a message</h2>
            <button id="sc-refresh" class="btn btn-ghost btn-sm" type="button">
              <span class="ico" data-icon="refresh"></span><span data-i18n="btn_refresh">Refresh</span>
            </button>
          </div>
          <div class="control-row">
            <label><span data-i18n="lbl_account">Account</span>
              <select id="sc-bot" style="min-width:200px"></select>
            </label>
            <label class="grow"><span data-i18n="lbl_text_link">Channel link or ID</span>
              <input id="sc-channel" type="text" autocomplete="off" data-i18n-ph="ph_quick_link" placeholder="Paste a channel link or ID">
            </label>
          </div>
          <div id="sc-recent-wrap" class="hidden">
            <div class="sub-label" data-i18n="recent_title">Recent</div>
            <div id="sc-recent" class="chip-row"></div>
          </div>
          <div class="sc-list-head">
            <div class="sub-label" data-i18n="sc_channels_title">Or pick a channel</div>
            <input id="sc-filter" type="text" autocomplete="off" data-i18n-ph="ph_filter" placeholder="Filter channels...">
          </div>
          <div id="sc-channels" class="qj-groups sc-channels"></div>
          <label style="margin-top:14px"><span data-i18n="lbl_message">Message</span>
            <textarea id="sc-message" data-i18n-ph="ph_message" placeholder="Message content..."></textarea>
          </label>
          <div class="sc-actions">
            <span class="hint" data-i18n="hint_ctrl_enter">Ctrl + Enter to send</span>
            <button id="sc-send" class="btn btn-primary" type="button">
              <span class="ico" data-icon="send"></span><span data-i18n="btn_send">Send</span>
            </button>
          </div>
        </section>
      </section>

      <!-- ===== Tokens ===== -->
      <section class="view hidden" data-view="tokens">
        <section class="panel">
          <div class="panel-head">
            <h2 data-i18n="token_title">Token Manager</h2>
            <span class="count" id="token-count">Managing 0 tokens</span>
          </div>
          <p class="hint" data-i18n="hint_tokens">One token per line. Quotes, "Authorization:" and duplicate lines are removed automatically when you save.</p>
          <textarea id="token-textarea" spellcheck="false" data-i18n-ph="ph_tokens" placeholder="One token per line..."></textarea>
          <div class="token-actions">
            <span class="hint" id="token-lines">0</span>
            <button id="save-tokens" class="btn btn-primary" type="button">
              <span class="ico" data-icon="save"></span><span data-i18n="btn_save_tokens">Save Tokens</span>
            </button>
          </div>
        </section>
      </section>

      <!-- ===== Advanced ===== -->
      <section class="view hidden" data-view="advanced">
        <div class="two-col">
          <section class="panel">
            <div class="panel-head"><h2 data-i18n="adv_start_title">Advanced start</h2></div>
            <p class="hint" data-i18n="hint_adv">Use this to spread accounts over several channels or for Auto-Room. For a single channel, the Voice page is simpler.</p>
            <div class="control-row mode-row">
              <label><span data-i18n="lbl_mode">Mode</span>
                <select id="mode-select">
                  <option value="normal" data-i18n="opt_normal">Normal</option>
                  <option value="auto_room" data-i18n="opt_auto_room">Auto-Room</option>
                </select>
              </label>
              <label><span data-i18n="lbl_delay">Delay between logins (s)</span>
                <input id="delay-input" type="number" value="5" min="0" step="0.5">
              </label>
            </div>
            <p class="hint" id="mode-hint" data-i18n="hint_mode_normal">Tokens are spread evenly across the channels below.</p>
            <div class="control-row">
              <label class="grow"><span data-i18n="lbl_channel_ids">Channel IDs</span>
                <input id="channel-ids" type="text" data-i18n-ph="ph_channel_ids" placeholder="e.g. 123456789 987654321">
              </label>
              <label id="leader-wrap" class="hidden"><span data-i18n="lbl_leader">Leaders</span>
                <input id="leader-count" type="number" value="5" min="1" style="width:90px">
              </label>
            </div>
            <div class="control-row">
              <button id="start-btn" class="btn btn-success grow" type="button">
                <span class="ico" data-icon="play"></span><span data-i18n="btn_start">START</span>
              </button>
            </div>
          </section>

          <section class="panel">
            <div class="panel-head"><h2 data-i18n="rename_title">Rename all accounts</h2></div>
            <div class="control-row">
              <label class="grow"><span data-i18n="lbl_rename">New nickname</span>
                <input id="rename-input" type="text" data-i18n-ph="ph_rename" placeholder="Enter nickname (reset = default)">
              </label>
              <button id="rename-all-btn" class="btn" type="button">
                <span class="ico" data-icon="user"></span><span data-i18n="btn_rename_all">Rename All</span>
              </button>
            </div>
          </section>

          <section class="panel">
            <div class="panel-head"><h2 data-i18n="chat_all_title">Chat All</h2></div>
            <div class="control-row">
              <label class="grow"><span data-i18n="lbl_channel_id">Channel ID</span>
                <input id="chatall-channel" type="text" data-i18n-ph="ph_text_channel" placeholder="Text channel ID">
              </label>
              <label><span data-i18n="lbl_delay_short">Delay (s)</span>
                <input id="chatall-delay" type="number" value="1" min="0" step="0.1" style="width:110px">
              </label>
            </div>
            <div class="control-row">
              <label class="grow"><span data-i18n="lbl_message">Message</span>
                <textarea id="chatall-message" data-i18n-ph="ph_message" placeholder="Message content..."></textarea>
              </label>
            </div>
            <div class="control-row">
              <button id="chatall-send" class="btn btn-primary btn-block" type="button">
                <span class="ico" data-icon="send"></span><span data-i18n="btn_send_all">Send to All</span>
              </button>
            </div>
          </section>

          <section class="panel">
            <div class="panel-head"><h2 data-i18n="reaction_title">Discord Message Reaction</h2></div>
            <div class="control-row">
              <label class="grow"><span data-i18n="lbl_channel_id">Channel ID</span>
                <input id="reaction-channel" type="text" inputmode="numeric" autocomplete="off" data-i18n-ph="ph_snowflake" placeholder="e.g. 123456789012345678">
              </label>
              <label class="grow"><span data-i18n="lbl_message_id">Message ID</span>
                <input id="reaction-message" type="text" inputmode="numeric" autocomplete="off" data-i18n-ph="ph_snowflake" placeholder="e.g. 123456789012345678">
              </label>
            </div>
            <div class="reaction-emoji">
              <label><span data-i18n="lbl_emoji">Emoji</span>
                <input id="reaction-emoji-input" type="text" autocomplete="off" data-i18n-ph="ph_emoji" placeholder="Unicode emoji, :name: or name:id">
              </label>
              <div class="emoji-preview" id="reaction-preview" aria-live="polite">?</div>
              <button id="reaction-emoji-toggle" class="icon-btn" type="button" aria-expanded="false" data-i18n-title="title_choose_emoji" aria-label="Choose emoji">
                <span class="ico" data-icon="smile"></span>
              </button>
            </div>
            <div id="reaction-picker" class="emoji-picker hidden" role="listbox" aria-label="Emoji picker"></div>
            <div class="control-row">
              <button id="reaction-send" class="btn btn-primary btn-block" type="button">
                <span class="spinner hidden" id="reaction-spinner"></span>
                <span class="ico" data-icon="send"></span>
                <span data-i18n="btn_send_reaction">Send Reaction</span>
              </button>
            </div>
          </section>
        </div>

        <section class="panel">
          <div class="panel-head">
            <h2 data-i18n="log_title">Log Console</h2>
            <button id="clear-logs" class="btn btn-ghost btn-sm" type="button">
              <span class="ico" data-icon="trash"></span><span data-i18n="btn_clear_logs">Clear</span>
            </button>
          </div>
          <div id="log-console" class="log-console" role="log" aria-live="polite"></div>
        </section>
      </section>

    </div>
  </div>

</div>

<script>
(function(){
  'use strict';

  /* ---------- SVG icons (no emoji) ---------- */
  var SVG_OPEN = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">';
  function svg(inner){ return SVG_OPEN + inner + '</svg>'; }
  var ICONS = {
    bot: svg('<rect x="4" y="8" width="16" height="12" rx="3"/><path d="M12 8V4"/><circle cx="12" cy="3" r="1"/><line x1="9" x2="9.01" y1="13" y2="13"/><line x1="15" x2="15.01" y1="13" y2="13"/>'),
    settings: svg('<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M4.9 4.9l2.1 2.1M17 17l2.1 2.1M2 12h3M19 12h3M4.9 19.1 7 17M17 7l2.1-2.1"/>'),
    tick: svg('<polyline points="20 6 9 17 4 12"/>'),
    refresh: svg('<path d="M21 12a9 9 0 1 1-2.64-6.36L21 8"/><path d="M21 3v5h-5"/>'),
    x: svg('<path d="M18 6 6 18"/><path d="m6 6 12 12"/>'),
    home: svg('<path d="m3 10 9-7 9 7v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><path d="M9 22V12h6v10"/>'),
    server: svg('<rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><line x1="6" x2="6.01" y1="6" y2="6"/><line x1="6" x2="6.01" y1="18" y2="18"/>'),
    wifi: svg('<path d="M5 12.55a11 11 0 0 1 14.08 0"/><path d="M1.42 9a16 16 0 0 1 21.16 0"/><path d="M8.53 16.11a6 6 0 0 1 6.95 0"/><line x1="12" x2="12.01" y1="20" y2="20"/>'),
    volume: svg('<polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/>'),
    alert: svg('<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" x2="12" y1="9" y2="13"/><line x1="12" x2="12.01" y1="17" y2="17"/>'),
    play: svg('<polygon points="6 3 20 12 6 21 6 3"/>'),
    stop: svg('<rect x="5" y="5" width="14" height="14" rx="2"/>'),
    mic: svg('<rect x="9" y="2" width="6" height="12" rx="3"/><path d="M5 10a7 7 0 0 0 14 0"/><line x1="12" y1="19" x2="12" y2="22"/>'),
    micOff: svg('<line x1="2" y1="2" x2="22" y2="22"/><path d="M9 9v3a3 3 0 0 0 4.5 2.6"/><path d="M15 9.3V5a3 3 0 0 0-5.7-1.3"/><path d="M5 10a7 7 0 0 0 10.7 6"/><path d="M19 10v2a7 7 0 0 1-.6 2.8"/><line x1="12" y1="19" x2="12" y2="22"/>'),
    deaf: svg('<path d="M3 14v-3a9 9 0 0 1 18 0v3"/><path d="M21 14v3a2 2 0 0 1-2 2h-1v-7h1a2 2 0 0 1 2 2Z"/><path d="M3 14v3a2 2 0 0 0 2 2h1v-7H5a2 2 0 0 0-2 2Z"/>'),
    deafOff: svg('<line x1="2" y1="2" x2="22" y2="22"/><path d="M3 14v-3a9 9 0 0 1 13.6-7.6"/><path d="M21 11v0"/><path d="M21 14v3a2 2 0 0 1-1.2 1.8"/><path d="M3 14v3a2 2 0 0 0 2 2h1v-7H5a2 2 0 0 0-2 2Z"/>'),
    cam: svg('<path d="M14.5 4h-5L7 7H4a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-3l-2.5-3z"/><circle cx="12" cy="13" r="3"/>'),
    camOff: svg('<line x1="2" y1="2" x2="22" y2="22"/><path d="M9.5 4h5L17 7h3a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H7"/><path d="M4 7h-.5A1.5 1.5 0 0 0 2 8.5V18a2 2 0 0 0 2 2h11"/><circle cx="12" cy="13" r="3"/>'),
    copy: svg('<rect width="14" height="14" x="8" y="8" rx="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/>'),
    logout: svg('<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" x2="9" y1="12" y2="12"/>'),
    trash: svg('<path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>'),
    chevron: svg('<path d="m6 9 6 6 6-6"/>'),
    user: svg('<path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>'),
    activity: svg('<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>'),
    hash: svg('<line x1="4" x2="20" y1="9" y2="9"/><line x1="4" x2="20" y1="15" y2="15"/><line x1="10" x2="8" y1="3" y2="21"/><line x1="16" x2="14" y1="3" y2="21"/>'),
    globe: svg('<circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/>'),
    eye: svg('<path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/>'),
    eyeOff: svg('<path d="M9.88 9.88a3 3 0 1 0 4.24 4.24"/><path d="M10.73 5.08A10.4 10.4 0 0 1 12 5c7 0 10 7 10 7a13.2 13.2 0 0 1-1.67 2.68"/><path d="M6.61 6.61A13.5 13.5 0 0 0 2 12s3 7 10 7a9.7 9.7 0 0 0 5.39-1.61"/><line x1="2" y1="2" x2="22" y2="22"/>'),
    save: svg('<path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/>'),
    check: svg('<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>'),
    info: svg('<circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="16" y2="12"/><line x1="12" x2="12.01" y1="8" y2="8"/>'),
    key: svg('<circle cx="7.5" cy="15.5" r="5.5"/><path d="m21 2-9.6 9.6"/><path d="m15.5 7.5 3 3L22 7l-3-3"/>'),
    move: svg('<path d="M3 5v14"/><path d="M21 12H7"/><path d="m15 18 6-6-6-6"/>'),
    message: svg('<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>'),
    send: svg('<line x1="22" x2="11" y1="2" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/>'),
    smile: svg('<circle cx="12" cy="12" r="10"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><line x1="9" x2="9.01" y1="9" y2="9"/><line x1="15" x2="15.01" y1="9" y2="9"/>'),
    screen: svg('<rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" x2="16" y1="21" y2="21"/><line x1="12" x2="12" y1="17" y2="21"/><path d="m10 8 4 2.5-4 2.5z"/>'),
    screenOff: svg('<line x1="2" y1="2" x2="22" y2="22"/><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" x2="16" y1="21" y2="21"/><line x1="12" x2="12" y1="17" y2="21"/>')
  };
  var FLAGS = {
    en: '<svg class="flag" viewBox="0 0 60 40" aria-hidden="true"><rect width="60" height="40" fill="#012169"/><path d="M0,0 L60,40 M60,0 L0,40" stroke="#ffffff" stroke-width="8"/><path d="M0,0 L60,40 M60,0 L0,40" stroke="#c8102e" stroke-width="4"/><path d="M30,0 L30,40 M0,20 L60,20" stroke="#ffffff" stroke-width="14"/><path d="M30,0 L30,40 M0,20 L60,20" stroke="#c8102e" stroke-width="8"/></svg>',
    vi: '<svg class="flag" viewBox="0 0 60 40" aria-hidden="true"><rect width="60" height="40" fill="#da251d"/><polygon points="30,10 32.35,16.76 39.51,16.91 33.80,21.24 35.88,28.09 30,24 24.12,28.09 26.20,21.24 20.49,16.91 27.65,16.76" fill="#ffdd00"/></svg>'
  };
  function icon(name){
    return '<span class="ico">' + (ICONS[name] || '') + '</span>';
  }
  function injectAssets(root){
    (root || document).querySelectorAll('[data-icon]').forEach(function(el){
      el.innerHTML = ICONS[el.getAttribute('data-icon')] || '';
    });
    (root || document).querySelectorAll('[data-flag]').forEach(function(el){
      el.innerHTML = FLAGS[el.getAttribute('data-flag')] || '';
    });
  }

  /* ---------- i18n ---------- */
  var I18N = {
    en: {
      login_sub: 'Sign in to manage your voice system',
      ph_password: 'Dashboard password',
      btn_login: 'Login',
      title_show_password: 'Show password',
      title_hide_password: 'Hide password',
      lang_english: 'English',
      lang_vietnamese: 'Tiếng Việt',
      err_wrong_password: 'Incorrect password.',
      err_enter_password: 'Please enter a password.',
      err_connect: 'Cannot connect to the server.',
      err_session_expired: 'Session expired, please log in again.',
      stats_mode: 'Mode', stats_uptime: 'Uptime', stats_tokens: 'Tokens',
      title_copy_tunnel: 'Copy tunnel URL', btn_copy: 'Copy', title_logout: 'Logout', btn_logout: 'Logout',
      stat_total: 'Total Bots', stat_online: 'Online', stat_in_voice: 'In Voice', stat_errors: 'Errors',
      bots_title: 'Bots', no_bots: 'No bots yet',
      controls_title: 'Controls',
      lbl_mode: 'Mode', opt_normal: 'Normal', opt_auto_room: 'Auto-Room',
      lbl_channel_ids: 'Channel IDs', ph_channel_ids: 'e.g. 123456789 987654321',
      lbl_delay: 'Delay between logins (s)', lbl_leader: 'Leaders',
      btn_start: 'START', btn_stop: 'STOP',
      btn_mute_all: 'Mute All', btn_unmute_all: 'Unmute All', btn_deaf_all: 'Deaf All',
      btn_undeaf_all: 'Undeaf All', btn_cam_on: 'Cam On', btn_cam_off: 'Cam Off',
      btn_stream_on: 'Stream On', btn_stream_off: 'Stream Off',
      lbl_stream: 'Stream', title_toggle_stream: 'Toggle stream',
      lbl_rename: 'New nickname', ph_rename: 'Enter nickname (reset = default)', btn_rename_all: 'Rename All',
      btn_move: 'Move', btn_chat: 'Chat',
      move_title: 'Move to voice channel', move_loading: 'Loading channels...', move_empty: 'No voice channels available.',
      chat_title: 'Send message', lbl_channel_id: 'Channel ID', ph_text_channel: 'Text channel ID',
      lbl_message: 'Message', ph_message: 'Message content...', btn_send: 'Send',
      chat_all_title: 'Chat All', btn_send_all: 'Send to All',
      reaction_title: 'Discord Message Reaction',
      lbl_message_id: 'Message ID', ph_snowflake: 'e.g. 123456789012345678',
      lbl_emoji: 'Emoji', ph_emoji: 'Unicode emoji, :name: or name:id',
      btn_send_reaction: 'Send Reaction', title_choose_emoji: 'Choose emoji',
      err_channel_id_invalid: 'Channel ID must be 17–20 digits.',
      err_enter_message_id: 'Message ID must be 17–20 digits.',
      err_emoji_empty: 'Choose or enter an emoji.',
      msg_reaction_sent: 'Successfully reacted with {n} bot(s).',
      msg_reaction_none: 'No bots reacted. Check the IDs and bot permissions.',
      log_title: 'Log Console', btn_clear_logs: 'Clear',
      token_title: 'Token Manager', token_count: 'Managing {n} tokens',
      btn_save_tokens: 'Save Tokens', ph_tokens: 'One token per line...',
      lbl_mic: 'Mic', lbl_deaf: 'Deaf', lbl_cam: 'Cam',
      title_toggle_mute: 'Toggle microphone', title_toggle_deaf: 'Toggle deafen', title_toggle_cam: 'Toggle camera',
      status_voice: 'VOICE', status_ready: 'READY', status_error: 'ERROR', status_connecting: 'CONNECTING',
      mode_idle: 'idle', mode_normal: 'normal', mode_auto_room: 'auto_room',
      msg_enter_channel: 'Enter at least one channel ID.',
      err_enter_nickname: 'Enter a new nickname.',
      err_request_failed: 'Request failed. Please try again.',
      err_server_error: 'Internal Server Error. Please try again.',
      msg_started_normal: 'Started normal mode with {n} channel(s).',
      msg_started_auto: 'Started auto-room mode at lobby {id} with {n} leader(s).',
      msg_stopped: 'All accounts left the channel.',
      msg_sent_command: '{action}: {n} bot(s).',
      msg_renamed: 'Renamed {n} bot(s).',
      msg_no_bots: 'No bots affected.',
      msg_moved: 'Moved {n} bot(s).',
      msg_chat_sent: 'Message sent.',
      msg_chat_all_sent: 'Sent by {n} bot(s).',
      err_enter_message: 'Enter a message.',
      err_enter_channel_id: 'Enter a text channel ID.',
      msg_not_logged_in: 'Not logged in',
      msg_copied: 'Tunnel URL copied.', msg_copy_failed: 'Could not copy the URL.',
      msg_tokens_saved: 'Saved {n} tokens.', msg_logs_cleared: 'Logs cleared.',
      msg_saved: 'Saved successfully.',
      confirm_title: 'Confirm action',
      confirm_stop_msg: 'Make all accounts leave voice and log out?',
      confirm_save_tokens_msg: 'Save tokens to the file? The old content will be backed up to tokens.txt.bak.',
      confirm_rename_all_msg: 'Rename all online bots?',
      btn_confirm: 'Confirm', btn_cancel: 'Cancel',
      title_clear_logs: 'Clear logs',
      nav_overview: 'Overview', nav_tokens: 'Tokens', nav_voice: 'Voice', nav_messages: 'Messages', nav_logs: 'Logs',
      setup_title: 'Getting started',
      step1_title: 'Add tokens', step1_desc: 'Paste your account tokens, one per line, then save.',
      step2_title: 'Start the bots', step2_desc: 'Enter a voice channel ID and press START.',
      step3_title: 'Bots are in voice', step3_desc: 'Control each bot from its card below.',
      btn_go_tokens: 'Add tokens', btn_go_voice: 'Go to Voice',
      no_bots_tokens: 'No tokens yet. Add tokens to get started.',
      no_bots_start: 'Tokens are saved but no bot is running. Press START on the Voice page.',
      connect_title: 'Connect to voice', bulk_title: 'Apply to all bots',
      hint_tokens: 'One token per line. Quotes, "Authorization:" and duplicate lines are removed automatically when you save.',
      hint_channel_ids: 'In Discord, enable Developer Mode, then right-click a voice channel → Copy Channel ID. Separate several IDs with spaces.',
      hint_mode_normal: 'Tokens are spread evenly across the channels below.',
      hint_mode_auto: 'Leaders join the lobby channel (first ID); the other bots follow them into the rooms that get created.',
      hint_single_chat: 'To send from a single account, use the Chat button on that bot\'s card in Overview.',
      token_lines: '{n} token(s)',
      msg_tokens_next: 'Next: open Voice and pick a channel.',
      quick_title: 'Quick join', btn_refresh: 'Refresh', btn_join: 'Join',
      lbl_quick_link: 'Channel link or ID', ph_quick_link: 'Paste a channel link or ID',
      hint_quick: 'Paste a channel link from the browser (or just the ID). If bots are running they move there; otherwise they start and join.',
      recent_title: 'Recent', channels_title: 'Or pick a channel',
      qj_loading: 'Loading channels...',
      qj_no_bots: 'The channel list appears once accounts are online. Paste a channel link above, or pick a Recent channel, to join.',
      qj_empty: 'Your bots cannot see any voice channel. Join a server with those accounts first.',
      qj_here: 'here', title_remove_recent: 'Remove',
      err_bad_link: 'Not a channel link. Use a link like discord.com/channels/<server>/<channel> or a 17–20 digit ID.',
      err_invite_link: 'This is a server invite. Open it with that account in Discord to join the server first, then paste the voice channel link here.',
      msg_joining: 'Starting bots and joining the channel...',
      sc_title: 'Send a message', lbl_account: 'Account', lbl_text_link: 'Channel link or ID',
      hint_sc: 'Paste a text channel link, or pick a channel below.',
      sc_channels_title: 'Or pick a channel', ph_filter: 'Filter channels...',
      hint_ctrl_enter: 'Ctrl + Enter to send',
      sc_no_bots: 'No account is online yet', sc_need_bot: 'Start the bots first (Voice page) to send messages.',
      sc_empty: 'This account cannot post in any text channel.', sc_no_match: 'No channel matches the filter.',
      hint_click_channel: '— click a channel to see who is in it',
      qj_join_this: 'Join this channel', title_close: 'Close',
      qj_people: '{n} in channel', qj_members_empty: 'Nobody is in this channel right now.',
      qj_members_gone: 'None of your bots can see this channel anymore.', qj_your_bot: 'Your bot',
      err_invite_text: 'This is a server invite. Open it with that account in Discord to join the server first, then paste the text channel link here.',
      nav_advanced: 'Advanced',
      run_running: 'Running',
      run_stopped: 'Stopped',
      onboard_title: 'Add a token to get started',
      onboard_desc: 'Each token is one Discord account. Paste them on the Tokens page, then come back here to pick a channel.',
      status_idle_title: 'Not in any voice channel',
      status_idle_sub: 'Pick a channel below and your accounts will join it.',
      status_connecting_title: 'Connecting...',
      status_not_in_title: 'Online but not in a channel',
      status_online_sub: '{n}/{t} accounts online',
      status_in_title: 'In {c}',
      status_in_sub: '{g} · {n}/{t} accounts in the channel',
      status_multi_title: 'Spread over {k} channels',
      status_multi_sub: '{n}/{t} accounts in voice',
      status_errors: '{n} account(s) failed: {e}',
      btn_view_log: 'View log',
      btn_leave: 'Leave channel',
      all_mute: 'Mute',
      all_deaf: 'Deafen',
      all_video: 'Camera',
      all_stream: 'Stream',
      join_title: 'Join a channel',
      btn_enter: 'Join',
      accounts_title: 'Accounts',
      accounts_empty: 'No account is running. Pick a channel above to start.',
      qj_already_here: 'Your accounts are already here',
      qj_detail_offline: 'Accounts are not online yet, so the member list is unavailable. You can still join.',
      token_lines_dup: '{n} token(s) · {d} duplicate line(s) will be removed',
      msg_tokens_cleaned: 'Removed {d} duplicate line(s).',
      adv_start_title: 'Advanced start',
      hint_adv: 'Use this to spread accounts over several channels or for Auto-Room. For a single channel, the Voice page is simpler.',
      lbl_delay_short: 'Delay (s)',
      rename_title: 'Rename all accounts',
      gx_title: 'Servers', gx_offline: 'Accounts are not online yet', gx_token_tag: 'Token',
      gx_no_voice: 'This server has no voice channel.', gx_here_btn: 'Here',
      start_title: 'Accounts are offline', start_desc: 'Log the accounts in first. Then pick a server, the accounts and a channel.',
      btn_start_all: 'Log in all accounts', btn_stop_all: 'Stop all', vp_channels_title: 'Voice channels',
      voice_pill: '{n}/{t} in voice', msg_starting: 'Logging accounts in...',
      gx_step_server: 'Server', gx_step_accounts: 'Choose accounts', gx_step_channel: 'Pick a voice channel to join',
      gx_select_all: 'Select all', gx_select_none: 'Clear', gx_no_accounts: 'None of your online accounts is in this server.',
      gx_not_in_voice: 'Not in voice', gx_selected: '{n}/{t} selected', gx_chat_one: 'Select exactly one account to chat',
      gx_pick_first: 'Select at least one account first.',
      msg_tokens_added: 'Logging in {n} new account(s) and joining the current channel...',
      msg_tokens_removed: 'Logged out {n} removed account(s).'
    },
    vi: {
      login_sub: 'Đăng nhập để quản lý hệ thống voice',
      ph_password: 'Mật khẩu dashboard',
      btn_login: 'Đăng nhập',
      title_show_password: 'Hiện mật khẩu',
      title_hide_password: 'Ẩn mật khẩu',
      lang_english: 'English',
      lang_vietnamese: 'Tiếng Việt',
      err_wrong_password: 'Sai mật khẩu.',
      err_enter_password: 'Vui lòng nhập mật khẩu.',
      err_connect: 'Không kết nối được server.',
      err_session_expired: 'Phiên đăng nhập đã hết hạn, vui lòng đăng nhập lại.',
      stats_mode: 'Chế độ', stats_uptime: 'Thời gian chạy', stats_tokens: 'Tổng Token',
      title_copy_tunnel: 'Copy URL tunnel', btn_copy: 'Copy', title_logout: 'Đăng xuất', btn_logout: 'Đăng xuất',
      stat_total: 'Tổng số Bot', stat_online: 'Online', stat_in_voice: 'Trong Voice', stat_errors: 'Lỗi',
      bots_title: 'Bot', no_bots: 'Chưa có bot nào',
      controls_title: 'Điều khiển',
      lbl_mode: 'Chế độ', opt_normal: 'Normal', opt_auto_room: 'Auto-Room',
      lbl_channel_ids: 'Channel IDs', ph_channel_ids: 'VD: 123456789 987654321',
      lbl_delay: 'Delay giữa các lần đăng nhập (giây)', lbl_leader: 'Số leader',
      btn_start: 'START', btn_stop: 'STOP',
      btn_mute_all: 'Tắt Mic', btn_unmute_all: 'Bật Mic', btn_deaf_all: 'Tắt Tai Nghe',
      btn_undeaf_all: 'Bật Tai Nghe', btn_cam_on: 'Bật Cam', btn_cam_off: 'Tắt Cam',
      btn_stream_on: 'Bật Stream', btn_stream_off: 'Tắt Stream',
      lbl_stream: 'Luồng', title_toggle_stream: 'Bật/tắt phát trực tiếp',
      lbl_rename: 'Nickname mới', ph_rename: 'Nhập nickname (reset = mặc định)', btn_rename_all: 'Đổi tên tất cả',
      btn_move: 'Chuyển kênh', btn_chat: 'Nhắn tin',
      move_title: 'Di chuyển tới voice channel', move_loading: 'Đang tải danh sách kênh...', move_empty: 'Không có voice channel khả dụng.',
      chat_title: 'Gửi tin nhắn', lbl_channel_id: 'ID kênh', ph_text_channel: 'ID kênh văn bản',
      lbl_message: 'Nội dung', ph_message: 'Nội dung tin nhắn...', btn_send: 'Gửi',
      chat_all_title: 'Chat All', btn_send_all: 'Gửi tất cả',
      reaction_title: 'Reaction Tin Nhắn Discord',
      lbl_message_id: 'ID tin nhắn', ph_snowflake: 'VD: 123456789012345678',
      lbl_emoji: 'Emoji', ph_emoji: 'Emoji Unicode, :name: hoặc name:id',
      btn_send_reaction: 'Gửi Reaction', title_choose_emoji: 'Chọn emoji',
      err_channel_id_invalid: 'ID kênh phải gồm 17–20 chữ số.',
      err_enter_message_id: 'ID tin nhắn phải gồm 17–20 chữ số.',
      err_emoji_empty: 'Chọn hoặc nhập emoji.',
      msg_reaction_sent: 'Đã thả reaction bằng {n} bot.',
      msg_reaction_none: 'Không có bot nào thả reaction. Kiểm tra ID và quyền của bot.',
      log_title: 'Log Console', btn_clear_logs: 'Xóa log',
      token_title: 'Quản lý Token', token_count: 'Đang quản lý {n} tokens',
      btn_save_tokens: 'Lưu Token', ph_tokens: 'Mỗi dòng một token...',
      lbl_mic: 'Mic', lbl_deaf: 'Tai nghe', lbl_cam: 'Cam',
      title_toggle_mute: 'Bật/tắt mic', title_toggle_deaf: 'Bật/tắt tai nghe', title_toggle_cam: 'Bật/tắt camera',
      status_voice: 'VOICE', status_ready: 'SẴN SÀNG', status_error: 'LỖI', status_connecting: 'ĐANG KẾT NỐI',
      mode_idle: 'chờ', mode_normal: 'normal', mode_auto_room: 'auto_room',
      msg_enter_channel: 'Nhập ít nhất một channel ID.',
      err_enter_nickname: 'Nhập nickname mới.',
      err_request_failed: 'Yêu cầu thất bại, vui lòng thử lại.',
      err_server_error: 'Lỗi máy chủ (500). Vui lòng thử lại.',
      msg_started_normal: 'Đã bắt đầu chế độ normal với {n} channel.',
      msg_started_auto: 'Đã bắt đầu chế độ auto_room tại lobby {id} với {n} leader.',
      msg_stopped: 'Tất cả tài khoản đã rời kênh.',
      msg_sent_command: '{action}: {n} bot.',
      msg_renamed: 'Đã đổi tên {n} bot.',
      msg_no_bots: 'Không có bot nào bị ảnh hưởng.',
      msg_moved: 'Đã di chuyển {n} bot.',
      msg_chat_sent: 'Đã gửi tin nhắn.',
      msg_chat_all_sent: 'Đã gửi bằng {n} bot.',
      err_enter_message: 'Nhập nội dung tin nhắn.',
      err_enter_channel_id: 'Nhập ID kênh văn bản.',
      msg_not_logged_in: 'Chưa đăng nhập',
      msg_copied: 'Đã copy tunnel URL.', msg_copy_failed: 'Không copy được URL.',
      msg_tokens_saved: 'Đã lưu {n} tokens.', msg_logs_cleared: 'Đã xóa log.',
      msg_saved: 'Đã lưu thành công.',
      confirm_title: 'Xác nhận hành động',
      confirm_stop_msg: 'Cho tất cả tài khoản rời kênh voice và đăng xuất?',
      confirm_save_tokens_msg: 'Lưu tokens vào file? Nội dung cũ sẽ được sao lưu vào tokens.txt.bak.',
      confirm_rename_all_msg: 'Đổi tên toàn bộ bot đang online?',
      btn_confirm: 'Xác nhận', btn_cancel: 'Hủy',
      title_clear_logs: 'Xóa log',
      nav_overview: 'Tổng quan', nav_tokens: 'Token', nav_voice: 'Voice', nav_messages: 'Tin nhắn', nav_logs: 'Log',
      setup_title: 'Bắt đầu',
      step1_title: 'Thêm token', step1_desc: 'Dán token tài khoản, mỗi dòng một token, rồi bấm Lưu.',
      step2_title: 'Khởi động bot', step2_desc: 'Nhập ID kênh voice rồi bấm START.',
      step3_title: 'Bot đã vào voice', step3_desc: 'Điều khiển từng bot bằng các nút trên thẻ bên dưới.',
      btn_go_tokens: 'Thêm token', btn_go_voice: 'Tới trang Voice',
      no_bots_tokens: 'Chưa có token nào. Thêm token để bắt đầu.',
      no_bots_start: 'Đã có token nhưng chưa có bot nào chạy. Bấm START ở trang Voice.',
      connect_title: 'Kết nối voice', bulk_title: 'Áp dụng cho tất cả bot',
      hint_tokens: 'Mỗi dòng một token. Khi lưu sẽ tự bỏ dấu ngoặc kép, chữ "Authorization:" và các dòng trùng.',
      hint_channel_ids: 'Trong Discord, bật Developer Mode, chuột phải vào kênh voice → Copy Channel ID. Nhiều ID thì cách nhau bằng dấu cách.',
      hint_mode_normal: 'Các token được chia đều vào những kênh bên dưới.',
      hint_mode_auto: 'Leader vào kênh lobby (ID đầu tiên); các bot còn lại theo leader vào những phòng được tạo ra.',
      hint_single_chat: 'Muốn gửi từ một tài khoản, bấm nút Chat trên thẻ của bot đó ở trang Tổng quan.',
      token_lines: '{n} token',
      msg_tokens_next: 'Tiếp theo: mở trang Voice và chọn kênh.',
      quick_title: 'Join nhanh', btn_refresh: 'Tải lại', btn_join: 'Join',
      lbl_quick_link: 'Link hoặc ID kênh', ph_quick_link: 'Dán link kênh hoặc ID',
      hint_quick: 'Dán link kênh copy từ trình duyệt (hoặc chỉ ID). Bot đang chạy sẽ chuyển sang kênh đó; chưa chạy thì tự START rồi vào.',
      recent_title: 'Gần đây', channels_title: 'Hoặc chọn kênh',
      qj_loading: 'Đang tải danh sách kênh...',
      qj_no_bots: 'Danh sách kênh hiện ra khi tài khoản đã online. Dán link kênh ở trên hoặc chọn một kênh Gần đây để vào.',
      qj_empty: 'Bot không thấy kênh voice nào. Hãy cho tài khoản vào server trước.',
      qj_here: 'đang ở đây', title_remove_recent: 'Xoá',
      err_bad_link: 'Không phải link kênh. Dùng link dạng discord.com/channels/<server>/<kênh> hoặc ID 17–20 chữ số.',
      err_invite_link: 'Đây là link mời vào server. Hãy mở link này bằng tài khoản đó trong Discord để vào server trước, rồi dán link kênh voice vào đây.',
      msg_joining: 'Đang khởi động bot và vào kênh...',
      sc_title: 'Gửi tin nhắn', lbl_account: 'Tài khoản', lbl_text_link: 'Link hoặc ID kênh',
      hint_sc: 'Dán link kênh chat, hoặc chọn kênh trong danh sách bên dưới.',
      sc_channels_title: 'Hoặc chọn kênh', ph_filter: 'Lọc kênh...',
      hint_ctrl_enter: 'Ctrl + Enter để gửi',
      sc_no_bots: 'Chưa có tài khoản nào online', sc_need_bot: 'Hãy khởi động bot trước (trang Voice) để gửi tin nhắn.',
      sc_empty: 'Tài khoản này không gửi được tin ở kênh chat nào.', sc_no_match: 'Không có kênh nào khớp bộ lọc.',
      hint_click_channel: '— bấm vào kênh để xem ai đang ở trong đó',
      qj_join_this: 'Vào kênh này', title_close: 'Đóng',
      qj_people: '{n} người trong kênh', qj_members_empty: 'Hiện chưa có ai trong kênh.',
      qj_members_gone: 'Không còn bot nào thấy kênh này.', qj_your_bot: 'Bot của bạn',
      err_invite_text: 'Đây là link mời vào server. Hãy mở link này bằng tài khoản đó trong Discord để vào server trước, rồi dán link kênh chat vào đây.',
      nav_advanced: 'Nâng cao',
      run_running: 'Đang chạy',
      run_stopped: 'Đang dừng',
      onboard_title: 'Thêm token để bắt đầu',
      onboard_desc: 'Mỗi token là một tài khoản Discord. Dán token ở trang Token, rồi quay lại đây chọn kênh.',
      status_idle_title: 'Chưa vào kênh voice nào',
      status_idle_sub: 'Chọn một kênh bên dưới, các tài khoản sẽ vào kênh đó.',
      status_connecting_title: 'Đang kết nối...',
      status_not_in_title: 'Đã online nhưng chưa vào kênh',
      status_online_sub: '{n}/{t} tài khoản online',
      status_in_title: 'Đang ở {c}',
      status_in_sub: '{g} · {n}/{t} tài khoản trong kênh',
      status_multi_title: 'Đang ở {k} kênh',
      status_multi_sub: '{n}/{t} tài khoản đang trong voice',
      status_errors: '{n} tài khoản lỗi: {e}',
      btn_view_log: 'Xem log',
      btn_leave: 'Rời kênh',
      all_mute: 'Tắt mic',
      all_deaf: 'Tắt tai nghe',
      all_video: 'Camera',
      all_stream: 'Stream',
      join_title: 'Vào kênh',
      btn_enter: 'Vào',
      accounts_title: 'Tài khoản',
      accounts_empty: 'Chưa có tài khoản nào chạy. Chọn kênh ở trên để bắt đầu.',
      qj_already_here: 'Các tài khoản đã ở kênh này',
      qj_detail_offline: 'Tài khoản chưa online nên chưa xem được ai trong kênh. Bạn vẫn có thể vào kênh.',
      token_lines_dup: '{n} token · {d} dòng trùng sẽ bị bỏ',
      msg_tokens_cleaned: 'Đã bỏ {d} dòng trùng.',
      adv_start_title: 'Khởi động nâng cao',
      hint_adv: 'Dùng khi muốn chia tài khoản ra nhiều kênh hoặc chạy Auto-Room. Nếu chỉ vào một kênh, dùng trang Voice cho đơn giản.',
      lbl_delay_short: 'Delay (giây)',
      rename_title: 'Đổi tên tất cả tài khoản',
      gx_title: 'Server', gx_offline: 'Tài khoản chưa online', gx_token_tag: 'Token',
      gx_no_voice: 'Server này không có kênh voice.', gx_here_btn: 'Đang ở đây',
      start_title: 'Tài khoản chưa đăng nhập', start_desc: 'Đăng nhập tài khoản trước. Sau đó chọn server, chọn tài khoản rồi chọn kênh.',
      btn_start_all: 'Đăng nhập tất cả', btn_stop_all: 'Dừng tất cả', vp_channels_title: 'Kênh voice',
      voice_pill: '{n}/{t} trong voice', msg_starting: 'Đang đăng nhập các tài khoản...',
      gx_step_server: 'Server', gx_step_accounts: 'Chọn tài khoản', gx_step_channel: 'Chọn kênh voice để vào',
      gx_select_all: 'Chọn tất cả', gx_select_none: 'Bỏ chọn', gx_no_accounts: 'Không có tài khoản online nào ở trong server này.',
      gx_not_in_voice: 'Chưa vào voice', gx_selected: 'Đã chọn {n}/{t}', gx_chat_one: 'Chọn đúng 1 tài khoản để nhắn tin',
      gx_pick_first: 'Hãy chọn ít nhất một tài khoản.',
      msg_tokens_added: 'Đang đăng nhập {n} tài khoản mới và vào kênh hiện tại...',
      msg_tokens_removed: 'Đã đăng xuất {n} tài khoản bị xoá.'
    }
  };
  var lang = localStorage.getItem('mgl_lang') === 'en' ? 'en' : 'vi';
  function t(key){
    var table = I18N[lang] || I18N.en;
    return table[key] || I18N.en[key] || key;
  }
  function tOr(key, fallback){
    var table = I18N[lang] || I18N.en;
    return table[key] || I18N.en[key] || fallback;
  }

  function $(id){ return document.getElementById(id); }
  function esc(value){
    return String(value === null || value === undefined ? '' : value).replace(/[&<>"']/g, function(ch){
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch];
    });
  }

  /* ---------- Elements ---------- */
  var loginScreen = $('login-screen'), dashboard = $('dashboard');
  var passwordInput = $('password-input'), loginBtn = $('login-btn'), loginError = $('login-error');
  var passwordToggle = $('password-toggle');
  var statMode = $('stat-mode'), statUptime = $('stat-uptime'), uptimeWrap = $('uptime-wrap');
  var tunnelWrap = $('tunnel-wrap'), tunnelLabel = $('tunnel-url'), copyBtn = $('copy-tunnel'), logoutBtn = $('logout-btn');
  var onboard = $('onboard'), voicePill = $('voice-pill'), voicePillText = $('voice-pill-text');
  var modeSelect = $('mode-select'), channelIdsInput = $('channel-ids'), delayInput = $('delay-input');
  var leaderWrap = $('leader-wrap'), leaderCountInput = $('leader-count');
  var startBtn = $('start-btn'), stopBtn = $('stop-btn');
  var renameInput = $('rename-input'), renameAllBtn = $('rename-all-btn');
  var logConsole = $('log-console'), clearLogsBtn = $('clear-logs');
  var tokenTextarea = $('token-textarea'), tokenLines = $('token-lines');
  var saveTokensBtn = $('save-tokens'), tokenCount = $('token-count'), navTokenCount = $('nav-token-count');
  var viewTitle = $('view-title'), runDot = $('run-dot'), modeHint = $('mode-hint');
  var toastContainer = $('toast-container');
  var confirmModal = $('confirm-modal'), confirmTitle = $('confirm-title'), confirmMessage = $('confirm-message');
  var confirmOk = $('confirm-ok'), confirmCancel = $('confirm-cancel');
  var chatAllChannel = $('chatall-channel'), chatAllMessage = $('chatall-message');
  var chatAllDelay = $('chatall-delay'), chatAllSend = $('chatall-send');
  var reactionChannel = $('reaction-channel'), reactionMessage = $('reaction-message');
  var reactionEmoji = $('reaction-emoji-input'), reactionPreview = $('reaction-preview');
  var reactionPicker = $('reaction-picker'), reactionToggle = $('reaction-emoji-toggle');
  var reactionSend = $('reaction-send'), reactionSpinner = $('reaction-spinner');

  var TOKEN_KEY = 'mgl_dashboard_token';
  var CHAT_CHANNEL_KEY = 'mgl_chat_channel';
  var VIEW_KEY = 'mgl_view';
  var VIEWS = ['voice', 'messages', 'tokens', 'advanced'];
  var currentView = VIEWS.indexOf(localStorage.getItem(VIEW_KEY)) >= 0 ? localStorage.getItem(VIEW_KEY) : 'voice';
  var token = localStorage.getItem(TOKEN_KEY) || '';
  var ws = null, wsRetryTimer = null, authFailed = false;
  var baseUptime = 0, baseUptimeStamp = Date.now();
  var lastSummary = null, lastTokenCount = 0, loginErrorKey = '';

  /* ---------- Toast ---------- */
  function toast(message, type){
    var el = document.createElement('div');
    el.className = 'toast ' + (type || 'info');
    el.setAttribute('role', 'status');
    el.innerHTML = icon(type === 'success' ? 'check' : (type === 'error' ? 'alert' : 'info'))
      + '<span>' + esc(message) + '</span>';
    el.addEventListener('click', function(){ el.remove(); });
    toastContainer.appendChild(el);
    setTimeout(function(){ el.remove(); }, 4000);
  }

  /* ---------- Confirm dialog ---------- */
  var confirmResolve = null, lastFocused = null;
  function askConfirm(messageKey){
    return new Promise(function(resolve){
      confirmResolve = resolve;
      lastFocused = document.activeElement;
      confirmTitle.textContent = t('confirm_title');
      confirmMessage.textContent = t(messageKey);
      confirmOk.textContent = t('btn_confirm');
      confirmCancel.textContent = t('btn_cancel');
      confirmModal.classList.remove('hidden');
      confirmOk.focus();
    });
  }
  function closeConfirm(result){
    if (confirmModal.classList.contains('hidden')) { return; }
    confirmModal.classList.add('hidden');
    if (lastFocused && lastFocused.focus) { lastFocused.focus(); }
    if (confirmResolve) { confirmResolve(result); confirmResolve = null; }
  }
  confirmOk.addEventListener('click', function(){ closeConfirm(true); });
  confirmCancel.addEventListener('click', function(){ closeConfirm(false); });
  confirmModal.addEventListener('click', function(event){
    if (event.target === confirmModal) { closeConfirm(false); }
  });
  document.addEventListener('keydown', function(event){
    if (event.key !== 'Escape') { return; }
    closeConfirm(false);
    toggleEmojiPicker(false);
  });

  /* ---------- i18n apply ---------- */
  function setLoginError(key){
    loginErrorKey = key || '';
    loginError.textContent = loginErrorKey ? t(loginErrorKey) : '';
  }
  function setTokenCount(count){
    lastTokenCount = count || 0;
    tokenCount.textContent = t('token_count').replace('{n}', lastTokenCount);
    navTokenCount.textContent = lastTokenCount;
    navTokenCount.classList.toggle('empty-badge', lastTokenCount === 0);
    renderVoicePage();
  }

  /* ---------- Navigation ---------- */
  function showView(name){
    if (VIEWS.indexOf(name) < 0) { name = 'voice'; }
    currentView = name;
    localStorage.setItem(VIEW_KEY, name);
    document.querySelectorAll('.view').forEach(function(el){
      el.classList.toggle('hidden', el.getAttribute('data-view') !== name);
    });
    document.querySelectorAll('.nav-item').forEach(function(btn){
      var active = btn.getAttribute('data-view') === name;
      btn.classList.toggle('active', active);
      if (active) { btn.setAttribute('aria-current', 'page'); } else { btn.removeAttribute('aria-current'); }
    });
    viewTitle.textContent = t('nav_' + name);
    if (name === 'tokens' && token) { loadTokens(); }
    if (name === 'voice' && token) { renderVoicePage(); }
    if (name === 'advanced') { logConsole.scrollTop = logConsole.scrollHeight; }
    window.scrollTo(0, 0);
  }
  document.addEventListener('click', function(event){
    var target = event.target.closest('[data-view].nav-item, [data-goto]');
    if (!target) { return; }
    showView(target.getAttribute('data-goto') || target.getAttribute('data-view'));
  });

  function updateModeHint(){
    var auto = modeSelect.value === 'auto_room';
    leaderWrap.classList.toggle('hidden', !auto);
    modeHint.setAttribute('data-i18n', auto ? 'hint_mode_auto' : 'hint_mode_normal');
    modeHint.textContent = t(auto ? 'hint_mode_auto' : 'hint_mode_normal');
  }
  function applyLanguage(){
    document.documentElement.lang = lang;
    document.querySelectorAll('[data-i18n]').forEach(function(el){
      el.textContent = t(el.getAttribute('data-i18n'));
    });
    document.querySelectorAll('[data-i18n-ph]').forEach(function(el){
      el.setAttribute('placeholder', t(el.getAttribute('data-i18n-ph')));
    });
    document.querySelectorAll('[data-i18n-title]').forEach(function(el){
      el.setAttribute('title', t(el.getAttribute('data-i18n-title')));
    });
    document.querySelectorAll('.lang-btn').forEach(function(btn){
      var active = btn.getAttribute('data-lang') === lang;
      btn.classList.toggle('active', active);
      btn.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
    updatePasswordToggle();
    if (loginErrorKey) { loginError.textContent = t(loginErrorKey); }
    setTokenCount(lastTokenCount);
    updateTokenLines();
    viewTitle.textContent = t('nav_' + currentView);
    if (lastSummary) { renderSummary(lastSummary); }
    gxSig = null;
    if (scChannels) { scRenderRecent(); renderBotOptions(); renderTextChannels(); }
  }
  document.addEventListener('click', function(event){
    var btn = event.target.closest('.lang-btn');
    if (!btn) { return; }
    lang = btn.getAttribute('data-lang') === 'vi' ? 'vi' : 'en';
    localStorage.setItem('mgl_lang', lang);
    applyLanguage();
  });

  /* ---------- Password toggle ---------- */
  function updatePasswordToggle(){
    var visible = passwordInput.type === 'text';
    passwordToggle.setAttribute('title', t(visible ? 'title_hide_password' : 'title_show_password'));
    passwordToggle.setAttribute('aria-label', t(visible ? 'title_hide_password' : 'title_show_password'));
    passwordToggle.querySelector('.ico').innerHTML = visible ? ICONS.eyeOff : ICONS.eye;
  }
  passwordToggle.addEventListener('click', function(){
    passwordInput.type = passwordInput.type === 'password' ? 'text' : 'password';
    updatePasswordToggle();
    passwordInput.focus();
  });

  /* ---------- API ---------- */
  function api(path, method, body){
    var headers = {};
    if (body !== null && body !== undefined) { headers['Content-Type'] = 'application/json'; }
    if (token) { headers['Authorization'] = 'Bearer ' + token; }
    return fetch(path, {
      method: method || 'POST',
      headers: headers,
      body: (body !== null && body !== undefined) ? JSON.stringify(body) : undefined
    }).then(function(res){
      if (res.status === 401) { doLogout(); return null; }
      return res.json().catch(function(){ return null; });
    }).catch(function(){ return null; });
  }
  function apiWithStatus(path, method, body){
    var headers = {};
    if (body !== null && body !== undefined) { headers['Content-Type'] = 'application/json'; }
    if (token) { headers['Authorization'] = 'Bearer ' + token; }
    return fetch(path, {
      method: method || 'POST',
      headers: headers,
      body: (body !== null && body !== undefined) ? JSON.stringify(body) : undefined
    }).then(function(res){
      if (res.status === 401) { doLogout(); return {status: 401, data: null}; }
      return res.json().then(function(data){
        return {status: res.status, data: data};
      }).catch(function(){
        return {status: res.status, data: null};
      });
    }).catch(function(){
      return {status: 0, data: null};
    });
  }

  /* ---------- Login / logout ---------- */
  function doLogin(){
    setLoginError('');
    var password = passwordInput.value;
    if (!password) { setLoginError('err_enter_password'); return; }
    loginBtn.disabled = true;
    fetch('/api/login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({password: password})
    }).then(function(res){
      if (!res.ok) { setLoginError('err_wrong_password'); return null; }
      return res.json();
    }).then(function(data){
      if (!data) { return; }
      token = data.token;
      localStorage.setItem(TOKEN_KEY, token);
      authFailed = false;
      showDashboard();
      connectWS();
      refreshStatus();
    }).catch(function(){
      setLoginError('err_connect');
    }).then(function(){
      loginBtn.disabled = false;
    });
  }
  function doLogout(){
    token = '';
    localStorage.removeItem(TOKEN_KEY);
    authFailed = true;
    if (ws) { try { ws.close(); } catch (err) {} ws = null; }
    if (wsRetryTimer) { clearTimeout(wsRetryTimer); wsRetryTimer = null; }
    passwordInput.value = '';
    setLoginError('');
    showLogin();
  }
  loginBtn.addEventListener('click', doLogin);
  passwordInput.addEventListener('keydown', function(event){
    if (event.key === 'Enter') { doLogin(); }
  });
  logoutBtn.addEventListener('click', doLogout);

  function showLogin(){
    loginScreen.classList.remove('hidden');
    dashboard.classList.add('hidden');
  }
  function showDashboard(){
    loginScreen.classList.add('hidden');
    dashboard.classList.remove('hidden');
  }
  function isTokenExpired(jwt){
    try {
      var payload = JSON.parse(atob(jwt.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')));
      return payload.exp ? payload.exp * 1000 <= Date.now() : false;
    } catch (err) {
      return true;
    }
  }

  /* ---------- WebSocket ---------- */
  function connectWS(){
    if (!token) { return; }
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) { return; }
    authFailed = false;
    var proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
    ws = new WebSocket(proto + location.host + '/ws');
    ws.onopen = function(){
      try { ws.send(JSON.stringify({type: 'auth', token: token})); } catch (err) {}
    };
    ws.onmessage = function(event){
      var msg;
      try { msg = JSON.parse(event.data); } catch (err) { return; }
      handleMessage(msg);
    };
    ws.onclose = function(){
      ws = null;
      if (!token || authFailed) { return; }
      if (wsRetryTimer) { clearTimeout(wsRetryTimer); }
      wsRetryTimer = setTimeout(connectWS, 3000);
    };
  }
  function handleMessage(msg){
    if (msg.type === 'state_update') { renderSummary(msg.data || {}); }
    else if (msg.type === 'logs_history') { renderLogs(msg.data || []); }
    else if (msg.type === 'log') { appendLog(msg.data || {}); }
    else if (msg.type === 'tunnel_status') { renderTunnel(msg.data || {}); }
    else if (msg.type === 'error' && msg.message === 'Unauthorized') {
      authFailed = true;
      token = '';
      localStorage.removeItem(TOKEN_KEY);
      setLoginError('err_session_expired');
      showLogin();
    }
  }

  /* ---------- Render ---------- */
  function fmtUptime(seconds){
    seconds = Math.max(0, Math.floor(seconds));
    var h = String(Math.floor(seconds / 3600)).padStart(2, '0');
    var m = String(Math.floor((seconds % 3600) / 60)).padStart(2, '0');
    var s = String(seconds % 60).padStart(2, '0');
    return h + ':' + m + ':' + s;
  }
  function renderSummary(summary){
    lastSummary = summary;
    var bots = summary.bots || [];

    var running = bots.length > 0;
    statMode.textContent = t(running ? 'run_running' : 'run_stopped');
    uptimeWrap.classList.toggle('hidden', !running);
    runDot.classList.toggle('running', running);
    baseUptime = summary.uptime_seconds || 0;
    baseUptimeStamp = Date.now();

    renderHeader(bots);
    setTokenCount(summary.total_tokens || 0);
    scheduleVoiceRefresh();
    renderBotOptions();
  }
  function statusOf(bot){
    if (bot.error) { return {label: t('status_error'), cls: 'badge-error'}; }
    if (bot.is_in_voice) { return {label: t('status_voice'), cls: 'badge-voice'}; }
    if (bot.ready) { return {label: t('status_ready'), cls: 'badge-ready'}; }
    return {label: t('status_connecting'), cls: 'badge-connecting'};
  }
  function renderHeader(bots){
    var inVoice = bots.filter(function(bot){ return bot.is_in_voice; }).length;
    stopBtn.classList.toggle('hidden', !bots.length);
    voicePill.classList.toggle('hidden', !bots.length);
    voicePillText.textContent = t('voice_pill').replace('{n}', inVoice).replace('{t}', bots.length);
  }
  function renderTunnel(status){
    var url = status.public_url || '';
    tunnelWrap.classList.toggle('hidden', !url);
    tunnelLabel.textContent = url || '—';
    tunnelLabel.href = url || '#';
    copyBtn.disabled = !url;
    copyBtn.setAttribute('data-url', url);
  }
  function refreshStatus(){
    if (!token) { return; }
    api('/api/status', 'GET').then(function(data){
      if (data) { renderTunnel(data); }
    });
  }
  function appendLog(entry){
    var line = document.createElement('div');
    var level = (entry.level || 'INFO').toLowerCase();
    line.className = 'log-line log-' + level;
    line.textContent = '[' + (entry.timestamp || '') + '] [' + (entry.level || 'INFO') + '] ' + (entry.message || '');
    logConsole.appendChild(line);
    while (logConsole.childElementCount > 500) { logConsole.removeChild(logConsole.firstChild); }
    logConsole.scrollTop = logConsole.scrollHeight;
  }
  function renderLogs(entries){
    logConsole.innerHTML = '';
    entries.forEach(appendLog);
  }

  /* ---------- Actions ---------- */
  function runToggle(payload, label){
    api('/api/toggle', 'POST', payload).then(function(data){
      if (!data) { toast(t('err_request_failed'), 'error'); return; }
      if (data.affected === 0) { toast(t('msg_no_bots'), 'error'); }
      else { toast(t('msg_sent_command').replace('{action}', label).replace('{n}', data.affected), 'success'); }
    });
  }
  startBtn.addEventListener('click', function(){
    var parts = channelIdsInput.value.split(/[\s,]+/).filter(Boolean);
    if (parts.some(function(part){ var p = parseChannelInput(part); return p && p.invite; })) {
      toast(t('err_invite_link'), 'error');
      return;
    }
    // Chấp nhận cả link kênh Discord lẫn ID thuần
    var ids = parts.map(function(part){ var p = parseChannelInput(part); return p ? p.id : part; });
    if (!ids.length) { toast(t('msg_enter_channel'), 'error'); return; }
    var payload = {
      mode: modeSelect.value,
      channel_ids: ids,
      delay: parseFloat(delayInput.value) || 0,
      leader_count: parseInt(leaderCountInput.value, 10) || 5
    };
    api('/api/start', 'POST', payload).then(function(data){
      if (!data) { toast(t('err_request_failed'), 'error'); return; }
      if (payload.mode === 'auto_room') {
        toast(t('msg_started_auto').replace('{id}', ids[0]).replace('{n}', payload.leader_count), 'success');
      } else {
        toast(t('msg_started_normal').replace('{n}', ids.length), 'success');
      }
      showView('voice');
    });
  });
  stopBtn.addEventListener('click', function(){
    askConfirm('confirm_stop_msg').then(function(ok){
      if (!ok) { return; }
      api('/api/stop', 'POST').then(function(data){
        if (!data) { toast(t('err_request_failed'), 'error'); return; }
        toast(t('msg_stopped'), 'success');
      });
    });
  });
  renameAllBtn.addEventListener('click', function(){
    var name = renameInput.value.trim();
    if (!name) { toast(t('err_enter_nickname'), 'error'); return; }
    askConfirm('confirm_rename_all_msg').then(function(ok){
      if (!ok) { return; }
      api('/api/rename', 'POST', {name: name}).then(function(data){
        if (!data) { toast(t('err_request_failed'), 'error'); return; }
        toast(t('msg_renamed').replace('{n}', data.affected), 'success');
      });
    });
  });
  clearLogsBtn.addEventListener('click', function(){
    api('/api/logs', 'DELETE').then(function(){
      logConsole.innerHTML = '';
      toast(t('msg_logs_cleared'), 'info');
    });
  });
  copyBtn.addEventListener('click', function(){
    var url = copyBtn.getAttribute('data-url') || '';
    if (!url) { return; }
    function done(){ toast(t('msg_copied'), 'success'); }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(done).catch(function(){ fallbackCopy(url, done); });
    } else {
      fallbackCopy(url, done);
    }
  });
  function fallbackCopy(text, done){
    var area = document.createElement('textarea');
    area.value = text;
    document.body.appendChild(area);
    area.select();
    try { document.execCommand('copy'); done(); }
    catch (err) { toast(t('msg_copy_failed'), 'error'); }
    document.body.removeChild(area);
  }

  /* ---------- Token manager ---------- */
  // Bỏ "Authorization:", dấu ngoặc và khoảng trắng hay dính khi copy từ DevTools
  function cleanTokenLine(line){
    return line.trim().replace(/^authorization\s*:?\s*/i, '').replace(/^["'`]+|["'`,;]+$/g, '').replace(/\s+/g, '');
  }
  function tokenEditorInfo(){
    var seen = {}, unique = [], duplicates = 0;
    tokenTextarea.value.split('\n').map(cleanTokenLine).filter(Boolean).forEach(function(item){
      if (seen[item]) { duplicates += 1; return; }
      seen[item] = true;
      unique.push(item);
    });
    return {tokens: unique, duplicates: duplicates};
  }
  function tokenEditorLines(){ return tokenEditorInfo().tokens; }
  function updateTokenLines(){
    var info = tokenEditorInfo();
    tokenLines.textContent = info.duplicates
      ? t('token_lines_dup').replace('{n}', info.tokens.length).replace('{d}', info.duplicates)
      : t('token_lines').replace('{n}', info.tokens.length);
  }
  tokenTextarea.addEventListener('input', updateTokenLines);
  function loadTokens(){
    api('/api/tokens', 'GET').then(function(data){
      if (!data) { return; }
      tokenTextarea.value = (data.tokens || []).map(function(item){ return item.full; }).join('\n');
      setTokenCount(data.count);
      updateTokenLines();
    });
  }
  saveTokensBtn.addEventListener('click', function(){
    var info = tokenEditorInfo();
    var lines = info.tokens;
    askConfirm('confirm_save_tokens_msg').then(function(ok){
      if (!ok) { return; }
      api('/api/tokens', 'PUT', {tokens: lines}).then(function(data){
        if (!data) { toast(t('err_request_failed'), 'error'); return; }
        tokenTextarea.value = lines.join('\n');
        updateTokenLines();
        setTokenCount(data.count);
        toast(t('msg_tokens_saved').replace('{n}', data.count), 'success');
        if (info.duplicates) { toast(t('msg_tokens_cleaned').replace('{d}', info.duplicates), 'info'); }
        if (data.added) { toast(t('msg_tokens_added').replace('{n}', data.added), 'info'); }
        if (data.removed) { toast(t('msg_tokens_removed').replace('{n}', data.removed), 'info'); }
        if (data.count > 0 && !(lastSummary && lastSummary.total_bots)) { toast(t('msg_tokens_next'), 'info'); }
      });
    });
  });

  /* ---------- Message Reaction ---------- */
  var EMOJI_GRID = ['👍', '❤️', '😂', '🔥', '✅', '🎉', '😍', '😎', '🤔', '😢', '👀', '💯', '🚀', '🙏', '💪', '🤣', '😅', '😊', '🎯', '⚡', '⭐', '🥳', '💀', '🤝'];
  var SNOWFLAKE_RE = /^\d{17,20}$/;

  function renderEmojiPicker(){
    reactionPicker.innerHTML = EMOJI_GRID.map(function(emoji){
      return '<button class="emoji-option" type="button" role="option" data-emoji="' + emoji + '" title="' + emoji + '">' + emoji + '</button>';
    }).join('');
  }
  function updateReactionPreview(){
    var value = reactionEmoji.value.trim();
    reactionPreview.textContent = value || '?';
  }
  function setReactionEmoji(value){
    reactionEmoji.value = value;
    updateReactionPreview();
  }
  function toggleEmojiPicker(show){
    if (typeof show !== 'boolean') { show = reactionPicker.classList.contains('hidden'); }
    reactionPicker.classList.toggle('hidden', !show);
    reactionToggle.setAttribute('aria-expanded', show ? 'true' : 'false');
  }
  function validateSnowflake(input, errorKey){
    var ok = SNOWFLAKE_RE.test(input.value.trim());
    input.classList.toggle('invalid', !ok);
    input.setAttribute('aria-invalid', ok ? 'false' : 'true');
    if (!ok) { toast(t(errorKey), 'error'); }
    return ok;
  }
  function submitReaction(){
    var channelOk = validateSnowflake(reactionChannel, 'err_channel_id_invalid');
    var messageOk = validateSnowflake(reactionMessage, 'err_enter_message_id');
    var emoji = reactionEmoji.value.trim();
    if (!channelOk || !messageOk) { return; }
    if (!emoji) { toast(t('err_emoji_empty'), 'error'); return; }

    reactionSend.disabled = true;
    reactionSpinner.classList.remove('hidden');
    apiWithStatus('/api/reaction', 'POST', {
      channel_id: reactionChannel.value.trim(),
      message_id: reactionMessage.value.trim(),
      emoji: emoji
    }).then(function(result){
      reactionSend.disabled = false;
      reactionSpinner.classList.add('hidden');
      var data = result.data;
      if (result.status === 200 && data && data.success && typeof data.reacted === 'number') {
        if (data.reacted > 0) {
          toast(t('msg_reaction_sent').replace('{n}', data.reacted), 'success');
        } else {
          toast(t('msg_reaction_none'), 'error');
        }
        return;
      }
      if (result.status === 500 || result.status === 503) {
        toast(t('err_server_error'), 'error');
        return;
      }
      toast(data && data.detail ? String(data.detail) : t('err_request_failed'), 'error');
    });
  }
  reactionToggle.addEventListener('click', function(){ toggleEmojiPicker(); });
  reactionPicker.addEventListener('click', function(event){
    var option = event.target.closest('.emoji-option');
    if (!option) { return; }
    setReactionEmoji(option.getAttribute('data-emoji'));
    toggleEmojiPicker(false);
  });
  reactionEmoji.addEventListener('input', updateReactionPreview);
  reactionSend.addEventListener('click', submitReaction);
  document.addEventListener('click', function(event){
    if (reactionPicker.classList.contains('hidden')) { return; }
    if (event.target.closest('#reaction-picker') || event.target.closest('#reaction-emoji-toggle')) { return; }
    toggleEmojiPicker(false);
  });

  /* ---------- Chat All ---------- */
  chatAllChannel.value = localStorage.getItem(CHAT_CHANNEL_KEY) || '';
  chatAllSend.addEventListener('click', function(){
    var channelId = chatAllChannel.value.trim();
    var message = chatAllMessage.value.trim();
    var delay = parseFloat(chatAllDelay.value);
    if (!channelId) { toast(t('err_enter_channel_id'), 'error'); return; }
    if (!message) { toast(t('err_enter_message'), 'error'); return; }
    if (!isFinite(delay) || delay < 0) { delay = 0; }
    localStorage.setItem(CHAT_CHANNEL_KEY, channelId);
    chatAllSend.disabled = true;
    api('/api/chat/all', 'POST', {channel_id: channelId, message: message, delay: delay}).then(function(data){
      chatAllSend.disabled = false;
      if (!data) { toast(t('err_request_failed'), 'error'); return; }
      toast(t('msg_chat_all_sent').replace('{n}', data.sent), 'success');
    });
  });

  /* ---------- Voice: chọn server → chọn tài khoản → chọn kênh ---------- */
  function parseChannelInput(value){
    value = String(value || '').trim();
    if (/discord(?:app)?\.(?:gg|com\/invite)\//i.test(value)) { return {invite: true}; }
    var link = value.match(/channels\/(?:\d{17,20}|@me)\/(\d{17,20})/);
    if (link) { return {id: link[1]}; }
    if (SNOWFLAKE_RE.test(value)) { return {id: value}; }
    return null;
  }
  /* Danh sách "gần đây" cho kênh chat */
  function recentLoad(key){
    try {
      var list = JSON.parse(localStorage.getItem(key) || '[]');
      return Array.isArray(list) ? list : [];
    } catch (err) {
      return [];
    }
  }
  function recentSave(key, entry){
    var list = recentLoad(key).filter(function(item){ return item.id !== entry.id; });
    list.unshift(entry);
    localStorage.setItem(key, JSON.stringify(list.slice(0, 6)));
  }
  function recentRemove(key, id){
    localStorage.setItem(key, JSON.stringify(recentLoad(key).filter(function(item){ return item.id !== id; })));
  }
  function recentFill(key, findMeta){
    var changed = false;
    var list = recentLoad(key).map(function(item){
      var meta = findMeta(item.id);
      if (!meta || item.name) { return item; }
      changed = true;
      return {id: item.id, name: meta.name, guild: meta.guild_name};
    });
    if (changed) { localStorage.setItem(key, JSON.stringify(list)); }
    return changed;
  }
  function recentRender(key, wrap, container){
    var list = recentLoad(key);
    wrap.classList.toggle('hidden', !list.length);
    container.innerHTML = list.map(function(item){
      var label = item.name
        ? esc(item.name) + (item.guild ? ' <span class="chip-sub">· ' + esc(item.guild) + '</span>' : '')
        : esc(item.id);
      return '<button class="chip" type="button" data-recent-id="' + esc(item.id) + '">'
        + '<span class="chip-text">' + label + '</span>'
        + '<span class="chip-x" data-remove-recent="' + esc(item.id) + '" title="' + esc(t('title_remove_recent')) + '">' + icon('x') + '</span>'
        + '</button>';
    }).join('');
  }

  var GX_KEY = 'mgl_guild';
  var startCard = $('start-card'), voicePanel = $('voice-panel'), startAllBtn = $('start-all-btn');
  var gxGuild = $('gx-guild'), qjRefresh = $('qj-refresh'), qjChannels = $('qj-channels');
  var qjInput = $('qj-input'), qjJoin = $('qj-join');
  var gxAccountsEl = $('gx-accounts'), gxSelectAll = $('gx-select-all');
  var gxSelectedHint = $('gx-selected-hint'), gxChat = $('gx-chat');
  var selTools = document.querySelectorAll('[data-sel-field]');
  var gxGuilds = [], gxData = null, gxLoadedGuild = '', gxSig = null, gxTimer = null;
  var gxPicked = {}, gxKnown = {};

  function isRunning(){ return !!(lastSummary && lastSummary.total_bots > 0); }
  function isOnline(){ return !!(lastSummary && lastSummary.ready_bots > 0); }
  function botById(id){
    return ((lastSummary && lastSummary.bots) || []).filter(function(bot){ return String(bot.id) === String(id); })[0] || null;
  }

  // Trang Voice chỉ có một trong ba trạng thái: chưa có token / chưa đăng nhập / đang chạy
  function renderVoicePage(){
    var hasTokens = lastTokenCount > 0;
    onboard.classList.toggle('hidden', hasTokens);
    startCard.classList.toggle('hidden', !hasTokens || isRunning());
    voicePanel.classList.toggle('hidden', !hasTokens || !isRunning());
    if (hasTokens && isRunning() && currentView === 'voice') { loadGuilds(); }
  }
  function scheduleVoiceRefresh(){
    if (currentView !== 'voice') { return; }
    if (gxTimer) { clearTimeout(gxTimer); }
    gxTimer = setTimeout(renderVoicePage, 800);
  }
  function explorerMessage(message){
    gxAccountsEl.innerHTML = '';
    qjChannels.innerHTML = '<div class="muted">' + esc(message) + '</div>';
    gxSelectedHint.textContent = '';
    gxChat.disabled = true;
    selTools.forEach(function(button){ button.disabled = true; });
  }
  function loadGuilds(){
    if (!token) { return; }
    if (!isOnline()) { gxGuilds = []; renderGuildSelect(); explorerMessage(t('gx_offline')); return; }
    api('/api/voice-channels', 'GET').then(function(data){
      var seen = {}, list = [];
      ((data && data.channels) || []).forEach(function(ch){
        if (!seen[ch.guild_id]) { seen[ch.guild_id] = {id: ch.guild_id, name: ch.guild_name, here: 0}; list.push(seen[ch.guild_id]); }
        seen[ch.guild_id].here += ch.bots_here || 0;
      });
      gxGuilds = list;
      renderGuildSelect();
      if (gxGuild.value) { loadGuild(gxGuild.value === gxLoadedGuild); }
      else { explorerMessage(t('qj_empty')); }
    });
  }
  function renderGuildSelect(){
    var signature = lang + '|' + gxGuilds.map(function(g){ return g.id + ':' + g.name + ':' + (g.here > 0); }).join(',');
    if (signature === gxSig) { return; }
    gxSig = signature;
    var current = gxGuild.value || localStorage.getItem(GX_KEY) || '';
    gxGuild.innerHTML = gxGuilds.length
      ? gxGuilds.map(function(g){
          return '<option value="' + esc(g.id) + '">' + esc(g.name) + (g.here > 0 ? ' — ' + esc(t('qj_here')) : '') + '</option>';
        }).join('')
      : '<option value="">' + esc(t(isOnline() ? 'qj_empty' : 'gx_offline')) + '</option>';
    gxGuild.disabled = !gxGuilds.length;
    // Giữ server đang chọn; lần đầu ưu tiên server có tài khoản đang ở trong
    var pick = gxGuilds.filter(function(g){ return g.id === current; })[0]
      || gxGuilds.filter(function(g){ return g.here > 0; })[0]
      || gxGuilds[0];
    if (pick) { gxGuild.value = pick.id; }
  }
  function loadGuild(silent){
    var guildId = gxGuild.value;
    if (!guildId || !token) { return; }
    gxLoadedGuild = guildId;
    localStorage.setItem(GX_KEY, guildId);
    if (!silent || !gxData) { qjChannels.innerHTML = '<div class="muted">' + esc(t('qj_loading')) + '</div>'; }
    api('/api/guilds/' + encodeURIComponent(guildId) + '/voice', 'GET').then(function(data){
      if (guildId !== gxGuild.value) { return; }
      if (!data) { explorerMessage(t('err_request_failed')); return; }
      gxData = data;
      renderExplorer();
    });
  }

  /* Tài khoản trong server đang chọn; tài khoản mới thấy lần đầu được chọn sẵn */
  function gxAccounts(){ return (gxData && gxData.accounts) || []; }
  function gxSelectedIds(){
    return gxAccounts().filter(function(acc){ return gxPicked[acc.id]; }).map(function(acc){ return acc.id; });
  }
  var SEL_FIELDS = {mute: 'is_mute', deaf: 'is_deaf', video: 'is_video', stream: 'is_streaming'};
  function renderAccounts(){
    var accounts = gxAccounts();
    accounts.forEach(function(acc){
      if (!gxKnown[acc.id]) { gxKnown[acc.id] = true; gxPicked[acc.id] = true; }
    });
    if (!accounts.length) {
      gxAccountsEl.innerHTML = '<div class="muted">' + esc(t('gx_no_accounts')) + '</div>';
    } else {
      gxAccountsEl.innerHTML = accounts.map(function(acc){
        var bot = botById(acc.id) || {};
        var checked = !!gxPicked[acc.id];
        var avatar = acc.avatar_url
          ? '<img class="avatar" src="' + esc(acc.avatar_url) + '" alt="">'
          : '<span class="avatar avatar-fallback">' + icon('user') + '</span>';
        var sub = bot.error
          ? '<div class="gx-acc-sub err" title="' + esc(bot.error) + '">' + esc(bot.error) + '</div>'
          : (acc.voice_channel_name
            ? '<div class="gx-acc-sub in">' + esc(acc.voice_channel_name) + '</div>'
            : '<div class="gx-acc-sub">' + esc(t('gx_not_in_voice')) + '</div>');
        var state = (bot.is_mute ? '<span class="off">' + icon('micOff') + '</span>' : '')
          + (bot.is_deaf ? '<span class="off">' + icon('deafOff') + '</span>' : '')
          + (bot.is_video ? '<span class="live">' + icon('cam') + '</span>' : '')
          + (bot.is_streaming ? '<span class="live">' + icon('screen') + '</span>' : '');
        return '<button class="gx-account' + (checked ? ' checked' : '') + '" type="button" role="checkbox" aria-checked="' + checked + '" data-gx-account="' + esc(acc.id) + '">'
          + '<span class="gx-check">' + (checked ? icon('tick') : '') + '</span>' + avatar
          + '<div class="gx-acc-info"><div class="gx-acc-name">' + esc(acc.name) + '</div>' + sub + '</div>'
          + '<span class="member-icons">' + state + '</span></button>';
      }).join('');
    }
    var selected = gxSelectedIds();
    gxSelectedHint.textContent = t('gx_selected').replace('{n}', selected.length).replace('{t}', accounts.length);
    gxSelectAll.textContent = t(selected.length === accounts.length && accounts.length ? 'gx_select_none' : 'gx_select_all');
    // Nhắn tin luôn từ đúng một tài khoản
    gxChat.disabled = selected.length !== 1;
    gxChat.title = selected.length === 1 ? '' : t('gx_chat_one');
    // Công tắc áp dụng cho tài khoản đã chọn đang trong voice
    var inVoice = selected.map(botById).filter(function(bot){ return bot && bot.is_in_voice; });
    selTools.forEach(function(button){
      var key = SEL_FIELDS[button.getAttribute('data-sel-field')];
      var onCount = inVoice.filter(function(bot){ return !!bot[key]; }).length;
      var allOn = inVoice.length > 0 && onCount === inVoice.length;
      button.disabled = !inVoice.length;
      button.classList.toggle('on', allOn);
      button.classList.toggle('mixed', onCount > 0 && !allOn);
      button.setAttribute('aria-pressed', allOn ? 'true' : (onCount > 0 ? 'mixed' : 'false'));
    });
  }
  function gxMember(member){
    var avatar = member.avatar_url
      ? '<img class="avatar" src="' + esc(member.avatar_url) + '" alt="">'
      : '<span class="avatar avatar-fallback">' + icon('user') + '</span>';
    var icons = (member.mute ? '<span class="off">' + icon('micOff') + '</span>' : '')
      + (member.deaf ? '<span class="off">' + icon('deafOff') + '</span>' : '')
      + (member.video ? '<span class="live">' + icon('cam') + '</span>' : '')
      + (member.stream ? '<span class="live">' + icon('screen') + '</span>' : '');
    return '<div class="gx-member' + (member.is_own ? ' own' : '') + '" title="@' + esc(member.name) + '">' + avatar
      + '<span class="gx-member-name">' + esc(member.display_name || member.name)
      + (member.is_own ? '<span class="gx-tag">' + esc(t('gx_token_tag')) + '</span>' : '') + '</span>'
      + '<span class="member-icons">' + icons + '</span></div>';
  }
  function renderExplorer(){
    renderAccounts();
    var channels = (gxData && gxData.channels) || [];
    if (!channels.length) {
      qjChannels.innerHTML = '<div class="muted">' + esc(t('gx_no_voice')) + '</div>';
      return;
    }
    var selected = gxSelectedIds();
    var whereIs = {};
    gxAccounts().forEach(function(acc){ whereIs[acc.id] = acc.voice_channel_id; });
    qjChannels.innerHTML = channels.map(function(ch){
      var limit = ch.user_limit ? String(ch.user_limit) : '∞';
      var ownHere = ch.members.filter(function(member){ return member.is_own; }).length;
      var selectedHere = selected.filter(function(id){ return whereIs[id] === ch.id; }).length;
      var allHere = selected.length > 0 && selectedHere === selected.length;
      var disabled = !selected.length || allHere;
      var label = allHere ? t('gx_here_btn') : t('btn_enter') + (selected.length ? ' (' + selected.length + ')' : '');
      return '<div class="gx-channel' + (ownHere ? ' here' : '') + '">'
        + '<div class="gx-head">' + icon('volume')
        + '<span class="gx-name" title="' + esc(ch.name) + '">' + esc(ch.name) + '</span>'
        + '<span class="gx-count">' + ch.members.length + '/' + limit + '</span>'
        + '<button class="btn btn-sm' + (disabled ? '' : ' btn-primary') + '" type="button" data-gx-join="' + esc(ch.id) + '"' + (disabled ? ' disabled' : '') + '>'
        + esc(label) + '</button></div>'
        + (ch.members.length ? '<div class="gx-members">' + ch.members.map(gxMember).join('') + '</div>' : '')
        + '</div>';
    }).join('');
  }
  function joinSelected(channelId){
    var ids = gxSelectedIds();
    if (!ids.length) { toast(t('gx_pick_first'), 'error'); return; }
    api('/api/move', 'POST', {target_channel_id: channelId, bot_ids: ids}).then(function(data){
      if (!data) { toast(t('err_request_failed'), 'error'); return; }
      if (data.moved > 0) { toast(t('msg_moved').replace('{n}', data.moved), 'success'); }
      else { toast(t('msg_no_bots'), 'error'); }
      loadGuild(true);
    });
  }
  function toggleSelected(field, value){
    var ids = gxSelectedIds().filter(function(id){ var bot = botById(id); return bot && bot.is_in_voice; });
    if (!ids.length) { return; }
    Promise.all(ids.map(function(id){
      var payload = {bot_id: id};
      payload[field] = value;
      return api('/api/toggle', 'POST', payload);
    })).then(function(results){
      var affected = results.reduce(function(sum, data){ return sum + ((data && data.affected) || 0); }, 0);
      if (!affected) { toast(t('msg_no_bots'), 'error'); }
    });
  }

  startAllBtn.addEventListener('click', function(){
    startAllBtn.disabled = true;
    api('/api/start', 'POST', {mode: 'normal', channel_ids: [], delay: parseFloat(delayInput.value) || 5, leader_count: 5}).then(function(data){
      startAllBtn.disabled = false;
      if (!data) { toast(t('err_request_failed'), 'error'); return; }
      toast(t('msg_starting'), 'info');
    });
  });
  gxGuild.addEventListener('change', function(){ gxData = null; loadGuild(false); });
  qjRefresh.addEventListener('click', loadGuilds);
  gxAccountsEl.addEventListener('click', function(event){
    var item = event.target.closest('[data-gx-account]');
    if (!item) { return; }
    var id = item.getAttribute('data-gx-account');
    gxPicked[id] = !gxPicked[id];
    renderExplorer();
  });
  gxSelectAll.addEventListener('click', function(){
    var accounts = gxAccounts();
    var allPicked = accounts.length && gxSelectedIds().length === accounts.length;
    accounts.forEach(function(acc){ gxPicked[acc.id] = !allPicked; });
    renderExplorer();
  });
  gxChat.addEventListener('click', function(){
    var ids = gxSelectedIds();
    if (ids.length === 1) { openChatPanel(ids[0], gxData && gxData.name); }
  });
  selTools.forEach(function(button){
    button.addEventListener('click', function(){
      toggleSelected(button.getAttribute('data-sel-field'), !button.classList.contains('on'));
    });
  });
  qjChannels.addEventListener('click', function(event){
    var button = event.target.closest('[data-gx-join]');
    if (button && !button.disabled) { joinSelected(button.getAttribute('data-gx-join')); }
  });
  qjJoin.addEventListener('click', function(){
    var parsed = parseChannelInput(qjInput.value);
    if (parsed && parsed.invite) { toast(t('err_invite_link'), 'error'); return; }
    if (!parsed) { qjInput.classList.add('invalid'); toast(t('err_bad_link'), 'error'); return; }
    qjInput.classList.remove('invalid');
    qjInput.value = '';
    joinSelected(parsed.id);
  });
  qjInput.addEventListener('keydown', function(event){
    if (event.key === 'Enter') { qjJoin.click(); }
  });
  qjInput.addEventListener('input', function(){ qjInput.classList.remove('invalid'); });
  // Người khác vào/ra kênh không phát sự kiện tới dashboard nên tự làm mới định kỳ
  setInterval(function(){
    if (currentView === 'voice' && gxLoadedGuild && isOnline() && !document.hidden) { loadGuild(true); }
  }, 5000);

  /* ---------- Single-account chat ---------- */
  var SC_RECENT_KEY = 'mgl_recent_text_channels';
  var scBot = $('sc-bot'), scChannel = $('sc-channel'), scRefresh = $('sc-refresh');
  var scRecentWrap = $('sc-recent-wrap'), scRecent = $('sc-recent'), scFilter = $('sc-filter');
  var scChannels = $('sc-channels'), scMessage = $('sc-message'), scSend = $('sc-send');
  var scList = [], scListBot = null, scBotSig = null, scBusy = false;

  function scMeta(id){
    return scList.filter(function(ch){ return ch.id === id; })[0] || null;
  }
  function scRenderRecent(){ recentRender(SC_RECENT_KEY, scRecentWrap, scRecent); }
  function scSelectedId(){
    var parsed = parseChannelInput(scChannel.value);
    return parsed && parsed.id ? parsed.id : '';
  }
  function renderBotOptions(){
    var bots = ((lastSummary && lastSummary.bots) || []).filter(function(bot){ return bot.ready; });
    var options = bots.map(function(bot){
      var user = bot.user || {};
      return {id: String(bot.id), name: user.display_name || user.username || bot.token_masked};
    });
    // Chỉ dựng lại khi danh sách đổi, để không đóng dropdown đang mở mỗi lần state cập nhật
    var signature = lang + '|' + options.map(function(o){ return o.id + ':' + o.name; }).join(',');
    if (signature !== scBotSig) {
      scBotSig = signature;
      var current = scBot.value;
      if (!options.length) {
        scBot.innerHTML = '<option value="">' + esc(t('sc_no_bots')) + '</option>';
      } else {
        scBot.innerHTML = options.map(function(o){
          return '<option value="' + esc(o.id) + '">' + esc(o.name) + '</option>';
        }).join('');
        if (options.some(function(o){ return o.id === current; })) { scBot.value = current; }
      }
      scBot.disabled = !options.length;
    }
    if (scBot.value !== scListBot) { loadTextChannels(); }
  }
  function renderTextChannels(){
    if (!scBot.value) {
      scChannels.innerHTML = '<div class="muted">' + esc(t('sc_need_bot')) + '</div>';
      return;
    }
    if (!scList.length) {
      scChannels.innerHTML = '<div class="muted">' + esc(t('sc_empty')) + '</div>';
      return;
    }
    var query = scFilter.value.trim().toLowerCase();
    var selected = scSelectedId();
    var items = scList.filter(function(ch){
      if (!query) { return true; }
      return [ch.name, ch.guild_name, ch.category || ''].some(function(text){
        return text.toLowerCase().indexOf(query) >= 0;
      });
    });
    if (!items.length) {
      scChannels.innerHTML = '<div class="muted">' + esc(t('sc_no_match')) + '</div>';
      return;
    }
    var groups = {}, order = [];
    items.forEach(function(ch){
      if (!groups[ch.guild_id]) { groups[ch.guild_id] = {name: ch.guild_name, items: []}; order.push(ch.guild_id); }
      groups[ch.guild_id].items.push(ch);
    });
    scChannels.innerHTML = order.map(function(guildId){
      var group = groups[guildId];
      var lastCategory = null;
      return '<div class="qj-group"><div class="qj-guild" title="' + esc(group.name) + '">' + esc(group.name) + '</div>'
        + group.items.map(function(ch){
          var heading = '';
          if (ch.category && ch.category !== lastCategory) {
            heading = '<div class="qj-cat">' + esc(ch.category) + '</div>';
          }
          lastCategory = ch.category;
          return heading
            + '<button class="qj-channel' + (ch.id === selected ? ' selected' : '') + '" type="button" data-text-channel-id="' + esc(ch.id) + '">'
            + icon('hash') + '<span class="name">' + esc(ch.name) + '</span></button>';
        }).join('')
        + '</div>';
    }).join('');
  }
  function loadTextChannels(){
    var botId = scBot.value;
    scListBot = botId;
    scList = [];
    if (!botId || !token) { renderTextChannels(); return; }
    scChannels.innerHTML = '<div class="muted">' + esc(t('qj_loading')) + '</div>';
    api('/api/bots/' + encodeURIComponent(botId) + '/text-channels', 'GET').then(function(data){
      if (botId !== scListBot) { return; }
      scList = (data && data.channels) || [];
      renderTextChannels();
      if (recentFill(SC_RECENT_KEY, scMeta)) { scRenderRecent(); }
    });
  }
  function selectTextChannel(id){
    scChannel.value = id;
    scChannel.classList.remove('invalid');
    renderTextChannels();
    scMessage.focus();
  }
  function sendSingle(){
    if (scBusy) { return; }
    var botId = scBot.value;
    var raw = scChannel.value.trim();
    var parsed = parseChannelInput(raw);
    var message = scMessage.value.trim();
    if (!botId) { toast(t('sc_need_bot'), 'error'); return; }
    if (parsed && parsed.invite) { toast(t('err_invite_text'), 'error'); return; }
    if (!parsed) {
      scChannel.classList.add('invalid');
      toast(t(raw ? 'err_bad_link' : 'err_enter_channel_id'), 'error');
      return;
    }
    if (!message) { toast(t('err_enter_message'), 'error'); return; }
    scBusy = true;
    scSend.disabled = true;
    api('/api/chat/single', 'POST', {bot_id: botId, channel_id: parsed.id, message: message}).then(function(data){
      scBusy = false;
      scSend.disabled = false;
      if (!data || !data.sent) { toast(t('err_request_failed'), 'error'); return; }
      toast(t('msg_chat_sent'), 'success');
      scMessage.value = '';
      var meta = scMeta(parsed.id);
      var previous = recentLoad(SC_RECENT_KEY).filter(function(item){ return item.id === parsed.id; })[0] || {};
      recentSave(SC_RECENT_KEY, {
        id: parsed.id,
        name: meta ? meta.name : (previous.name || ''),
        guild: meta ? meta.guild_name : (previous.guild || '')
      });
      scRenderRecent();
    });
  }
  function openChatPanel(botId, guildName){
    showView('messages');
    scFilter.value = guildName || '';
    if ([].some.call(scBot.options, function(option){ return option.value === botId; })) {
      scBot.value = botId;
    }
    if (scBot.value !== scListBot) { loadTextChannels(); } else { renderTextChannels(); }
    scMessage.focus();
  }
  scBot.addEventListener('change', loadTextChannels);
  scRefresh.addEventListener('click', loadTextChannels);
  scFilter.addEventListener('input', renderTextChannels);
  scChannel.addEventListener('input', function(){
    scChannel.classList.remove('invalid');
    renderTextChannels();
  });
  scChannels.addEventListener('click', function(event){
    var item = event.target.closest('[data-text-channel-id]');
    if (item) { selectTextChannel(item.getAttribute('data-text-channel-id')); }
  });
  scRecent.addEventListener('click', function(event){
    var remove = event.target.closest('[data-remove-recent]');
    if (remove) { recentRemove(SC_RECENT_KEY, remove.getAttribute('data-remove-recent')); scRenderRecent(); return; }
    var chip = event.target.closest('[data-recent-id]');
    if (chip) { selectTextChannel(chip.getAttribute('data-recent-id')); }
  });
  scMessage.addEventListener('keydown', function(event){
    if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { sendSingle(); }
  });
  scSend.addEventListener('click', sendSingle);

  /* ---------- Init ---------- */
  scRenderRecent();
  injectAssets();
  applyLanguage();
  renderEmojiPicker();
  updateReactionPreview();
  modeSelect.addEventListener('change', updateModeHint);
  updateModeHint();
  showView(currentView);
  setInterval(function(){
    statUptime.textContent = fmtUptime(baseUptime + (Date.now() - baseUptimeStamp) / 1000);
  }, 1000);
  setInterval(function(){ if (token) { refreshStatus(); } }, 30000);

  if (token && !isTokenExpired(token)) {
    showDashboard();
    connectWS();
    refreshStatus();
  } else {
    token = '';
    localStorage.removeItem(TOKEN_KEY);
    showLogin();
    passwordInput.focus();
  }
})();
</script>
</body>
</html>
"""

# scripts/notifications/senders.py
# 總覽：
# - 本模組提供多通道通知：Email、LINE、Discord（Webhook/DM/Channel）。
# - format_summary_text 用於整理管線執行結果為可讀摘要；notify_all 會統一發送並記錄各通道錯誤。
# - 每個 send_* 函式都依據 cfg 取用必要憑證與目標 ID；缺參數時安全返回不拋錯，利於可選通道使用。

import json
import smtplib
import ssl
import time
import requests
from typing import Dict, List, Optional
from email.message import EmailMessage

def _get(cfg: Dict[str, str], key: str, default: Optional[str] = None) -> Optional[str]:
    """
    從設定字典 cfg 安全取得字串值：
    - 若值為 None 或空白字串，回傳 default
    - 若為字串，會 strip 去除前後空白
    """
    v = cfg.get(key)
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return default
    return v.strip() if isinstance(v, str) else v

def _split_emails(value: Optional[str]) -> List[str]:
    """
    將以逗號分隔的 Email 清單字串拆解成列表，並去除空白與空項
    例："a@x.com, b@y.com" -> ["a@x.com", "b@y.com"]
    """
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]

def format_summary_text(status: str, started_at: float, steps: List[Dict], extra_details: Optional[str] = None) -> str:
    """
    將管線跑程的狀態、步驟與耗時整理為純文字摘要。
    參數：
      - status：整體狀態（如 SUCCESS/FAILURE/partial）
      - started_at：流程開始時間戳（time.time()）
      - steps：每個步驟的 dict，建議欄位：
          name: 步驟名稱
          ok: 是否成功（bool）
          elapsed: 該步驟耗時（秒）
          attempts: 嘗試次數（int）
          error: 失敗時的錯誤訊息（可選）
      - extra_details：補充文字（可選）
    回傳：格式良好的多行文字
    """
    total_elapsed = time.time() - started_at
    lines = [
        f"YouTube Data Pipeline 結果：{status}",
        f"總耗時：{total_elapsed:.2f}s",
        "",
        "步驟摘要：",
    ]
    for s in steps:
        name = s.get("name")
        ok = s.get("ok", False)
        elapsed = s.get("elapsed", 0.0)
        attempts = s.get("attempts", 1)
        err = s.get("error")
        if ok:
            lines.append(f" - {name}: 成功 ({elapsed:.2f}s, 嘗試 {attempts} 次)")
        else:
            lines.append(f" - {name}: 失敗 ({elapsed:.2f}s, 嘗試 {attempts} 次)")
            if err:
                lines.append(f"   錯誤：{err}")
    if extra_details:
        lines.append("")
        lines.append("詳細：")
        lines.append(extra_details)
    return "\n".join(lines)

# ============ Email ============
def send_email(cfg: Dict[str, str], subject: str, body: str) -> None:
    """
    使用 SMTP 寄送純文字 Email。
    設定值（cfg）：
      - SMTP_HOST：SMTP 主機，預設 smtp.gmail.com
      - SMTP_PORT：SMTP 連接埠，預設 587
      - SMTP_USER / SMTP_PASS：登入帳密，若缺失則嘗試匿名（依伺服器政策）
      - SMTP_FROM：寄件者地址，預設等同 SMTP_USER
      - SMTP_TO：收件者清單（逗號分隔）
      - SMTP_BCC：密件副本清單（逗號分隔）
      - SMTP_USE_SSL：是否使用 SMTPS（true/false，預設 false -> STARTTLS）
      - SMTP_TIMEOUT：逾時秒數，預設 15
    行為：
      - 若收件者（TO+BCC）皆為空則直接返回
      - 依據是否使用 SSL 選擇 SMTP_SSL 或 SMTP + STARTTLS
    """
    host = _get(cfg, "SMTP_HOST", "smtp.gmail.com")
    port = int(_get(cfg, "SMTP_PORT", "587"))
    user = _get(cfg, "SMTP_USER")
    password = _get(cfg, "SMTP_PASS")
    sender = _get(cfg, "SMTP_FROM", user or "")
    to_list = _split_emails(_get(cfg, "SMTP_TO"))
    bcc_list = _split_emails(_get(cfg, "SMTP_BCC"))
    use_ssl = (_get(cfg, "SMTP_USE_SSL", "false").lower() == "true")
    timeout = int(_get(cfg, "SMTP_TIMEOUT", "15"))

    if not to_list and not bcc_list:
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    if to_list:
        msg["To"] = ", ".join(to_list)
    msg.set_content(body)

    recipients = list(to_list) + list(bcc_list)

    if use_ssl:
        # 直連 SMTPS（常用於 465）
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as server:
            if user and password:
                server.login(user, password)
            server.send_message(msg, from_addr=sender, to_addrs=recipients)
    else:
        # 明文連線後升級 STARTTLS（常用於 587）
        with smtplib.SMTP(host, port, timeout=timeout) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            if user and password:
                server.login(user, password)
            server.send_message(msg, from_addr=sender, to_addrs=recipients)

# ============ LINE ============
def send_line_push(cfg: Dict[str, str], text: str) -> None:
    """
    以 LINE Messaging API 的 push message 發送文字訊息。
    設定值（cfg）：
      - CHANNEL_ACCESS_TOKEN：LINE Bot 的 channel access token
      - LINE_USER_ID：接收者的使用者 ID
    限制：
      - 單則文字長度上限 5000 字元，此處保守切至 4999
      - 若缺 token 或 user_id，則直接返回
    """
    token = _get(cfg, "CHANNEL_ACCESS_TOKEN")
    user_id = _get(cfg, "LINE_USER_ID")
    if not token or not user_id:
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"to": user_id, "messages": [{"type": "text", "text": text[:4999]}]}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=15)
    r.raise_for_status()

# ============ Discord (Webhook) ============
def send_discord_webhook(cfg: Dict[str, str], content: str) -> None:
    """
    使用 Discord Webhook 發送訊息到指定頻道。
    設定值（cfg）：
      - DISCORD_WEBHOOK_URL：Webhook URL（必填）
      - DISCORD_USER_ID：可選，若提供則嘗試在訊息前加上 <@userId> 以提及（伺服器設定可能影響是否成功 ping）
    限制：
      - Webhook content 實務上約 2000 字符，本實作保守截斷至 1999
    """
    webhook = _get(cfg, "DISCORD_WEBHOOK_URL")
    if not webhook:
        return
    # 嘗試在 webhook 中提及使用者（非保證生效，與伺服器設定相關）
    mention_id = _get(cfg, "DISCORD_USER_ID")
    mention_text = f"<@{mention_id}>\n" if mention_id else ""
    r = requests.post(webhook, json={"content": f"{mention_text}{content}"[:1999]}, timeout=15)
    r.raise_for_status()

# ============ Discord (Bot API) ============
def _discord_bot_headers(bot_token: str) -> Dict[str, str]:
    """
    建立 Discord Bot API 需要的標頭（Authorization: Bot <token>）
    """
    return {"Authorization": f"Bot {bot_token}", "Content-Type": "application/json"}

def send_discord_dm(cfg: Dict[str, str], content: str) -> None:
    """
    以 Bot Token 對指定使用者建立 DM channel 並發送訊息。
    需要：
      - DISCORD_BOT_TOKEN（或相容 key：TOKEN_Asa_Bot、TOKEN_Asa_Box）
      - DISCORD_USER_ID
    可選：
      - DISCORD_USER_CHANNEL（若已知，則直接發送可少一次 API）
    常見錯誤（raise_for_status 會拋出）：
      - 403：對方關閉 DM、或與 Bot 無共同伺服器
      - 400：User ID 無效或參數錯誤
    """
    bot_token = _get(cfg, "DISCORD_BOT_TOKEN") or _get(cfg, "TOKEN_Asa_Bot") or _get(cfg, "TOKEN_Asa_Box")
    user_id = _get(cfg, "DISCORD_USER_ID")
    channel_id = _get(cfg, "DISCORD_USER_CHANNEL")
    if not bot_token or not user_id:
        return

    base = "https://discord.com/api/v10"
    headers = _discord_bot_headers(bot_token)

    # 若未知 channel，先建立 DM 對話
    if not channel_id:
        r = requests.post(f"{base}/users/@me/channels", headers=headers, json={"recipient_id": user_id}, timeout=15)
        # 常見錯誤：
        #  - 403: 使用者不允許 DM 或無共同伺服器
        #  - 400: 參數錯誤、User ID 無效
        r.raise_for_status()
        channel_id = r.json()["id"]

    # 發送 DM 訊息（allowed_mentions 清空避免誤 ping）
    payload = {"content": content[:1999], "allowed_mentions": {"parse": []}}
    r = requests.post(f"{base}/channels/{channel_id}/messages", headers=headers, data=json.dumps(payload), timeout=15)
    r.raise_for_status()

def send_discord_channel_message(cfg: Dict[str, str], content: str, mention_user: bool = False) -> None:
    """
    使用 Bot Token 在指定頻道發送訊息，可選擇 @ 指定使用者。
    需要：
      - DISCORD_BOT_TOKEN（或相容 key）
      - DISCORD_CHANNEL_ID
    可選：
      - DISCORD_USER_ID（若 mention_user=True 需要）
    注意：
      - 為避免過度提及，allowed_mentions 僅在指定 user_id 時開放 users 白名單
    """
    bot_token = _get(cfg, "DISCORD_BOT_TOKEN") or _get(cfg, "TOKEN_Asa_Bot") or _get(cfg, "TOKEN_Asa_Box")
    channel_id = _get(cfg, "DISCORD_CHANNEL_ID")
    user_id = _get(cfg, "DISCORD_USER_ID")
    if not bot_token or not channel_id:
        return

    base = "https://discord.com/api/v10"
    headers = _discord_bot_headers(bot_token)

    final_content = content
    allowed_mentions = {"parse": []}
    if mention_user and user_id:
        # 加入明確的 mention 與 allowed_mentions 限定
        final_content = f"<@{user_id}>\n{content}"
        allowed_mentions = {"users": [user_id], "parse": []}

    payload = {"content": final_content[:1999], "allowed_mentions": allowed_mentions}
    r = requests.post(f"{base}/channels/{channel_id}/messages", headers=headers, data=json.dumps(payload), timeout=15)
    r.raise_for_status()

def notify_all(cfg: Dict[str, str], status: str, started_at: float, steps: List[Dict], extra_details: Optional[str] = None) -> None:
    """
    綜合通知入口：同時嘗試以 Email、LINE、Discord（Webhook/DM/Channel）發送摘要。
    邏輯：
      1) 產生標題與內文（支援自訂前綴 body_prefix）
      2) 依序呼叫各通道，任何通道出錯皆記錄於 errors 陣列（不中斷其他通道）
      3) 若有任一通道失敗，最後再嘗試補寄一封 Email 說明失敗清單（若 Email 也失敗則忽略）
    設定值摘要：
      - Email：SMTP_HOST/PORT/USER/PASS/FROM/TO/BCC/USE_SSL/TIMEOUT/SMTP_SUBJECT/SMTP_BODY
      - LINE：CHANNEL_ACCESS_TOKEN/LINE_USER_ID
      - Discord：DISCORD_WEBHOOK_URL、DISCORD_BOT_TOKEN 或相容 token、DISCORD_USER_ID、DISCORD_CHANNEL_ID、DISCORD_MENTION_IN_CHANNEL
    """
    subject = _get(cfg, "SMTP_SUBJECT", "YouTube_Data_Pipeline通知信")
    body_prefix = _get(cfg, "SMTP_BODY", "「YouTube_Data_Pipeline通知信」內文")
    text = format_summary_text(status, started_at, steps, extra_details)

    errors = []

    # Email
    try:
        send_email(cfg, subject, f"{body_prefix}\n\n{text}")
    except Exception as e:
        errors.append(f"Email: {e}")

    # LINE
    try:
        send_line_push(cfg, text)
    except Exception as e:
        errors.append(f"LINE: {e}")

    # Discord webhook（可嘗試 mention，但不保證一定 ping）
    try:
        send_discord_webhook(cfg, text)
    except Exception as e:
        errors.append(f"Discord webhook: {e}")

    # Discord DM（Bot 私訊）
    try:
        send_discord_dm(cfg, text)
    except Exception as e:
        errors.append(f"Discord DM: {e}")

    # Discord Channel via Bot（可確保在頻道中 @ 使用者）
    try:
        mention_in_channel = (_get(cfg, "DISCORD_MENTION_IN_CHANNEL", "false").lower() == "true")
        send_discord_channel_message(cfg, text, mention_user=mention_in_channel)
    except Exception as e:
        errors.append(f"Discord channel message: {e}")

    # 若部分通道失敗，再補一封簡報到 Email（若 Email 也失敗則忽略）
    if errors:
        try:
            send_email(cfg, f"[通知失敗部分] {subject}", "以下通道發送失敗：\n" + "\n".join(errors))
        except Exception:
            pass
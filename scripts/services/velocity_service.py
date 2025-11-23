import time
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from sqlalchemy import text

from scripts.db.db import get_engine
from scripts.youtube.playlists import fetch_channel_video_ids
from scripts.youtube.videos import fetch_videos_details_batch

# 用來儲存 Discord 訊息 ID 的檔案，實現「編輯」而非「洗版」
DISCORD_STATE_FILE = "discord_state.json"

def run_velocity_track(channel_id: str, settings: Dict[str, str], dry_run: bool = False):
    """
    每 15 分鐘執行：
    1. 抓取最新數據
    2. 計算 Delta 並寫入 fact_video_velocity
    3. 更新 dim_video 為最新狀態
    4. 生成排行榜並更新 Discord
    """
    engine = get_engine()
    yt_api_key = settings.get("YPKG_API_KEY")
    webhook_url = settings.get("DISCORD_WEBHOOK_URL") # 需在 .env 新增

    # 1. 準備數據：抓取所有影片 ID
    print("[Velocity] Fetching all video IDs...")
    all_video_ids = fetch_channel_video_ids(yt_api_key, channel_id)
    
    # 2. 讀取 DB 現有狀態 (用於比對)
    # 格式: { 'video_id': {'views': 100, 'likes': 10, 'comments': 5} }
    print("[Velocity] Loading current DB state...")
    current_state = {}
    with engine.connect() as conn:
        # 假設 dim_video 欄位: video_id, view_count, like_count, comment_count
        rows = conn.execute(text("SELECT video_id, view_count, like_count, comment_count FROM dim_video"))
        for r in rows:
            current_state[r.video_id] = {
                "views": r.view_count or 0,
                "likes": r.like_count or 0,
                "comments": r.comment_count or 0
            }

    # 3. 批次處理 API 並計算 Delta
    print(f"[Velocity] Processing {len(all_video_ids)} videos...")
    
    velocity_records = [] # 準備寫入 fact_video_velocity
    update_records = []   # 準備更新 dim_video
    
    captured_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch_size = 50

    for i in range(0, len(all_video_ids), batch_size):
        batch_ids = all_video_ids[i : i + batch_size]
        try:
            api_results = fetch_videos_details_batch(yt_api_key, batch_ids)
            
            for item in api_results:
                vid = item["video_id"]
                stats = item.get("statistics", {})
                
                new_views = int(stats.get("viewCount", 0))
                new_likes = int(stats.get("likeCount", 0))
                new_comments = int(stats.get("commentCount", 0))
                
                # 取得舊數據 (若為新影片，舊數據預設為 0)
                old_data = current_state.get(vid, {"views": 0, "likes": 0, "comments": 0})
                
                delta_views = new_views - old_data["views"]
                delta_likes = new_likes - old_data["likes"]
                delta_comments = new_comments - old_data["comments"]
                
                # 只有當數據有變化時才記錄 Delta (或新影片)
                # 這裡設定：只要有任一數據變動，就記錄
                if delta_views != 0 or delta_likes != 0 or delta_comments != 0:
                    velocity_records.append({
                        "video_id": vid,
                        "captured_at": captured_at,
                        "delta_views": delta_views,
                        "delta_likes": delta_likes,
                        "delta_comments": delta_comments
                    })
                
                # 無論有無變化，都要準備更新 dim_video 到最新狀態
                # 這樣下次比對才會正確
                update_records.append({
                    "video_id": vid,
                    "view_count": new_views,
                    "like_count": new_likes,
                    "comment_count": new_comments,
                    # 這裡可以順便更新 title, published_at 等，確保新影片資料完整
                    "title": item.get("snippet", {}).get("title", "")[:255], 
                    "published_at": item.get("snippet", {}).get("publishedAt"),
                    "updated_at": captured_at
                })

        except Exception as e:
            print(f"[Error] Batch {i} failed: {e}")

    # 4. 寫入資料庫 (Transaction)
    if not dry_run:
        with engine.begin() as conn:
            # A. 寫入 Delta
            if velocity_records:
                conn.execute(text("""
                    INSERT INTO fact_video_velocity 
                    (video_id, captured_at, delta_views, delta_likes, delta_comments)
                    VALUES (:video_id, :captured_at, :delta_views, :delta_likes, :delta_comments)
                """), velocity_records)
                print(f"[DB] Inserted {len(velocity_records)} velocity records.")

            # B. 更新 dim_video (Upsert: 存在則更新，不存在則插入)
            # MySQL 的 ON DUPLICATE KEY UPDATE
            if update_records:
                # 這裡為了簡化，使用逐筆或小批次 Upsert，或使用 SQLAlchemy 的特定語法
                # 為了效能，建議使用 INSERT ... ON DUPLICATE KEY UPDATE
                # 這裡示範概念 SQL
                stmt = text("""
                    INSERT INTO dim_video (video_id, title, published_at, view_count, like_count, comment_count, updated_at)
                    VALUES (:video_id, :title, :published_at, :view_count, :like_count, :comment_count, :updated_at)
                    ON DUPLICATE KEY UPDATE
                        view_count = VALUES(view_count),
                        like_count = VALUES(like_count),
                        comment_count = VALUES(comment_count),
                        updated_at = VALUES(updated_at)
                """)
                conn.execute(stmt, update_records)
                print(f"[DB] Updated {len(update_records)} videos in dim_video.")

    # 5. 生成排行榜並發送 Discord
    if webhook_url:
        report_text = generate_leaderboard_report(engine, captured_at)
        update_discord_message(webhook_url, report_text)

def generate_leaderboard_report(engine, current_time_str) -> str:
    """
    查詢資料庫生成各時段排行榜文字
    """
    # 這裡使用 SQL 聚合查詢
    # 範例：查詢最近 1 小時的觀看增長前 5 名
    # 注意：要關聯 dim_video 取得影片標題
    
    def get_top(interval_sql, label):
        sql = f"""
            SELECT v.title, SUM(f.delta_views) as total_delta
            FROM fact_video_velocity f
            JOIN dim_video v ON f.video_id = v.video_id
            WHERE f.captured_at >= {interval_sql}
            GROUP BY f.video_id
            ORDER BY total_delta DESC
            LIMIT 5
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
        
        txt = f"**{label}**\n"
        if not rows:
            txt += "Wait for data...\n"
        for i, r in enumerate(rows, 1):
            txt += f"{i}. {r.title} (+{r.total_delta})\n"
        return txt + "\n"

    report = f"📊 **YouTube 即時戰情室** (更新: {current_time_str})\n\n"
    report += get_top("NOW() - INTERVAL 15 MINUTE", "🚀 最近 15 分鐘飆升")
    report += get_top("NOW() - INTERVAL 1 HOUR", "🔥 最近 1 小時熱門")
    report += get_top("CURDATE()", "📅 今日累計 (00:00~Now)")
    
    # 週與月可以類推
    
    return report

def update_discord_message(webhook_url: str, content: str):
    """
    使用 Webhook 編輯訊息。
    注意：Discord Webhook 預設只能 '發送'。要 '編輯' 必須知道 message_id。
    策略：
    1. 讀取本地 discord_state.json 找 message_id。
    2. 嘗試 PATCH 該 message_id。
    3. 如果失敗 (404/403) 或沒有 ID，則 POST 新訊息並儲存 ID。
    """
    msg_id = None
    if os.path.exists(DISCORD_STATE_FILE):
        try:
            with open(DISCORD_STATE_FILE, "r") as f:
                data = json.load(f)
                msg_id = data.get("message_id")
        except:
            pass

    # 嘗試編輯
    if msg_id:
        patch_url = f"{webhook_url}/messages/{msg_id}"
        resp = requests.patch(patch_url, json={"content": content})
        if resp.status_code in [200, 204]:
            print("[Discord] Message updated.")
            return
        else:
            print(f"[Discord] Edit failed ({resp.status_code}), sending new message...")

    # 發送新訊息 (當編輯失敗或第一次執行)
    # 必須加上 ?wait=true 才能在回應中拿到 message_id
    post_url = f"{webhook_url}?wait=true"
    resp = requests.post(post_url, json={"content": content})
    if resp.status_code in [200, 201]:
        new_msg_id = resp.json().get("id")
        with open(DISCORD_STATE_FILE, "w") as f:
            json.dump({"message_id": new_msg_id}, f)
        print(f"[Discord] New message sent. ID: {new_msg_id}")

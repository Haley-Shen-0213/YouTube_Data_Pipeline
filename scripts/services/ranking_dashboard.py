import requests
import pymysql
from datetime import datetime, timedelta, date, timezone
from dateutil.relativedelta import relativedelta
from scripts.db.db import get_engine
from sqlalchemy import text, bindparam

def get_dashboard_status(conn, category):
    """查詢特定榜單目前的 Message ID"""
    sql = text("SELECT message_id FROM discord_ranking_dashboard WHERE category = :category")
    result = conn.execute(sql, {"category": category}).fetchone()
    if result and result[0]:
        return result[0]
    return None

def update_dashboard_status(conn, category, msg_id):
    """
    更新資料庫中的 Message ID。
    last_updated_at 會由 MySQL 的 ON UPDATE CURRENT_TIMESTAMP 自動維護，
    所以這裡不需要寫入時間。
    """
    sql = text("""
        UPDATE discord_ranking_dashboard 
        SET message_id = :msg_id
        WHERE category = :category
    """)
    conn.execute(sql, {"msg_id": msg_id, "category": category})
    conn.commit()

def get_time_ranges(conn, category):
    """
    根據分類計算「本期」與「上期」的時間範圍
    回傳: (curr_start, curr_end, prev_start, prev_end, period_name)
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
    if category == '15min':
        # 15min: 直接抓取資料庫中最新的兩個 captured_at 時間點
        sql = text("SELECT DISTINCT captured_at FROM fact_video_velocity ORDER BY captured_at DESC LIMIT 2")
        rows = conn.execute(sql).fetchall()
        
        if not rows:
            raise ValueError("資料庫中沒有任何數據")
            
        curr_time = rows[0][0]
        
        # 判斷是否有上一期資料
        if len(rows) > 1:
            prev_time = rows[1][0]
        else:
            # 如果只有一筆資料，上一期就暫時設為跟本期一樣 (成長值會顯示為 0)
            prev_time = curr_time

        curr_start = curr_time
        curr_end = curr_time
        prev_start = prev_time
        prev_end = prev_time

        # 顯示用的時間 (為了讓人類看懂，這裡手動 +8 小時轉成台灣時間顯示)
        display_time = (curr_time + timedelta(hours=8)).strftime('%H:%M')
        
        return curr_time, curr_time, prev_time, prev_time, f"最新數據 ({display_time})"

    elif category == 'hourly':
        # 小時榜: 本小時 (XX:00:00 ~ XX:59:59) vs 上一小時完整區間
        # 例如現在 07:34，curr 就是 07:00:00 ~ 07:59:59
        curr_start = now.replace(minute=0, second=0, microsecond=0)
        # 設定為本小時的最後一秒 (例如 07:59:59)
        curr_end = curr_start + timedelta(hours=1) - timedelta(seconds=1)
        
        # 上一小時 (例如 06:00:00 ~ 06:59:59)
        prev_start = curr_start - timedelta(hours=1)
        prev_end = curr_start - timedelta(seconds=1)
        
        return curr_start, curr_end, prev_start, prev_end, "本小時累積"

    elif category == 'daily':
        # 日榜: 今天 (00:00:00 ~ 23:59:59) vs 昨天整天
        curr_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        # 設定為今天的最後一秒
        curr_end = curr_start + timedelta(days=1) - timedelta(seconds=1)
        
        prev_start = curr_start - timedelta(days=1)
        prev_end = curr_start - timedelta(seconds=1)
        return curr_start, curr_end, prev_start, prev_end, "本日累積"

    elif category == 'weekly':
        # 周榜: 本周 (周一 00:00 ~ 周日 23:59) vs 上周
        today = now.today()
        start_of_week = today - timedelta(days=today.weekday()) # Monday
        curr_start = datetime.combine(start_of_week, datetime.min.time())
        # 設定為本週日的最後一秒 (週一 + 7天 - 1秒)
        curr_end = curr_start + timedelta(weeks=1) - timedelta(seconds=1)
        
        prev_start = curr_start - timedelta(weeks=1)
        prev_end = curr_start - timedelta(seconds=1)
        return curr_start, curr_end, prev_start, prev_end, "本週累積"

    elif category == 'monthly':
        # 月榜: 本月 (1號 ~ 月底) vs 上個月整月
        today = now.today()
        curr_start = datetime(today.year, today.month, 1)
        
        # 計算下個月1號，再減1秒即為本月月底
        next_month = curr_start + relativedelta(months=1)
        curr_end = next_month - timedelta(seconds=1)
        
        # 上個月
        prev_month_date = today - relativedelta(months=1)
        prev_start = datetime(prev_month_date.year, prev_month_date.month, 1)
        # 上個月底 = 本月1號 - 1秒
        prev_end = curr_start - timedelta(seconds=1)
        return curr_start, curr_end, prev_start, prev_end, "本月累積"
    
    else:
        raise ValueError(f"未知的分類: {category}")

def get_ranking_data(conn, category):
    """取得排行榜數據"""
    # 1. 取得時間範圍
    curr_start, curr_end, prev_start, prev_end, period_desc = get_time_ranges(conn, category)

    # 2. Step 1: 抓取本期 Top 10 (根據 delta_views)
    # 使用 SUM 聚合，這樣無論是 15min (單點) 還是 hourly (區間) 都能適用
    sql_top = text("""
        SELECT 
            v.video_id, 
            v.video_title,
            v.is_short,
            SUM(f.delta_views) as curr_views,
            SUM(f.delta_likes) as curr_likes,
            SUM(f.delta_comments) as curr_comments            
        FROM fact_video_velocity f
        JOIN dim_video v ON f.video_id = v.video_id
        WHERE f.captured_at >= :curr_start AND f.captured_at <= :curr_end
        GROUP BY v.video_id, v.video_title
        HAVING curr_views > 0
        ORDER BY curr_views DESC
        LIMIT 10
    """)
    
    top_rows = conn.execute(sql_top, {
        "curr_start": curr_start, 
        "curr_end": curr_end
    }).fetchall()

    if not top_rows:
        return [], period_desc

    # 3. 整理 Step 1 的結果，並準備 ID 列表給 Step 2
    video_ids = []
    ranking_map = {}
    
    for row in top_rows:
        vid = row[0]
        video_ids.append(vid)
        ranking_map[vid] = {
            "video_id": vid,
            "title": row[1],
            "is_short": row[2],      # 補上 is_short
            "curr_views": row[3],
            "curr_likes": row[4],
            "curr_comments": row[5],
            "prev_views": 0,
            "prev_likes": 0,
            "prev_comments": 0
        }

    # 4. Step 2: 根據 ID 抓取上期數據
    # 使用 bindparam(expanding=True) 來處理 IN 列表
    sql_prev = text("""
        SELECT 
            video_id,
            SUM(delta_views) as prev_views,
            SUM(delta_likes) as prev_likes,
            SUM(delta_comments) as prev_comments
        FROM fact_video_velocity
        WHERE captured_at >= :prev_start 
          AND captured_at <= :prev_end
          AND video_id IN :vids
        GROUP BY video_id
    """).bindparams(bindparam('vids', expanding=True))

    prev_rows = conn.execute(sql_prev, {
        "prev_start": prev_start,
        "prev_end": prev_end,
        "vids": video_ids
    }).fetchall()

    # 5. Step 3: 合併數據
    for row in prev_rows:
        vid = row[0]
        if vid in ranking_map:
            ranking_map[vid]["prev_views"] = row[1]
            ranking_map[vid]["prev_likes"] = row[2]
            ranking_map[vid]["prev_comments"] = row[3]

    # 6. Step 4: 計算差異與百分比，並輸出最終列表
    final_list = []
    
    # 依照 Step 1 的順序 (video_ids) 輸出，確保排名正確
    for vid in video_ids:
        data = ranking_map[vid]
        
        # 輔助函式：計算 diff 和 pct
        def calc_metrics(curr, prev):
            diff = curr - prev
            if prev == 0:
                pct = 100.0 if curr > 0 else 0.0
            else:
                pct = ((curr - prev) / prev) * 100
            return {"curr": curr, "diff": diff, "pct": pct}

        final_list.append({
            "video_id": data["video_id"],
            "title": data["title"],
            "is_short": data["is_short"], 
            "metrics": {
                "views": calc_metrics(data["curr_views"], data["prev_views"]),
                "likes": calc_metrics(data["curr_likes"], data["prev_likes"]),
                "comments": calc_metrics(data["curr_comments"], data["prev_comments"])
            }
        })

    return final_list, period_desc

def format_discord_message(category, period_desc, top_videos):
    """
    將數據格式化為 Discord 訊息內容
    """
    if not top_videos:
        return f"**📊 YouTube 流量飆升榜 - {period_desc}**\n目前沒有數據變化。"
        
    # 定義 UTC+8 時區
    tz_taipei = timezone(timedelta(hours=8))
    msg = f"**📊 YouTube 流量飆升榜 - {period_desc}**\n"
    msg += f"更新時間: {datetime.now(tz_taipei).strftime('%Y-%m-%d %H:%M')}\n\n"
    
    rank_emojis = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    
    for i, video in enumerate(top_videos):
        emoji = rank_emojis[i] if i < len(rank_emojis) else f"{i+1}."
        
        title = video['title']
        vid = video['video_id']
        is_short = video.get('is_short', 0) # 預設為 0 以防萬一

        # --- 處理標題與連結 ---
        if is_short == 1:
            # Shorts: 網址用 /shorts/，標題只取第一個 # 之前
            video_url = f"https://youtube.com/shorts/{vid}"
            if '#' in title:
                title = title.split('#')[0].strip()
        else:
            # 一般影片: 網址用 youtu.be，標題只取第一個 # 之前
            video_url = f"https://youtu.be/{vid}"
            if '#' in title:
                title = title.split('#')[0].strip()
            
        
        # --- 處理指標數據 ---
        m = video['metrics']
        
        def fmt_stat(data):
            curr = data['curr']
            diff = data['diff']
            pct = data['pct']
            
            base_str = f"+{curr:,}"
            
            if diff == 0:
                return base_str
            
            trend = "📈" if diff > 0 else "📉"
            diff_sign = "+" if diff > 0 else ""
            
            return f"本期數據：{base_str} (趨勢：{trend}，差異值：{diff_sign}{diff:,}，差異百分比：{diff_sign}{pct:.0f}%)"

        view_str = fmt_stat(m['views'])
        like_str = fmt_stat(m['likes'])
        comment_str = fmt_stat(m['comments'])

        msg += f"{emoji} **[{title}]({video_url})**\n"
        # 因為字串變長了，建議每個指標換行顯示，不然會太擠
        msg += f"   👁️ {view_str}\n"
        msg += f"   👍 {like_str}\n"
        msg += f"   💬 {comment_str}\n"
        
    return msg

def send_discord_notification(webhook_url, content, message_id=None):
    """發送或編輯 Discord 訊息"""
    if not webhook_url:
        return None

    data = {"content": content}
    try:
        if message_id:
            url = f"{webhook_url}/messages/{message_id}"
            response = requests.patch(url, json=data)
            if response.status_code == 404:
                message_id = None
            else:
                response.raise_for_status()
        
        if not message_id:
            response = requests.post(webhook_url + "?wait=true", json=data)
            response.raise_for_status()
            return response.json().get('id')
            
        return message_id
    except Exception as e:
        print(f"Discord API Error: {e}")
        return None

def run_ranking_update(category, settings=None, cfg=None, **kwargs):
    """
    主程式：更新排行榜
    支援多頻道分流
    """
    config = settings or cfg
    
    if config is None:
        from scripts.utils.env import load_settings
        config = load_settings()
    
    # --- Webhook 路由邏輯 ---
    # 優先尋找專屬的 Webhook，如果找不到，則使用預設的 YOUTUBE_TOP10_WEBHOOK
    
    # 定義分類與環境變數的對應關係
    # 您可以在 .env 設定:
    # WEBHOOK_REALTIME=https://discord... (給 15min, hourly)
    # WEBHOOK_SUMMARY=https://discord...  (給 daily, weekly, monthly)
    # 或者針對每一個都設定 WEBHOOK_15MIN, WEBHOOK_DAILY...
    
    webhook_key_map = {
        '15min': 'WEBHOOK_REALTIME',
        'hourly': 'WEBHOOK_REALTIME',
        'daily': 'WEBHOOK_SUMMARY',
        'weekly': 'WEBHOOK_SUMMARY',
        'monthly': 'WEBHOOK_SUMMARY'
    }
    
    # 1. 先找專屬對應 (例如 WEBHOOK_REALTIME)
    target_env_key = webhook_key_map.get(category)
    webhook_url = config.get(target_env_key)
    
    # 2. 如果沒設定專屬的，嘗試找更精細的 (例如 WEBHOOK_15MIN)
    if not webhook_url:
         specific_key = f"WEBHOOK_{category.upper()}"
         webhook_url = config.get(specific_key)
    
    if not webhook_url:
        print(f"[{category}] ❌ 略過: 未設定任何可用的 Webhook (找不到 {target_env_key} 或 YOUTUBE_TOP10_WEBHOOK)")
        return

    engine = get_engine()
    with engine.connect() as conn:
        try:
            top_videos, period_desc = get_ranking_data(conn, category)
            message_content = format_discord_message(category, period_desc, top_videos)
            
            # 取得該分類目前紀錄的 message_id
            msg_id = get_dashboard_status(conn, category)
            
            # 發送請求
            new_msg_id = send_discord_notification(webhook_url, message_content, msg_id)
            
            if new_msg_id and new_msg_id != msg_id:
                update_dashboard_status(conn, category, new_msg_id)
                print(f"[{category}] ✅ 看板已建立/更新 (ID: {new_msg_id}) -> {target_env_key or 'Default'}")
            else:
                print(f"[{category}] ✅ 看板已更新")
                
        except Exception as e:
            print(f"[{category}] ❌ 更新失敗: {e}")


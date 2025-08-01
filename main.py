import base64
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime
import json
import os
import argparse

def job(event, context):
    # --- バッチ番号受信（Cloud Functions用） ---
    batch_index = 0
    if "data" in event:
        try:
            message = base64.b64decode(event["data"]).decode("utf-8")
            payload = json.loads(message)
            batch_index = int(payload.get("batch_index", 0))
        except Exception as e:
            print("バッチインデックスの取得失敗", e)

    print(f"処理対象バッチ: {batch_index}")

    # --- BQ設定 ---
    project_id = "hogeticlab-legs-prd"
    dataset_id = "z_personal_morikawa"
    source_table = f"{project_id}.{dataset_id}.pixiv_search_middle_class_ip_name_url"
    destination_table = f"{project_id}.{dataset_id}.pixiv_detail_info"
    client = bigquery.Client(project=project_id)

    limit = 100
    # limit = 5
    offset = batch_index * limit

    # --- URL取得クエリ ---
    query = f"""
    SELECT URL FROM `{source_table}`
    WHERE URL IS NOT NULL
    ORDER BY URL
    LIMIT {limit} OFFSET {offset}
    """
    urls = [row.URL for row in client.query(query).result()]
    print(f"対象URL数: {len(urls)}")

    # --- 詳細取得関数 ---
    def parse_pixiv_detail(url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
        except Exception as e:
            print(f"URL取得失敗：{url} エラー：{e}")
            return None

        soup = BeautifulSoup(res.text, 'html.parser')
        article = soup.find('article')
        if not article:
            return None

        info_div = article.find('div', class_='w-full')
        if not info_div:
            return None

        title = info_div.find('h1').text if info_div.find('h1') else ''
        sub_tag = article.select_one('p.text-text3.typography-12')
        sub_title = sub_tag.text.strip() if sub_tag else ''
        book_tag = article.find('div', class_='text-text3 typography-14')
        bookmark = book_tag.text.strip() if book_tag else ''
        summary_tag = info_div.find('div', class_='text-text2')
        summary = summary_tag.text.strip() if summary_tag else ''

        illust_view_url = novel_view_url = illust_post_url = novel_post_url = ''
        for link in info_div.find_all('a', href=True):
            text = link.get_text(strip=True)
            href = link['href']
            if "イラストを見る" in text:
                illust_view_url = href
            elif "小説を読む" in text:
                novel_view_url = href
            elif "イラストを投稿する" in text:
                illust_post_url = href
            elif "小説を投稿する" in text:
                novel_post_url = href

        stats = {"view_count": None, "comment_count": None, "works_count": None}
        right_section = soup.find('div', class_='sc-362d9791-0 jobEfL')
        if right_section:
            numbers = []
            for item in right_section.select('li'):
                div = item.select_one('div.typography-14')
                if div:
                    try:
                        numbers.append(int(div.get_text(strip=True).replace(",", "")))
                    except ValueError:
                        continue
            if len(numbers) >= 3:
                stats["view_count"], stats["comment_count"], stats["works_count"] = numbers[:3]

        return {
            "url": url,
            "title": title,
            "sub_title": sub_title,
            "bookmark": bookmark,
            "summary": summary,
            "illust_view_url": illust_view_url,
            "novel_view_url": novel_view_url,
            "illust_post_url": illust_post_url,
            "novel_post_url": novel_post_url,
            "view_count": stats["view_count"],
            "comment_count": stats["comment_count"],
            "works_count": stats["works_count"]
        }

    # --- URLをシャッフルして処理 ---
    random.shuffle(urls)
    rows_to_insert = []

    for idx, url in enumerate(urls, start=1):
        wait_sec = round(random.uniform(1.5, 5.0), 2)
        print(f"[{idx}/{len(urls)}] 処理中: {url} (待機: {wait_sec}s)")
        time.sleep(wait_sec)

        detail = parse_pixiv_detail(url)
        if detail:
            detail["loaded_at"] = datetime.utcnow().isoformat()
            rows_to_insert.append(detail)
        else:
            print(f"スキップ: {url}")

        if idx % 100 == 0:
            rest = round(random.uniform(60, 120), 2)
            print(f"{idx}件完了 → 長めに間を空ける {rest}秒")
            time.sleep(rest)

    # --- BigQueryへの書き込み ---
    if rows_to_insert:
        errors = client.insert_rows_json(destination_table, rows_to_insert)
        if errors:
            print("BigQuery挿入エラー:", errors)
        else:
            print(f"{len(rows_to_insert)} 件のデータを正常に挿入しました")
    else:
        print("登録対象データなし（すべて取得失敗）")

# --- CLI対応（ローカル実行用） ---
def main():
    parser = argparse.ArgumentParser(description="Pixiv詳細取得バッチ")
    parser.add_argument("--batch_index", type=int, default=0, help="処理対象バッチ番号 (0開始)")
    args = parser.parse_args()

    fake_event = {
        "data": base64.b64encode(json.dumps({
            "batch_index": args.batch_index
        }).encode("utf-8"))
    }

    job(event=fake_event, context=None)

if __name__ == "__main__":
    main()

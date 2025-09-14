import argparse
import os
import random
import time
from datetime import timezone, datetime

import requests
from bs4 import BeautifulSoup  # type: ignore
from google.cloud import bigquery

# タイトルから数値を見つけ出す（HTMLの変化対応）

def extract_text_from_title(soup, text_label):
    try:
        tag = soup.find(
            lambda t: (t.name == 'li' or t.name == 'a') and
                      t.has_attr('title') and
                      text_label in t.get('title', '')
        )

        if tag:
            # 見つかったタグの中から、数値テキストを持つdivを探す
            div = tag.find('div', class_='typography-14')
            if div and div.text:
                # カンマを削除して整数に変換
                return int(div.text.strip().replace(",", ""))
        return None
    except (ValueError, TypeError):
        return None

# --- メインのスクレイピング処理 ---

def run_scraping_job(batch_index):
    # 環境変数から設定を読み込む
    project_id = os.getenv("GCP_PROJECT_ID", "hogeticlab-legs-prd")
    dataset_id = os.getenv("BIGQUERY_DATASET", "z_personal_morikawa")
    
    print(f"処理開始。対象バッチ: {batch_index}")
    print(f"Project ID: {project_id}, Dataset ID: {dataset_id}")

    client = bigquery.Client(project=project_id)
    source_table = f"{project_id}.{dataset_id}.pixiv_search_middle_class_ip_name_url"
    destination_table = f"{project_id}.{dataset_id}.pixiv_detail_info"
    
    limit = 100
    offset = batch_index * limit
    
    # 1. 処理対象のURLバッチを取得
    query_get_urls = f"SELECT URL FROM `{source_table}` WHERE URL IS NOT NULL ORDER BY URL LIMIT {limit} OFFSET {offset}"
    
    try:
        urls_to_process = [row.URL for row in client.query(query_get_urls).result()]
        print(f"対象URL数: {len(urls_to_process)}件を取得しました。")
        if not urls_to_process:
            print("処理対象のURLがありません。バッチ処理を終了します。")
            return
    except Exception as e:
        print(f"BigQueryからのURL取得に失敗しました: {e}")
        return

    # 2. 参照先テーブルをチェックし、既に存在するURLを特定する
    try:
        query_check_existing = f"""
            SELECT URL
            FROM `{destination_table}`
            WHERE URL IN UNNEST(@urls)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("urls", "STRING", urls_to_process),
            ]
        )
        existing_urls_result = client.query(query_check_existing, job_config=job_config).result()
        existing_urls = {row.URL for row in existing_urls_result}
        print(f"宛先テーブルに既に存在するURL: {len(existing_urls)}件")
    except Exception as e:
        print(f"既存URLのチェック中にエラーが発生しました: {e}")
        # チェックに失敗した場合は、安全のため処理を中断
        return

    # 3. 処理対象から既存のURLを除外する
    urls_to_scrape = [url for url in urls_to_process if url not in existing_urls]
    print(f"新規にスクレイピングするURL: {len(urls_to_scrape)}件")
    
    if not urls_to_scrape:
        print("新規に処理するURLがありません。バッチ処理を終了します。")
        return

    # 4. 残りのURLをスクレイピング
    random.shuffle(urls_to_scrape)
    rows_to_insert = []
    total_urls = len(urls_to_scrape)
    
    for idx, url in enumerate(urls_to_scrape, start=1):
        wait_sec = round(random.uniform(3.0, 8.0), 2)
        print(f"[{idx}/{total_urls}] 処理中: {url} (待機: {wait_sec}s)")
        time.sleep(wait_sec)
        
        detail = parse_pixiv_detail(url)
        if detail:
            detail["loaded_at"] = datetime.now(timezone.utc).isoformat()
            rows_to_insert.append(detail)
        else:
            print(f"スキップ: {url} の詳細情報が取得できませんでした。")

        # 50リクエストごとに長めの待機時間を追加
        if idx % 50 == 0 and idx < total_urls:
            rest = round(random.uniform(90.0, 150.0), 2)
            print(f"{idx}件完了 → 長めに間を空ける {rest}秒")
            time.sleep(rest)

    # 5. 新規データをBigQueryに挿入
    if rows_to_insert:
        errors = client.insert_rows_json(destination_table, rows_to_insert)
        if not errors:
            print(f"{len(rows_to_insert)} 件のデータを正常に挿入しました。")
        else:
            print(f"BigQuery挿入エラー: {errors}")
    else:
        print("登録対象の新規データはありませんでした。")

def parse_pixiv_detail(url):
    # 詳細ページのHTMLをパースして情報を抽出
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
    except Exception as e:
        print(f"URL取得失敗：{url} エラー：{e}")
        return None

    soup = BeautifulSoup(res.text, 'html.parser')
    
    # --- 主要情報の抽出 ---
    title_tag = soup.find('h1')
    title = title_tag.text.strip() if title_tag else ''

    sub_title_tag = soup.select_one('p.text-text3.typography-12')
    sub_title = sub_title_tag.text.strip() if sub_title_tag else ''
    
    bookmark_tag = soup.find('div', class_='text-text3 typography-14')
    bookmark = bookmark_tag.text.strip() if bookmark_tag else ''
    
    summary_tag = soup.find('div', class_='text-text2')
    summary = summary_tag.text.strip() if summary_tag else ''
    
    # --- 各カウント数の抽出 ---
    stats_section = soup.find('ul', class_='mt-16')
    if stats_section:
        view_count = extract_text_from_title(stats_section, "閲覧数")
        comment_count = extract_text_from_title(stats_section, "コメント数")
        works_count = extract_text_from_title(stats_section, "作品数")
    else:
        view_count, comment_count, works_count = None, None, None

    return {
        "url": url,
        "title": title,
        "sub_title": sub_title,
        "bookmark": bookmark,
        "summary": summary,
        "view_count": view_count,
        "comment_count": comment_count,
        "works_count": works_count,
    }

#   --- 実行処理 ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pixiv百科事典スクレイピングバッチ")
    parser.add_argument("--batch_index", type=int, default=0, help="処理するバッチ番号")
    args = parser.parse_args()
    run_scraping_job(args.batch_index)

import argparse
import os
import random
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup  # type: ignore
from google.cloud import bigquery

def parse_pixiv_detail(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }
    # 時間を確認する
    start_time = time.time()
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        print(f"URL取得成功: {url} ({time.time() - start_time:.2f}s)")
    except Exception as e:
        print(f"URL取得失敗：{url} エラー：{e}")
        print(f"URL取得失敗: {url} エラー: {e} ({time.time() - start_time:.2f}s)")
        return None
    
    soup = BeautifulSoup(res.text, 'html.parser')
    article = soup.find('article')
    if not article:
        print(f"解析スキップ: articleタグが見つかりません。 URL: {url}")
        return None

    info_div = article.find('div', class_='w-full')
    if not info_div:
        print(f"解析スキップ: info_divが見つかりません。 URL: {url}")
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
        if "イラストを見る" in text: illust_view_url = href
        elif "小説を読む" in text: novel_view_url = href
        elif "イラストを投稿する" in text: illust_post_url = href
        elif "小説を投稿する" in text: novel_post_url = href

    stats = {"view_count": None, "comment_count": None, "works_count": None}
    
    def extract_count(text_label):
        found_tag = soup.find(lambda tag: (tag.name == 'li' or tag.name == 'a') and tag.has_attr('title') and text_label in tag['title'])
        if not found_tag:
            return None
        
        count_div = found_tag.find('div', class_='typography-14')
        if not count_div:
            return None
        
        try:
            return int(count_div.get_text(strip=True).replace(",", ""))
        except (ValueError, AttributeError):
            return None

    stats["view_count"] = extract_count("閲覧数")
    stats["comment_count"] = extract_count("コメント数")
    stats["works_count"] = extract_count("作品数")

    return {
        "url": url, "title": title, "sub_title": sub_title, "bookmark": bookmark,
        "summary": summary, "illust_view_url": illust_view_url, "novel_view_url": novel_view_url,
        "illust_post_url": illust_post_url, "novel_post_url": novel_post_url,
        "view_count": stats["view_count"], "comment_count": stats["comment_count"],
        "works_count": stats["works_count"]
    }

def run_scraping_job():
    ## メインの処理
    project_id = os.getenv("GCP_PROJECT_ID", "hogeticlab-legs-prd")
    dataset_id = os.getenv("BIGQUERY_DATASET", "z_personal_morikawa")
    
    print(f"処理開始")
    print(f"Project ID: {project_id}, Dataset ID: {dataset_id}")

    source_table = f"{project_id}.{dataset_id}.pixiv_search_middle_class_ip_name_url"
    destination_table = f"{project_id}.{dataset_id}.pixiv_detail_info"
    client = bigquery.Client(project=project_id)
    
    limit = 600
    # offset = batch_index * limit
    
    # URLとmiddle_class_ip_nameを固定の順序で取得
   # query = f"SELECT URL, middle_class_ip_name FROM `{source_table}` WHERE URL IS NOT NULL AND (ORDER BY URL LIMIT {limit} OFFSET {offset}"
    query = f"""
        SELECT
            URL,
            middle_class_ip_name
        FROM
            `{source_table}`
        WHERE
            -- まだ一度も取得されていない新規のデータ
            URL IS NOT NULL
            AND ( -- 7日以上前に最終アクセスされたデータ
                last_scraped_at IS NULL
                OR
                last_scraped_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP("Asia/Tokyo"), INTERVAL 7 DAY)
            )
        ORDER BY
            RAND()
        LIMIT {limit}
        """

    try:
        # タプルでリスト化
        source_data = [(row.URL, row.middle_class_ip_name) for row in client.query(query).result()]
        print(f"対象データ数: {len(source_data)}")
    except Exception as e:
        print(f"BigQueryからのデータ取得に失敗しました: {e}")
        return

    # 取得したデータの処理順序をランダム化
    random.shuffle(source_data)
    
    rows_to_insert = []
    for idx, (url, middle_class_ip_name) in enumerate(source_data, start=1):
        wait_sec = round(random.uniform(4.0, 10.0), 2)
        print(f"[{idx}/{len(source_data)}] 処理中: {url} (待機: {wait_sec}s)")
        time.sleep(wait_sec)
        
        detail = parse_pixiv_detail(url)
        if detail:
            detail["loaded_at"] = datetime.now(timezone.utc).isoformat()
            detail["middle_class_ip_name"] = middle_class_ip_name
            rows_to_insert.append(detail)
        else:
            print(f"スキップ: {url}")
        
        if idx % 50 == 0:
            rest = round(random.uniform(120, 240), 2)
            print(f"{idx}件完了 → 長めに間を空ける {rest}秒")
            time.sleep(rest)

    if rows_to_insert:
        errors = client.insert_rows_json(destination_table, rows_to_insert)
        if not errors:
            print(f"{len(rows_to_insert)} 件のデータを正常に挿入しました")
        else:
            print("BigQuery挿入エラー:", errors)
    else:
        print("登録対象データなし（すべて取得失敗）")

    # 処理に成功したURLのリストを作成
    successful_urls = [row["url"] for row in rows_to_insert]

    if successful_urls:
        # 処理に成功したURLのlast_scraped_atを現在時刻で更新する
        
        now_utc_iso = datetime.now(timezone.utc).isoformat()
        
        # BigQueryのSQLで使用するために、URLリストを文字列にフォーマット
        # 例: "'url1', 'url2', 'url3'"
        url_list_str = ", ".join(f"'{url}'" for url in successful_urls)
        
        update_query = f"""
            UPDATE `{source_table}`
            SET last_scraped_at = TIMESTAMP('{now_utc_iso}')
            WHERE URL IN ({url_list_str})
        """
        
        try:
            # クエリを実行
            update_job = client.query(update_query)
            update_job.result() # 完了を待つ
            print(f"ソーステーブルのlast_scraped_atを {len(successful_urls)} 件更新しました。")
        except Exception as e:
            print(f"ソーステーブルの更新に失敗しました: {e}")


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="pixiv百科事典スクレイピングバッチ")
    # parser.add_argument("--batch_index", type=int, default=0, help="処理対象のバッチ番号 (0から始まる)")
    # args = parser.parse_args()
    # run_scraping_job(args.batch_index)
    run_scraping_job()
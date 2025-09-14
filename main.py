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
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
    except Exception as e:
        print(f"URL取得失敗：{url} エラー：{e}")
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
        # 閲覧数、コメント数、作品数のliタグを柔軟に検索する
        found_tag = soup.find(lambda tag: (tag.name == 'li' or tag.name == 'a') and tag.has_attr('title') and text_label in tag['title'])
        if not found_tag:
            return None
        
        # 数値が含まれるdivタグを探す
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

def run_scraping_job(batch_index):
    """
    スクレイピングジョブのメイン処理。
    BigQueryからURLを取得し、スクレイピングして結果を保存する。
    """
    project_id = os.getenv("GCP_PROJECT_ID", "hogeticlab-legs-prd")
    dataset_id = os.getenv("BIGQUERY_DATASET", "z_personal_morikawa")
    
    print(f"処理開始。対象バッチ: {batch_index}")
    print(f"Project ID: {project_id}, Dataset ID: {dataset_id}")

    source_table = f"{project_id}.{dataset_id}.pixiv_search_middle_class_ip_name_url"
    destination_table = f"{project_id}.{dataset_id}.pixiv_detail_info"
    client = bigquery.Client(project=project_id)
    
    limit = 100
    offset = batch_index * limit
    
    # URLを固定の順序で取得
    query = f"SELECT URL FROM `{source_table}` WHERE URL IS NOT NULL ORDER BY URL LIMIT {limit} OFFSET {offset}"
    
    try:
        urls = [row.URL for row in client.query(query).result()]
        print(f"対象URL数: {len(urls)}")
    except Exception as e:
        print(f"BigQueryからのURL取得に失敗しました: {e}")
        return # URLが取得できなければ処理を終了

    # 取得したURLの処理順序をランダム化
    random.shuffle(urls)
    
    rows_to_insert = []
    for idx, url in enumerate(urls, start=1):
        # 待機時間を長くして、サーバーへの負荷を軽減
        wait_sec = round(random.uniform(3.0, 8.0), 2)
        print(f"[{idx}/{len(urls)}] 処理中: {url} (待機: {wait_sec}s)")
        time.sleep(wait_sec)
        
        detail = parse_pixiv_detail(url)
        if detail:
            # 現在時刻
            detail["loaded_at"] = datetime.now(timezone.utc).isoformat()
            rows_to_insert.append(detail)
        else:
            print(f"スキップ: {url}")
        
        # 50件ごとにさらに長めに待機
        if idx % 50 == 0:
            rest = round(random.uniform(90, 150), 2)
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="pixiv百科事典スクレイピングバッチ")
    parser.add_argument("--batch_index", type=int, default=0, help="処理対象のバッチ番号 (0から始まる)")
    args = parser.parse_args()
    run_scraping_job(args.batch_index)


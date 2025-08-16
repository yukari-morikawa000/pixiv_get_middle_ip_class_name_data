# Python 3.12 の軽量イメージをベースにする
FROM python:3.12-slim

# 作業ディレクトリを設定
WORKDIR /app

# 必要なファイルをコピー
COPY requirements.txt .
COPY main.py .

# 依存ライブラリをインストール
RUN pip install --no-cache-dir -r requirements.txt

# Cloud Run Jobがコンテナを実行する

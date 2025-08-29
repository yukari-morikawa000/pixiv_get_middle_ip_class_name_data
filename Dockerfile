FROM python:3.12-slim

# 作業ディレクトリ
WORKDIR /app

# 必要なファイルをコピー
COPY requirements.txt .
COPY main.py .

# 依存ライブラリをインストール
RUN pip install --no-cache-dir -r requirements.txt

# コンテナ実行時のデフォルトコマンドを指定
CMD ["python", "main.py"]
FROM python:3.12-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

# Webサーバーは不要なのでEXPOSEは削除
# CMDで直接Pythonスクリプトを実行する
CMD ["python", "main.py"]

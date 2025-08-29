## Pixiv百科事典データ収集バッチ
### 1. 概要
このプロジェクトは、pixiv百科事典からmiddle_class_ip_nameから詳細データをスクレイピングし、結果をBigQueryに保存するためのバッチ処理システムです。

処理はDockerコンテナ上で実行され、Cloud Buildによって自動的にビルド・デプロイされます。最終的にDigdagワークフローエンジンから定期的に、または手動で実行されることを想定しています。

### 2. 技術スタック
```
言語: Python 3.12

主要ライブラリ:

google-cloud-bigquery: BigQueryとの連携

google-cloud-logging: 構造化ログの出力

requests: HTTPリクエスト

beautifulsoup4: HTML/XMLのパース

プラットフォーム: Google Cloud Platform (GCP)

Artifact Registry: Dockerイメージの保管

Cloud Build: CI/CD (Dockerイメージのビルドとプッシュ)

BigQuery: データ保存先

Cloud Logging: ログの収集と閲覧

ワークフローエンジン: Digdag

コンテナ技術: Docker
```

### 3. 実行フロー
```
このシステムのCI/CDと実行フローは以下のようになります。

開発とプッシュ: main.py などのソースコードを編集し、Gitリポジトリ (GitHubなど) にプッシュします。

Cloud Buildトリガー: Gitリポジトリへのプッシュをトリガーとして、Cloud Buildが自動的に実行されます。

Dockerイメージのビルド: cloudbuild.yaml の定義に基づき、Dockerfile を使ってPython実行環境を含むDockerイメージがビルドされます。

Artifact Registryへプッシュ: ビルドされたDockerイメージは、バージョン情報（コミットハッシュ）と共にArtifact Registryにプッシュされ、保管されます。

Digdagから実行: Digdagに定義されたワークフローが、指定されたスケジュールまたは手動実行によってトリガーされます。

バッチ処理の実行: DigdagはArtifact Registryから対象のDockerイメージをプルし、コンテナとして起動します。main.py スクリプトがコンテナ内で実行され、スクレイピングとBigQueryへのデータ保存が行われます。
```
### 4. ファイル構成
```
main.py

スクレイピングとBigQueryへのデータ挿入を行うメインスクリプトです。

--batch_index という引数を受け取り、処理対象のデータ範囲を決定します。

GCPプロジェクトIDやデータセットIDは、環境変数 (GCP_PROJECT_ID, BIGQUERY_DATASET) から読み込むように実装されています。

Dockerfile

main.py を実行するためのDockerイメージの定義ファイルです。

Python 3.12の軽量イメージをベースに、必要なライブラリをインストールします。

requirements.txt

Pythonの依存ライブラリ一覧です。

cloudbuild.yaml

Cloud BuildでのCI/CDパイプラインを定義するファイルです。

docker build と docker push コマンドを実行し、Artifact Registryへイメージを登録します。
```

### 5. 実行手順
```
ステップ1: BigQueryの準備
スクレイピング元データ(source_table)と保存先データ(destination_table)となるテーブルをBigQuery内に準備しておきます。

読み取り元テーブル例: hogeticlab-legs-prd.z_personal_morikawa.pixiv_search_middle_class_ip_name_url

書き込み先テーブル例: hogeticlab-legs-prd.z_personal_morikawa.pixiv_detail_info

ステップ2: Cloud Buildの設定
GCPプロジェクトでCloud Build APIを有効化します。

ソースコードを管理するGitリポジトリ（GitHub, Cloud Source Repositories 등）とCloud Buildを連携させ、トリガーを設定します。

特定のブランチへのプッシュなどをトリガーに、cloudbuild.yaml を使ったビルドが実行されるように構成します。

ステップ3: Digdagワークフローの作成
Digdagサーバーに以下の内容でワークフローファイル（例: scraping.dig）を作成し、プロジェクトを登録します。

scraping.dig:

timezone: Asia/Tokyo

 毎日深夜3時に実行するスケジュール設定
schedule:
  daily>: 03:00:00

 _exportセクションで共通の変数を定義
_export:
  # 実行するDockerイメージのパスを指定
  # ご自身の環境に合わせて [ ] の部分を書き換えてください
  docker_image: "asia-northeast1-docker.pkg.dev/[あなたのGCPプロジェクトID]/[リポジトリ名]/[イメージ名]:${session_uuid}" # タグは実行ごとに変えるか、latestに固定
  
GCPのプロジェクトIDとデータセット名を環境変数として設定
  GCP_PROJECT_ID: "hogeticlab-legs-prd"
  BIGQUERY_DATASET: "z_personal_morikawa"

# for_each を使って複数のバッチを並列実行
+main:
  _parallel: true
  for_each>:
    batch_index: [0, 1, 2, 3, 4] # 実行したいバッチ番号のリスト
  _do:
    +run_batch_${batch_index}:
      docker>:
        image: "${docker_image}"
        # 環境変数をコンテナに渡す
        env:
          - GCP_PROJECT_ID=${GCP_PROJECT_ID}
          - BIGQUERY_DATASET=${BIGQUERY_DATASET}
      
      # コンテナ内で実行するコマンド
      command: ["python", "main.py", "--batch_index", "${batch_index}"]


ステップ4: 実行と確認
設定したスケジュールになると、Digdagは指定されたバッチ番号の数だけ並列でタスクを実行します。 実行ログはDigdagのUIおよびGCPのCloud Loggingで確認できます。Cloud Loggingでは、main.pyから出力される構造化ログによって、より詳細な分析が可能です。
```
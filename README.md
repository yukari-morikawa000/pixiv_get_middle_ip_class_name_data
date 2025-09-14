# Pixiv百科事典データ収集バッチ
### 1. 概要
このプロジェクトは、pixiv百科事典から詳細データをスクレイピングし、結果をBigQueryに保存するためのバッチ処理システムです。

処理はDockerコンテナ上で実行され、Cloud Buildによって自動的にビルド・デプロイされます。最終的にDigdagワークフローエンジンからKubernetesクラスタ上で定期的に実行されることを想定しています。

冪等性の担保: このパイプラインは冪等性（べきとうせい）を考慮して設計されています。main.pyスクリプトは、処理を開始する前に宛先テーブルをチェックし、既にデータが存在するURLの処理をスキップします。これにより、手動での再実行やスケジュールの重複があっても、データの重複や無駄な処理を防ぎます。

### 2. 仕様
```
言語: Python 3.12

主要ライブラリ:

google-cloud-bigquery: BigQueryとの連携

requests: HTTPリクエスト

beautifulsoup4: HTMLのパース

プラットフォーム: Google Cloud Platform (GCP)

Artifact Registry: Dockerイメージの保管

Cloud Build: CI/CD (Dockerイメージのビルドとプッシュ)

BigQuery: データ保存先

Google Kubernetes Engine (GKE): コンテナ実行環境

ワークフローエンジン: Digdag

コンテナ仕様:

Docker

Kubernetes (kubectl)
```

### 3. 実行フロー
このシステムのCI/CDと実行フローは以下のようになります。

開発とプッシュ: main.py などのソースコードを編集し、Gitリポジトリ (GitHubなど) にプッシュします。

Cloud Buildトリガー: Gitリポジトリへのプッシュをトリガーとして、Cloud Buildが自動的に実行されます。

Dockerイメージのビルド: cloudbuild.yaml の定義に基づき、Dockerfile を使ってPython実行環境を含むDockerイメージがビルドされます。

Artifact Registryへプッシュ: ビルドされたDockerイメージは、バージョン情報と共にArtifact Registryにプッシュされ、保管されます。

Digdagから実行: Digdagに定義されたワークフローが、指定されたスケジュールまたは手動実行によってトリガーされます。

Kubernetesジョブの作成: Digdagはrun_kube_job.shスクリプトを実行します。このスクリプトはkubectlコマンドを使い、job.yamlのマニフェストを元にKubernetesクラスタ上にJobを作成します。

バッチ処理の実行: KubernetesがArtifact Registryから対象のDockerイメージをプルし、コンテナとして起動します。main.pyスクリプトがコンテナ内で実行され、スクレイピングとBigQueryへのデータ保存が行われます。

### 4. ファイル構成
```
main.py

スクレイピングとBigQueryへのデータ挿入を行うメインスクリプトです。

--batch_index という引数を受け取り、処理対象のデータ範囲を決定します。

Dockerfile

main.py を実行するためのDockerイメージの定義ファイルです。

requirements.txt

Pythonの依存ライブラリ一覧です。

cloudbuild.yaml

Cloud BuildでのCI/CDパイプラインを定義するファイルです。

pixiv_scraping.dig

Digdagのワークフロー定義ファイルです。

4000件のデータを処理するため、40個のバッチタスクを順番に定義し、run_kube_job.shを呼び出します。

run_kube_job.sh

Digdagから呼び出されるシェルスクリプトです。

kubectlコマンドを実行してKubernetesジョブの作成、監視、ログ取得、後片付けを行います。

job.yaml

KubernetesのJobを定義するマニフェストファイルです。

どのDockerイメージを、どのネームスペースとサービスアカウントで実行するかを定義します。
```

### 5. 実行手順
#### ステップ1: BigQueryの準備
スクレイピング元データ(source_table)と保存先データ(destination_table)となるテーブルをBigQuery内に準備しておきます。

読み取り元テーブル例: hogeticlab-legs-prd.z_personal_morikawa.pixiv_search_middle_class_ip_name_url

書き込み先テーブル例: hogeticlab-legs-prd.z_personal_morikawa.pixiv_detail_info

#### ステップ2: Cloud Buildの設定
GCPプロジェクトでCloud Build APIを有効化します。

ソースコードを管理するGitリポジトリとCloud Buildを連携させ、トリガーを設定します。

特定のブランチへのプッシュなどをトリガーに、cloudbuild.yaml を使ったビルドが実行されるように構成します。

#### ステップ3: Digdagワークフローの作成
Digdagサーバーに、このリポジトリのファイル (pixiv_scraping.dig, run_kube_job.sh, job.yaml) を配置し、プロジェクトを登録します。pixiv_scraping.digは、4000件のデータを処理するために40個のタスクを直列で実行するように設定されています。

pixiv_scraping.dig (抜粋):

timezone: Asia/Tokyo

#### 毎週月曜のAM4時に定期実行する
schedule:
  weekly>: Mon, 04:00:00

_export:
```
  GCP_PROJECT_ID: "hogeticlab-legs-prd"
  BIGQUERY_DATASET: "z_personal_morikawa"
```

+main:
  #### 30個のタスクを順番に実行する
```
  +run_kube_job_0:
    _export: {batch_index: 0, JOB_NAME: "pixiv-scraping-job-${session_uuid}-0"}
    sh>: ./run_kube_job.sh ${batch_index} ${JOB_NAME} ${GCP_PROJECT_ID} ${BIGQUERY_DATASET}
  +run_kube_job_1:
    _export: {batch_index: 1, JOB_NAME: "pixiv-scraping-job-${session_uuid}-1"}
    sh>: ./run_kube_job.sh ${batch_index} ${JOB_NAME} ${GCP_PROJECT_ID} ${BIGQUERY_DATASET}
```
  #### ... (30まで続く) ...

#### ステップ4: 実行と確認
設定したスケジュールになると、Digdagは定義されたタスクを順番に実行します。
実行ログはDigdagのUIで確認できます。run_kube_job.shスクリプトは、Kubernetes上で実行されたmain.pyのコンテナログを自動で取得するため、スクレイピングの進捗や結果をDigdagのUIから直接確認することが可能です。
# AGENTS.md - telecom-data-ai-handson

## プロジェクト概要

通信会社（テレコム）のネットワーク品質データを題材にした Snowflake AI ハンズオンワークショップ。
生データの取り込みから AI エージェント構築・評価・ML 推論までを一貫して体験する約90分＋オプション40分の教材。

## アーキテクチャ

```
CSV (area_master / network_quality / equipment_status)
  → Snowflake Tables (RAW スキーマ)
    → AI_CLASSIFY / AI_COMPLETE による分類・説明生成 (ANALYTICS.AI_AREA_MASTER)
    → Marketplace 気象データ結合 (ANALYTICS.DAILY_WEATHER)
    → Cortex Search Service (エリア検索)
    → Semantic View (Cortex Analyst)
    → Cortex Agent (Analyst + Search + Web Search, claude-4-sonnet)
    → [オプション] Agent 評価 (answer_correctness / logical_consistency)
    → [オプション] ML モデル (IsolationForest / XGBoost → Model Registry)
```

## ディレクトリ構造

```
telecom-data-ai-handson/
├── csv/
│   ├── area_master.csv            # エリアマスタ（50エリア, 4リージョン）
│   ├── equipment_status.csv       # 設備キャパシティ（スナップショット）
│   └── network_quality.csv        # 通信品質日次データ（2022-01〜2026-02）
├── TELECOM_AI_HANDSON.ipynb       # メイン Snowflake Notebook（全9ステップ）
├── setup_database.sql             # 環境セットアップ SQL
├── model_registry_inference.sql   # Model Registry SQL 推論ワークシート（Step 9 用）
├── eval_config.yaml               # Cortex Agent 評価設定（Step 7 用）
└── AGENTS.md                      # このファイル
```

## Snowflake 環境

- **データベース**: `TELECOM_AI_HANDSON`
- **スキーマ**: `RAW`（生データ）/ `ANALYTICS`（分析用）
- **ウェアハウス**: `COMPUTE_WH`（SMALL）
- **コンピュートプール**: `TELECOM_ML_POOL`（CPU_X64_S, Step 9 用）
- **クロスリージョン推論**: `CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'`

## 主要テーブル

| テーブル | スキーマ | 説明 |
|---------|---------|------|
| `AREA_MASTER` | RAW | エリアマスタ（area_id, area_name, region_name, region_code） |
| `AI_AREA_MASTER` | ANALYTICS | AI 拡張済みエリアマスタ（area_type, area_description を AI で自動付与） |
| `NETWORK_QUALITY` | ANALYTICS | 通信品質日次データ（download/upload速度, レイテンシ, 接続成功率 等） |
| `EQUIPMENT_STATUS` | ANALYTICS | 設備キャパシティ（基地局数, 最大容量, 負荷率, ステータス） |
| `DAILY_WEATHER` | ANALYTICS | 日次気象データ（Marketplace 由来, 華氏→摂氏/インチ→mm 変換済み） |

## 使用する Snowflake AI 機能

| ステップ | 機能 | 用途 |
|:-------:|------|------|
| 1 | AI_CLASSIFY / AI_COMPLETE | エリア種別の自動分類、エリア説明文の生成 |
| 2 | Snowflake Marketplace | Weather Source 気象データの取り込み・変換 |
| 3 | Cortex Search Service | snowflake-arctic-embed-l-v2.0 によるセマンティック検索 |
| 4 | Semantic View (Cortex Analyst) | 3テーブル結合のセマンティックビュー定義 |
| 5 | Cortex Agent | Analyst + Search + Web Search のオーケストレーション（claude-4-sonnet） |
| 6 | Cortex Agent UI | Snowsight 上でエージェントを対話的に操作 |
| 7 | Cortex Agent Evaluations | answer_correctness / logical_consistency 評価【オプション】 |
| 8 | Resource Budgets | タグベースの AI コスト管理 |
| 9 | Snowpark ML / Model Registry | IsolationForest 異常検知 + XGBoost 需要予測【オプション】 |

## コーディング規約

- SQL は Snowflake SQL で記述する
- カラム名・テーブル名は snake_case を使用する
- コメント・ドキュメントは日本語で記述する
- テーブルやビューには必ず COMMENT を付与する
- 集計クエリでは CTE（WITH句）を使って段階的に組み立てる
- 数値は ROUND(value, 2) で小数2桁に丸める
- デフォルトの集計期間は直近1年間とする
- ランキングには QUALIFY + ROW_NUMBER を使用する

## ハンズオン受講者への注意

- Step 2（Marketplace）は事前にデータ取得が必要
- Step 7（Agent Evaluations）はトライアルアカウントでは実行不可（有償契約アカウントが必要）
- Step 9（ML）は Container Runtime 対応の Notebook が必要
- `model_registry_inference.sql` は Step 9 で登録したモデルを SQL ワークシートから推論するためのスクリプト
- `eval_config.yaml` は Step 7 で使用する評価設定ファイル（ステージへのアップロードが必要）

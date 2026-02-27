-- ============================================================
-- TELECOM_AI_HANDSON セットアップスクリプト
-- データベース・ウェアハウス・スキーマの作成
-- GitHubからCSVを取得し、3テーブルを作成
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- STEP 1: クロスリージョン推論を有効化
-- ============================================================
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ============================================================
-- STEP 2: ウェアハウスの作成
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Telecom AI Handson';

-- ============================================================
-- STEP 2b: Container Runtime 用コンピュートプールの作成
--   Notebook を Container Runtime で実行するために必要です。
-- ============================================================
CREATE COMPUTE POOL IF NOT EXISTS TELECOM_ML_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_SUSPEND_SECS = 300
    COMMENT = 'Compute pool for Telecom ML Handson (Container Runtime)';

-- ============================================================
-- STEP 3: データベースの作成
-- ============================================================
CREATE DATABASE IF NOT EXISTS TELECOM_AI_HANDSON;
USE DATABASE TELECOM_AI_HANDSON;

-- ============================================================
-- STEP 4: スキーマの作成
-- ============================================================
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = '生データ格納用スキーマ';
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = '分析・AI処理結果格納用スキーマ';

SHOW SCHEMAS IN DATABASE TELECOM_AI_HANDSON;

-- ============================================================
-- STEP 5: ステージの作成
-- ============================================================
USE SCHEMA TELECOM_AI_HANDSON.RAW;

CREATE OR REPLACE STAGE TELECOM_AI_HANDSON.RAW.HANDSON_RESOURCES
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for Telecom AI Handson resources';

-- ============================================================
-- STEP 6: GitHub連携 — Git Integrationの作成
-- ============================================================
CREATE OR REPLACE API INTEGRATION telecom_handson_git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY TELECOM_AI_HANDSON.RAW.GIT_TELECOM_HANDSON
    API_INTEGRATION = telecom_handson_git_api_integration
    ORIGIN = 'https://github.com/sfc-gh-ttakahashi/telecom-data-ai-handson.git';

-- ============================================================
-- STEP 6b: GitリポジトリからNotebookを作成
--   GitHubリポジトリ上の TELECOM_AI_HANDSON.ipynb を
--   Snowflake Notebook として取り込みます。
--   取り込み後、Snowsight の「Notebooks」からアクセスできます。
-- ============================================================
CREATE OR REPLACE NOTEBOOK TELECOM_AI_HANDSON
    FROM @TELECOM_AI_HANDSON.RAW.GIT_TELECOM_HANDSON/branches/main/
    MAIN_FILE = 'TELECOM_AI_HANDSON.ipynb'
    QUERY_WAREHOUSE = COMPUTE_WH
    COMPUTE_POOL = TELECOM_ML_POOL
    RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME';

-- リポジトリの内容確認
LIST @TELECOM_AI_HANDSON.RAW.GIT_TELECOM_HANDSON/branches/main;

-- ============================================================
-- STEP 7: GitHubからCSVをステージにコピー
-- ============================================================
COPY FILES INTO @TELECOM_AI_HANDSON.RAW.HANDSON_RESOURCES/csv/
    FROM @TELECOM_AI_HANDSON.RAW.GIT_TELECOM_HANDSON/branches/main/csv/
    PATTERN = '.*\.csv$';

-- コピーされたファイルの確認
LIST @TELECOM_AI_HANDSON.RAW.HANDSON_RESOURCES/csv/;

-- ============================================================
-- STEP 8: CSVファイルフォーマットの作成
-- ============================================================
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE FILE FORMAT TELECOM_AI_HANDSON.RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    ENCODING = 'UTF8';

-- ============================================================
-- STEP 9: エリアマスタテーブルの作成とロード
-- ============================================================
CREATE OR REPLACE TABLE TELECOM_AI_HANDSON.RAW.AREA_MASTER (
    AREA_ID       VARCHAR(20)  NOT NULL,
    AREA_NAME     VARCHAR(200) NOT NULL,
    REGION_NAME   VARCHAR(50),
    REGION_CODE   VARCHAR(20)
);

COPY INTO TELECOM_AI_HANDSON.RAW.AREA_MASTER
    (AREA_ID, AREA_NAME, REGION_NAME, REGION_CODE)
FROM @TELECOM_AI_HANDSON.RAW.HANDSON_RESOURCES/csv/area_master.csv
FILE_FORMAT = (FORMAT_NAME = 'TELECOM_AI_HANDSON.RAW.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS TOTAL_RECORDS FROM TELECOM_AI_HANDSON.RAW.AREA_MASTER;
SELECT * FROM TELECOM_AI_HANDSON.RAW.AREA_MASTER LIMIT 10;

-- ============================================================
-- STEP 10: 通信品質データテーブルの作成とロード
-- ============================================================
CREATE OR REPLACE TABLE TELECOM_AI_HANDSON.ANALYTICS.NETWORK_QUALITY (
    MEASURE_DATE              DATE         NOT NULL,
    AREA_ID                   VARCHAR(20)  NOT NULL,
    AREA_NAME                 VARCHAR(200),
    REGION_NAME               VARCHAR(50),
    AVG_DOWNLOAD_MBPS         NUMBER(10,1),
    AVG_UPLOAD_MBPS           NUMBER(10,1),
    AVG_LATENCY_MS            NUMBER(10,1),
    CONNECTION_SUCCESS_RATE   NUMBER(5,2),
    CALL_DROP_RATE            NUMBER(5,2),
    PACKET_LOSS_RATE          NUMBER(5,2),
    TOTAL_TRAFFIC_GB          NUMBER(10,1)
);

COPY INTO TELECOM_AI_HANDSON.ANALYTICS.NETWORK_QUALITY
    (MEASURE_DATE, AREA_ID, AREA_NAME, REGION_NAME,
     AVG_DOWNLOAD_MBPS, AVG_UPLOAD_MBPS, AVG_LATENCY_MS,
     CONNECTION_SUCCESS_RATE, CALL_DROP_RATE, PACKET_LOSS_RATE,
     TOTAL_TRAFFIC_GB)
FROM @TELECOM_AI_HANDSON.RAW.HANDSON_RESOURCES/csv/network_quality.csv
FILE_FORMAT = (FORMAT_NAME = 'TELECOM_AI_HANDSON.RAW.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS TOTAL_RECORDS FROM TELECOM_AI_HANDSON.ANALYTICS.NETWORK_QUALITY;
SELECT * FROM TELECOM_AI_HANDSON.ANALYTICS.NETWORK_QUALITY LIMIT 10;

-- ============================================================
-- STEP 11: 設備キャパシティデータテーブルの作成とロード
-- ============================================================
CREATE OR REPLACE TABLE TELECOM_AI_HANDSON.ANALYTICS.EQUIPMENT_STATUS (
    SNAPSHOT_DATE       DATE         NOT NULL,
    AREA_ID             VARCHAR(20)  NOT NULL,
    AREA_NAME           VARCHAR(200),
    REGION_NAME         VARCHAR(50),
    BASE_STATION_COUNT  NUMBER(10,0),
    MAX_CAPACITY_GBPS   NUMBER(10,1),
    CURRENT_LOAD_PCT    NUMBER(5,1),
    EQUIPMENT_STATUS    VARCHAR(20)
);

COPY INTO TELECOM_AI_HANDSON.ANALYTICS.EQUIPMENT_STATUS
    (SNAPSHOT_DATE, AREA_ID, AREA_NAME, REGION_NAME,
     BASE_STATION_COUNT, MAX_CAPACITY_GBPS, CURRENT_LOAD_PCT,
     EQUIPMENT_STATUS)
FROM @TELECOM_AI_HANDSON.RAW.HANDSON_RESOURCES/csv/equipment_status.csv
FILE_FORMAT = (FORMAT_NAME = 'TELECOM_AI_HANDSON.RAW.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS TOTAL_RECORDS FROM TELECOM_AI_HANDSON.ANALYTICS.EQUIPMENT_STATUS;
SELECT * FROM TELECOM_AI_HANDSON.ANALYTICS.EQUIPMENT_STATUS LIMIT 10;

-- ============================================================
-- STEP 12: データ確認サマリ
-- ============================================================
SELECT 'AREA_MASTER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM TELECOM_AI_HANDSON.RAW.AREA_MASTER
UNION ALL
SELECT 'NETWORK_QUALITY',            COUNT(*)              FROM TELECOM_AI_HANDSON.ANALYTICS.NETWORK_QUALITY
UNION ALL
SELECT 'EQUIPMENT_STATUS',           COUNT(*)              FROM TELECOM_AI_HANDSON.ANALYTICS.EQUIPMENT_STATUS
ORDER BY TABLE_NAME;
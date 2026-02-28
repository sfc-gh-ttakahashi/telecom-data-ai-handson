-- ============================================================
-- Model Registry からの推論 — SQL ワークシート
-- ============================================================
-- Notebook のパート C と同じ推論を、SQL ワークシートから実行します。
-- Container Runtime や Python 環境は不要です。
--
-- 前提: Notebook の Step 7（パート A・B）でモデル訓練・Registry 登録が完了していること
-- ============================================================

USE DATABASE TELECOM_AI_HANDSON;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. Registry に登録されたモデルの確認
-- ============================================================

SHOW MODELS IN SCHEMA TELECOM_AI_HANDSON.ANALYTICS;

SHOW VERSIONS IN MODEL NQ_ISOLATION_FOREST;
SHOW VERSIONS IN MODEL NQ_TRAFFIC_XGBOOST;

-- ============================================================
-- 2. IsolationForest: 直近30日の異常検知
-- ============================================================
-- 特徴量カラム: CALL_DROP_RATE, AVG_DOWNLOAD_MBPS, AVG_LATENCY_MS,
--              PACKET_LOSS_RATE, PRECIPITATION_MM, WIND_SPEED_KMH
--
-- predict の出力:
--   output_feature_0 = 1（正常）または -1（異常）

WITH recent_data AS (
    SELECT
        nq.MEASURE_DATE,
        AVG(nq.CALL_DROP_RATE)          AS CALL_DROP_RATE,
        AVG(nq.AVG_DOWNLOAD_MBPS)       AS AVG_DOWNLOAD_MBPS,
        AVG(nq.AVG_LATENCY_MS)          AS AVG_LATENCY_MS,
        AVG(nq.PACKET_LOSS_RATE)        AS PACKET_LOSS_RATE,
        AVG(w.AVG_RAINFALL)             AS PRECIPITATION_MM,
        AVG(w.AVG_WIND_SPEED)           AS WIND_SPEED_KMH
    FROM TELECOM_AI_HANDSON.ANALYTICS.NETWORK_QUALITY nq
    LEFT JOIN TELECOM_AI_HANDSON.ANALYTICS.DAILY_WEATHER w
        ON nq.MEASURE_DATE = w.WEATHER_DATE
    WHERE nq.MEASURE_DATE >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY nq.MEASURE_DATE
)
SELECT
    MEASURE_DATE,
    CALL_DROP_RATE,
    PRECIPITATION_MM,
    WIND_SPEED_KMH,
    PREDICTION,
    CASE WHEN PREDICTION = -1 THEN '異常' ELSE '正常' END AS ANOMALY_LABEL
FROM (
    SELECT
        rd.MEASURE_DATE,
        rd.CALL_DROP_RATE,
        rd.PRECIPITATION_MM,
        rd.WIND_SPEED_KMH,
        MODEL(TELECOM_AI_HANDSON.ANALYTICS.NQ_ISOLATION_FOREST, V1)!PREDICT(
            rd.CALL_DROP_RATE,
            rd.AVG_DOWNLOAD_MBPS,
            rd.AVG_LATENCY_MS,
            rd.PACKET_LOSS_RATE,
            rd.PRECIPITATION_MM,
            rd.WIND_SPEED_KMH
        ):output_feature_0::INT AS PREDICTION
    FROM recent_data rd
)
ORDER BY MEASURE_DATE DESC;

-- ============================================================
-- 3. XGBRegressor: 直近30日のトラフィック予測
-- ============================================================
-- 特徴量カラム: TRAFFIC_LAG_1, TRAFFIC_LAG_3, TRAFFIC_LAG_7, TRAFFIC_MA_7,
--              DAY_OF_WEEK, MONTH, AVG_TEMPERATURE_C, PRECIPITATION_MM, WIND_SPEED_KMH
--
-- Python の dt.dayofweek (Monday=0) に合わせるため DAYOFWEEKISO() - 1 を使用
-- predict の出力:
--   output_feature_0 = 予測トラフィック量（GB）

WITH traffic_base AS (
    -- 直近45日分を取得（ラグ7日 + 移動平均7日の計算に余裕を持たせる）
    SELECT
        nq.MEASURE_DATE,
        AVG(nq.TOTAL_TRAFFIC_GB)        AS TOTAL_TRAFFIC_GB,
        AVG(w.AVG_TEMPERATURE)           AS AVG_TEMPERATURE_C,
        AVG(w.AVG_RAINFALL)              AS PRECIPITATION_MM,
        AVG(w.AVG_WIND_SPEED)            AS WIND_SPEED_KMH
    FROM TELECOM_AI_HANDSON.ANALYTICS.NETWORK_QUALITY nq
    LEFT JOIN TELECOM_AI_HANDSON.ANALYTICS.DAILY_WEATHER w
        ON nq.MEASURE_DATE = w.WEATHER_DATE
    WHERE nq.MEASURE_DATE >= DATEADD('day', -45, CURRENT_DATE())
    GROUP BY nq.MEASURE_DATE
),
traffic_features AS (
    -- ラグ特徴量・移動平均・カレンダー特徴量を SQL ウィンドウ関数で生成
    SELECT
        MEASURE_DATE,
        TOTAL_TRAFFIC_GB,
        LAG(TOTAL_TRAFFIC_GB, 1) OVER (ORDER BY MEASURE_DATE)  AS TRAFFIC_LAG_1,
        LAG(TOTAL_TRAFFIC_GB, 3) OVER (ORDER BY MEASURE_DATE)  AS TRAFFIC_LAG_3,
        LAG(TOTAL_TRAFFIC_GB, 7) OVER (ORDER BY MEASURE_DATE)  AS TRAFFIC_LAG_7,
        AVG(TOTAL_TRAFFIC_GB) OVER (
            ORDER BY MEASURE_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                                       AS TRAFFIC_MA_7,
        DAYOFWEEKISO(MEASURE_DATE) - 1                          AS DAY_OF_WEEK,
        MONTH(MEASURE_DATE)                                     AS MONTH,
        AVG_TEMPERATURE_C,
        PRECIPITATION_MM,
        WIND_SPEED_KMH
    FROM traffic_base
),
traffic_recent AS (
    -- ラグ特徴量が NULL でない行のみ、直近30日に絞る
    SELECT *
    FROM traffic_features
    WHERE TRAFFIC_LAG_7 IS NOT NULL
      AND MEASURE_DATE >= DATEADD('day', -30, CURRENT_DATE())
)
SELECT
    MEASURE_DATE,
    ACTUAL_GB,
    PREDICTED_GB,
    ABS(ACTUAL_GB - PREDICTED_GB)                               AS ERROR_GB
FROM (
    SELECT
        tr.MEASURE_DATE,
        tr.TOTAL_TRAFFIC_GB                                     AS ACTUAL_GB,
        MODEL(TELECOM_AI_HANDSON.ANALYTICS.NQ_TRAFFIC_XGBOOST, V1)!PREDICT(
            tr.TRAFFIC_LAG_1,
            tr.TRAFFIC_LAG_3,
            tr.TRAFFIC_LAG_7,
            tr.TRAFFIC_MA_7,
            tr.DAY_OF_WEEK,
            tr.MONTH,
            tr.AVG_TEMPERATURE_C,
            tr.PRECIPITATION_MM,
            tr.WIND_SPEED_KMH
        ):output_feature_0::FLOAT                               AS PREDICTED_GB
    FROM traffic_recent tr
)
ORDER BY MEASURE_DATE DESC;

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from pathlib import Path

# 親ディレクトリをパスに追加
module_path = str(Path(__file__).parent.parent.parent)
if module_path not in sys.path:
    sys.path.append(module_path)

from config.config import config

def get_db_engine():
    """SQLAlchemyエンジンを取得

    Returns:
        Engine: SQLAlchemyエンジン
    """
    try:
        params = config()
        url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
        return create_engine(url)
    except Exception as e:
        print(f"データベース接続エラー: {e}")
        sys.exit(1)

def execute_query(query, params=None, chunk_size=None):
    """SQLクエリを実行し、結果をDataFrameで返す

    Args:
        query (str): 実行するSQLクエリ
        params (dict, optional): SQLパラメータ. デフォルトはNone.
        chunk_size (int, optional): チャンクサイズ. デフォルトはNone (一括取得).

    Returns:
        DataFrame または generator: クエリ結果のDataFrame
    """
    engine = get_db_engine()
    
    try:
        if chunk_size:
            # チャンク単位で取得
            return pd.read_sql_query(query, engine, params=params, chunksize=chunk_size)
        else:
            # 一括取得
            return pd.read_sql_query(query, engine, params=params)
    except Exception as e:
        print(f"クエリ実行エラー: {e}")
        print(f"実行クエリ: {query}")
        if params:
            print(f"パラメータ: {params}")
        return pd.DataFrame()

def get_race_info(year=None, start_date=None, end_date=None, race_id=None):
    """レース基本情報を取得

    Args:
        year (str, optional): 開催年. デフォルトはNone.
        start_date (str, optional): 開始日(YYYYMMDD). デフォルトはNone.
        end_date (str, optional): 終了日(YYYYMMDD). デフォルトはNone.
        race_id (str, optional): レースID. デフォルトはNone.

    Returns:
        DataFrame: レース基本情報のDataFrame
    """
    conditions = []
    params = {}
    
    if year:
        conditions.append("kaisai_nen = :year")
        params['year'] = year
    
    if start_date:
        conditions.append("kaisai_tsukihi >= :start_date")
        params['start_date'] = start_date
    
    if end_date:
        conditions.append("kaisai_tsukihi <= :end_date")
        params['end_date'] = end_date
    
    if race_id:
        # レースIDからパーツを抽出
        year = race_id[:4]
        date = race_id[4:12]
        course = race_id[12:14]
        race_num = race_id[14:16]
        
        conditions.append("kaisai_nen = :race_year AND kaisai_tsukihi = :race_date AND keibajo_code = :race_course AND race_bango = :race_num")
        params['race_year'] = year
        params['race_date'] = date
        params['race_course'] = course
        params['race_num'] = race_num
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
    SELECT 
        kaisai_nen, 
        kaisai_tsukihi, 
        keibajo_code, 
        race_bango,
        kyori,
        track_code,
        tenko_code,
        CASE 
            WHEN SUBSTRING(track_code, 1, 1) = '1' THEN babajotai_code_shiba
            ELSE babajotai_code_dirt
        END as babajotai_code,
        kaisai_nen || kaisai_tsukihi || keibajo_code || race_bango AS race_id,
        CASE 
            WHEN keibajo_code = '01' THEN '札幌'
            WHEN keibajo_code = '02' THEN '函館'
            WHEN keibajo_code = '03' THEN '福島'
            WHEN keibajo_code = '04' THEN '新潟'
            WHEN keibajo_code = '05' THEN '東京'
            WHEN keibajo_code = '06' THEN '中山'
            WHEN keibajo_code = '07' THEN '中京'
            WHEN keibajo_code = '08' THEN '京都'
            WHEN keibajo_code = '09' THEN '阪神'
            WHEN keibajo_code = '10' THEN '小倉'
            ELSE keibajo_code
        END AS course_name,
        CASE 
            WHEN SUBSTRING(track_code, 1, 1) = '1' THEN '芝' 
            WHEN SUBSTRING(track_code, 1, 1) = '2' THEN 'ダート'
            ELSE 'その他'
        END AS track_type,
        CASE 
            WHEN CAST(kyori AS INTEGER) <= 1400 THEN '短距離'
            WHEN CAST(kyori AS INTEGER) <= 2000 THEN '中距離'
            ELSE '長距離'
        END AS distance_category
    FROM 
        jvd_ra
    WHERE 
        {where_clause}
    ORDER BY 
        kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    """
    
    return execute_query(query, params)

def get_race_results(race_id=None, year=None, start_date=None, end_date=None, horse_id=None):
    """レース結果データを取得

    Args:
        race_id (str, optional): レースID. デフォルトはNone.
        year (str, optional): 開催年. デフォルトはNone.
        start_date (str, optional): 開始日(YYYYMMDD). デフォルトはNone.
        end_date (str, optional): 終了日(YYYYMMDD). デフォルトはNone.
        horse_id (str, optional): 馬ID. デフォルトはNone.

    Returns:
        DataFrame: レース結果のDataFrame
    """
    conditions = []
    params = {}
    
    if race_id:
        # レースIDからパーツを抽出
        year = race_id[:4]
        date = race_id[4:12]
        course = race_id[12:14]
        race_num = race_id[14:16]
        
        conditions.append("r.kaisai_nen = :race_year AND r.kaisai_tsukihi = :race_date AND r.keibajo_code = :race_course AND r.race_bango = :race_num")
        params['race_year'] = year
        params['race_date'] = date
        params['race_course'] = course
        params['race_num'] = race_num
    
    if year:
        conditions.append("r.kaisai_nen = :year")
        params['year'] = year
    
    if start_date:
        conditions.append("r.kaisai_tsukihi >= :start_date")
        params['start_date'] = start_date
    
    if end_date:
        conditions.append("r.kaisai_tsukihi <= :end_date")
        params['end_date'] = end_date
    
    if horse_id:
        conditions.append("s.ketto_toroku_bango = :horse_id")
        params['horse_id'] = horse_id
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
    SELECT 
        r.kaisai_nen || r.kaisai_tsukihi || r.keibajo_code || r.race_bango AS race_id,
        r.kaisai_nen, 
        r.kaisai_tsukihi, 
        r.keibajo_code, 
        r.race_bango,
        r.kyori,
        r.track_code,
        r.tenko_code,
        CASE 
            WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN r.babajotai_code_shiba
            ELSE r.babajotai_code_dirt
        END as babajotai_code,
        s.ketto_toroku_bango AS horse_id,
        TRIM(s.bamei) AS horse_name,
        s.wakuban,
        s.umaban,
        s.barei,
        s.seibetsu_code,
        s.bataiju,
        s.zogen_fugo,
        s.zogen_sa,
        s.kishu_code,
        TRIM(s.kishumei_ryakusho) AS jockey_name,
        s.chokyoshi_code,
        TRIM(s.chokyoshimei_ryakusho) AS trainer_name,
        CAST(NULLIF(s.kakutei_chakujun, '') AS INTEGER) AS rank,
        CAST(NULLIF(s.tansho_ninkijun, '') AS INTEGER) AS popularity,
        CAST(NULLIF(s.tansho_odds, '') AS NUMERIC) / 10.0 AS odds,
        CAST(NULLIF(s.soha_time, '') AS INTEGER) AS time_value,
        CAST(NULLIF(s.kohan_3f, '') AS INTEGER) AS last_3f,
        CASE 
            WHEN r.keibajo_code = '01' THEN '札幌'
            WHEN r.keibajo_code = '02' THEN '函館'
            WHEN r.keibajo_code = '03' THEN '福島'
            WHEN r.keibajo_code = '04' THEN '新潟'
            WHEN r.keibajo_code = '05' THEN '東京'
            WHEN r.keibajo_code = '06' THEN '中山'
            WHEN r.keibajo_code = '07' THEN '中京'
            WHEN r.keibajo_code = '08' THEN '京都'
            WHEN r.keibajo_code = '09' THEN '阪神'
            WHEN r.keibajo_code = '10' THEN '小倉'
            ELSE r.keibajo_code
        END AS course_name,
        CASE 
            WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN '芝' 
            WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN 'ダート'
            ELSE 'その他'
        END AS track_type,
        CASE 
            WHEN CAST(r.kyori AS INTEGER) <= 1400 THEN '短距離'
            WHEN CAST(r.kyori AS INTEGER) <= 2000 THEN '中距離'
            ELSE '長距離'
        END AS distance_category
    FROM 
        jvd_ra r
    JOIN 
        jvd_se s ON r.kaisai_nen = s.kaisai_nen 
                AND r.kaisai_tsukihi = s.kaisai_tsukihi
                AND r.keibajo_code = s.keibajo_code
                AND r.race_bango = s.race_bango
    WHERE 
        {where_clause}
    ORDER BY 
        r.kaisai_nen DESC, r.kaisai_tsukihi DESC, r.keibajo_code, r.race_bango, CAST(NULLIF(s.kakutei_chakujun, '') AS INTEGER)
    """
    
    return execute_query(query, params)

def get_horse_data(horse_id=None, limit=100):
    """馬の基本情報を取得

    Args:
        horse_id (str, optional): 馬ID. デフォルトはNone.
        limit (int, optional): 取得上限. デフォルトは100.

    Returns:
        DataFrame: 馬情報のDataFrame
    """
    conditions = []
    params = {}
    
    if horse_id:
        conditions.append("ketto_toroku_bango = :horse_id")
        params['horse_id'] = horse_id
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
    SELECT 
        ketto_toroku_bango AS horse_id,
        TRIM(bamei) AS horse_name,
        seinengappi AS birth_date,
        ketto_joho_01a AS sire_id,
        TRIM(ketto_joho_01b) AS sire_name,
        ketto_joho_02a AS dam_id,
        TRIM(ketto_joho_02b) AS dam_name
    FROM 
        jvd_um
    WHERE 
        {where_clause}
    LIMIT {limit}
    """
    
    return execute_query(query, params)

def get_last_n_runs(horse_id, n=5, current_race_date=None):
    """馬の直近n走の成績を取得

    Args:
        horse_id (str): 馬ID
        n (int, optional): 取得レース数. デフォルトは5.
        current_race_date (str, optional): 基準日(YYYYMMDD). デフォルトはNone.

    Returns:
        DataFrame: 直近のレース結果
    """
    params = {'horse_id': horse_id}
    
    date_condition = ""
    if current_race_date:
        date_condition = "AND (r.kaisai_nen || r.kaisai_tsukihi) < :current_date"
        params['current_date'] = current_race_date
    
    query = f"""
    SELECT 
        r.kaisai_nen || r.kaisai_tsukihi || r.keibajo_code || r.race_bango AS race_id,
        r.kaisai_nen, 
        r.kaisai_tsukihi, 
        r.keibajo_code, 
        r.race_bango,
        r.kyori,
        r.track_code,
        CASE 
            WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN r.babajotai_code_shiba
            ELSE r.babajotai_code_dirt
        END as babajotai_code,
        CAST(NULLIF(s.kakutei_chakujun, '') AS INTEGER) AS rank,
        CAST(NULLIF(s.tansho_ninkijun, '') AS INTEGER) AS popularity,
        CAST(NULLIF(s.tansho_odds, '') AS NUMERIC) / 10.0 AS odds,
        CAST(NULLIF(s.soha_time, '') AS INTEGER) AS time_value,
        CAST(NULLIF(s.kohan_3f, '') AS INTEGER) AS last_3f,
        CASE 
            WHEN r.keibajo_code = '01' THEN '札幌'
            WHEN r.keibajo_code = '02' THEN '函館'
            WHEN r.keibajo_code = '03' THEN '福島'
            WHEN r.keibajo_code = '04' THEN '新潟'
            WHEN r.keibajo_code = '05' THEN '東京'
            WHEN r.keibajo_code = '06' THEN '中山'
            WHEN r.keibajo_code = '07' THEN '中京'
            WHEN r.keibajo_code = '08' THEN '京都'
            WHEN r.keibajo_code = '09' THEN '阪神'
            WHEN r.keibajo_code = '10' THEN '小倉'
            ELSE r.keibajo_code
        END AS course_name,
        CASE 
            WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN '芝' 
            WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN 'ダート'
            ELSE 'その他'
        END AS track_type,
        CASE 
            WHEN CAST(r.kyori AS INTEGER) <= 1400 THEN '短距離'
            WHEN CAST(r.kyori AS INTEGER) <= 2000 THEN '中距離'
            ELSE '長距離'
        END AS distance_category
    FROM 
        jvd_se s
    JOIN 
        jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                AND s.kaisai_tsukihi = r.kaisai_tsukihi
                AND s.keibajo_code = r.keibajo_code
                AND s.race_bango = r.race_bango
    WHERE 
        s.ketto_toroku_bango = :horse_id
        AND s.kakutei_chakujun ~ '^[0-9]+$'
        AND s.kakutei_chakujun NOT IN ('00', '99')
        {date_condition}
    ORDER BY 
        r.kaisai_nen DESC, r.kaisai_tsukihi DESC
    LIMIT {n}
    """
    
    return execute_query(query, params)


if __name__ == "__main__":
    # 簡単な使用例
    races = get_race_info(year="2022", start_date="20220101", end_date="20220131")
    print(f"取得レース数: {len(races)}")
    
    if not races.empty:
        race_id = races.iloc[0]['race_id']
        results = get_race_results(race_id=race_id)
        print(f"レース {race_id} の出走数: {len(results)}")
        
        if not results.empty:
            horse_id = results.iloc[0]['horse_id']
            last_runs = get_last_n_runs(horse_id, n=3)
            print(f"馬 {horse_id} の過去レース数: {len(last_runs)}")

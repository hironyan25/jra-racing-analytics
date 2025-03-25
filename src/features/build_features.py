#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import seaborn as sns

# 親ディレクトリをパスに追加
module_path = str(Path(__file__).parent.parent.parent)
if module_path not in sys.path:
    sys.path.append(module_path)

from src.data.extraction import get_race_results, get_race_info, execute_query
from src.data.preprocessing import clean_race_data, calculate_last_3f_rank, add_previous_races_features

class SireTrackROIBuilder:
    """種牡馬×馬場適性ROIを計算するクラス"""
    
    def __init__(self, start_year="2010", end_year="2020", min_runs=30):
        """初期化

        Args:
            start_year (str, optional): 開始年. デフォルトは"2010".
            end_year (str, optional): 終了年. デフォルトは"2020".
            min_runs (int, optional): 最小出走回数. デフォルトは30.
        """
        self.start_year = start_year
        self.end_year = end_year
        self.min_runs = min_runs
        self.sire_track_roi_data = None
    
    def build(self, force_rebuild=False):
        """種牡馬×馬場適性ROIデータを構築

        Args:
            force_rebuild (bool, optional): 強制的に再構築するかどうか. デフォルトはFalse.

        Returns:
            DataFrame: 種牡馬×馬場適性ROIデータ
        """
        # すでに構築済みでforce_rebuildがFalseの場合は既存データを返す
        if self.sire_track_roi_data is not None and not force_rebuild:
            return self.sire_track_roi_data
        
        # データの取得
        query = f"""
        WITH race_conditions AS (
            -- レース条件（馬場状態）
            SELECT 
                kaisai_nen,
                kaisai_tsukihi,
                keibajo_code,
                race_bango,
                track_code,
                CASE 
                    WHEN SUBSTRING(track_code, 1, 1) = '1' THEN babajotai_code_shiba
                    ELSE babajotai_code_dirt
                END as babajotai_code,
                CASE 
                    WHEN SUBSTRING(track_code, 1, 1) = '1' THEN '芝' 
                    WHEN SUBSTRING(track_code, 1, 1) = '2' THEN 'ダート'
                    ELSE 'その他'
                END AS track_type,
                CASE 
                    WHEN babajotai_code_shiba = '1' OR babajotai_code_dirt = '1' THEN '良'
                    WHEN babajotai_code_shiba = '2' OR babajotai_code_dirt = '2' THEN '稍重'
                    WHEN babajotai_code_shiba = '3' OR babajotai_code_dirt = '3' THEN '重'
                    WHEN babajotai_code_shiba = '4' OR babajotai_code_dirt = '4' THEN '不良'
                    ELSE 'その他'
                END AS baba_condition
            FROM 
                jvd_ra
            WHERE 
                kaisai_nen BETWEEN '{self.start_year}' AND '{self.end_year}'
        ),
        sire_info AS (
            -- 血統情報（父馬）
            SELECT 
                ketto_toroku_bango,
                ketto_joho_01a AS sire_id,
                TRIM(ketto_joho_01b) AS sire_name
            FROM 
                jvd_um
        ),
        race_results AS (
            -- レース結果
            SELECT 
                s.kaisai_nen,
                s.kaisai_tsukihi,
                s.keibajo_code,
                s.race_bango,
                s.ketto_toroku_bango,
                CAST(s.kakutei_chakujun AS INTEGER) AS rank,
                CAST(s.tansho_ninkijun AS INTEGER) AS popularity,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS odds,
                rc.track_type,
                rc.baba_condition,
                si.sire_name
            FROM 
                jvd_se s
            JOIN 
                race_conditions rc ON s.kaisai_nen = rc.kaisai_nen 
                                  AND s.kaisai_tsukihi = rc.kaisai_tsukihi
                                  AND s.keibajo_code = rc.keibajo_code
                                  AND s.race_bango = rc.race_bango
            JOIN 
                sire_info si ON s.ketto_toroku_bango = si.ketto_toroku_bango
            WHERE 
                s.kakutei_chakujun ~ '^[0-9]+$'
                AND s.kakutei_chakujun NOT IN ('00', '99')
                AND s.tansho_ninkijun IS NOT NULL
                AND s.tansho_odds IS NOT NULL
                AND si.sire_name IS NOT NULL
                AND si.sire_name != ''
                AND si.sire_name != '0000000000'
        )
        -- 種牡馬×馬場条件の集計
        SELECT 
            sire_name,
            track_type,
            baba_condition,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE rank = 1) AS wins,
            ROUND(COUNT(*) FILTER (WHERE rank = 1) * 100.0 / NULLIF(COUNT(*), 0), 2) AS win_rate,
            ROUND(SUM(CASE WHEN rank = 1 THEN odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
            AVG(popularity) AS avg_popularity,
            AVG(CASE WHEN rank = 1 THEN odds ELSE NULL END) AS avg_win_odds,
            COUNT(*) FILTER (WHERE rank = 1 AND popularity > 3) AS non_favorite_wins
        FROM 
            race_results
        GROUP BY 
            sire_name, track_type, baba_condition
        HAVING 
            COUNT(*) >= {self.min_runs}  -- 最低出走数
        ORDER BY 
            roi_percentage DESC
        """
        
        self.sire_track_roi_data = execute_query(query)
        return self.sire_track_roi_data
    
    def get_top_roi_sires(self, track_type=None, baba_condition=None, top_n=10):
        """指定した条件での上位ROI種牡馬を取得

        Args:
            track_type (str, optional): トラックタイプ（'芝', 'ダート', 'その他'）. デフォルトはNone.
            baba_condition (str, optional): 馬場状態（'良', '稍重', '重', '不良'）. デフォルトはNone.
            top_n (int, optional): 上位N件. デフォルトは10.

        Returns:
            DataFrame: 上位ROI種牡馬データ
        """
        if self.sire_track_roi_data is None:
            self.build()
        
        df = self.sire_track_roi_data.copy()
        
        # 条件でフィルタリング
        if track_type:
            df = df[df['track_type'] == track_type]
        
        if baba_condition:
            df = df[df['baba_condition'] == baba_condition]
        
        # ROI順にソートして上位N件を返す
        return df.sort_values('roi_percentage', ascending=False).head(top_n)
    
    def get_sire_track_roi_score(self, sire_name, track_type, baba_condition):
        """特定の種牡馬×馬場条件のROIスコアを取得

        Args:
            sire_name (str): 種牡馬名
            track_type (str): トラックタイプ
            baba_condition (str): 馬場状態

        Returns:
            float: ROIスコア（平均100に対する相対値）
        """
        if self.sire_track_roi_data is None:
            self.build()
        
        # 全体の平均ROI
        avg_roi = self.sire_track_roi_data['roi_percentage'].mean()
        
        # 指定された条件のROI
        condition = (
            (self.sire_track_roi_data['sire_name'] == sire_name) & 
            (self.sire_track_roi_data['track_type'] == track_type) & 
            (self.sire_track_roi_data['baba_condition'] == baba_condition)
        )
        
        if condition.any():
            sire_roi = self.sire_track_roi_data.loc[condition, 'roi_percentage'].values[0]
            # 平均を100とした相対値
            return (sire_roi / avg_roi) * 100
        else:
            # データがない場合はデフォルト値（平均）
            return 100.0
    
    def plot_top_sires(self, track_type=None, baba_condition=None, top_n=10):
        """上位種牡馬のROIをプロット

        Args:
            track_type (str, optional): トラックタイプ. デフォルトはNone.
            baba_condition (str, optional): 馬場状態. デフォルトはNone.
            top_n (int, optional): 上位N件. デフォルトは10.

        Returns:
            matplotlib.figure.Figure: 作成した図
        """
        top_sires = self.get_top_roi_sires(track_type, baba_condition, top_n)
        
        condition_str = ""
        if track_type:
            condition_str += f"{track_type}"
        if baba_condition:
            condition_str += f"・{baba_condition}"
        
        if not condition_str:
            condition_str = "全馬場条件"
        
        fig, ax = plt.subplots(figsize=(12, 8))
        bars = ax.barh(top_sires['sire_name'], top_sires['roi_percentage'], color='lightblue')
        
        # 勝率に基づいて色付け
        norm = plt.Normalize(top_sires['win_rate'].min(), top_sires['win_rate'].max())
        sm = plt.cm.ScalarMappable(cmap="Blues", norm=norm)
        sm.set_array([])
        
        for i, bar in enumerate(bars):
            bar.set_color(plt.cm.Blues(norm(top_sires['win_rate'].iloc[i])))
        
        ax.set_xlabel('回収率 (%)')
        ax.set_title(f'上位種牡馬のROI ({condition_str})')
        ax.grid(axis='x', linestyle='--', alpha=0.7)
        
        # 値を追加
        for i, bar in enumerate(bars):
            win_rate = top_sires['win_rate'].iloc[i]
            roi = top_sires['roi_percentage'].iloc[i]
            ax.text(roi + 10, bar.get_y() + bar.get_height()/2, 
                    f'{roi:.1f}% (勝率: {win_rate:.1f}%)', 
                    va='center')
        
        # カラーバーを追加
        cbar = fig.colorbar(sm)
        cbar.set_label('勝率 (%)')
        
        plt.tight_layout()
        return fig


class JockeyCourseProfitBuilder:
    """騎手のコース別平均配当を計算するクラス"""
    
    def __init__(self, start_year="2010", end_year="2020", min_rides=20):
        """初期化

        Args:
            start_year (str, optional): 開始年. デフォルトは"2010".
            end_year (str, optional): 終了年. デフォルトは"2020".
            min_rides (int, optional): 最小騎乗回数. デフォルトは20.
        """
        self.start_year = start_year
        self.end_year = end_year
        self.min_rides = min_rides
        self.jockey_course_data = None
    
    def build(self, force_rebuild=False):
        """騎手のコース別成績データを構築

        Args:
            force_rebuild (bool, optional): 強制的に再構築するかどうか. デフォルトはFalse.

        Returns:
            DataFrame: 騎手のコース別成績データ
        """
        # すでに構築済みでforce_rebuildがFalseの場合は既存データを返す
        if self.jockey_course_data is not None and not force_rebuild:
            return self.jockey_course_data
        
        # データの取得
        query = f"""
        WITH race_conditions AS (
            -- レース条件（競馬場・距離など）
            SELECT 
                kaisai_nen,
                kaisai_tsukihi,
                keibajo_code,
                race_bango,
                kyori,
                track_code,
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
                kaisai_nen BETWEEN '{self.start_year}' AND '{self.end_year}'
        ),
        jockey_results AS (
            -- 騎手別の成績
            SELECT 
                s.kishu_code,
                TRIM(s.kishumei_ryakusho) AS jockey_name,
                s.kaisai_nen,
                s.kaisai_tsukihi,
                s.keibajo_code,
                s.race_bango,
                CAST(s.kakutei_chakujun AS INTEGER) AS rank,
                CAST(s.tansho_ninkijun AS INTEGER) AS popularity,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS odds,
                rc.course_name,
                rc.track_type,
                rc.distance_category
            FROM 
                jvd_se s
            JOIN 
                race_conditions rc ON s.kaisai_nen = rc.kaisai_nen 
                                  AND s.kaisai_tsukihi = rc.kaisai_tsukihi
                                  AND s.keibajo_code = rc.keibajo_code
                                  AND s.race_bango = rc.race_bango
            WHERE 
                s.kakutei_chakujun ~ '^[0-9]+$'
                AND s.kakutei_chakujun NOT IN ('00', '99')
                AND s.tansho_ninkijun IS NOT NULL
                AND s.tansho_odds IS NOT NULL
                AND s.kishu_code IS NOT NULL
                AND s.kishu_code != ''
        )
        -- 騎手×コース×距離カテゴリの集計
        SELECT 
            jockey_name,
            kishu_code,
            course_name,
            track_type,
            distance_category,
            COUNT(*) AS total_rides,
            COUNT(*) FILTER (WHERE rank = 1) AS wins,
            ROUND(COUNT(*) FILTER (WHERE rank = 1) * 100.0 / NULLIF(COUNT(*), 0), 2) AS win_rate,
            ROUND(SUM(CASE WHEN rank = 1 THEN odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
            AVG(popularity) AS avg_popularity,
            AVG(CASE WHEN rank = 1 THEN odds ELSE NULL END) AS avg_win_odds,
            COUNT(*) FILTER (WHERE rank <= 3) AS top3_count,
            ROUND(COUNT(*) FILTER (WHERE rank <= 3) * 100.0 / NULLIF(COUNT(*), 0), 2) AS top3_rate
        FROM 
            jockey_results
        GROUP BY 
            jockey_name, kishu_code, course_name, track_type, distance_category
        HAVING 
            COUNT(*) >= {self.min_rides}  -- 最低騎乗回数
        ORDER BY 
            roi_percentage DESC
        """
        
        self.jockey_course_data = execute_query(query)
        return self.jockey_course_data
    
    def get_top_roi_jockeys(self, course_name=None, track_type=None, distance_category=None, top_n=10):
        """指定した条件での上位ROI騎手を取得

        Args:
            course_name (str, optional): 競馬場名. デフォルトはNone.
            track_type (str, optional): トラックタイプ. デフォルトはNone.
            distance_category (str, optional): 距離区分. デフォルトはNone.
            top_n (int, optional): 上位N件. デフォルトは10.

        Returns:
            DataFrame: 上位ROI騎手データ
        """
        if self.jockey_course_data is None:
            self.build()
        
        df = self.jockey_course_data.copy()
        
        # 条件でフィルタリング
        if course_name:
            df = df[df['course_name'] == course_name]
        
        if track_type:
            df = df[df['track_type'] == track_type]
        
        if distance_category:
            df = df[df['distance_category'] == distance_category]
        
        # ROI順にソートして上位N件を返す
        return df.sort_values('roi_percentage', ascending=False).head(top_n)
    
    def get_jockey_course_roi_score(self, jockey_code, course_name, track_type, distance_category):
        """特定の騎手×コース条件のROIスコアを取得

        Args:
            jockey_code (str): 騎手コード
            course_name (str): 競馬場名
            track_type (str): トラックタイプ
            distance_category (str): 距離区分

        Returns:
            float: ROIスコア（平均100に対する相対値）
        """
        if self.jockey_course_data is None:
            self.build()
        
        # 全体の平均ROI
        avg_roi = self.jockey_course_data['roi_percentage'].mean()
        
        # 指定された条件のROI
        condition = (
            (self.jockey_course_data['kishu_code'] == jockey_code) & 
            (self.jockey_course_data['course_name'] == course_name) & 
            (self.jockey_course_data['track_type'] == track_type) & 
            (self.jockey_course_data['distance_category'] == distance_category)
        )
        
        if condition.any():
            jockey_roi = self.jockey_course_data.loc[condition, 'roi_percentage'].values[0]
            # 平均を100とした相対値
            return (jockey_roi / avg_roi) * 100
        else:
            # データがない場合はデフォルト値（平均）
            return 100.0
    
    def plot_top_jockeys(self, course_name=None, track_type=None, distance_category=None, top_n=10):
        """上位騎手のROIをプロット

        Args:
            course_name (str, optional): 競馬場名. デフォルトはNone.
            track_type (str, optional): トラックタイプ. デフォルトはNone.
            distance_category (str, optional): 距離区分. デフォルトはNone.
            top_n (int, optional): 上位N件. デフォルトは10.

        Returns:
            matplotlib.figure.Figure: 作成した図
        """
        top_jockeys = self.get_top_roi_jockeys(course_name, track_type, distance_category, top_n)
        
        condition_parts = []
        if course_name:
            condition_parts.append(course_name)
        if track_type:
            condition_parts.append(track_type)
        if distance_category:
            condition_parts.append(distance_category)
        
        condition_str = "・".join(condition_parts) if condition_parts else "全コース条件"
        
        fig, ax = plt.subplots(figsize=(12, 8))
        bars = ax.barh(top_jockeys['jockey_name'], top_jockeys['roi_percentage'], color='lightblue')
        
        # 勝率に基づいて色付け
        norm = plt.Normalize(top_jockeys['win_rate'].min(), top_jockeys['win_rate'].max())
        sm = plt.cm.ScalarMappable(cmap="Blues", norm=norm)
        sm.set_array([])
        
        for i, bar in enumerate(bars):
            bar.set_color(plt.cm.Blues(norm(top_jockeys['win_rate'].iloc[i])))
        
        ax.set_xlabel('回収率 (%)')
        ax.set_title(f'上位騎手のROI ({condition_str})')
        ax.grid(axis='x', linestyle='--', alpha=0.7)
        
        # 値を追加
        for i, bar in enumerate(bars):
            win_rate = top_jockeys['win_rate'].iloc[i]
            roi = top_jockeys['roi_percentage'].iloc[i]
            ax.text(roi + 10, bar.get_y() + bar.get_height()/2, 
                    f'{roi:.1f}% (勝率: {win_rate:.1f}%)', 
                    va='center')
        
        # カラーバーを追加
        cbar = fig.colorbar(sm)
        cbar.set_label('勝率 (%)')
        
        plt.tight_layout()
        return fig


class HorseCourseProfitBuilder:
    """馬のコース別実績ROIを計算するクラス"""
    
    def __init__(self, start_year="2010", end_year="2020", min_races=3):
        """初期化

        Args:
            start_year (str, optional): 開始年. デフォルトは"2010".
            end_year (str, optional): 終了年. デフォルトは"2020".
            min_races (int, optional): 最小レース数. デフォルトは3.
        """
        self.start_year = start_year
        self.end_year = end_year
        self.min_races = min_races
        self.horse_course_data = None
    
    def build(self, force_rebuild=False):
        """馬のコース別成績データを構築

        Args:
            force_rebuild (bool, optional): 強制的に再構築するかどうか. デフォルトはFalse.

        Returns:
            DataFrame: 馬のコース別成績データ
        """
        # すでに構築済みでforce_rebuildがFalseの場合は既存データを返す
        if self.horse_course_data is not None and not force_rebuild:
            return self.horse_course_data
        
        # データの取得
        query = f"""
        WITH race_conditions AS (
            -- レース条件（競馬場・距離など）
            SELECT 
                kaisai_nen,
                kaisai_tsukihi,
                keibajo_code,
                race_bango,
                kyori,
                track_code,
                CASE 
                    WHEN SUBSTRING(track_code, 1, 1) = '1' THEN babajotai_code_shiba
                    ELSE babajotai_code_dirt
                END as babajotai_code,
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
                END AS distance_category,
                CASE 
                    WHEN babajotai_code_shiba = '1' OR babajotai_code_dirt = '1' THEN '良'
                    WHEN babajotai_code_shiba = '2' OR babajotai_code_dirt = '2' THEN '稍重'
                    WHEN babajotai_code_shiba = '3' OR babajotai_code_dirt = '3' THEN '重'
                    WHEN babajotai_code_shiba = '4' OR babajotai_code_dirt = '4' THEN '不良'
                    ELSE 'その他'
                END AS baba_condition
            FROM 
                jvd_ra
            WHERE 
                kaisai_nen BETWEEN '{self.start_year}' AND '{self.end_year}'
        ),
        horse_results AS (
            -- 馬の成績
            SELECT 
                s.ketto_toroku_bango AS horse_id,
                TRIM(s.bamei) AS horse_name,
                s.kaisai_nen,
                s.kaisai_tsukihi,
                s.keibajo_code,
                s.race_bango,
                CAST(s.kakutei_chakujun AS INTEGER) AS rank,
                CAST(s.tansho_ninkijun AS INTEGER) AS popularity,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS odds,
                rc.course_name,
                rc.track_type,
                rc.distance_category,
                rc.baba_condition
            FROM 
                jvd_se s
            JOIN 
                race_conditions rc ON s.kaisai_nen = rc.kaisai_nen 
                                  AND s.kaisai_tsukihi = rc.kaisai_tsukihi
                                  AND s.keibajo_code = rc.keibajo_code
                                  AND s.race_bango = rc.race_bango
            WHERE 
                s.kakutei_chakujun ~ '^[0-9]+$'
                AND s.kakutei_chakujun NOT IN ('00', '99')
                AND s.tansho_ninkijun IS NOT NULL
                AND s.tansho_odds IS NOT NULL
        )
        -- 馬×コース×距離カテゴリの集計
        SELECT 
            horse_id,
            horse_name,
            course_name,
            track_type,
            distance_category,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE rank = 1) AS wins,
            ROUND(COUNT(*) FILTER (WHERE rank = 1) * 100.0 / NULLIF(COUNT(*), 0), 2) AS win_rate,
            ROUND(SUM(CASE WHEN rank = 1 THEN odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
            AVG(popularity) AS avg_popularity,
            AVG(CASE WHEN rank = 1 THEN odds ELSE NULL END) AS avg_win_odds,
            COUNT(*) FILTER (WHERE rank <= 3) AS top3_count,
            ROUND(COUNT(*) FILTER (WHERE rank <= 3) * 100.0 / NULLIF(COUNT(*), 0), 2) AS top3_rate
        FROM 
            horse_results
        GROUP BY 
            horse_id, horse_name, course_name, track_type, distance_category
        HAVING 
            COUNT(*) >= {self.min_races}  -- 最低レース数
        ORDER BY 
            roi_percentage DESC
        """
        
        self.horse_course_data = execute_query(query)
        return self.horse_course_data
    
    def get_top_course_performers(self, course_name=None, track_type=None, distance_category=None, top_n=10):
        """指定した条件での上位パフォーマンス馬を取得

        Args:
            course_name (str, optional): 競馬場名. デフォルトはNone.
            track_type (str, optional): トラックタイプ. デフォルトはNone.
            distance_category (str, optional): 距離区分. デフォルトはNone.
            top_n (int, optional): 上位N件. デフォルトは10.

        Returns:
            DataFrame: 上位パフォーマンス馬データ
        """
        if self.horse_course_data is None:
            self.build()
        
        df = self.horse_course_data.copy()
        
        # 条件でフィルタリング
        if course_name:
            df = df[df['course_name'] == course_name]
        
        if track_type:
            df = df[df['track_type'] == track_type]
        
        if distance_category:
            df = df[df['distance_category'] == distance_category]
        
        # ROI順にソートして上位N件を返す
        return df.sort_values('roi_percentage', ascending=False).head(top_n)
    
    def get_horse_course_history(self, horse_id):
        """馬の各コース・トラックでの成績履歴を取得

        Args:
            horse_id (str): 馬ID

        Returns:
            DataFrame: 馬のコース別成績データ
        """
        if self.horse_course_data is None:
            self.build()
        
        return self.horse_course_data[self.horse_course_data['horse_id'] == horse_id].sort_values('roi_percentage', ascending=False)
    
    def get_course_roi_score(self, horse_id, course_name, track_type, distance_category):
        """特定の馬×コース条件のROIスコアを取得

        Args:
            horse_id (str): 馬ID
            course_name (str): 競馬場名
            track_type (str): トラックタイプ
            distance_category (str): 距離区分

        Returns:
            float: ROIスコア（平均100に対する相対値）
        """
        if self.horse_course_data is None:
            self.build()
        
        # 全体の平均ROI
        avg_roi = self.horse_course_data['roi_percentage'].mean()
        
        # 指定された条件のROI
        condition = (
            (self.horse_course_data['horse_id'] == horse_id) & 
            (self.horse_course_data['course_name'] == course_name) & 
            (self.horse_course_data['track_type'] == track_type) & 
            (self.horse_course_data['distance_category'] == distance_category)
        )
        
        if condition.any():
            horse_roi = self.horse_course_data.loc[condition, 'roi_percentage'].values[0]
            # 平均を100とした相対値
            return (horse_roi / avg_roi) * 100
        else:
            # データがない場合はデフォルト値（平均）
            return 100.0


class Last3FRankBuilder:
    """上がり3F順位と回収率の関係を分析するクラス"""
    
    def __init__(self, start_year="2010", end_year="2020"):
        """初期化

        Args:
            start_year (str, optional): 開始年. デフォルトは"2010".
            end_year (str, optional): 終了年. デフォルトは"2020".
        """
        self.start_year = start_year
        self.end_year = end_year
        self.last_3f_data = None
    
    def build(self, force_rebuild=False):
        """上がり3F順位と成績の関係データを構築

        Args:
            force_rebuild (bool, optional): 強制的に再構築するかどうか. デフォルトはFalse.

        Returns:
            DataFrame: 上がり3F順位と成績の関係データ
        """
        # すでに構築済みでforce_rebuildがFalseの場合は既存データを返す
        if self.last_3f_data is not None and not force_rebuild:
            return self.last_3f_data
        
        # レース抽出（各年からサンプル）
        races_query = f"""
        SELECT DISTINCT
            kaisai_nen || kaisai_tsukihi || keibajo_code || race_bango AS race_id
        FROM 
            jvd_ra
        WHERE 
            kaisai_nen BETWEEN '{self.start_year}' AND '{self.end_year}'
        ORDER BY 
            race_id
        LIMIT 10000  -- サンプル数
        """
        
        race_ids = execute_query(races_query)['race_id'].tolist()
        
        # 各レースの結果を取得して処理
        all_results = []
        
        for race_id in race_ids:
            # レース結果取得
            results = get_race_results(race_id=race_id)
            
            if results.empty:
                continue
            
            # データクリーニング
            results_cleaned = clean_race_data(results)
            
            # 上がり3F順位の計算
            results_with_3f = calculate_last_3f_rank(results_cleaned)
            
            all_results.append(results_with_3f)
        
        # 全結果を結合
        if all_results:
            combined_results = pd.concat(all_results, ignore_index=True)
            
            # 上がり3F順位の集計
            last_3f_stats = combined_results.groupby('last_3f_rank').agg(
                total_horses=('horse_id', 'count'),
                wins=('rank', lambda x: (x == 1).sum()),
                top3=('rank', lambda x: (x <= 3).sum()),
                avg_odds=('odds', 'mean'),
                avg_win_odds=('odds', lambda x: x[combined_results['rank'] == 1].mean()),
                avg_popularity=('popularity', 'mean')
            ).reset_index()
            
            # 勝率、連対率、回収率の計算
            last_3f_stats['win_rate'] = last_3f_stats['wins'] / last_3f_stats['total_horses'] * 100
            last_3f_stats['top3_rate'] = last_3f_stats['top3'] / last_3f_stats['total_horses'] * 100
            last_3f_stats['roi'] = last_3f_stats['avg_win_odds'] * last_3f_stats['wins'] / last_3f_stats['total_horses'] * 100
            
            # 人気別の集計も追加
            popularity_groups = [
                (1, 3, '人気馬(1-3位)'),
                (4, 8, '中穴馬(4-8位)'),
                (9, 999, '大穴馬(9位以下)')
            ]
            
            popularity_stats = []
            
            for min_pop, max_pop, label in popularity_groups:
                pop_filter = (combined_results['popularity'] >= min_pop) & (combined_results['popularity'] <= max_pop)
                pop_data = combined_results[pop_filter]
                
                if pop_data.empty:
                    continue
                
                pop_stats = pop_data.groupby('last_3f_rank').agg(
                    popularity_group=('popularity', lambda x: label),
                    total_horses=('horse_id', 'count'),
                    wins=('rank', lambda x: (x == 1).sum()),
                    top3=('rank', lambda x: (x <= 3).sum()),
                    avg_odds=('odds', 'mean'),
                    avg_win_odds=('odds', lambda x: x[pop_data['rank'] == 1].mean()),
                    avg_popularity=('popularity', 'mean')
                ).reset_index()
                
                pop_stats['win_rate'] = pop_stats['wins'] / pop_stats['total_horses'] * 100
                pop_stats['top3_rate'] = pop_stats['top3'] / pop_stats['total_horses'] * 100
                pop_stats['roi'] = pop_stats['avg_win_odds'] * pop_stats['wins'] / pop_stats['total_horses'] * 100
                
                popularity_stats.append(pop_stats)
            
            # 人気別統計を結合
            if popularity_stats:
                popularity_combined = pd.concat(popularity_stats, ignore_index=True)
                
                # 全体と人気別の両方を含むデータセットを作成
                last_3f_stats['popularity_group'] = '全体'
                self.last_3f_data = pd.concat([last_3f_stats, popularity_combined], ignore_index=True)
            else:
                last_3f_stats['popularity_group'] = '全体'
                self.last_3f_data = last_3f_stats
            
            return self.last_3f_data
        else:
            return pd.DataFrame()
    
    def get_last_3f_roi_stats(self, popularity_group='全体'):
        """上がり3F順位別のROI統計を取得

        Args:
            popularity_group (str, optional): 人気グループ. デフォルトは'全体'.
                選択肢: '全体', '人気馬(1-3位)', '中穴馬(4-8位)', '大穴馬(9位以下)'

        Returns:
            DataFrame: 上がり3F順位別のROI統計
        """
        if self.last_3f_data is None:
            self.build()
        
        filtered_data = self.last_3f_data[self.last_3f_data['popularity_group'] == popularity_group].copy()
        
        # 上がり3F順位でソート
        return filtered_data.sort_values('last_3f_rank')
    
    def get_last_3f_roi_adjustment(self, last_3f_rank, popularity_group='全体'):
        """上がり3F順位に基づくROI調整値を取得

        Args:
            last_3f_rank (float): 上がり3F順位
            popularity_group (str, optional): 人気グループ. デフォルトは'全体'.

        Returns:
            float: ROI調整値（倍率）
        """
        stats = self.get_last_3f_roi_stats(popularity_group)
        
        if stats.empty:
            return 1.0
        
        # デフォルトの平均ROIを取得（基準値）
        avg_roi = stats['roi'].mean()
        
        # 最も近い順位のROIを取得
        closest_rank = stats.iloc[(stats['last_3f_rank'] - last_3f_rank).abs().argsort()[0]]
        rank_roi = closest_rank['roi']
        
        # 平均を1.0とした相対値
        return rank_roi / avg_roi
    
    def plot_last_3f_roi(self, popularity_group='全体'):
        """上がり3F順位とROIの関係をプロット

        Args:
            popularity_group (str, optional): 人気グループ. デフォルトは'全体'.

        Returns:
            matplotlib.figure.Figure: 作成した図
        """
        stats = self.get_last_3f_roi_stats(popularity_group)
        
        if stats.empty:
            return None
        
        fig, ax1 = plt.subplots(figsize=(12, 8))
        
        # ROIのプロット
        color = 'tab:blue'
        ax1.set_xlabel('上がり3F順位')
        ax1.set_ylabel('回収率 (%)', color=color)
        ax1.plot(stats['last_3f_rank'], stats['roi'], '-o', color=color, linewidth=2, markersize=8)
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.grid(True, linestyle='--', alpha=0.7)
        
        # 勝率の副軸
        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.set_ylabel('勝率 (%)', color=color)
        ax2.plot(stats['last_3f_rank'], stats['win_rate'], '-o', color=color, linewidth=2, markersize=8)
        ax2.tick_params(axis='y', labelcolor=color)
        
        # ラベル追加
        for i, row in stats.iterrows():
            ax1.annotate(f"{row['roi']:.1f}%", 
                        (row['last_3f_rank'], row['roi']),
                        textcoords="offset points", 
                        xytext=(0, 10), 
                        ha='center',
                        fontweight='bold')
            
            ax2.annotate(f"{row['win_rate']:.1f}%", 
                        (row['last_3f_rank'], row['win_rate']),
                        textcoords="offset points", 
                        xytext=(0, -15), 
                        ha='center',
                        color='tab:red')
        
        title = f'上がり3F順位と回収率・勝率の関係 [{popularity_group}]'
        plt.title(title)
        plt.tight_layout()
        
        return fig


if __name__ == "__main__":
    # 簡単な使用例
    print("特徴量構築クラスのサンプル実行")
    
    # 種牡馬×馬場適性ROI
    sire_builder = SireTrackROIBuilder(start_year="2018", end_year="2020", min_runs=20)
    top_sires = sire_builder.get_top_roi_sires(track_type="ダート", baba_condition="良", top_n=5)
    print("\n種牡馬×馬場適性ROI上位5件：")
    print(top_sires[['sire_name', 'track_type', 'baba_condition', 'total_races', 'win_rate', 'roi_percentage']])
    
    # 騎手のコース別平均配当
    jockey_builder = JockeyCourseProfitBuilder(start_year="2018", end_year="2020", min_rides=10)
    top_jockeys = jockey_builder.get_top_roi_jockeys(course_name="東京", track_type="芝", distance_category="中距離", top_n=5)
    print("\n騎手のコース別平均配当上位5件：")
    print(top_jockeys[['jockey_name', 'course_name', 'track_type', 'total_rides', 'win_rate', 'roi_percentage']])
    
    # 上がり3F順位と回収率
    last_3f_builder = Last3FRankBuilder(start_year="2018", end_year="2020")
    last_3f_stats = last_3f_builder.get_last_3f_roi_stats(popularity_group='全体')
    print("\n上がり3F順位と回収率：")
    print(last_3f_stats[['last_3f_rank', 'total_horses', 'win_rate', 'roi']])

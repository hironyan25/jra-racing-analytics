#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os

# 親ディレクトリをパスに追加
module_path = str(Path(__file__).parent.parent.parent)
if module_path not in sys.path:
    sys.path.append(module_path)

from src.data.extraction import get_race_results, get_last_n_runs

def clean_race_data(df):
    """レースデータの基本的なクリーニング

    Args:
        df (DataFrame): 処理対象のDataFrame

    Returns:
        DataFrame: クリーニング済みのDataFrame
    """
    if df.empty:
        return df
    
    # コピーを作成して元のデータを変更しないようにする
    df_cleaned = df.copy()
    
    # 数値型への変換
    numeric_cols = [
        'kyori', 'bataiju', 'zogen_sa', 'odds', 
        'time_value', 'last_3f', 'rank', 'popularity'
    ]
    
    for col in numeric_cols:
        if col in df_cleaned.columns:
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
    
    # 日付の処理
    if 'kaisai_nen' in df_cleaned.columns and 'kaisai_tsukihi' in df_cleaned.columns:
        try:
            df_cleaned['race_date'] = pd.to_datetime(
                df_cleaned['kaisai_nen'] + df_cleaned['kaisai_tsukihi'], 
                format='%Y%m%d', 
                errors='coerce'
            )
        except Exception as e:
            print(f"日付変換エラー: {e}")
    
    # 欠損値の処理
    df_cleaned = df_cleaned.replace([np.inf, -np.inf], np.nan)
    
    # トラックタイプのマッピング
    if 'track_code' in df_cleaned.columns:
        df_cleaned['track_type'] = df_cleaned['track_code'].apply(
            lambda x: '芝' if str(x).startswith('1') else 'ダート' if str(x).startswith('2') else 'その他'
        )
    
    # 馬場状態のマッピング
    if 'babajotai_code' in df_cleaned.columns:
        babajotai_map = {
            '1': '良',
            '2': '稍重',
            '3': '重',
            '4': '不良',
            '0': 'その他'
        }
        df_cleaned['baba_condition'] = df_cleaned['babajotai_code'].map(
            lambda x: babajotai_map.get(str(x), 'その他')
        )
    
    # 年齢の計算（if barei exists）
    if 'barei' in df_cleaned.columns:
        df_cleaned['age'] = pd.to_numeric(df_cleaned['barei'], errors='coerce')
    
    # 性別のマッピング
    if 'seibetsu_code' in df_cleaned.columns:
        seibetsu_map = {
            '1': '牡',
            '2': '牝',
            '3': 'セン',
            '0': '不明'
        }
        df_cleaned['sex'] = df_cleaned['seibetsu_code'].map(
            lambda x: seibetsu_map.get(str(x), '不明')
        )
    
    # 馬体重の増減符号を考慮
    if 'bataiju' in df_cleaned.columns and 'zogen_fugo' in df_cleaned.columns and 'zogen_sa' in df_cleaned.columns:
        # 増減量の計算
        df_cleaned['weight_change'] = df_cleaned.apply(
            lambda row: int(row['zogen_sa']) if pd.notna(row['zogen_sa']) else 0, 
            axis=1
        )
        
        # 符号の適用
        df_cleaned['weight_change'] = df_cleaned.apply(
            lambda row: row['weight_change'] * -1 if row['zogen_fugo'] == '-' else row['weight_change'],
            axis=1
        )
    
    return df_cleaned

def calculate_last_3f_rank(df):
    """上がり3ハロンの順位を計算する

    Args:
        df (DataFrame): レース結果のDataFrame

    Returns:
        DataFrame: 上がり3ハロン順位を追加したDataFrame
    """
    if df.empty or 'last_3f' not in df.columns:
        return df
    
    # コピーを作成
    df_result = df.copy()
    
    # レース単位でグループ化
    grouped = df_result.groupby('race_id')
    
    # 上がり3Fの順位を計算
    def calc_rank(group):
        if 'last_3f' in group.columns:
            # 欠損値を除外して順位付け
            valid_last_3f = group['last_3f'].dropna()
            if not valid_last_3f.empty:
                # 値が小さい方が良いので昇順でランク付け
                ranks = valid_last_3f.rank(method='min')
                group.loc[ranks.index, 'last_3f_rank'] = ranks
        return group
    
    # 各レースに適用
    df_result = grouped.apply(calc_rank)
    
    # グループ化による階層インデックスを解除
    if isinstance(df_result.index, pd.MultiIndex):
        df_result = df_result.reset_index(drop=True)
    
    return df_result

def add_previous_races_features(df, n_previous=5):
    """各馬の過去レース実績に基づく特徴を追加

    Args:
        df (DataFrame): レース結果のDataFrame
        n_previous (int, optional): 参照する過去レース数. デフォルトは5.

    Returns:
        DataFrame: 特徴を追加したDataFrame
    """
    if df.empty:
        return df
    
    # コピーを作成
    df_result = df.copy()
    
    # 必要なカラムがあるか確認
    required_cols = ['horse_id', 'race_id', 'race_date', 'kyori', 'track_type']
    missing_cols = [col for col in required_cols if col not in df_result.columns]
    if missing_cols:
        print(f"カラム不足: {missing_cols}")
        return df_result
    
    # 馬ごとに過去レース特徴を計算
    unique_horses = df_result['horse_id'].unique()
    
    for horse_id in unique_horses:
        # 馬のすべてのレースを取得（日付順）
        horse_races = df_result[df_result['horse_id'] == horse_id].sort_values('race_date')
        
        # 各レースについて
        for idx, race in horse_races.iterrows():
            race_date = race['race_date']
            
            # 現在のレースより前のレースを取得
            previous_races = horse_races[horse_races['race_date'] < race_date].tail(n_previous)
            
            if not previous_races.empty:
                # 過去5走の平均着順
                df_result.loc[idx, 'avg_last_n_rank'] = previous_races['rank'].mean()
                
                # 過去5走の勝率
                df_result.loc[idx, 'win_rate_last_n'] = (previous_races['rank'] == 1).mean()
                
                # 過去5走の連対率（1着or2着）
                df_result.loc[idx, 'top2_rate_last_n'] = (previous_races['rank'] <= 2).mean()
                
                # 過去5走の複勝率（3着以内）
                df_result.loc[idx, 'top3_rate_last_n'] = (previous_races['rank'] <= 3).mean()
                
                # 同じトラックタイプでの平均着順
                same_track = previous_races[previous_races['track_type'] == race['track_type']]
                if not same_track.empty:
                    df_result.loc[idx, 'avg_rank_same_track'] = same_track['rank'].mean()
                
                # 同じ距離区分での平均着順
                if 'distance_category' in previous_races.columns and 'distance_category' in race:
                    same_distance = previous_races[previous_races['distance_category'] == race['distance_category']]
                    if not same_distance.empty:
                        df_result.loc[idx, 'avg_rank_same_distance'] = same_distance['rank'].mean()
                
                # 前走からの間隔（日数）
                last_race_date = previous_races.iloc[-1]['race_date']
                df_result.loc[idx, 'days_since_last_race'] = (race_date - last_race_date).days
                
                # 前走着順
                df_result.loc[idx, 'last_race_rank'] = previous_races.iloc[-1]['rank']
                
                # 前走人気
                if 'popularity' in previous_races.columns:
                    df_result.loc[idx, 'last_race_popularity'] = previous_races.iloc[-1]['popularity']
                
                # 前走との斤量差
                if 'futan_juryo' in previous_races.columns and 'futan_juryo' in race:
                    df_result.loc[idx, 'weight_diff_from_last'] = race['futan_juryo'] - previous_races.iloc[-1]['futan_juryo']
                
                # 過去5走の上がり3F最速タイム
                if 'last_3f' in previous_races.columns:
                    valid_3f = previous_races['last_3f'].dropna()
                    if not valid_3f.empty:
                        df_result.loc[idx, 'best_last_3f'] = valid_3f.min()
                
                # 過去5走の上がり順位1位回数
                if 'last_3f_rank' in previous_races.columns:
                    df_result.loc[idx, 'count_last_3f_rank1'] = (previous_races['last_3f_rank'] == 1).sum()
    
    return df_result

def prepare_race_data_for_prediction(race_id):
    """予測用にレースデータを準備

    Args:
        race_id (str): レースID

    Returns:
        DataFrame: 予測用に前処理したDataFrame
    """
    # レース結果を取得
    results = get_race_results(race_id=race_id)
    
    if results.empty:
        print(f"レース {race_id} の出走馬情報が見つかりません")
        return pd.DataFrame()
    
    # データクリーニング
    results_cleaned = clean_race_data(results)
    
    # 上がり3F順位の計算
    results_with_3f = calculate_last_3f_rank(results_cleaned)
    
    # 過去レース特徴の追加
    results_with_features = add_previous_races_features(results_with_3f)
    
    return results_with_features

def get_horse_history_with_features(horse_id, n_races=10):
    """馬の過去レース履歴と特徴量を取得

    Args:
        horse_id (str): 馬ID
        n_races (int, optional): 取得レース数. デフォルトは10.

    Returns:
        DataFrame: 馬の過去レース履歴（特徴量付き）
    """
    # 馬の過去レースを取得
    horse_races = get_race_results(horse_id=horse_id)
    
    if horse_races.empty:
        print(f"馬 {horse_id} のレース履歴が見つかりません")
        return pd.DataFrame()
    
    # 直近n走に限定
    horse_races = horse_races.sort_values('kaisai_tsukihi', ascending=False).head(n_races)
    
    # データクリーニング
    races_cleaned = clean_race_data(horse_races)
    
    # 上がり3F順位の計算
    races_with_3f = calculate_last_3f_rank(races_cleaned)
    
    # レース日付順にソート
    races_sorted = races_with_3f.sort_values('race_date')
    
    # 過去レース特徴の追加
    races_with_features = add_previous_races_features(races_sorted)
    
    return races_with_features

if __name__ == "__main__":
    # 簡単な使用例
    race_id = "202201010101"  # 例: 2022/01/01 東京1R
    race_data = prepare_race_data_for_prediction(race_id)
    
    if not race_data.empty:
        print(f"レース {race_id} の処理済みデータ:")
        print(race_data.head())
        
        # 最初の馬のIDを取得
        horse_id = race_data.iloc[0]['horse_id']
        horse_history = get_horse_history_with_features(horse_id, n_races=5)
        
        print(f"\n馬 {horse_id} の過去レース:")
        print(horse_history[['race_id', 'race_date', 'track_type', 'kyori', 'rank', 'popularity', 'last_3f_rank']].head())

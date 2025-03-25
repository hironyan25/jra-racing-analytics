#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configparser import ConfigParser
import os
from pathlib import Path

def config(filename='database.ini', section='postgresql'):
    """設定ファイルからデータベース接続情報を読み込む

    Args:
        filename (str, optional): 設定ファイル名. デフォルトは 'database.ini'.
        section (str, optional): セクション名. デフォルトは 'postgresql'.

    Returns:
        dict: データベース接続用パラメータ

    Raises:
        Exception: セクションが見つからない場合やファイルが存在しない場合
    """
    # 設定ファイルのパスを取得
    config_path = Path(__file__).parent / filename
    
    # 環境変数からの読み込みを優先
    db_params = {}
    db_params['host'] = os.environ.get('DB_HOST')
    db_params['port'] = os.environ.get('DB_PORT')
    db_params['database'] = os.environ.get('DB_NAME')
    db_params['user'] = os.environ.get('DB_USER')
    db_params['password'] = os.environ.get('DB_PASS')
    
    # 環境変数が設定されていない場合、設定ファイルから読み込む
    if not all(db_params.values()):
        # 設定ファイルの存在チェック
        if not config_path.exists():
            raise Exception(f"{filename} ファイルが {config_path} に見つかりません")
            
        parser = ConfigParser()
        parser.read(config_path)
        
        # セクションの存在チェック
        if not parser.has_section(section):
            raise Exception(f"セクション {section} が {filename} に見つかりません")
            
        # セクションからパラメータを取得
        for param in parser.items(section):
            db_params[param[0]] = param[1]
    
    # Noneを削除（環境変数が一部だけ設定されている場合）
    db_params = {k: v for k, v in db_params.items() if v is not None}
    
    return db_params

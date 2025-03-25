# JRA競馬データ分析プロジェクト

## 概要
JRA（日本中央競馬会）の競馬データを活用した回収率向上を目指す分析・予測プロジェクトです。従来の予測モデルでは考慮されにくい特徴量に焦点を当て、特に中穴〜大穴の馬の好走を予測することで、馬券回収率の大幅な向上を目指します。

## 主要特徴量

本プロジェクトでは、以下の特徴量に特に注目しています：

1. **種牡馬×馬場適性ROI**
   - 特定の種牡馬の産駒が特定の馬場条件で示す単勝回収率
   - 例：エーシントップ産駒はダート良馬場で959.41%のROI

2. **騎手のコース別平均配当**
   - 騎手が特定コースで勝利した際の平均配当
   - 例：南田雅昭騎手は新潟ダート中距離で989.80%のROI

3. **上がりタイム順位**
   - レース終盤の上がり3ハロンのタイム順位
   - 上がり1位馬は平均337.19%のROI

4. **馬のコース実績ROI**
   - 馬が特定コースで示すROI履歴
   - 例：ティーエスネオは阪神芝中距離で833.33%のROI

5. **前走ペース偏差（展開不利指標）**
   - 前走で展開不利だった馬の次走成績
   - 例：前走展開不利→大敗の馬は次走で64.07%のROI

6. **スタミナ指数**
   - 長距離レース（3000m以上）でのスタミナ評価指標
   - 上位ランク馬は平均212.37%のROI

## プロジェクト構成

```
jra-racing-analytics/
├── README.md                # プロジェクト概要説明
├── requirements.txt         # 必要なPythonパッケージ
├── .gitignore               # 無視するファイル設定
├── config/                  # 設定ファイル
│   ├── database.ini         # データベース接続設定
│   └── config.py            # 設定読み込みモジュール
├── sql/                     # SQLクエリ
│   ├── schema/              # スキーマ定義
│   ├── queries/             # 分析用クエリ
│   └── views/               # ビュー定義
├── notebooks/               # Jupyter notebooks
│   ├── exploratory/         # 探索的分析
│   └── models/              # モデル開発
├── src/                     # ソースコード
│   ├── data/                # データ処理
│   │   ├── __init__.py
│   │   ├── extraction.py    # データ抽出
│   │   └── preprocessing.py # 前処理
│   ├── features/            # 特徴量エンジニアリング
│   │   ├── __init__.py
│   │   └── build_features.py
│   ├── models/              # モデル開発
│   │   ├── __init__.py
│   │   ├── train_model.py
│   │   └── predict_model.py
│   └── visualization/       # 可視化
│       ├── __init__.py
│       └── visualize.py
└── tests/                   # テストコード
    └── __init__.py
```

## JVDデータベース構造

主要テーブルと役割：

- **jvd_ra**: レース基本情報テーブル
  - 識別情報: kaisai_nen(開催年), kaisai_tsukihi(開催月日), keibajo_code(競馬場), race_bango(レース番号)
  - レース条件: kyori(距離), track_code(トラック種), tenko_code(天候), babajotai_code_shiba/dirt(馬場状態)
  
- **jvd_se**: 出走馬情報テーブル
  - 馬識別: ketto_toroku_bango(血統登録番号), bamei(馬名), wakuban(枠番), umaban(馬番)
  - 騎手・調教師: kishu_code(騎手コード), kishumei_ryakusho(騎手名), chokyoshi_code(調教師コード)
  - 結果: kakutei_chakujun(着順), soha_time(走破タイム), kohan_3f(上がり3F)

- **jvd_hr**: レース払戻情報テーブル
  - 各種払戻金: haraimodoshi_tansho_*(単勝), haraimodoshi_fukusho_*(複勝) など

- **jvd_um**: 馬基本情報テーブル
  - 基本情報: ketto_toroku_bango(血統登録番号), bamei(馬名), seinengappi(生年月日)
  - 血統情報: ketto_joho_01a/b(父), ketto_joho_02a/b(母) など

- **jvd_hn**: 血統・繁殖馬情報テーブル
  - 血統情報: hanshoku_toroku_bango(繁殖登録番号), ketto_toroku_bango(血統登録番号)

## インストールと設定

```bash
# リポジトリのクローン
git clone https://github.com/hironyan25/jra-racing-analytics.git
cd jra-racing-analytics

# 仮想環境の作成と有効化
python -m venv venv
source venv/bin/activate  # Windowsの場合: venv\Scripts\activate

# 依存パッケージのインストール
pip install -r requirements.txt

# データベース接続設定の構成
cp config/database.ini.sample config/database.ini
# database.iniファイルを編集して接続情報を設定
```

## ライセンス
MITライセンスで提供されています。詳細はLICENSEファイルをご覧ください。

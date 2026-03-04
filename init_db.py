import sqlite3
import pandas as pd
import os

DB_NAME = "Trails.db"

CSV_FILES = [
    "foothills_list.csv",
    "小百岳list.csv",
    "BaiYue_list.csv",
    "Top100_list.csv",
    "trail_list.csv"
]

TABLE_NAME = "HikingTrails"

def create_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            name TEXT NOT NULL,
            region TEXT,
            difficulty TEXT,
            time TEXT,
            url TEXT,
            img_url TEXT
        )
    """)

def insert_csv_to_db(cursor, csv_file):
    if not os.path.exists(csv_file):
        print(f" 找不到 CSV：{csv_file}")
        return

    df = pd.read_csv(csv_file)

    # 填補缺失欄位，避免 CSV 欄位不齊
    expected_cols = ["name", "region", "difficulty", "time", "url", "img_url"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # 移除完全重複資料（避免多次部署爆量）
    df = df.drop_duplicates(subset=["name", "region"])

    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (name, region, difficulty, time, url, img_url)
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME}
                WHERE name = ? AND region = ?
            )
        """, (row["name"], row["region"], row["difficulty"], row["time"],
              row["url"], row["img_url"], row["name"], row["region"]))

def main():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    create_table(cursor)

    for csv_file in CSV_FILES:
        print(f" 匯入 {csv_file} ...")
        insert_csv_to_db(cursor, csv_file)

    conn.commit()
    cursor.close()
    conn.close()
    print(f" {DB_NAME} 已建立 / 已更新")

if __name__ == "__main__":
    main()
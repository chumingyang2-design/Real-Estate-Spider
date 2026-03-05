import sqlite3
import csv


def export_to_csv(db_name="real_estate_data.db", csv_name="房源数据结果.csv"):
    # 1. 连接数据库并读取所有数据
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM houses")
    rows = cursor.fetchall()

    # 2. 写入 CSV 文件
    # 注意：encoding='utf-8-sig' 是关键！它能保证导出的中文在 Excel 里打开绝不乱码
    with open(csv_name, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)

        # 写入表头 (列名)
        writer.writerow(['数据库ID', '房源标题', '所在小区', '户型', '面积', '总价'])

        # 批量写入数据
        writer.writerows(rows)

    print(f"✅ 成功导出 {len(rows)} 条数据到文件：{csv_name}")
    conn.close()


if __name__ == "__main__":
    export_to_csv()
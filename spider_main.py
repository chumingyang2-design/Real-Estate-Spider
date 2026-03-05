import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import random


# ==========================================
# 模块 1: 数据库设计与初始化
# ==========================================
def setup_database(db_name="real_estate_data.db"):
    """创建用于存储房源信息的数据库表"""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS houses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            community TEXT,
            layout TEXT,
            area TEXT,
            total_price TEXT
        )
    ''')
    conn.commit()
    return conn


# ==========================================
# 模块 2 & 3: 核心爬取与精准解析逻辑
# ==========================================
def start_scraping_real_estate(conn, target_count=1000):
    cursor = conn.cursor()

    base_url = "https://nanjing.esf.fang.com/house/i3{}/"

    total_scraped = 0
    page = 1

    print("---  开始执行自动化房产数据采集 ---")

    while total_scraped < target_count and page <= 50:
        url = base_url.format(page)
        headers = {
            'User-Agent': 'csrfToken=UB7sCWPldLhN-Ua9HoOUjm4e; global_cookie=bymz8c9crccryq5n364xl2t4t2ymmdcjvh5; otherid=685f4e4ab15a12d0052c16e588a6e985; city.sig=OGYSb1kOr8YVFH0wBEXukpoi1DeOqwvdseB7aTrJ-zE; city=nanjing; token=0dca243d2441401386861bd196748174; sfut=E32929BA65CEC58C56569E4402E35A2111F009B48895AACC197383065B32EED3B2D6B7E176FFFD3B2EF84AF55BFF267BE51DA5D4CDE2D91F2C3F8AF88DB4B07072F384DC80DFB4878CFB9969BE2B0BDD01F6650B05BBC161FDD66BC8F48EC7A0709B90DCAAF46202; g_sourcepage=esf_fy%5Elb_pc; unique_cookie=U_bymz8c9crccryq5n364xl2t4t2ymmdcjvh5*20; new_loginid=131424819; login_username=fang35642603872 '  # 记得替换这里！
        }

        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.encoding = 'utf-8'
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"第 {page} 页网络请求异常: {e}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')

        # ==== 基于真实 DOM 结构的精确定位 ====
        # 1. 找到大容器
        list_container = soup.find('div', class_='shop_list')

        if not list_container:
            print(f" 第 {page} 页未能找到 div.shop_list，可能触发了反爬验证或 cookie 已失效。")
            break

        # 2. 找到所有独立房源节点
        house_items = list_container.find_all('dl')

        # 记录本页抓取数量，用于批量入库
        page_data = []

        for item in house_items:
            try:
                # 过滤无标题的非房源数据（如穿插的广告）
                title_tag = item.find('h4', class_='clearfix')
                if not title_tag:
                    continue

                    # --- 提取字段 ---
                title = title_tag.text.strip()

                info_p = item.find('p', class_='tel_shop')
                info_text = info_p.text.strip() if info_p else ""
                infos = info_text.split('|')
                layout = infos[0].strip() if len(infos) > 0 else "未知户型"
                area = infos[1].strip() if len(infos) > 1 else "未知面积"

                add_p = item.find('p', class_='add_shop')
                community = add_p.text.strip() if add_p else "未知小区"

                price_dd = item.find('dd', class_='price_right')
                total_price = price_dd.text.strip() if price_dd else "未知价格"

                # 将当前房源加入本页列表
                page_data.append((title, community, layout, area, total_price))
                total_scraped += 1

                if total_scraped >= target_count:
                    break  # 达到1000条随时停止

            except AttributeError:
                continue

                # ==== 模块 4: 数据批量入库 ====
        if page_data:
            # 使用 executemany 进行批量插入，大幅提升数据库写入速度
            cursor.executemany('''
                INSERT INTO houses (title, community, layout, area, total_price)
                VALUES (?, ?, ?, ?, ?)
            ''', page_data)
            conn.commit()
            print(f" 已成功抓取第 {page} 页... 当前总入库数据: {total_scraped} 条")
        else:
            print(f" 第 {page} 页没有提取到有效房源数据。")

        page += 1

        # 礼貌性延迟，防止被反爬系统封锁 IP
        time.sleep(random.uniform(2, 5))

    print(f"---  爬取任务结束！共抓取 {total_scraped} 条数据 ---")


# ==========================================
# 模块 5: 数据验证
# ==========================================
def verify_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM houses")
    print(f"\n 数据库最终统计: 共存入 {cursor.fetchone()[0]} 条房源数据")

    print("\n 随机抽取 3 条数据抽查:")
    print("-" * 60)
    cursor.execute("SELECT title, layout, area, total_price FROM houses ORDER BY RANDOM() LIMIT 3")
    for row in cursor.fetchall():
        print(f"标题: {row[0][:20]}... | 户型: {row[1]} | 面积: {row[2]} | 总价: {row[3]}")
    print("-" * 60)


# ==========================================
# 主程序入口
# ==========================================
if __name__ == "__main__":
    # 1. 连接数据库
    db_connection = setup_database()

    # 测试阶段：每次运行前清空旧数据，方便调试
    db_connection.cursor().execute("DELETE FROM houses")
    db_connection.commit()

    # 2. 启动爬虫
    start_scraping_real_estate(db_connection, target_count=1100)

    # 3. 验证数据
    verify_data(db_connection)

    # 4. 关闭连接
    db_connection.close()
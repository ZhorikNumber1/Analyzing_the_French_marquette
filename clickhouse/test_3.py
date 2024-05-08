import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
import json


def extract_full_category_path(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path.lower().strip()
    except json.JSONDecodeError:
        return None


# Параметры для обработки данных пакетами
batch_size = 100
start_index = 0

# Создать клиента для подключения к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
    user='default',
    password='KIa.AG53Vxaf1',
    port=8443,
    secure=True
)
target_categories = [
    'celulares y telefonía',
    'computación',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]
all_data = []
thirty_days_ago = datetime.now() - timedelta(days=30)
date_str = thirty_days_ago.strftime('%Y-%m-%d')

query_combined = f"""
SELECT 
    psd.url, 
    psd.breadcrumbs, 
    pvd.available_quantity, 
    pvd.price,
    pvd.created AS product_created
FROM 
    (SELECT * FROM product_static_data where product_id = 'MLC1743347352') psd  -- добавляем фильтр который сильно уменьшает выборку
LEFT JOIN 
    (SELECT * FROM product_variable_data WHERE created >= '2024-01-15' and created <= '2024-02-15' and product_id = 'MLC1743347352') pvd  -- добавляем фильтр который сильно уменьшает выборку (определенный товар + ограничение по датам
    ON psd.url = pvd.url_id
ORDER BY 
    psd.url ASC, 
    pvd.created DESC, 
    pvd.url_id ASC
LIMIT 1000  -- наверное, можно убрать




    """

# Выполнение запроса к ClickHouse
result = client.query(query_combined)
# print(result.result_rows)
# Список, указывающий названия столбцов, соответствующие тому порядку, в котором они появляются в SELECT запросе
columns = ["url", "breadcrumbs", "available_quantity", "price", "product_created"]

# Создание DataFrame из результатов
df_combined = pd.DataFrame(result.result_rows, columns=columns)
df_combined.to_excel("test_3.xlsx", index=False)

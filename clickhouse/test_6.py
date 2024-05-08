import pandas as pd
import clickhouse_connect

# Подключение к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
    user='default',
    password='KIa.AG53Vxaf1',
    port=8443,
    secure=True
)

# Выгружаем static data
static_data_query = """
SELECT
    product_id,
    url,
    breadcrumbs AS category
FROM
    product_static_data
"""

result = client.query(static_data_query)
# Создание DataFrame с результатами запроса
static_data = pd.DataFrame(
    result.result_rows,
    columns=['product_id', 'url', 'category']
)
# Выгружаем variable data
variable_data_query = f"""
SELECT 
    url_id,
    price,
    created
FROM 
    product_variable_data
WHERE 
    created >= '2024-01-15' AND created <= '2024-02-15'
"""
result = client.query(variable_data_query)

# Создание DataFrame с результатами запроса
variable_data = pd.DataFrame(
    result.result_rows,
    columns=['url_id', 'price', 'created']
)
# Присоединяем данные используя pandas
merged_data = pd.merge(left=static_data, right=variable_data, left_on='url', right_on='url_id')

# Группируем и агрегируем по product_id и category
summary = merged_data.groupby(['product_id', 'category'])['price'].agg(
    min_price='min',
    max_price='max',
    mean_price='mean',
    median_price='median'
).reset_index()

# Вывод результатов
print(summary)

# Сохраняем результаты в файл если нужно
summary.to_csv('aggregated_product_data.csv', index=False)

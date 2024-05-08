import pandas as pd
import clickhouse_connect

# Параметры подключения к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
    user='default',
    password='KIa.AG53Vxaf1',
    port=8443,
    secure=True
)

# Параметры даты для фильтрации данных
start_date = '2024-01-15'
end_date = '2024-02-15'

# Основной запрос для получения всех данных из таблицы product_variable_data и product_static_data
query = f"""
SELECT 
    psd.product_id,
    psd.breadcrumbs AS category,
    pvd.price
FROM 
    product_variable_data AS pvd
JOIN 
    product_static_data AS psd
    ON pvd.url_id = psd.url
WHERE 
    pvd.created >= '{start_date}' AND pvd.created <= '{end_date}'
"""

# Выполнение запроса
result = client.query(query)

# Создание DataFrame с результатами запроса
data = pd.DataFrame(
    result.result_rows,
    columns=['product_id', 'category', 'price']
)

# Группировка по 'product_id' и 'category' и расчет статистик в Python
result_df = data.groupby(['product_id', 'category'])['price'].agg(
    min_price='min',
    max_price='max',
    mean_price='mean',
    median_price='median'
).reset_index()

# Вывод результирующего DataFrame
print(result_df)

# Сохраняем DataFrame в CSV файл
result_df.to_csv('price_statistics_by_product_and_category.csv', index=False)

import pandas as pd
from datetime import datetime, timedelta
import clickhouse_connect

# Параметры подключения к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
    user='default',
    password='KIa.AG53Vxaf1',
    port=8443,
    secure=True
)

# Подготовка даты за последние 30 дней
thirty_days_ago = datetime.now() - timedelta(days=30)
date_str = thirty_days_ago.strftime('%Y-%m-%d')

# Столбцы для основного DataFrame
columns = ["url", "breadcrumbs", "available_quantity", "price", "product_created"]

# Основной запрос данных
query = f"""
SELECT DISTINCT psd.url, 
       psd.breadcrumbs, 
       pvd.available_quantity, 
       pvd.price,
       pvd.created AS product_created
FROM product_static_data psd
LEFT JOIN product_variable_data pvd ON psd.url = pvd.url_id
WHERE psd.created >= '{date_str}'
AND pvd.created >= '{date_str}'
ORDER BY psd.url
"""

# Выполнение запроса
result = client.query(query)

# Создание DataFrame с результатами запроса
df_combined = pd.DataFrame(result.result_rows, columns=columns)

# Анализ данных по категориям
df_category_prices = df_combined.groupby('breadcrumbs').agg(
    Минимальная_цена=pd.NamedAgg(column='price', aggfunc='min'),
    Максимальная_цена=pd.NamedAgg(column='price', aggfunc='max'),
    Средняя_цена=pd.NamedAgg(column='price', aggfunc='mean'),
    Медианная_цена=pd.NamedAgg(column='price', aggfunc='median')
).reset_index()

# Переименование столбцов
df_category_prices.columns = ['Категория', 'Минимальная цена', 'Максимальная цена', 'Средняя цена', 'Медианная цена']

# Вывод результирующего DataFrame
print(df_category_prices)

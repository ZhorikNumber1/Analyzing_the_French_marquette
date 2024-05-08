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
batch_size = 100
start_index = 0
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
# Вычислить дату 30 дней назад
thirty_days_ago = datetime.now() - timedelta(days=30)
date_str = thirty_days_ago.strftime('%Y-%m-%d')

# Запрос для получения уникальных URL
query_urls = f"""
SELECT DISTINCT url
FROM product_static_data
WHERE product_created >= '{date_str}'
ORDER BY url
"""

urls = client.query(query_urls)
urls_df = pd.DataFrame(urls.result_rows, columns=["url"])
while True:
    query_combined = f"""
    SELECT psd.url, 
           psd.breadcrumbs, 
           pvd.available_quantity, 
           pvd.price,
           pvd.created AS product_created
    FROM product_static_data psd
    LEFT JOIN product_variable_data pvd ON psd.url = pvd.url_id
    ORDER BY psd.url ASC, pvd.created DESC, pvd.url_id ASC
    LIMIT {batch_size} OFFSET {start_index}


    """

    result = client.query(query_combined)
    columns = ["url", "breadcrumbs", "available_quantity", "price", "product_created"]

    df_combined = pd.DataFrame(result.result_rows, columns=columns)




    if df_combined.empty:
        print("No more data to process.")
        break

    df_combined['categories'] = df_combined['breadcrumbs'].apply(extract_full_category_path)  # ensure you define this function
    df_combined = df_combined.drop(columns=['breadcrumbs'])
    df_combined['price'] = pd.to_numeric(df_combined['price'], errors='coerce')
    df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'], errors='coerce')
    df_combined = df_combined.dropna(subset=['available_quantity'])

    start_index += batch_size
    filtered_df = df_combined[
        df_combined['categories'].apply(lambda x: any(cat in x for cat in target_categories) if x else False)]
    if not filtered_df.empty:
        print("______________Обнаружены записи, соответствующие одной или нескольким целевым категориям.______________")
        all_data.append(filtered_df)
    else:
        print("Записи, соответствующие целевым категориям, НЕ обнаружены в текущем наборе данных.")
    print(f"Обработанно: {start_index}")
    if start_index >= 10000:
        break
all_data = pd.concat(all_data, ignore_index=True)
df_filtered_with_category = all_data.assign(
    last_category=all_data['categories'].apply(lambda x: x[-1] if x else None)
)
df_category_prices = df_filtered_with_category.groupby('last_category')['price'].agg(
    ['min', 'max', 'mean', 'median']
).reset_index()
df_category_prices.columns = ['Категория', 'Минимальная цена', 'Максимальная цена', 'Средняя цена',
                              'Медианная цена']
df_category_prices.to_csv('combined_data.csv', index=False, encoding='utf-8')
print("Data processing complete.")
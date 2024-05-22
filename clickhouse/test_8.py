import pandas as pd
from sqlalchemy import create_engine
import os
import clickhouse_connect
import json

# Параметры подключения к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
    user='default',
    password='KIa.AG53Vxaf1',
    port=8443,
    secure=True
)

# Постоянные параметры
batch_size = 10000
start_index = 0
date_str = '2024-03-07'
target_categories = [
    'celulares y telefonía',
    'computación',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]


def extract_full_category_path(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path.lower().strip()
    except json.JSONDecodeError:
        return None


current_date = pd.to_datetime(date_str)
end_date = pd.Timestamp.now()
all_data = []

while current_date < end_date:
    next_date = current_date + pd.Timedelta(days=1)
    while True:
        # Выборка данных из таблицы product_static_data
        query_product_static_data = f"""
        SELECT 
            url, 
            breadcrumbs
        FROM 
            product_static_data
        ORDER BY 
            url ASC
        LIMIT {batch_size} OFFSET {start_index}
    """
        result_static_data = client.query(query_product_static_data)
        df_static_data = pd.DataFrame(
            result_static_data.result_rows,
            columns=['url', 'breadcrumbs']
        )

        # Выборка данных из таблицы product_variable_data
        query_product_variable_data = f"""
        SELECT 
            url_id, 
            available_quantity, 
            price,
            created
        FROM 
            product_variable_data
        ORDER BY 
            url_id ASC, 
            created DESC
        LIMIT {batch_size} OFFSET {start_index}
    """
        result_variable_data = client.query(query_product_variable_data)
        df_variable_data = pd.DataFrame(
            result_variable_data.result_rows,
            columns=['url', 'available_quantity', 'price', 'product_created']
        )

        if df_variable_data.empty:
            print("No more data to process.")
            break

        # Объединение DataFrame аналогично LEFT JOIN
        df_combined = df_static_data.merge(df_variable_data, on='url', how='left')
        print(df_combined.tail(5))

        # Продолжаем обработку данных
        df_combined['categories'] = df_combined['breadcrumbs'].apply(extract_full_category_path)
        df_combined = df_combined.drop(columns=['breadcrumbs'], axis=1)
        df_combined['price'] = pd.to_numeric(df_combined['price'], errors='coerce')
        df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'], errors='coerce')
        df_combined = df_combined.dropna(subset=['available_quantity'])

        # Фильтруем по целевым категориям
        filtered_df = df_combined[
            df_combined['categories'].apply(lambda x: any(cat in x for cat in target_categories) if x else False)]
        if not filtered_df.empty:
            print("______________Обнаружены записи, соответствующие одной или нескольким целевым категориям.______________")
            all_data.append(filtered_df)
        else:
            print("Записи, соответствующие целевым категориям, НЕ обнаружены в текущем наборе данных.")

        start_index += batch_size
        print(f"Обработано: {start_index} для {current_date.strftime('%Y-%m-%d')}")

    current_date = next_date
    start_index = 0
    print(f"Обработано: {current_date}")

# Объединение всех отфильтрованных данных в один DataFrame
if all_data:
    all_data = pd.concat(all_data, ignore_index=True)
    df_filtered_with_category = all_data.assign(
        last_category=all_data['categories'].apply(lambda x: x[-1] if x else None)
    )

    # Вывод результата в консоль и сохранение в CSV-файл
    print(df_filtered_with_category)
    output_filename = 'filtered_data.csv'
    df_filtered_with_category.to_csv(output_filename, index=False)
    print(f"Файл сохранён как {output_filename}")
else:
    print("Нет данных для сохранения.")

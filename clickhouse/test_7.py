import pandas as pd
from sqlalchemy import create_engine
import os
import clickhouse_connect

# Параметры подключения к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
    user='default',
    password='KIa.AG53Vxaf1',
    port=8443,
    secure=True
)

# Постоянные параметры
batch_size = 1000
start_index = 0
date_str = '2024-05-07'
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
        query_combined = f"""
        SELECT 
            psd.url, 
            psd.breadcrumbs, 
            pvd.available_quantity, 
            pvd.price,
            pvd.created AS product_created
        FROM 
            product_static_data psd
        LEFT JOIN 
            product_variable_data pvd 
        ON 
            psd.url = pvd.url_id 
        WHERE 
            pvd.created >= '{current_date.strftime('%Y-%m-%d')}' AND pvd.created < '{next_date.strftime('%Y-%m-%d')}'
        ORDER BY 
            psd.url ASC, 
            pvd.created DESC 
        LIMIT {batch_size} OFFSET {start_index}
    """

        # Выполнение SQL-запроса и загрузка результатов в DataFrame
        result = client.query(query_combined)
        # Создание DataFrame с результатами запроса
        df_combined = pd.DataFrame(
            result.result_rows,
            columns=['url', 'breadcrumbs', 'available_quantity', 'price', 'product_created']
        )
        if df_combined.empty:
            print("No more data to process.")
            break

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
        if start_index >= 10000:
            break

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

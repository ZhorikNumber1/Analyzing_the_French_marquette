import pandas as pd
from sqlalchemy import create_engine
import json
from datetime import datetime, timedelta

engine = create_engine(
    'postgresql://mercadolibre:YSquJluh2hTpvon@db-postgresql-nyc3-89544-do-user-11417380-0.c.db.ondigitalocean.com:25060/defaultdb?sslmode=require')


def extract_full_category_path(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path.lower().strip()  # Уже приводим к нижнему регистру и удаляем пробелы тут
    except json.JSONDecodeError:
        return None


batch_size = 1000
start_index = 0
thirty_days_ago = datetime.now() - timedelta(days=30)
date_str = thirty_days_ago.strftime('%Y-%m-%d')
all_data = []
target_categories = [
    'tecnología',
    'celulares y telefonía',
    'computación',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]

while True:
    print(f"Processing offset {start_index}...")

    query_combined = f"""
    SELECT psd.url, 
           psd.breadcrumbs, 
           pvd.available_quantity, 
           pvd.price,
           pvd.created AS product_created
    FROM product_static_data psd
    LEFT JOIN product_variable_data pvd ON psd.url = pvd.url_id AND pvd.created >= '{date_str}'
    ORDER BY psd.url ASC, pvd.created DESC, pvd.url_id ASC
    LIMIT {batch_size} OFFSET {start_index};
    """
    df_combined = pd.read_sql_query(query_combined, engine)
    if df_combined.empty:
        print("No more data to process.")
        break

    # Продолжаем обработку данных
    df_combined['categories'] = df_combined['breadcrumbs'].apply(extract_full_category_path)
    df_combined = df_combined.drop(columns=['breadcrumbs'], axis=1)
    df_combined['price'] = pd.to_numeric(df_combined['price'], errors='coerce')
    df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'])

    # Фильтруем по первой категории 'tecnología'
    filtered_df = df_combined[df_combined['categories'].apply(lambda x: any(cat in x for cat in target_categories) if x else False)]
    if not filtered_df.empty:
        print("______________Обнаружены записи, соответствующие одной или нескольким целевым категориям.______________")
        all_data.append(filtered_df)
    else:
        print("Записи, соответствующие целевым категориям, НЕ обнаружены в текущем наборе данных.")
    start_index += batch_size

# Объединяем все полученные части данных
df_final = pd.concat(all_data, ignore_index=True)
df_final = (
    df_final.sort_values(by=['url', 'product_created'])
    .assign(
        stock_change=lambda x: x.groupby('url')['available_quantity'].diff().fillna(0),
        sale_detected=lambda x: x['stock_change'] < 0,
        movement_detected=lambda x: x['stock_change'] != 0
    )
)

metrics = df_final.groupby('categories').agg(
    total_products=('url', 'nunique'),
    products_with_movement=('movement_detected', 'sum'),
    products_with_sales=('sale_detected', 'sum'),
    total_stock=('available_quantity', 'sum'),
).reset_index()

metrics['%_with_movement'] = (metrics['products_with_movement'] / metrics['total_products']) * 100
metrics['%_with_sales'] = (metrics['products_with_sales'] / metrics['total_products']) * 100
metrics['products_with_available_quantity'] = df_final[df_final['available_quantity'] > 0].groupby('categories')['url'].nunique()


revenue_per_category = df_final.groupby('categories')['price'].sum().rename('revenue')

metrics = metrics.join(revenue_per_category, on='categories')

excel_path = 'product_report_full.xlsx'
metrics.to_excel(excel_path, index=False)
print(f"Отчет успешно сохранен в файле {excel_path}")
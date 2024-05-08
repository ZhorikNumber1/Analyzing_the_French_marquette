import pandas as pd
import json
from sqlalchemy import create_engine
import ast
from datetime import datetime, timedelta


# Функция для преобразования JSON Breadcrumb в читаемый текстовый путь категории

def extract_all_categories(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path
    except json.JSONDecodeError:
        return None


# Функция для преобразования JSON Brands в читаемый текстовый путь категории

def extract_all_brands(brand_json):
    if brand_json is None:
        return None
    try:
        brand_data = json.loads(brand_json)
        if isinstance(brand_data, list):
            brand_names = [brand.get('name') for brand in brand_data if brand]
            return brand_names
        elif isinstance(brand_data, dict):
            return [brand_data.get('name')]
        else:
            return []
    except json.JSONDecodeError:
        return None


engine = create_engine(
    'postgresql://mercadolibre:YSquJluh2hTpvon@db-postgresql-nyc3-89544-do-user-11417380-0.c.db.ondigitalocean.com:25060/defaultdb?sslmode=require')

# Параметры для извлечения данных
batch_size = 10000
start_index = 0
data_loaded = True

target_categories = [
    'celulares y telefonía',
    'computación',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]

i = 0
thirty_days_ago = datetime.now() - timedelta(days=30)
date_str = thirty_days_ago.strftime('%Y-%m-%d')
while data_loaded:
    query_combined = f"""
    SELECT psd.url, 
           psd.breadcrumbs, 
           psd.brand, 
           psd.seller_id, 
           psi.seller_name, 
           psi.created AS seller_created,
           pvd.available_quantity, 
           pvd.price,
           pvd.created AS product_created
    FROM product_static_data psd
    JOIN product_seller_info psi ON psd.seller_id = psi.seller_id
    LEFT JOIN product_variable_data pvd ON psd.url = pvd.url_id AND pvd.created >= '{date_str}'
    ORDER BY psd.url ASC, pvd.created DESC
    LIMIT {batch_size} OFFSET {start_index};
    """
    df_combined = pd.read_sql_query(query_combined, engine)
    if df_combined.empty:
        break
    df_combined['categories'] = df_combined['breadcrumbs'].apply(extract_all_categories)
    df_combined['brands'] = df_combined['brand'].apply(extract_all_brands)
    # Фильтруем по первой категории
    df_combined = df_combined[
        df_combined['categories'].apply(lambda x: any(cat in x for cat in target_categories) if x else False)]
    if not df_combined.empty:
        print("______________Обнаружены записи, соответствующие одной или нескольким целевым категориям.______________")
    else:
        print("Записи, соответствующие целевым категориям, НЕ обнаружены в текущем наборе данных.")
    df_combined = df_combined.drop(columns=['breadcrumbs', 'brand'], axis=1)
    print(f"Процесс: {start_index}")

    # Теперь df_combined уже содержит данные по продавцам
    df_exploded = df_combined.explode('categories').explode('brands')
    df_exploded.to_csv('brands', index=True)
    # Кол-во уникальных брендов
    categories_metrics = df_exploded.groupby('categories').agg(
        unique_brands_count=('brands', pd.Series.nunique),
    ).reset_index()
    # Отладочный файл
    # categories_metrics.to_csv('Brands_Otchet.csv', index=False)

    # Продолжаем обработку данных
    df_sorted = df_exploded.sort_values(by=['brands', 'categories', 'product_created'])

    df_sorted['available_quantity'] = pd.to_numeric(df_sorted['available_quantity'],
                                                    errors='coerce')
    df_sorted = df_sorted.dropna(subset=['available_quantity'])

    df_sorted['quantity_change'] = df_sorted.groupby(['url', 'brands'])['available_quantity'].diff()

    df_sorted['sale_occurred'] = df_sorted['quantity_change'] < 0

    # Аггрегация данных
    brands_with_sales = df_sorted.groupby(['categories', 'brands']).agg(
        sale_detected=('sale_occurred', 'max')
    ).reset_index()
    categories_metrics = brands_with_sales.groupby('categories').agg(
        brands_with_sales=('sale_detected', 'sum'),
        total_brands=('brands', 'count'),
    ).reset_index()
    # Рассчет метрик
    categories_metrics['percent_brands_with_sales'] = (categories_metrics['brands_with_sales'] / categories_metrics[
        'total_brands']) * 100

    brands_sellers_sales = df_sorted.groupby(['categories', 'brands', 'seller_id']).agg(
        sale_detected=('sale_occurred', 'max')
    ).reset_index()

    sellers_counts = brands_sellers_sales.groupby('categories').agg(
        sellers_count=('seller_id', pd.Series.nunique)
    ).reset_index()

    sellers_with_sales = brands_sellers_sales[brands_sellers_sales['sale_detected']].groupby('categories').agg(
        sellers_with_sales_count=('seller_id', pd.Series.nunique)
    ).reset_index()

    # Объединяем данные
    categories_metrics = pd.merge(sellers_counts, sellers_with_sales, on='categories', how='left')
    categories_metrics['%_sellers_with_sales'] = (categories_metrics['sellers_with_sales_count'] / categories_metrics[
        'sellers_count']) * 100

    brands_with_sales = brands_sellers_sales.groupby(['categories', 'brands']).agg(
        sale_detected=('sale_detected', 'max')
    ).reset_index()

    # Считаем агрегированную статистику по категориям
    categories_metrics_brands = brands_with_sales.groupby('categories').agg(
        brands_with_sales=('sale_detected', 'sum'),
        total_brands=('brands', 'count'),
    ).reset_index()
    categories_metrics_brands['%_brands_with_sales'] = (categories_metrics_brands['brands_with_sales'] /
                                                        categories_metrics_brands[
                                                            'total_brands']) * 100
    final_categories_metrics = pd.merge(categories_metrics_brands, categories_metrics, on='categories')

    i += 1
    # Выгрузка
    final_categories_metrics.to_csv(f'Final_Categories_Metrics_{i}.csv', index=False)
    start_index += batch_size
import pandas as pd
from sqlalchemy import create_engine
import json

# Подключение к базе данных
engine = create_engine('postgresql://mercadolibre:YSquJluh2hTpvon@wss-postgresql-do-user-11417380-0.b.db.ondigitalocean.com:25060/defaultdb?sslmode=require')

# Загрузка данных
query_static = "SELECT url, breadcrumbs, brand FROM product_static_data;"
query_variable = "SELECT url_id, available_quantity, price, created FROM product_variable_data;"
df_static = pd.read_sql(query_static, engine)
df_variable = pd.read_sql(query_variable, engine)

# Обработка данных о категориях
def extract_all_categories(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path
    except json.JSONDecodeError:
        return None

df_static['categories'] = df_static['breadcrumbs'].apply(extract_all_categories)
df_variable.rename(columns={'url_id': 'url'}, inplace=True)
df_combined = pd.merge(df_static, df_variable, on='url', how='outer')

# Преобразование и расчет изменений
df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'], errors='coerce').fillna(0)
df_combined['sale_detected'] = df_combined.groupby('url')['available_quantity'].diff().fillna(0) < 0

# Подготовка данных для анализа
df_exploded = df_combined.explode('categories')

# Агрегация данных по категориям и брендам
df_metrics = df_exploded.groupby(['categories', 'brand']).agg(
    total_products=('url', 'nunique'),
    products_with_sales=('sale_detected', lambda x: (x.sum() > 0).astype(int))
).reset_index()

# Агрегация данных по категориям
df_category_metrics = df_metrics.groupby('categories').agg(
    total_brands=('brand', 'nunique'),
    brands_with_sales=('products_with_sales', 'sum')
)

# Расчет процентов
df_category_metrics['%_brands_with_sales'] = (df_category_metrics['brands_with_sales'] / df_category_metrics['total_brands']) * 100

print(df_category_metrics)

# Сохранение в Excel
df_category_metrics.to_excel('product_category_and_brand_analysis.xlsx')
print("Отчет сохранен в файле 'product_category_and_brand_analysis.xlsx'")
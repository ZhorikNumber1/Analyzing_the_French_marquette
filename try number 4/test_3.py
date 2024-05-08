import pandas as pd
from sqlalchemy import create_engine
import json

# Подключение к базе данных
engine = create_engine('postgresql://mercadolibre:YSquJluh2hTpvon@wss-postgresql-do-user-11417380-0.b.db.ondigitalocean.com:25060/defaultdb?sslmode=require')

# Загрузка данных о продуктах
query_static = "SELECT url, breadcrumbs, brand, seller_id FROM product_static_data;"
query_variable = "SELECT url_id, available_quantity, price, created FROM product_variable_data;"
query_sellers = "SELECT seller_id, seller_name FROM product_seller_info;"

df_static = pd.read_sql(query_static, engine)
df_variable = pd.read_sql(query_variable, engine)
df_sellers = pd.read_sql(query_sellers, engine)

# Обработка данных о категориях
def extract_all_categories(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_names = [breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb]
        return category_names
    except json.JSONDecodeError:
        return None

df_static['categories'] = df_static['breadcrumbs'].apply(extract_all_categories)
df_variable.rename(columns={'url_id': 'url'}, inplace=True)

# Соединение данных
df_combined = pd.merge(df_static, df_variable, on='url', how='outer')
df_combined = pd.merge(df_combined, df_sellers, on='seller_id', how='left')

# Преобразование и расчет изменений
df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'], errors='coerce').fillna(0)
df_combined['sale_detected'] = df_combined.groupby('url')['available_quantity'].diff().fillna(0) < 0

# Подготовка данных для анализа
df_exploded = df_combined.explode('categories')

# Расчет последней даты и последней цены для каждого продукта
df_exploded['last_date'] = pd.to_datetime(df_exploded['created'])
latest_date = df_exploded['last_date'].max()
df_exploded['last_price'] = df_exploded.groupby('url')['price'].transform(lambda x: x.iloc[-1])

# Агрегация и расчет метрик по категориям
df_category_metrics = df_exploded.groupby('categories').agg(
    total_sellers=('seller_id', 'nunique'),
    sellers_with_sales=('sale_detected', lambda x: x.any()),
    min_price=('last_price', 'min'),
    max_price=('price', 'max'),
    avg_price=('price', 'mean'),
    median_price=('price', 'median')
).reset_index()

df_category_metrics['%_sellers_with_sales'] = (df_category_metrics['sellers_with_sales'] / df_category_metrics['total_sellers']) * 100

print(df_category_metrics)

# Сохранение в Excel
df_category_metrics.to_excel('product_category_seller_and_price_analysis.xlsx')
print("Отчет сохранен в файле 'product_category_seller_and_price_analysis.xlsx'")
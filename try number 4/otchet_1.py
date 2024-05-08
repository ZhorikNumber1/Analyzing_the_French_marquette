import pandas as pd
from sqlalchemy import create_engine
import json

engine = create_engine('postgresql://mercadolibre:YSquJluh2hTpvon@db-postgresql-nyc3-89544-do-user-11417380-0.c.db.ondigitalocean.com:25060/defaultdb?sslmode=require')

query_static = f"SELECT url, breadcrumbs, brand FROM product_static_data ORDER BY url LIMIT {20000} OFFSET {0};"
query_variable = f"SELECT url_id, available_quantity, price, created FROM product_variable_data ORDER BY url_id ASC, created DESC LIMIT {20000} OFFSET {0};"

df_static = pd.read_sql(query_static, engine)
df_variable = pd.read_sql(query_variable, engine)

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
df_combined = pd.merge(df_static, df_variable, on='url', how='inner')

df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'], errors='coerce').fillna(0)
df_combined['stock_change'] = df_combined.groupby('url')['available_quantity'].diff().fillna(0)
df_combined['sale_detected'] = df_combined['stock_change'] < 0

df_exploded = df_combined.explode('categories')

df_metrics = df_exploded.groupby(['categories', 'sale_detected']).size().reset_index(name='total_sales')

df_metrics = df_exploded.groupby('categories', as_index=False).agg(
    total_products=('url', 'nunique'),
    products_with_movement=('stock_change', lambda x: (x != 0).sum()),
    products_with_sales=('sale_detected', 'sum'),
)
df_metrics['%_with_movement'] = (df_metrics['products_with_movement'] / df_metrics['total_products']) * 100
df_metrics['%_with_sales'] = (df_metrics['products_with_sales'] / df_metrics['total_products']) * 100
df_metrics['total_sales'] = df_exploded[df_exploded['sale_detected']].groupby('categories')['sale_detected'].count()
excel_path = 'count_%_sales.csv'
df_metrics.to_csv(excel_path)

print(f"Отчет успешно сохранен в файле {excel_path}")

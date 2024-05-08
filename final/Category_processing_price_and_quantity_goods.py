import pandas as pd
from sqlalchemy import create_engine
import json
from datetime import datetime, timedelta

engine = create_engine(
    'postgresql://mercadolibre:YSquJluh2hTpvon@db-postgresql-nyc3-89544-do-user-11417380-0.c.db.ondigitalocean.com:25060/defaultdb?sslmode=disable&sslrootcert=~/.postgresql/root.crt')

# Функция для преобразования JSON Breadcrumb в читаемый текстовый путь категории
def extract_full_category_path(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path.lower().strip()
    except json.JSONDecodeError:
        return None

# Параметры управления процессом извлечения данных
batch_size = 10000
start_index = 0
thirty_days_ago = datetime.now() - timedelta(days=30)
current_date = datetime.now().strftime('%Y-%m-%d')
date_str = thirty_days_ago.strftime('%Y-%m-%d')
all_data = []
target_categories = [
    'celulares y telefonía',
    'computación',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]

# Цикл для последовательного извлечения и обработки данных из базы
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
LIMIT {batch_size} OFFSET {start_index}
    """
    df_combined = pd.read_sql_query(query_combined, engine)
    if df_combined.empty:
        print("No more data to process.")
        break
    # Преобразование breadcrumbs и очистка данных
    df_combined['categories'] = df_combined['breadcrumbs'].apply(extract_full_category_path)
    df_combined = df_combined.drop(columns=['breadcrumbs'], axis=1)
    df_combined['price'] = pd.to_numeric(df_combined['price'], errors='coerce')
    df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'], errors='coerce')
    df_combined = df_combined.dropna(subset=['available_quantity'])
    # Фильтрация данных по целевым категориям
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
        stock_change=lambda x: x.groupby('url')['available_quantity'].diff(),
    )
)
df_final = df_final.assign(
    sale_detected=lambda x: x['stock_change'] < 0,
    movement_detected=lambda x: x['stock_change'] != 0
)

current_day_df = df_final[df_final['product_created'] == current_date]

total_stock_current_day = (
    current_day_df.groupby('categories').agg(
        total_stock=('available_quantity', 'sum')
    )
).reset_index()
# Группировка данных для получения ключевых метрик по категориям
metrics = df_final.groupby('categories').agg(
    total_products=('url', 'nunique'),
    products_with_movement=('movement_detected', 'sum'),
    products_with_sales=('sale_detected', 'sum'),
).reset_index()

# Добавление процентных показателей для дальнейшего анализа
metrics['%_with_movement'] = (metrics['products_with_movement'] / metrics['total_products']) * 100
metrics['%_with_sales'] = (metrics['products_with_sales'] / metrics['total_products']) * 100
metrics['products_with_available_quantity'] = df_final[df_final['available_quantity'] > 0].groupby('categories')['url'].nunique()
revenue_per_category = df_final.groupby('categories').apply(lambda x: ((x['price'].shift() + x['price']) / 2 * -x['stock_change']).sum()).clip(lower=0)

# Объединение данных о продажах с ключевыми метриками
metrics = metrics.merge(revenue_per_category, on='categories', how='left')
metrics = metrics.merge(total_stock_current_day, on='categories', how='left')

# Сохранение
excel_path = 'Category_processing_price_and_quantity_goods.xlsx'
metrics.to_excel(excel_path, index=False)
print(f"Отчет успешно сохранен в файле {excel_path}")
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

engine = create_engine('postgresql://mercadolibre:YSquJluh2hTpvon@db-postgresql-nyc3-89544-do-user-11417380-0.c.db.ondigitalocean.com:25060/defaultdb?sslmode=require')

# Установка размера порции данных для обработки за один раз и начального индекса
batch_size = 10000
start_index = 0

# Определение целевых категорий для фильтрации
target_categories = [
    'celulares y telefonía',
    'computación',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]

all_data = []
thirty_days_ago = datetime.now() - timedelta(days=30)
date_str = thirty_days_ago.strftime('%Y-%m-%d')

# Функция для извлечения полного пути категории из JSON-строки breadcrumbs
def extract_full_category_path(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path.lower().strip()
    except json.JSONDecodeError:
        return None


while True:

    # Перебираем данные из базы данных порциями до тех пор, пока есть данные для обработки
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
    # Выполнение SQL-запроса и загрузка результатов в DataFrame
    df_combined = pd.read_sql_query(query_combined, engine)
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
    print(f"Обработанно: {start_index}")
    if start_index >= 10000:
        break

# Объединение всех отфильтрованных данных в один DataFrame
all_data = pd.concat(all_data, ignore_index=True)
df_filtered_with_category = all_data.assign(
    last_category=all_data['categories'].apply(lambda x: x[-1] if x else None)
)
# Группировка по последней категории и расчет статистик цен
df_category_prices = df_filtered_with_category.groupby('last_category')['price'].agg(
    ['min', 'max', 'mean', 'median']
).reset_index()
df_category_prices.columns = ['Категория', 'Минимальная цена', 'Максимальная цена', 'Средняя цена',
                              'Медианная цена']
# Сохранение данных
df_category_prices.to_csv('combined_data.csv', index=False, encoding='utf-8')
print("Data processing complete.")
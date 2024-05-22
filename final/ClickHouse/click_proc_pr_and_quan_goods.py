import pandas as pd
import json
from datetime import datetime, timedelta
import clickhouse_connect

# Подключение к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
    user='default',
    password='KIa.AG53Vxaf1',
    port=8443,
    secure=True
)

# Функция для преобразования JSON Breadcrumb в читаемый текстовый путь категории
def extract_full_category_path(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)
        # Соединяем имена категорий в одну строку, разделенную запятыми
        category_path = ','.join([breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])
        return category_path.lower().strip()
    except json.JSONDecodeError:
        return None

# Параметры управления процессом извлечения данных
batch_size = 10000  # Размер пакета данных для выборки
start_index = 0  # Начальный индекс для смещения
thirty_days_ago = datetime.now() - timedelta(days=30)  # Дата 30 дней назад от текущей даты
current_date = datetime.now().strftime('%Y-%m-%d')  # Текущая дата в формате строки
date_str = thirty_days_ago.strftime('%Y-%m-%d')  # Дата 30 дней назад в формате строки
all_data = []  # Список для хранения всех полученных данных

# Список целевых категорий
target_categories = [
    'celulares y telefonía',
    'computación',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]

# Цикл для последовательного извлечения и обработки данных из базы
while True:
    print(f"Processing offset {start_index}...")  # Печать текущего смещения

    # SQL-запрос для извлечения данных с использованием смещения и лимита
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
    product_variable_data pvd ON psd.url = pvd.url_id AND pvd.created >= '{date_str}'
ORDER BY 
    psd.url ASC, pvd.created DESC, pvd.url_id ASC
LIMIT 
    {batch_size} OFFSET {start_index}
    """

    # Выполнение запроса и создание DataFrame из результатов
    result_combined = client.query(query_combined)
    df_combined = pd.DataFrame(result_combined.result_rows,
                               columns=['url', 'breadcrumbs', 'available_quantity', 'price', 'product_created'])

    if df_combined.empty:
        print("No more data to process.")  # Если данных больше нет, прерываем цикл
        break

    # Преобразование breadcrumbs и очистка данных
    df_combined['categories'] = df_combined['breadcrumbs'].apply(extract_full_category_path)
    df_combined = df_combined.drop(columns=['breadcrumbs'], axis=1)  # Удаление столбца breadcrumbs
    df_combined['price'] = pd.to_numeric(df_combined['price'], errors='coerce')  # Преобразование цены в числовой формат
    df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'], errors='coerce')  # Преобразование доступного количества в числовой формат
    df_combined = df_combined.dropna(subset=['available_quantity'])  # Удаление строк с пустыми значениями в available_quantity

    # Фильтрация данных по целевым категориям
    filtered_df = df_combined[
        df_combined['categories'].apply(lambda x: any(cat in x for cat in target_categories) if x else False)]

    if not filtered_df.empty:
        print("______________Обнаружены записи, соответствующие одной или нескольким целевым категориям.______________")
        all_data.append(filtered_df)  # Добавление отфильтрованных данных в список
    else:
        print("Записи, соответствующие целевым категориям, НЕ обнаружены в текущем наборе данных.")

    start_index += batch_size  # Увеличение смещения для следующей итерации

# Объединяем все полученные части данных
df_final = pd.concat(all_data, ignore_index=True)
df_final = (
    df_final.sort_values(by=['url', 'product_created'])
    .assign(
        stock_change=lambda x: x.groupby('url')['available_quantity'].diff(),  # Расчет изменения запаса
    )
)
df_final = df_final.assign(
    sale_detected=lambda x: x['stock_change'] < 0,  # Обнаружение продаж по снижению запаса
    movement_detected=lambda x: x['stock_change'] != 0  # Обнаружение любых движений запаса
)

# Фильтрация данных за текущий день
current_day_df = df_final[df_final['product_created'] == current_date]

# Расчет общего запаса по категориям за текущий день
total_stock_current_day = (
    current_day_df.groupby('categories').agg(
        total_stock=('available_quantity', 'sum')
    )
).reset_index()

# Группировка данных для получения ключевых метрик по категориям
metrics = df_final.groupby('categories').agg(
    total_products=('url', 'nunique'),  # Общее количество уникальных продуктов
    products_with_movement=('movement_detected', 'sum'),  # Количество продуктов с движением запаса
    products_with_sales=('sale_detected', 'sum'),  # Количество продуктов с продажами
).reset_index()

# Добавление процентных показателей для дальнейшего анализа
metrics['%_with_movement'] = (metrics['products_with_movement'] / metrics['total_products']) * 100
metrics['%_with_sales'] = (metrics['products_with_sales'] / metrics['total_products']) * 100
metrics['products_with_available_quantity'] = df_final[df_final['available_quantity'] > 0].groupby('categories')[
    'url'].nunique()

# Расчет доходов по категориям
revenue_per_category = df_final.groupby('categories').apply(
    lambda x: ((x['price'].shift() + x['price']) / 2 * -x['stock_change']).sum()).clip(lower=0)

# Объединение данных о продажах с ключевыми метриками
metrics = metrics.merge(revenue_per_category, on='categories', how='left')
metrics = metrics.merge(total_stock_current_day, on='categories', how='left')

# Сохранение итоговых метрик в файл Excel
excel_path = 'Category_processing_price_and_quantity_goods.xlsx'
metrics.to_excel(excel_path, index=False)
print(f"Отчет успешно сохранен в файле {excel_path}")

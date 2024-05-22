import pandas as pd  # Импортируем библиотеку pandas для работы с данными в табличном формате
from sqlalchemy import create_engine  # Импортируем create_engine из SQLAlchemy для работы с базами данных
import os  # Импортируем os для работы с операционной системой (например, для чтения переменных окружения)
import clickhouse_connect  # Импортируем библиотеку для подключения к ClickHouse
import json  # Импортируем json для работы с JSON-данными

# Параметры подключения к ClickHouse
client = clickhouse_connect.get_client(
    host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',  # Адрес сервера ClickHouse
    user='default',  # Имя пользователя для подключения
    password='KIa.AG53Vxaf1',  # Пароль пользователя
    port=8443,  # Порт для подключения
    secure=True  # Использование защищенного соединения (HTTPS)
)

# Постоянные параметры
batch_size = 1000  # Размер пакета данных для выборки
start_index = 0  # Начальный индекс для выборки
target_categories = [  # Целевые категории для фильтрации данных
    'celulares y telefonía',
    'Celulares y Telefon\u00eda',
    'computación',
    'Computaci\u00f3n',
    'cámaras y accesorios',
    'electrónica',
    'consolas y videojuegos'
]


# Функция для извлечения полного пути категории из JSON-строки
def extract_full_category_path(breadcrumb_json):
    if breadcrumb_json is None:
        return None
    try:
        breadcrumbs = json.loads(breadcrumb_json)  # Парсим JSON
        category_path = ','.join(
            [breadcrumb['name'] for breadcrumb in breadcrumbs if breadcrumb])  # Формируем путь категории
        return category_path.lower().strip()  # Приводим к нижнему регистру и убираем пробелы
    except json.JSONDecodeError:
        return None


# Задаем диапазон дат для обработки
date_str = '2024-01-03'
date_str_now = '2024-02-03'
current_date = pd.to_datetime(date_str)
end_date = pd.to_datetime(date_str_now)
all_data = []

while current_date < end_date:
    next_date = current_date + pd.Timedelta(days=1)  # Переходим к следующей дате
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
        WHERE
            DATE(created) = '{current_date.strftime('%Y-%m-%d')}'
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
            print("No more data to process.")  # Если больше нет данных для обработки, выходим из цикла
            break

        # Объединение DataFrame аналогично LEFT JOIN
        df_combined = df_static_data.merge(df_variable_data, on='url', how='left')

        print(df_combined.head())  # Выводим первые несколько строк для проверки

        # Продолжаем обработку данных
        df_combined['categories'] = df_combined['breadcrumbs'].apply(extract_full_category_path)  # Извлекаем категории
        df_combined = df_combined.drop(columns=['breadcrumbs'], axis=1)  # Удаляем колонку breadcrumbs
        df_combined['price'] = pd.to_numeric(df_combined['price'],
                                             errors='coerce')  # Преобразуем price к числовому типу
        df_combined['available_quantity'] = pd.to_numeric(df_combined['available_quantity'],
                                                          errors='coerce')  # Преобразуем available_quantity к числовому типу
        df_combined = df_combined.dropna(
            subset=['available_quantity'])  # Удаляем строки с пустыми значениями available_quantity

        # Фильтруем по целевым категориям
        filtered_df = df_combined[
            df_combined['categories'].apply(lambda x: any(cat in x for cat in target_categories) if x else False)
        ]
        if not filtered_df.empty:
            print(
                "______________Обнаружены записи, соответствующие одной или нескольким целевым категориям.______________")
            all_data.append(filtered_df)  # Добавляем отфильтрованные данные в общий список
        else:
            print("Записи, соответствующие целевым категориям, НЕ обнаружены в текущем наборе данных.")

        start_index += batch_size  # Увеличиваем стартовый индекс для следующей выборки
        print(f"Обработано: {start_index} для {current_date.strftime('%Y-%m-%d')}")

    current_date = next_date  # Переходим к следующей дате
    start_index = 0  # Сбрасываем стартовый индекс
    print(f"Обработано: {current_date}")

# Объединение всех отфильтрованных данных в один DataFrame
if all_data:
    all_data = pd.concat(all_data, ignore_index=True)  # Объединяем все данные в один DataFrame
    df_filtered_with_category = all_data.assign(
        last_category=all_data['categories'].apply(lambda x: x[-1] if x else None)
        # Добавляем столбец с последней категорией
    )

    # Вывод результата в консоль и сохранение в CSV-файл
    print(df_filtered_with_category)
    output_filename = 'filtered_data.csv'
    df_filtered_with_category.to_csv(output_filename, index=False)
    print(f"Файл сохранён как {output_filename}")

    # Анализ данных по категориям
    df_category_prices = df_filtered_with_category.groupby('categories').agg(
        Минимальная_цена=pd.NamedAgg(column='price', aggfunc='min'),
        Максимальная_цена=pd.NamedAgg(column='price', aggfunc='max'),
        Средняя_цена=pd.NamedAgg(column='price', aggfunc='mean'),
        Медианная_цена=pd.NamedAgg(column='price', aggfunc='median')
    ).reset_index()

    # Переименование столбцов
    df_category_prices.columns = ['Категория', 'Минимальная цена', 'Максимальная цена', 'Средняя цена',
                                  'Медианная цена']

    # Вывод результирующего DataFrame
    print(df_category_prices)
    df_category_prices.to_csv('filtered_data_1.csv')
else:
    print("Нет данных для сохранения.")  # Если данных нет, выводим сообщение

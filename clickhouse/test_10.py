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

# SQL-запрос для выборки данных из таблицы product_variable_data
query_product_variable_data = f"""
        SELECT 
            url_id,  # Идентификатор URL
            available_quantity,  # Доступное количество
            price,  # Цена
            created  # Дата создания записи
        FROM 
            product_variable_data
        ORDER BY 
            created DESC  # Сортировка по дате создания в обратном порядке (от последних к первым)
        LIMIT 20000  # Ограничение выборки до 20000 записей
    """
# Выполняем запрос к базе данных ClickHouse
result_variable_data = client.query(query_product_variable_data)

# Преобразуем результат запроса в DataFrame pandas
df_variable_data = pd.DataFrame(
    result_variable_data.result_rows,  # Результаты запроса (строки)
    columns=['url', 'available_quantity', 'price', 'product_created']  # Задаем имена столбцов
)

# Выводим первые 5 строк DataFrame для проверки
print(df_variable_data.head())

# Выводим последние 5 строк DataFrame для проверки
print(df_variable_data.tail(5))

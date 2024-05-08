import pandas as pd

# Загружаем данные
df_static = pd.read_csv('product_static_data.csv', converters={'breadcrumbs': pd.eval})
df_variable = pd.read_csv('product_variable_data.csv')

# Сортировка df_variable по url_id и дате создания, чтобы упростить анализ изменений стоков
df_variable = df_variable.sort_values(['url_id', 'created'])

# Определение изменений в стоке для каждого товара
df_variable['stock_changed'] = df_variable.groupby('url_id')['available_quantity'].diff().ne(0)

# Идентифицируем товары, у которых были изменения в стоке
df_variable['changed'] = df_variable.groupby('url_id')['stock_changed'].transform('any')

# Расширяем статические данные для получения строк по каждой категории
df_static_exploded = df_static.explode('breadcrumbs')

# Соединяем статические и переменные данные
df_joined = pd.merge(df_static_exploded, df_variable, left_on='url', right_on='url_id')

# Подсчет количества товаров в категории
total_products_per_category = df_joined.groupby('breadcrumbs')['url'].nunique().reset_index(name='Count_of_Products')

# Подсчет изменившихся товаров в категории, учитывая только изменения стока
changed_products_per_category = df_joined[df_joined['changed']].groupby('breadcrumbs')['url'].nunique().reset_index(name='Changed_Products')

# Объединяем данные в итоговый отчет
report = pd.merge(total_products_per_category, changed_products_per_category, on='breadcrumbs', how='left').fillna(0)

# Преобразуем значения колонки количества измененных товаров в int (могут быть NaN после объединения)
report['Changed_Products'] = report['Changed_Products'].astype(int)

# Сохраняем итоговый отчет
report.to_csv('final_corrected_report.csv', index=False)

print("Исправленный отчет создан.")

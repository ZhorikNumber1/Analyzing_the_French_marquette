import clickhouse_connect

if __name__ == '__main__':
    # Создать клиента для подключения к ClickHouse
    client = clickhouse_connect.get_client(
        host='zkabe5kjqt.eu-west-2.aws.clickhouse.cloud',
        user='default',
        password='KIa.AG53Vxaf1',
        port=8443,
        secure=True
    )
    # Выполнить запрос и напечатать результат
    try:
        result = client.query('SELECT breadcrumbs FROM "product_static_data" LIMIT 10 OFFSET 0')

        print("Result:", result.result_rows)
    except Exception as e:
        print("An error occurred:", e)



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
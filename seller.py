import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список всех товаров из магазина Ozon.

    Делает запрос к Ozon API и возвращает часть списка товаров.

    Args:
        last_id (str): Идентификатор последнего товара из предыдущего запроса.
        client_id (str): Идентификатор продавца в Ozon.
        seller_token (str): Ключ доступа к API Ozon.

    Returns:
        dict: Часть списка товаров магазина.

    Raises:
        requests.exceptions.RequestException: При ошибке запроса к Ozon API.

    Example:
        >>> get_product_list("", "123456", "abc123")
        {'items': [...], 'total': 100, ...}
    """
    
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает список артикулов (offer_id) всех товаров в магазине.

    Args:
        client_id (str): Идентификатор продавца в Ozon.
        seller_token (str): Ключ доступа к API.

    Returns:
        list: Список offer_id товаров.

    Raises:
        requests.exceptions.RequestException: Если не удалось получить данные.

    Example:
        >>> get_offer_ids("123456", "abc123")
        ['0001', '0002', '0003']
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Отправляет обновлённые цены на Ozon.

    Args:
        prices (list): Список словарей с ценами.
        client_id (str): Идентификатор продавца.
        seller_token (str): Ключ доступа к API.

    Returns:
        dict: Ответ от сервера.

    Raises:
        requests.exceptions.RequestException: При ошибке загрузки цен.

    Example:
        >>> update_price([{'offer_id': '123', 'price': '990'}], "id", "token")
        {'result': 'ok'}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на Ozon.

    Args:
        stocks (list): Список остатков с offer_id и количеством.
        client_id (str): Идентификатор продавца.
        seller_token (str): Ключ API.

    Returns:
        dict: Ответ сервера.

    Raises:
        requests.exceptions.RequestException: Если запрос не прошёл.

    Example:
        >>> update_stocks([{'offer_id': '123', 'stock': 5}], "id", "token")
        {'result': 'ok'}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает Excel с остатками часов Casio с сайта поставщика.

    Загружает zip-архив, распаковывает его и читает Excel-файл. Затем превращает его в список словарей.

    Returns:
        list: Остатки часов в виде списка словарей.

    Raises:
        requests.exceptions.RequestException: Если не удалось скачать файл.
        FileNotFoundError: Если файл не найден после распаковки.
        ValueError: Если структура Excel-файла некорректна.

    Example:
        >>> download_stock()
        [{'Код': '123', 'Количество': '10', ...}, ...]
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список остатков товаров для загрузки на Ozon.

    Приводит данные к нужному формату и выставляет нули для отсутствующих товаров.

    Args:
        watch_remnants (list): Остатки из Excel-файла.
        offer_ids (list): Артикулы товаров, загруженных на Ozon.

    Returns:
        list: Список словарей с остатками.

    Example:
        >>> create_stocks([{'Код': '123', 'Количество': '5'}], ['123'])
        [{'offer_id': '123', 'stock': 5}]
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен для загрузки на Ozon.

    Args:
        watch_remnants (list): Остатки с полем "Цена".
        offer_ids (list): Список артикулов, загруженных в магазин.

    Returns:
        list: Цены в формате, понятном для Ozon API.

    Example:
        >>> create_prices([{'Код': '123', 'Цена': "5'990.00 руб."}], ['123'])
        [{'offer_id': '123', 'price': '5990', ...}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует цену в числовой формат.

    Args:
        price (str): Цена вида "5'990.00 руб."

    Returns:
        str: Только целое число, без копеек. Например, "5990".

    Raises:
        TypeError: Если на вход пришёл None вместо строки.

    Example:
        >>> price_conversion("5'990.00 руб.")
        '5990'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Делит список на части по n элементов.

    Args:
        lst (list): Исходный список.
        n (int): Максимальный размер каждой части.

    Yields:
        list: Подсписки длиной до n элементов.

    Example:
        >>> list(divide([1, 2, 3, 4], 2))
        [[1, 2], [3, 4]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает цены на все товары в магазин Ozon.

    Args:
        watch_remnants (list): Остатки с полем "Цена".
        client_id (str): Идентификатор магазина.
        seller_token (str): Ключ API.

    Returns:
        list: Список всех отправленных цен.

    Raises:
        requests.exceptions.RequestException: При ошибке API-запроса.

    Example:
        >>> await upload_prices(remnants, "id", "token")
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает остатки товаров в магазин Ozon.

    Args:
        watch_remnants (list): Остатки из Excel.
        client_id (str): Идентификатор магазина.
        seller_token (str): Ключ API.

    Returns:
        tuple: Два списка — не нулевые остатки и все отправленные.

    Raises:
        requests.exceptions.RequestException: При ошибке API-запроса.

    Example:
        >>> await upload_stocks(remnants, "id", "token")
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()

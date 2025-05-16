import datetime
import logging.config
from environs import Env
from seller1 import download_stock

import requests

from seller1 import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров из магазина Яндекс Маркет.

    Делает запрос по API и возвращает товары с учётом пагинации.

    Args:
        page (str): Токен страницы. Если пустой — начнёт с начала.
        campaign_id (str): ID кампании на Яндекс Маркете.
        access_token (str): OAuth-токен для доступа к API.

    Returns:
        dict: Результат с данными о товарах.

    Raises:
        requests.exceptions.RequestException: Если запрос не удался.

    Example:
        >>> get_product_list("", "123", "abc")
        {'offerMappingEntries': [...], 'paging': {...}}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет остатки товаров на Яндекс Маркете.

    Args:
        stocks (list): Список словарей с остатками.
        campaign_id (str): ID кампании.
        access_token (str): OAuth-токен.

    Returns:
        dict: Ответ от сервера Яндекс Маркета.

    Raises:
        requests.exceptions.RequestException: Если запрос не удался.

    Example:
        >>> update_stocks([...], "123", "abc")
        {'result': 'ok'}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров на Яндекс Маркете.

    Args:
        prices (list): Список словарей с ценами.
        campaign_id (str): ID кампании.
        access_token (str): OAuth-токен.

    Returns:
        dict: Ответ от API Яндекса.

    Raises:
        requests.exceptions.RequestException: Если запрос не прошёл.

    Example:
        >>> update_price([...], "123", "abc")
        {'result': 'ok'}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает список артикулов (SKU) с Яндекс Маркета.

    Проходит по всем страницам и собирает shopSku всех товаров.

    Args:
        campaign_id (str): ID кампании.
        market_token (str): OAuth-токен для доступа к API.

    Returns:
        list: Список артикулов (shopSku).

    Raises:
        requests.exceptions.RequestException: Если запрос не удался.

    Example:
        >>> get_offer_ids("123", "abc")
        ['SKU123', 'SKU456']
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Формирует список остатков для загрузки на Яндекс Маркет.

    Подставляет количество для имеющихся артикулов, остальные заполняет нулями.

    Args:
        watch_remnants (list): Остатки из Excel-файла.
        offer_ids (list): Список артикулов с маркета.
        warehouse_id (str): ID склада на Яндекс Маркете.

    Returns:
        list: Список словарей с остатками в нужном формате.

    Example:
        >>> create_stocks([...], ['123'], 'wh_001')
        [{'sku': '123', 'warehouseId': 'wh_001', 'items': [...]}]
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен для загрузки на Яндекс Маркет.

    Args:
        watch_remnants (list): Остатки с полем "Цена".
        offer_ids (list): Артикулы, загруженные в магазин.

    Returns:
        list: Цены в формате, который принимает API Яндекса.

    Example:
        >>> create_prices([...], ['123'])
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загружает цены на все товары в магазин Яндекс Маркета.

    Делит список на части и отправляет пакетами.

    Args:
        watch_remnants (list): Остатки из Excel-файла.
        campaign_id (str): ID кампании.
        market_token (str): OAuth-токен.

    Returns:
        list: Все цены, которые были отправлены.

    Raises:
        requests.exceptions.RequestException: Если API вернул ошибку.

    Example:
        >>> await upload_prices([...], "123", "abc")
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загружает остатки в Яндекс Маркет.

    Формирует список остатков и отправляет партиями. Возвращает список не пустых остатков.

    Args:
        watch_remnants (list): Данные из Excel-файла.
        campaign_id (str): ID кампании.
        market_token (str): OAuth-токен.
        warehouse_id (str): ID склада.

    Returns:
        tuple: (товары с остатками больше 0, все отправленные остатки)

    Raises:
        requests.exceptions.RequestException: Если API вызов завершился ошибкой.

    Example:
        >>> await upload_stocks([...], "123", "abc", "wh_001")
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()

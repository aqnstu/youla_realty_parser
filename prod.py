from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent    # для изменения заголовка user-agent на случайный
import json
import random
import re
import requests
import sys
import pprint

from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
                                        ElementClickInterceptedException)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.proxy import Proxy, ProxyType

from sqlalchemy import create_engine, Column, ForeignKey, String, TIMESTAMP, text
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker

BASE_URL = 'https://youla.ru'
CITY = 'moskva'
SECTION = 'nedvijimost'

PARSE_URL = f'{BASE_URL}/{CITY}/{SECTION}'

# !тут пропишите данные для вашей БД (после 'mysql+pymysql://')
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:system32@localhost/parser'
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)

Base = declarative_base()
metadata = Base.metadata
factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
session = factory()


class Url(Base):
    __tablename__ = 'url'
    __table_args__ = {'comment': 'URL "карточек" недвижимости'}

    id = Column(INTEGER(11), primary_key=True)
    url = Column(String(1000))
    is_parsed = Column(TINYINT(1), server_default=text("0"))
    number_of_attempts = Column(INTEGER(11), nullable=False, server_default=text("1"))
    date_added = Column(TIMESTAMP, nullable=False, server_default=text("current_timestamp()"))
    date_parsed = Column(TIMESTAMP)


class Log(Base):
    __tablename__ = 'log'
    __table_args__ = {'comment': 'Лог ошибок для парсера'}

    id = Column(INTEGER(11), primary_key=True)
    id_url = Column(ForeignKey('url.id'), index=True)
    error_line = Column(INTEGER(11))
    error_type = Column(String(1000))
    error = Column(String(10000), nullable=False)
    date_add = Column(TIMESTAMP, nullable=False, server_default=text("current_timestamp()"))

    url = relationship('Url')


# список прокси, такая структура, так как могут http пригодиться
# один прокси рабочий только нашел
proxy_list = [
    {
        'http':     'http://185.187.197.108:8080',
        # 'https':    'https://91.225.226.39:44388'
    },
    {
        'http':     'http://185.187.197.108:8080',
        # 'https':    'https://91.225.226.39:44388'
    }
]


def main():
    try:
        ua = UserAgent()
        s = requests.Session()
        # случайный выбор прокси из списка доступных прокси,
        # чтобы не забанили по порядку перебора все прокси,
        # ! современные веб-серверы защищены от последовательного перебора прокси,
        # ! поэтому стоит подстраховаться
        s.proxies = random.choice(proxy_list)
        headers = {'User-Agent': ua.random}
        r_page = s.get(PARSE_URL, headers=headers)

        # полчаем объект страницы со вложенной структорой,
        # чтобы удобно получить относительные пути (url) для карточек
        soup = BeautifulSoup(r_page.content, 'html.parser')

        # получаем относительные пути для "карточек"
        card_section_list = [
            i.a['href'] for i in soup.find_all('li', class_='product_item')
        ]

        # получаем полный URL для "карточек"
        card_url_list = [BASE_URL + i for i in card_section_list]

        # соответствие численного значений категории их API Юлы и текстового наименования (не используется в коде)
        subcategory_dict = {
            2001: 'Продажа квартиры',
            2002: 'Продажа комнаты',
            2003: 'Продажа дома',
            2004: 'Продажа участка',
            2005: 'Аренда квартиры длительно',
            2006: 'Аренда комнаты длительно',
            2007: 'Аренда дома длительно',
            2008: 'Прочие строения',
            2010: 'Аренда квартиры посуточно',
            2011: 'Аренда комнаты посуточно',
            2012: 'Аренда дома посуточно',
            2013: 'Коммерческая недвижимость',
        }

        # соответствие 'taskcode' с 'subcategory'
        taskcode_dict = {
            2001: 1,    # продажа
            2002: 1,
            2003: 1,
            2004: 1,
            2005: 2,    # аренда
            2006: 2,
            2007: 2,
            2008: 0,    # неизвестно
            2010: 2,
            2011: 2,
            2012: 2,
            2013: 0
        }

        # соответствие 'typecode' с 'subcategory'
        typecode_dict = {
            2001: 1,    # квартира
            2002: 2,    # комната
            2003: 3,    # загородная
            2004: 3,
            2005: 1,
            2006: 2,
            2007: 3,
            2008: 0,    # неизвестно
            2010: 1,
            2011: 2,
            2012: 3,
            2013: 4     # коммерческая
        }

        # соответсвтие именования Юлы с выходным именованием словаря (json) ad_dict
        attribute_dict = {
            "tip_sdelki":               "taskcode",
            "name":                     "name",
            "description":              "text",
            "price":                    "cost",
            "realty_obshaya_ploshad":   "totalarea",
            "realty_ploshad_kuhni":     "kitchenarea",
            "komnat_v_kvartire":        "roomquantity",
            "realty_etaj":              "floor",
            "realty_etajnost_doma":     "floors",
            "sobstvennik_ili_agent":    "is_agent",
            "realty_building_type":     "housing",
            "realty_hidden_location":   "fullAddress",
            "posudomoechnaya_mashina":  "dishWasher",
            "holodilnik":               "refr",
            "remont":                   "repair",
            "lift":                     "cargoLift",
            "realty_god_postroyki":     "buildYear",
        }

        # конфиги selenium
        # добавил прокси
        prox = Proxy()
        prox.proxy_type = ProxyType.MANUAL
        prox.http_proxy = random.choice(proxy_list)['http'].replace('http://', '')
        capabilities = webdriver.DesiredCapabilities.CHROME
        prox.add_to_capabilities(capabilities)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument(f'user-agent="{ua.random}"')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    except Exception as e:
        _log = Log(
                error_line=sys.exc_info()[2].tb_lineno,
                error_type=type(e).__name__,
                error=str(e))
        session.add(_log)
        session.commit()
        # экстренный выход из программы
        sys.exit(1)
    # обрабатываем последовательно URL
    for card_url in card_url_list:
        try:
            browser = webdriver.Chrome(
                'chromedriver.exe',
                options=chrome_options,
                desired_capabilities=capabilities
                )
            browser.get(card_url)
            js = '__YOULA_STATE__.entities.products[0]'
            result = browser.execute_script(f"return {js}")
            browser.close()

            # пишем url в БД
            _url = Url(url=card_url)
            session.add(_url)
            session.commit()
        except Exception as e:
            # пишем лог в БД
            _log = Log(
                error_line=sys.exc_info()[2].tb_lineno,
                error_type=type(e).__name__,
                error=str(e))
            session.add(_log)
            session.commit()
            continue            # если проблема с получением содержания "карточки",
                                # то переходим на следующий url
        # предварительная инициализация базовых полей словаря ad_dict значения
        ad_dict = {
            'forum_id':             284,
            'currency_mortgage':    0,
            'typecode':             0,
            'taskcode':             0,
        }
        # разбор базовых тегов Юлы (они существуют вне зависисмости от категории)
        base_tage_list = [
            'subcategory',
            'isReserved',
            'name',
            'description',
            'location',
            'price',
            'images'
            ]
        try:
            for i in base_tage_list:
                if i in result.keys():
                    # по 'subcategory' поулчаем 'typecode' и 'taskcode'
                    if i == 'subcategory':
                        ad_dict['typecode'] = typecode_dict[int(result[i])]
                        ad_dict['taskcode'] = taskcode_dict[int(result[i])]

                    # по базовому тегу 'isReserved' определяем taskcode "в резерве" (3)
                    if i == 'isReserved':
                        ad_dict['taskcode'] = 3 if str(
                            result[i]) == 'True' else ad_dict['taskcode']

                    # заголовок
                    if i == "name":
                        ad_dict[attribute_dict[i]] = result[i]

                        # доопределяем записи с 'typecode' 4 и 5 если они остались не сопоставлены
                        if "свободного назначения" in result[i].lower():
                            ad_dict["typecode"] = 4
                        if "гараж" in result[i].lower():
                            ad_dict["typecode"] = 5

                    # описание
                    if i == "description":
                        ad_dict[attribute_dict[i]] = result[i]

                    # геолокация
                    if i == "location":
                        ad_dict["latitude"] = result[i]["latitude"]
                        ad_dict["longitude"] = result[i]["longitude"]

                    # стоимость как целое
                    if i == "price":
                        ad_dict[attribute_dict[i]] = result[i] // 100

                    # связанные ссылки на изображения
                    if i == "images":
                        ad_dict["images"] = ";".join([i['url'] for i in result[i]])

            # разбор индивидуальных тегов для раздела "Недвижимость"
            for i in result['attributes']:
                attr_name = i['slug']
                if attr_name in attribute_dict.keys():
                    # площадь, площадь кухни
                    if attr_name in ["realty_obshaya_ploshad", "realty_ploshad_kuhni"]:
                        ad_dict[attribute_dict[attr_name]] = int(i['rawValue']) // 100

                    # число комнат в квартире (поправил регулярное выражение)
                    if attr_name == "komnat_v_kvartire":
                        ad_dict[attribute_dict[attr_name]] = int(
                            re.search(r'\d+', i['rawValue'])[0])

                    # этаж квартиры, число этажей в доме
                    if attr_name in ["realty_etaj", "realty_etajnost_doma"]:
                        ad_dict[attribute_dict[attr_name]] = int(i['rawValue'])

                    # флаг, является ли собсвенником (тут ошибку поправил)
                    if attr_name == "sobstvennik_ili_agent":
                        ad_dict[attribute_dict[attr_name]] = 1 if i['rawValue'].lower() == 'собственник' else 0

                    # статус жилья
                    if attr_name == "realty_building_type":
                        if i['rawValue'] == "Новостройка":
                            ad_dict[attribute_dict[attr_name]] = 1
                        elif i['rawValue'] == "Вторичка":
                            ad_dict[attribute_dict[attr_name]] = 2
                        else:
                            ad_dict[attribute_dict[attr_name]] = 0
                    # наличие холодильника, посудомоечной машины
                    if attr_name in ["holodilnik", "posudomoechnaya_mashina"]:
                        if i["rawValue"] == "Есть":
                            ad_dict[attribute_dict[attr_name]] = 1
                        else:
                            ad_dict[attribute_dict[attr_name]] = 0
                    # уровень ремонта
                    if attr_name == "remont":
                        if i["rawValue"] == "Без отделки":
                            ad_dict[attribute_dict[attr_name]] = 1
                        elif i["rawValue"] == "Чистовая отделка":
                            ad_dict[attribute_dict[attr_name]] = 2
                        elif i["rawValue"] == "Муниципальный ремонт":
                            ad_dict[attribute_dict[attr_name]] = 3
                        elif i["rawValue"] == "Хороший ремонт":
                            ad_dict[attribute_dict[attr_name]] = 4
                        elif i["rawValue"] == "Евроремонт":
                            ad_dict[attribute_dict[attr_name]] = 5
                        elif i["rawValue"] == "Эксклюзивный":
                            ad_dict[attribute_dict[attr_name]] = 6
                        else:
                            ad_dict[attribute_dict[attr_name]] = 0
                    # наличие лифтов в доме по классам
                    if attr_name == "lift":
                        if i['rawValue'] in ["Легковой и грузовой", "Грузовой", "Легковой"]:
                            ad_dict[attribute_dict[attr_name]] = 1
                        else:
                            ad_dict[attribute_dict[attr_name]] = 0
                    # год окончания строительства
                    if attr_name == "realty_god_postroyki":
                        ad_dict[attribute_dict[attr_name]] = int(i['rawValue'])
                    # адрес
                    if attr_name == "realty_hidden_location":
                        ad_dict[attribute_dict[attr_name]] = i['rawValue']

            pprint.pprint(ad_dict)
        except Exception as e:
            _url = session.query(Url).filter(Url.url == card_url).one()
            _log = Log(
                id_url=_url.id,
                error_line=sys.exc_info()[2].tb_lineno,
                error_type=type(e).__name__,
                error=str(e)
            )
            session.add(_log)
            session.commit()
            continue
        # пишем, что url отработан успешно
        _url = session.query(Url).filter(Url.url == card_url).one()
        _url.is_parsed = True
        _url.date_parsed = datetime.now()
        session.commit()


if __name__ == '__main__':
    main()

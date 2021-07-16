# coding: utf-8
from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
from sqlalchemy import create_engine, Column, ForeignKey, String, TIMESTAMP, text
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import sessionmaker
import hashlib
import json
import logging
import random
import re
import requests
import sys
import time

# простой логгер, пишем в файл 'app.log'
logging.basicConfig(
    filename='app.log',
    filemode='a+',
    format='%(levelname)s -- %(filename)s, %(lineno)d: %(name)s.%(funcName)s: %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO
    )

BASE_URL = 'https://youla.ru'
CITY = 'moskva'
SECTION = 'nedvijimost'
SUBCATEGORY = ''

PARSE_URL = f'{BASE_URL}/{CITY}/{SECTION}/{SUBCATEGORY}'

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
    2008: 0,
    2010: 2,
    2011: 2,
    2012: 2,
    2013: 0     # неизвестно
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
    2008: 5,    # гараж
    2010: 1,
    2011: 2,
    2012: 3,
    2013: 4     # коммерческая
}

# каждый из словарей есть в описание, которое приложенно отдельным файлом
repair_type = {
    "Не требуется":         1,
    "Требуется ремонт":     2,
    "Косметический":        3,
    "Евроремонт":           4,
    "Дизайнерский":         5,
    "Капитальный ремонт":   6,

}

bathroom_type = {
    "Совмещенный":          1,
    "Раздельный":           2,
    "2 и более":            3,
    "Несколько санузлов":   3,
    "На улице":             4,
    "В доме":               5,

}

wall_material = {
    "Панельный":           1,
    "Кирпичный":           2,
    "Монолит":             3,
    "Монолитный":          3,
    "Кирпично-монолитный": 4,
    "Блочный":             5,
    "Деревянный":          6,
    "Щитовой":             7,
}

tenure_dict = {
    "До 3-х лет":       1,
    "От 3 до 5 лет":    2,
    "Более 5 лет":      3,
}

housing_dict = {
    "Вторичка":     1,
    "Новостройка":  2,
}

commission_type = {
    "Нет":      1,
    "30%":      2,
    "50%":      3,
    "100%":     4,
    "Другая":   5
}

prepay_type = {
    "Без предоплаты":       1,
    "1 месяц":              2,
    "2 месяца":             3,
    "3 месяца":             4,
    "4 и более месяцев":    5,
}

building_type = {
    "Дом":                                  1,
    "Таунхаус":                             2,
    "Коттедж":                              3,
    "Дача":                                 4,
    "Помещение свободного назначения":      5,
    "Торговое помещение":                   6,
    "Офисное помещение":                    7,
    "Производство":                         8,
    "Склад":                                9,
    "Другая коммерческая недвижимость":     10,
}

plot_type = {
    "Сельхоз (СНТ или ДНП)":                1,
    "Фермерское хоз-во":                    2,
    "Поселения (ИЖС)":                      3,
    "Земля промназначения":                 4,
    "Инвестпроект":                         5,
}


def main():
    start_time = time.time()
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
        card_section_list = [i.a['href'] for i in soup.find_all('li', class_='product_item')]
        # получаем полный URL для "карточек"
        card_url_list = [BASE_URL + i for i in card_section_list]
    except Exception as e:
        _log = Log(
                error_line=sys.exc_info()[2].tb_lineno,
                error_type=type(e).__name__,
                error=str(e))
        session.add(_log)
        session.commit()
        logging.error("Exception occurred", exc_info=True)
        # экстренный выход из программы
        sys.exit(1)

    try:
        # конфиги selenium
        prox = Proxy()
        prox.proxy_type = ProxyType.MANUAL
        prox.http_proxy = random.choice(proxy_list)['http'].replace('http://', '')
        capabilities = webdriver.DesiredCapabilities.CHROME
        prox.add_to_capabilities(capabilities)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument(f'user-agent="{ua.random}"')
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--window-size=1920,1920")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    except Exception as e:
        _log = Log(
                error_line=sys.exc_info()[2].tb_lineno,
                error_type=type(e).__name__,
                error=str(e))
        session.add(_log)
        session.commit()
        logging.error("Exception occurred", exc_info=True)
        # экстренный выход из программы
        sys.exit(2)

    if not len(card_url_list):
        sys.exit(3)
    else:
        i = 0
        j = 0
        browser = webdriver.Chrome('chromedriver.exe', options=chrome_options, desired_capabilities=capabilities)
        for card_url in card_url_list:
            try:
                if not session.query(Url).filter(Url.url == card_url).one_or_none():
                    _url = Url(url=card_url)
                    session.add(_url)
                    browser.get(card_url)
                    result = browser.execute_script("return __YOULA_STATE__.entities.products[0]")
                else:
                    j += 1
                    continue
            except Exception as e:
                _log = Log(
                    error_line=sys.exc_info()[2].tb_lineno,
                    error_type=type(e).__name__,
                    error=str(e)
                )
                session.add(_log)
                session.commit()
                continue
            try:
                base_fields = {
                        'id':               hashlib.md5(result.get('url').encode()).hexdigest(),
                        'forumId':          284,
                        'name':             result.get('name'),
                        'text':             result.get('description'),
                        'images':           ';'.join([el['url'] for el in result.get('images')]) if len(result.get('images')) else None,
                        'cost':             int(result.get('rawValue')) // 100 if str(result.get('rawValue')).isnumeric() else 0,
                        'url':              BASE_URL + result.get('url'),
                        'fullAddress':      result.get('location')['description'] if result.get('location') else None,
                        'latitude':         result.get('location')['latitude'] if result.get('location') else None,
                        'longitude':        result.get('location')['longitude'] if result.get('location') else None,
                    }

                custom_fields = {}
                if typecode_dict[int(result.get('subcategory'))] == 1:  # квартира
                    custom_fields['typeСode'] = 1
                    for d in result['attributes']:
                        if d.get('slug') == 'balkon':
                            custom_fields['balcony'] = True if 'Балкон' in d.get('rawValue') or 'Несколько балконов' in d.get('rawValue') else False
                            custom_fields['loggia'] = True if 'Лоджия' in d.get('rawValue') else False
                        if d.get('slug') == 'komnat_v_kvartire':
                            custom_fields['roomQuantity'] = int(re.search(r'\d+', d.get('rawValue'))[0]) if any(i.isdigit() for i in d.get('rawValue')) else None
                        if d.get('slug') == 'lift':
                            custom_fields['passLift'] = True if 'легковой' in d.get('rawValue').lower() or 'лифтов' in d.get('rawValue').lower() else False
                            custom_fields['cargoLift'] = True if 'грузовой' in d.get('rawValue').lower() or 'лифтов' in d.get('rawValue').lower() else False
                        if d.get('slug') == 'realty_etaj':
                            custom_fields['floor'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_etajnost_doma':
                            custom_fields['floors'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_god_postroyki':
                            custom_fields['buildYear'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_obshaya_ploshad':
                            custom_fields['totalArea'] = float(d.get('rawValue')) / 100 if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_ploshad_kuhni':
                            custom_fields['kitchenArea'] = float(d.get('rawValue')) / 100 if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'remont':
                            custom_fields['repair'] = repair_type.get(d.get('rawValue')) if repair_type.get(d.get('rawValue')) else 1
                        if d.get('slug') == 'sanuzli':
                            custom_fields['bathroomType'] = bathroom_type.get(d.get('rawValue')) if bathroom_type.get(d.get('rawValue')) else 2
                        if d.get('slug') == 'sobstvennik_ili_agent':
                            custom_fields['isOwner'] = True if 'Собственник' in d.get('rawValue') else False
                        if d.get('slug') == 'tip_doma':
                            custom_fields['wallMaterial'] = wall_material.get(d.get('rawValue'))
                        if d.get('slug') == 'building_flat_living_area':
                            custom_fields['livingArea'] = float(d.get('rawValue')) / 100 if str(d.get('rawValue')).isnumeric() else 0
                    if taskcode_dict[int(result.get('subcategory'))] == 1:  # продажа
                        custom_fields['taskСode'] = 1
                        for d in result['attributes']:
                            if d.get('slug') == 'let_v_sobstvennosti':
                                custom_fields['tenure'] = tenure_dict.get(d.get('rawValue'))
                            if d.get('slug') == 'realty_building_type':
                                custom_fields['housing'] = housing_dict.get(d.get('rawValue'))
                    if taskcode_dict[int(result.get('subcategory'))] == 2:  # аренда
                        custom_fields['taskСode'] = 2
                        for d in result['attributes']:
                            if d.get('slug') == 'holodilnik':
                                custom_fields['fridge'] = True if 'Есть' in d.get('rawValue') else False
                            if d.get('slug') == 'posudomoechnaya_mashina':
                                custom_fields['dishWasher'] = True if 'Есть' in d.get('rawValue') else False
                            if d.get('slug') == 'stiralnaya_mashina':
                                custom_fields['washer'] = True if 'Есть' in d.get('rawValue') else False
                            if d.get('slug') == 'komissiya':
                                custom_fields['commissionType'] = commission_type.get(d.get('rawValue')) if commission_type.get(d.get('rawValue')) else 5
                            if d.get('slug') == 'kommunalnie_uslugi_vhodyat':
                                custom_fields['utilitiesInclude'] = True if 'Включены' in d.get('rawValue') else False
                            if d.get('slug') == 'predoplata_mesechnaya':
                                custom_fields['prepayType'] = prepay_type.get(d.get('rawValue')) if prepay_type.get(d.get('rawValue')) else 1

                if typecode_dict[int(result.get('subcategory'))] == 2:  # комната
                    custom_fields['typeСode'] = 2
                    for d in result['attributes']:
                        if d.get('slug') == 'balkon':
                            custom_fields['balcony'] = True if 'Балкон' in d.get('rawValue') or 'Несколько балконов' in d.get('rawValue') else False
                            custom_fields['loggia'] = True if 'Лоджия' in d.get('rawValue') else False
                        if d.get('slug') == 'komnat_v_kvartire':
                            custom_fields['roomQuantity'] = int(re.search(r'\d+', d.get('rawValue'))[0]) if any(i.isdigit() for i in d.get('rawValue')) else None
                        if d.get('slug') == 'lift':
                            custom_fields['passLift'] = True if 'легковой' in d.get('rawValue').lower() or 'лифтов' in d.get('rawValue').lower() else False
                            custom_fields['cargoLift'] = True if 'грузовой' in d.get('rawValue').lower() or 'лифтов' in d.get('rawValue').lower() else False
                        if d.get('slug') == 'realty_etaj':
                            custom_fields['floor'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_etajnost_doma':
                            custom_fields['floors'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_god_postroyki':
                            custom_fields['buildYear'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_ploshad_komnati':
                            custom_fields['totalArea'] = float(d.get('rawValue')) / 100 if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'remont':
                            custom_fields['repair'] = repair_type.get(d.get('rawValue')) if repair_type.get(d.get('rawValue')) else 1
                        if d.get('slug') == 'sanuzli':
                            custom_fields['bathroomType'] = bathroom_type.get(d.get('rawValue')) if bathroom_type.get(d.get('rawValue')) else 2
                        if d.get('slug') == 'sobstvennik_ili_agent':
                            custom_fields['isOwner'] = True if 'Собственник' in d.get('rawValue') else False
                        if d.get('slug') == 'tip_doma':
                            custom_fields['wallMaterial'] = wall_material.get(d.get('rawValue'))
                    if taskcode_dict[int(result.get('subcategory'))] == 1:  # продажа
                        custom_fields['taskСode'] = 1
                        for d in result['attributes']:
                            if d.get('slug') == 'let_v_sobstvennosti':
                                custom_fields['tenure'] = tenure_dict.get(d.get('rawValue'))
                            if d.get('slug') == 'realty_building_type':
                                custom_fields['housing'] = housing_dict.get(d.get('rawValue'))
                    if taskcode_dict[int(result.get('subcategory'))] == 2:  # аренда
                        custom_fields['taskСode'] = 2
                        for d in result['attributes']:
                            if d.get('slug') == 'holodilnik':
                                custom_fields['fridge'] = True if 'Есть' in d.get('rawValue') else False
                            if d.get('slug') == 'komissiya':
                                custom_fields['commissionType'] = commission_type.get(d.get('rawValue')) if commission_type.get(d.get('rawValue')) else 5
                            if d.get('slug') == 'kommunalnie_uslugi_vhodyat':
                                custom_fields['utilitiesInclude'] = True if 'Включены' in d.get('rawValue') else False
                            if d.get('slug') == 'posudomoechnaya_mashina':
                                custom_fields['dishWasher'] = True if 'Есть' in d.get('rawValue') else False
                            if d.get('slug') == 'predoplata_mesechnaya':
                                custom_fields['prepayType'] = prepay_type.get(d.get('rawValue')) if prepay_type.get(d.get('rawValue')) else 1
                            if d.get('slug') == 'stiralnaya_mashina':
                                custom_fields['washer'] = True if 'Есть' in d.get('rawValue') else False

                if typecode_dict[int(result.get('subcategory'))] == 3:      # загородная
                    custom_fields['typeСode'] = 3
                    for d in result['attributes']:
                        if d.get('slug') == 'realty_etaj':
                            custom_fields['floor'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_etajnost_doma':
                            custom_fields['floors'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_god_postroyki':
                            custom_fields['buildYear'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_ploshad_doma':
                            custom_fields['totalArea'] = float(d.get('rawValue')) / 10 if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'realty_ploshad_uchastka':
                            custom_fields['landArea'] = float(d.get('rawValue')) / 10 if str(d.get('rawValue')).isnumeric() else 0
                        if d.get('slug') == 'sobstvennik_ili_agent':
                            custom_fields['isOwner'] = True if 'Собственник' in d.get('rawValue') else False
                        if d.get('slug') == 'tip_postroyki':
                            custom_fields['buildingType'] = building_type.get(d.get('rawValue')) if building_type.get(d.get('rawValue')) else 1
                        if d.get('slug') == 'elektrichestvo':
                            custom_fields['electricity'] = True if 'Подключено' in d.get('rawValue') else False
                        if d.get('slug') == 'garaj_mashinomesto':
                            custom_fields['garage'] = False if 'Нет' in d.get('rawValue') else True
                        if d.get('slug') == 'gaz':
                            custom_fields['gas'] = False if 'Нет' in d.get('rawValue') else True
                        if d.get('slug') == 'let_v_sobstvennosti':
                            custom_fields['tenure'] = tenure_dict.get(d.get('rawValue'))
                        if d.get('slug') == 'material_doma':
                            custom_fields['wallMaterial'] = wall_material.get(d.get('rawValue'))
                        if d.get('slug') == 'otoplenie':
                            custom_fields['heating'] = False if 'Нет' in d.get('rawValue') else True
                        if d.get('slug') == 'prodaja_uchastka_elektrichestvo':
                            custom_fields['electricity'] = True if 'Есть' in d.get('rawValue') else False
                        if d.get('slug') == 'prodaja_uchastka_gaz':
                            custom_fields['gas'] = False if 'Нет' in d.get('rawValue') else True
                        if d.get('slug') == 'sanuzel':
                            custom_fields['bathroomType'] = bathroom_type.get(d.get('rawValue')) if bathroom_type.get(d.get('rawValue')) else 4
                        if d.get('slug') == 'tip_uchastka':
                            custom_fields['plotType'] = plot_type.get(d.get('rawValue'))
                        if d.get('slug') == 'vodosnabjenie_i_kanalizaciya':
                            custom_fields['waterSupply'] = False if 'Нет' in d.get('rawValue') else True
                        if d.get('slug') == 'realty_kolichestvo_spalen':
                            custom_fields['bedrooms'] = int(d.get('rawValue')) if str(d.get('rawValue')).isnumeric() else 0
                    if taskcode_dict[int(result.get('subcategory'))] == 1: # продажа
                        custom_fields['taskСode'] = 1
                    if taskcode_dict[int(result.get('subcategory'))] == 2: # аренда
                        custom_fields['taskСode'] = 2
                        for d in result['attributes']:
                            if d.get('slug') == 'komissiya':
                                custom_fields['commissionType'] = commission_type.get(d.get('rawValue')) if commission_type.get(d.get('rawValue')) else 5
                            if d.get('slug') == 'kommunalnie_uslugi_vhodyat':
                                custom_fields['utilitiesInclude'] = True if 'Включены' in d.get('rawValue') else False
                            if d.get('slug') == 'predoplata_mesechnaya':
                                custom_fields['prepayType'] = prepay_type.get(d.get('rawValue')) if prepay_type.get(d.get('rawValue')) else 1
                            if d.get('slug') == 'holodilnik':
                                custom_fields['fridge'] = True if 'Есть' in d.get('rawValue') else False
                            if d.get('slug') == 'stiralnaya_mashina':
                                custom_fields['washer'] = True if 'Есть' in d.get('rawValue') else False
                            if d.get('slug') == 'posudomoechnaya_mashina':
                                custom_fields['dishWasher'] = True if 'Есть' in d.get('rawValue') else False
                
                if typecode_dict[int(result.get('subcategory'))] == 4: # коммерческая
                    custom_fields['typeСode'] = 4
                    for d in result['attributes']:
                        if d.get('slug') == 'tip_sdelki':
                            custom_fields['taskСode'] = 1 if d.get('rawValue') == 'Продажа' else 2
                        if d.get('slug') == 'kommer_realty_tip_stroeniya':
                            custom_fields['buildingType'] = building_type.get(d.get('rawValue'))
                        if d.get('slug') == 'sobstvennik_ili_agent':
                            custom_fields['isOwner'] = True if 'Собственник' in d.get('rawValue') else False
                        if d.get('slug') == 'realty_obshaya_ploshad':
                            custom_fields['totalArea'] = float(d.get('rawValue')) / 100 if str(d.get('rawValue')).isnumeric() else 0

                if typecode_dict[int(result.get('subcategory'))] == 5: # гараж и машиноместо
                        for d in result['attributes']:
                            if d.get('slug') == 'tip_stroeniya':
                                custom_fields['typeСode'] = 5 if d.get('rawValue') == 'Гараж' or d.get('rawValue') == 'Машиноместо' else None
                            if d.get('slug') == 'tip_sdelki':
                                custom_fields['taskСode'] = 1 if d.get('rawValue') == 'Продажа' else 2
                            if d.get('slug') == 'sobstvennik_ili_agent':
                                custom_fields['isOwner'] = True if 'Собственник' in d.get('rawValue') else False
                        
                        if custom_fields.get('typeСode') is None:
                            custom_fields = {}
                
                if result.get('isReserved'):
                    custom_fields['taskСode'] = 3
                            
                if len(custom_fields):
                    fields = {**base_fields, **custom_fields}
                    # коммит, если typeСode определен
                    session.commit()
                    # пишем, что url отработан успешно в БД
                    _url = session.query(Url).filter(Url.url == card_url).one()
                    _url.is_parsed = True
                    _url.date_parsed = datetime.now()
                    session.commit()
                    """
                    ***************************
                    ЗДЕСЬ ОТПРАВКА ПО ЭНДПОИНТУ,
                    НЕ ЗАБУДЬТЕ ПРО TRY-EXCEPT,
                    И ПРО UnicodeEncodeError 
                    ПРИ ensure_ascii=False В json.dumps()
                    ***************************
                    """
                    # print(fields)
                    i += 1
                else:
                    # откат транзакции, если typeCode не определен
                    session.rollback()
                    j += 1
            except Exception as e:
                _url = session.query(Url).filter(Url.url == card_url).scalar()
                _log = Log(
                    id_url=_url.id,
                    error_line=sys.exc_info()[2].tb_lineno,
                    error_type=type(e).__name__,
                    error=str(e)
                )
                session.add(_log)
                session.commit()
                logging.error("Exception occurred", exc_info=True)
                continue
        browser.close()
        # пишем в лог статистику по работе скрипта
        logging.info(f"Execution time: {time.time() - start_time} sec")
        logging.info(f"Processed: {i}")
        logging.info(f"Skipped: {j}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

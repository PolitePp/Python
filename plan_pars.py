"""
Данный сервис обрабатывает плановые данные с сайта ТФОМС
"""

import re
import logging
import sys
from datetime import datetime
import configparser
import os
from pathlib import Path
from zipfile import ZipFile
import requests
from bs4 import BeautifulSoup, SoupStrainer
import rarfile
import pandas as pd
from sqlalchemy import create_engine, exc

CONFIG = configparser.ConfigParser()
CONFIG.read('config.ini', encoding='utf-8')


def get_link(max_date):
    """
    Данная функция возвращает последнюю ссылку на архив, который не загружен в базу
    """
    months = {'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04', 'мая': '05', 'июн': '06',
              'июл': '07', 'авг': '08', 'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12'}
    url = 'https://ofoms.ru/tp-comissy/'
    try:
        all_a_tags = BeautifulSoup(requests.get(url).text, 'html.parser', parse_only=SoupStrainer('a'))
    except requests.RequestException:
        logging.exception('Exception occurred by requests')
        sys.exit()
    for link in all_a_tags:
        text_inside_tag = link.get_text()
        if CONFIG['UserSettings'].get('archive_tag') in text_inside_tag:
            protocol_date = re.search(r'\d{1,2} [а-яА-Я]* \d{4}', text_inside_tag).group().lower().split(' ')
            protocol_date = datetime.strptime('{0}.{1}.{2}'.format(protocol_date[0],
                                                                   months[protocol_date[1][:3]], protocol_date[2]),
                                              '%d.%m.%Y').date()
            if protocol_date > max_date:
                return 'https://ofoms.ru' + link.get('href'), protocol_date
            break
    return None, None


def download(url):
    """
    Функция скачивания файлов по входной ссылке
    """
    archive_filepath = Path(CONFIG['UserSettings'].get('filepath')) / re.search(r'[^ /]*$', url).group()
    with open(archive_filepath, 'wb') as archive:
        try:
            archive.write(requests.get(url).content)
        except requests.RequestException:
            logging.exception('Exception occurred by requests')
            sys.exit()
    return Path(archive_filepath.absolute())


def detect_file(filename):
    """
    Проверка того, что наименование внутри архива соответствует нашим требованиям. Возвращает признак того
    к какому типу относится эксель файл (1 - плановые данные, 2 - уровень МО)
    """
    if filename.endswith('.xlsx'):
        if filename.startswith(CONFIG['UserSettings'].get('file_tag')):
            return 1
        elif CONFIG['UserSettings'].get('levels_mo_tag') in filename.lower().replace(')', ''):
            return 2
    return 0


def unzip_file(loc):
    """
    Разархивация файлов с расширением .zip
    """
    with ZipFile(loc, 'r') as zip_obj:
        for filename in zip_obj.namelist():
            filename_ru = filename.encode('cp437').decode('cp866')
            business_type = detect_file(filename_ru)
            if business_type in [1, 2]:
                # Проблема с кодировкой русских файлов, поэтому мы
                # создаём свой файл на основе того, что есть в архиве
                with zip_obj.open(filename) as xl_f:
                    content = xl_f.read()
                    full_path = Path(os.path.join(CONFIG['UserSettings'].get('filepath'), filename_ru))
                    with open(full_path, 'wb') as file:
                        file.write(content)
                    os.remove(loc)
                    return full_path, filename_ru, business_type
    return Path, '', 0


def unrar_file(loc):
    """
    Разархивация файлов с расширением .rar
    """
    with rarfile.RarFile(loc) as rar_obj:
        for filename in rar_obj.namelist():
            business_type = detect_file(filename)
            if business_type in [1, 2]:
                # Для использования необходимо создать системную переменную UnRAR.exe
                rar_obj.extract(filename, CONFIG['UserSettings'].get('filepath'))
                return Path(os.path.join(CONFIG['UserSettings'].get('filepath'), filename)), filename, business_type
    return Path, '', 0


def get_excel(loc):
    """
    Вход - расположение архива. Из архивов получаем только тот эксель файл
    , который содержат признак из файла CONFIG.ini
    """
    if loc.suffix == '.zip':
        file_path, excel_name, business_type = unzip_file(loc)
    elif loc.suffix == '.rar':
        file_path, excel_name, business_type = unrar_file(loc)
    os.remove(loc)
    return file_path, excel_name, business_type


def find_sheets_excel(excel_loc, excel_name, excel_date, db_conf):
    """
    Функция для поиска листов аналитической справки внутри эксель файла и загрузки в базу
    """
    data_frame = pd.DataFrame(pd.read_sql_query(
        '''select id_report_sheet
            , split_part(lower(name), ' ', 1) as first_part
            , split_part(lower(name), ' ', 2) as second_part
    from reports.ref_report_sheet
    where id_report_sheet != 16
    order by id_report_sheet
    ''', db_conf))
    xl_file = pd.ExcelFile(excel_loc)
    analyt_sheets = {}
    page_sheets = list()  # Хранится информация о листах с объёмом, финансами и id листа из базы
    j = 0
    for row in data_frame.itertuples():
        for i in xl_file.sheet_names:
            words = re.sub(r'[\(\)]*', '', i).replace('онкол.', 'онко').replace('проф.', 'проф') \
                .replace('дисп.', 'дисп').lower().split(' ')
            if row.first_part in words and row.second_part == '' and re.search(r'(онко|проф|дисп|диагност)', i) is None:
                page_sheets.append(j)
            elif row.first_part in words and row.second_part in words:
                page_sheets.append(j)
            j += 1
        page_sheets.append(row.id_report_sheet)
        analyt_sheets['{0} {1}'.format(row.first_part, row.second_part).rstrip()] = page_sheets
        page_sheets = list()
        j = 0
        # Т.к. ЭКО находится на одном листе с дн.стационаром, берём инфу по нему, отличаются только id листа
        if row.first_part == 'эко':
            analyt_sheets[row.first_part] = analyt_sheets['дн.стационар'][0:2]
            analyt_sheets[row.first_part].append(row.id_report_sheet)
    # Формируем датафреймы по объёмам и финансам определённого листа
    for i in analyt_sheets:
        logging.info(f'Trying to load sheet {i} in db')
        if xl_file.sheet_names[analyt_sheets[i][0]].find('объёмы') != -1:
            df_volume = transform_excel(xl_file, analyt_sheets[i][0], i)
            df_fin = transform_excel(xl_file, analyt_sheets[i][1], i)
        else:
            df_volume = transform_excel(xl_file, analyt_sheets[i][1], i)
            df_fin = transform_excel(xl_file, analyt_sheets[i][0], i)
        # Для вмп и диализа появляется дополнительное поле (id_additional_group)
        if i in ['вмп', 'диализ']:
            df_list = df_volume.merge(df_fin, on=['code_mo', 'variable', 'id_additional_group'], how='inner')
        else:
            df_list = df_volume.merge(df_fin, on=['code_mo', 'variable'], how='inner')
        df_list.rename(columns={'variable': 'code_smo', 'value_x': 'volume_plan', 'value_y': 'finance_plan'},
                       inplace=True)
        df_list['id_sheet'], df_list['list_name'], df_list['comment'], df_list['date_ins'] = \
            [int(analyt_sheets[i][2]), i, excel_name, excel_date]
        df_list.to_sql('op_plan', schema='tfoms', con=db_conf, if_exists='append', index=False, method='multi')


def transform_excel(xl_file, num_sheet, name_sheet):
    """
    Функция получения данных с листа
    """
    data_frame = xl_file.parse(xl_file.sheet_names[num_sheet], nrows=15)
    columns_const = {'Код МО': 'code_mo', 'ООО "АльфаСтрахование-ОМС"': '81008', 'АО "СК"СОГАЗ-Мед"': '81001',
                     'ООО "Капитал МС"': '81007', 'Наименование медицинской организации': 'id_additional_group',
                     '№ группы ВМП': 'id_additional_group'}
    columns_dic = {}
    # Т.к. места колонок могут меняться, ищем колонки, которые нам нужны и переименовываем
    for row in range(len(data_frame.index)):
        for column in range(len(data_frame.columns)):
            if data_frame.loc[row][column] in list(columns_const.keys())[0:4] \
                    or (data_frame.loc[row][column] == list(columns_const.keys())[5] and name_sheet == 'вмп')\
                    or (data_frame.loc[row][column] == list(columns_const.keys())[4]
                            and name_sheet == 'диализ'):
                columns_dic[column] = columns_const[data_frame.loc[row][column]]
    # Эко и дн.стац на одном листе. Ищем строку, где второй раз встречается словосочетание "Код МО".
    # Всё, что до - дн.стац, после - эко
    if name_sheet in ['дн.стационар', 'эко']:
        df_col_code_mo = xl_file.parse(xl_file.sheet_names[num_sheet], parse_cols='code_mo')
        code_mo_row_num = list()
        for row in range(len(df_col_code_mo)):
            if df_col_code_mo.loc[row][0] == 'Код МО':
                code_mo_row_num.append(row)
        if name_sheet == 'дн.стационар':
            data_frame = xl_file.parse(xl_file.sheet_names[num_sheet], nrows=code_mo_row_num[1])
        else:
            data_frame = xl_file.parse(xl_file.sheet_names[num_sheet], skiprows=code_mo_row_num[1])
    else:
        data_frame = xl_file.parse(xl_file.sheet_names[num_sheet])
    data_frame = data_frame.fillna('')
    data_frame.rename(columns=lambda x: columns_dic[data_frame.columns.get_loc(x)]
                      if data_frame.columns.get_loc(x) in columns_dic.keys() else x, inplace=True)
    data_frame = data_frame[columns_dic.values()]
    data_frame = (data_frame[data_frame['code_mo'].str.contains('81', regex=True).fillna(True)])
    if name_sheet == 'диализ':
        id_additional_group = ['гемодиализ', 'перитонеальный диализ']
        data_frame = data_frame[data_frame.id_additional_group.isin(id_additional_group)]
        data_frame.loc[data_frame['id_additional_group'] == id_additional_group[0], 'id_additional_group'] = 101
        data_frame.loc[data_frame['id_additional_group'] == id_additional_group[1], 'id_additional_group'] = 102
        data_frame = pd.melt(data_frame, id_vars=['code_mo', 'id_additional_group'],
                             value_vars=['81008', '81001', '81007'])
        data_frame['value'] = pd.to_numeric(data_frame['value'], errors='coerce')
    elif name_sheet == 'вмп':
        data_frame = data_frame[data_frame.id_additional_group != '']
        data_frame = pd.melt(data_frame, id_vars=['code_mo', 'id_additional_group'],
                             value_vars=['81008', '81001', '81007'])
        data_frame['value'] = pd.to_numeric(data_frame['value'], errors='coerce')
    else:
        data_frame = pd.melt(data_frame, id_vars=['code_mo'], value_vars=['81008', '81001', '81007'])
        data_frame['value'] = pd.to_numeric(data_frame['value'], errors='coerce')
    return data_frame


def update_lvl_mo(excel_path, db_engine):
    """
    Функция обновления уровней МО из эксель файла (%перечень мо по уровням%).
    На вход подаётся путь до файла и коннект к бд.
    Обновляются только те записи, где уровень МО не совпадает
    """
    xl_file = pd.ExcelFile(excel_path)
    data_frame = xl_file.parse(xl_file.sheet_names[0], usecols='A')
    data_frame.rename(columns={data_frame.columns[0]: 'federal_code'}, inplace=True)
    data_frame['levels'] = 0
    i = 0
    for row in data_frame.itertuples():
        if 'к I-му' in str(row.federal_code):
            i = 1
        elif 'ко II-му' in str(row.federal_code):
            i = 2
        elif 'к III-му' in str(row.federal_code):
            i = 3
        data_frame.at[row.Index, 'levels'] = i
    data_frame['federal_code'] = data_frame['federal_code'].astype('str')
    data_frame = data_frame[data_frame.federal_code.str.contains('81')]
    ref_mo = pd.DataFrame(
        pd.read_sql_table(schema='nsi', table_name='ref_mo', columns=['federal_code', 'level_mo'], con=db_engine))
    data_frame = data_frame.set_index('federal_code').join(ref_mo.set_index('federal_code'), how='inner')
    data_frame = data_frame[data_frame['levels'] != data_frame['level_mo']]
    with db_engine.connect() as con:
        for row in data_frame.itertuples():
            con.execute(f'UPDATE nsi.ref_mo set level_mo = {row.levels} where federal_code::numeric = {row.Index}')


def main_func():
    """
    Основная функция из которой происходит вызов остальных
    """
    global CONFIG
    logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s] %(message)s', filename='tfoms_excel_parser.log'
                        , level=logging.INFO)
    logging.info('Excel parser start')
    logging.info('Trying to connect to database')
    try:
        engine = create_engine('{0}{1}:{2}@{3}/{4}'.format('postgresql+psycopg2://', CONFIG.get('pgDB', 'user'),
                                                           CONFIG.get('pgDB', 'password'),
                                                           CONFIG.get('pgDB', 'host'),
                                                           CONFIG.get('pgDB', 'dbname')))
    except exc.SQLAlchemyError:
        logging.exception('Exception occurred when trying to connect to database')
        sys.exit()
    logging.info('Connection success. Trying to get max date from tfoms.op_plan')
    try:
        with engine.begin() as connection:
            max_date = engine.execute('SELECT max(date_ins) FROM tfoms.op_plan').fetchone()[0]
    except exc.OperationalError:
        logging.exception('Exception occurred when trying to execute script')
        sys.exit()
    logging.info(f'Maximum date ({max_date}) is successfully received')
    logging.info('Trying to find link for archive where date > max_date')
    link, protocol_date = get_link(max_date)
    if link is not None:
        logging.info(f'Trying to download archive by link - {link}')
        file_path_arch = download(link)
        logging.info(f'Download is successfully completed in {file_path_arch}')
        logging.info('Trying to find excel files by mask in archive')
        file_path_excel, excel_name, business_type = get_excel(file_path_arch)
        logging.info('Excel files found successfully')
        logging.info(f'Trying to parse {excel_name} in db')
        if business_type == 1:
            find_sheets_excel(file_path_excel, excel_name, protocol_date, engine)
        elif business_type == 2:
            update_lvl_mo(file_path_excel, engine)
        logging.info(f'The program was completed successfully. Actual date of data is {protocol_date}')
    else:
        logging.info(f'Nothing to parse. We have actual data')

main_func()


from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import os
import requests
import zipfile
import hashlib
import json 

""" Настройка логгирования """
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='parser.log'
)
logger = logging.getLogger(__name__)

CONFIG = {
    'data_url': 'https://islod.obrnadzor.gov.ru/accredreestr/opendata/',
    'cache_dir': 'cache',
    'state_file': 'state.json',
    'db_file': 'education.db'
}


""" Создание базового класса для всех моделей. Все классы таблиц будут наследоваться от него. """
Base = declarative_base()

class Organization(Base):
    """ Таблица организаций """
    __tablename__ = 'organizations' # Имя таблицы в БД 
    
    type_id = Column(Integer, ForeignKey('organization_types.id'))
    parent_id = Column(Integer, ForeignKey('organizations.id')) 
    
    id = Column(Integer, primary_key=True)  
    EduOrgShortName = Column(String) # Сокращенное название организации 
    EduOrgFullName = Column(String)  # Полное название организаци
    Phone = Column(String) # Телефон
    Fax = Column(String) # Факс
    Email = Column(String) # Адрес электронной почты
    WebSite = Column(String) # Сайт
    PostAddress = Column(String)  # Юридический адрес
    INN = Column(String)  # ИНН организации
    KPP = Column(String)  # КПП организации
    OGRN = Column(String)  # ОГРН организации
    HeadPost = Column(String)  # Должность руководителя
    HeadName = Column(String)  # ФИО руководителя
    FormName = Column(String)  # Организационно-правовая форма
    KindName = Column(String)  # Вид организации
    TypeName = Column(String)  # Тип организации
    RegionName = Column(String)  # Субъект РФ
    FederalDistrictName = Column(String)  # Федеральный округ
    FederalDistrictShortName = Column(String)  # Сокращенное название округа
    IsBranch = Column(Boolean, nullable=True)  # Является ли филиалом
    HeadEduOrgId = Column(Integer)  # ID головной организации
    
    org_type = relationship("OrganizationType")
    branches = relationship("Organization", back_populates="parent")
    parent = relationship("Organization", remote_side=[id], back_populates="branches")
    programs = relationship("EducationalProgram", back_populates="organization")
    
class EducationalProgram(Base):
    """ Таблица образовательных программ """
    __tablename__ = 'educational_programs'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Integer, ForeignKey('organizations.id'))
    level_id = Column(Integer, ForeignKey('education_levels.id'))
    form_id = Column(Integer, ForeignKey('education_forms.id'))

    
    TypeName = Column(String) # Тип программы
    EduLevelName = Column(String) # Уровень ОП
    ProgrammName = Column(String) # Наименование ОП
    ProgrammCode = Column(String) # Код ОП
    UGSName = Column(String) # Наименование УГСН
    UGSCode = Column(String) # Код УГСН
    EduNormativePeriod = Column(String) # Нормативный период
    Qualification = Column(String) # Наименование квалификации
    IsAccredited = Column(Boolean, nullable=True) # Аккредитована / отказ (аккредитована - 0, отказ - 1)
    IsCanceled = Column(Boolean, nullable=True) # Лишение программы ОП
    IsSuspended = Column(Boolean, nullable=True) # Приостановка ОП
    okso_code = Column(String)
    
    organization = relationship("Organization", back_populates="programs")
    education_level = relationship("EducationLevel")
    education_form = relationship("EducationForm")
    
class OrganizationType(Base):
    """Типы образовательных организаций (высшее, среднее и т.д.)"""
    __tablename__ = 'organization_types'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)  # 'высшее', 'среднее профессиональное', 'среднее общее'
    code = Column(String, unique=True)  # 'higher', 'secondary_pro', 'secondary'
    
class EducationLevel(Base):
    """Уровни образования (бакалавриат, магистратура и т.д.)"""
    __tablename__ = 'education_levels'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)  # 'Бакалавриат', 'Магистратура'
    code = Column(String)  # 'bachelor', 'master'
    
class EducationForm(Base):
    """Формы обучения (очная, заочная)"""
    __tablename__ = 'education_forms'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)  # 'Очная', 'Заочная'
    code = Column(String)  # 'full_time', 'part_time'

def safe_text(element, default=''):
    """Гарантированно возвращает строку, даже если элемент или его текст отсутствуют"""
    if element is not None and element.text is not None:
        return element.text.strip()
    return default  

def safe_date(element, fmts=('%Y-%m-%d', '%d.%m.%Y', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S%z')):
    if element is None or element.text is None:
        return None
    text = element.text.strip()
    """ Удаляем временную зону если она есть """
    if '+' in text:
        text = text.split('+')[0].strip()
    for fmt in fmts:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.date()
        except ValueError:
            continue
    logger.warning(f"Не удалось распознать дату: {element.text}")
    return None
    
def safe_bool(element):
    if element is None or element.text is None:
        return None
    text = element.text.strip()
    if text == '1':
        return True
    elif text == '0':
        return False
    return None

def download_and_extract_archive(url):
    """Загрузка и распаковка архива с данными"""
    try:
        logger.info(f"Загрузка данных с {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        """ Создание папку для кэша, если ее нет """
        os.makedirs(CONFIG['cache_dir'], exist_ok=True)
        
        """ Сохранение архива """
        zip_path = os.path.join(CONFIG['cache_dir'], 'data.zip')
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        """ Распаковка архива """
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(CONFIG['cache_dir'])
            extracted_files = zip_ref.namelist()
        
        logger.info(f"Архив успешно распакован. Файлы: {extracted_files}")
        return extracted_files
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке/распаковке архива: {str(e)}")
        raise

def get_file_hash(file_path):
    """Вычисление хеша файла для проверки изменений"""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def check_for_updates(file_path):
    """Проверка изменений в файле"""
    state = load_state()
    current_hash = get_file_hash(file_path)
    
    if file_path in state and state[file_path] == current_hash:
        logger.info("Файл не изменился с момента последней обработки")
        return False
    
    state[file_path] = current_hash
    save_state(state)
    logger.info("Обнаружены изменения в файле или файл новый")
    return True

def load_state():
    """Загрузка состояния обработки файлов"""
    try:
        if os.path.exists(CONFIG['state_file']):
            with open(CONFIG['state_file'], 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Ошибка загрузки состояния: {str(e)}")
    return {}

def save_state(state):
    """Сохранение состояния обработки файлов"""
    try:
        with open(CONFIG['state_file'], 'w') as f:
            json.dump(state, f)
    except Exception as e:
        logger.error(f"Ошибка сохранения состояния: {str(e)}")
        
def find_xml_file(directory):
    """Поиск XML файла в директории"""
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.xml'):
                return os.path.join(root, file)
    return None

def parse_xml_to_db(xml_file, session):
    """ Парсер XML """
    try:
        # Кэшируем типы организаций
        org_types_cache = {ot.code: ot for ot in session.query(OrganizationType).all()}
        
        file_size = os.path.getsize(xml_file) / (1024 * 1024)
        logger.info(f'Начало обработки файла {xml_file} (размер: {file_size:.2f} MB)')
        
        for event, elem in ET.iterparse(xml_file, events=('end',)):
            if elem.tag.endswith('Certificate'):
                try:
                    # Собираем программы для передачи в process_organization
                    programs = []
                    for supp_elem in elem.findall('.//Supplement'):
                        for prog_elem in supp_elem.findall('.//EducationalProgram'):
                            programs.append(prog_elem)
                    
                    # Обработка организации
                    org = process_organization(elem, session, org_types_cache, programs)
                    if org is None:
                        continue
                    
                    # Проверяем наличие типа организации
                    if org.org_type is None:
                        logger.warning(f"Организация {org.EduOrgFullName} не имеет типа. Пропуск.")
                        continue
                    
                    # Фильтрация: включаем вузы (org_type.code == 'higher') или филиалы (IsBranch == True)
                    if org.org_type.code != 'higher' and not org.IsBranch:
                        logger.debug(f"Пропуск организации {org.EduOrgFullName}, TypeName: {org.TypeName}, IsBranch: {org.IsBranch}, org_type.code: {org.org_type.code}")
                        continue
                    
                    session.add(org)
                    session.flush()
                    
                    # Обработка филиалов
                    if org.IsBranch:
                        parent = session.query(Organization).filter(
                            (Organization.id == org.HeadEduOrgId) |
                            (Organization.INN == org.HeadEduOrgId)
                        ).first()
                        if parent:
                            org.parent_id = parent.id

                    # Обработка программ
                    for supp_elem in elem.findall('.//Supplement'):
                        for prog_elem in supp_elem.findall('.//EducationalProgram'):
                            program = process_program(prog_elem, session)
                            program.organization_id = org.id
                            session.add(program)
                    
                    if len(session.new) % 100 == 0:
                        session.commit()
                        
                except Exception as e:
                    logger.error(f'Ошибка обработки сертификата: {str(e)}')
                    session.rollback()
                
                elem.clear()
        
        session.commit()
        
        # Логируем статистику
        main_universities = session.query(Organization).filter(
            Organization.type_id == session.query(OrganizationType).filter_by(code='higher').first().id,
            Organization.IsBranch == False
        ).count()
        branches = session.query(Organization).filter(Organization.IsBranch == True).count()
        logger.info(f"Количество основных вузов: {main_universities}")
        logger.info(f"Количество филиалов: {branches}")
        
        return True
        
    except Exception as e:
        logger.error(f'Критическая ошибка: {str(e)}')
        session.rollback()
        return False
        
    except Exception as e:
        logger.error(f'Критическая ошибка: {str(e)}')
        session.rollback()
        return False

def process_organization(cert_elem, session, org_types_cache, programs=None):
    """ Извлекает данные об образовательной организации из XML и создаёт объект Organization. """
    org_elem = cert_elem.find('ActualEducationOrganization')
    if org_elem is None:
        logger.warning("Не найден элемент ActualEducationOrganization")
        return None
    
    # Получаем тип организации и филиальность
    type_elem = org_elem.find('TypeName')
    type_name = safe_text(type_elem, '').lower()
    is_branch = safe_bool(org_elem.find('IsBranch'))
    
    # Логируем для диагностики
    logger.debug(f"Обработка организации: {safe_text(org_elem.find('FullName'))}, TypeName: {type_name}, IsBranch: {is_branch}")
    
    # Определяем тип организации
    org_type_code = None
    
    # Явно исключаем школы
    if any(x in type_name for x in ['школа', 'лицей', 'гимназия', 'среднее общеобразовательное', 'средняя общеобразовательная']):
        org_type_code = 'secondary'
        logger.debug(f"Организация классифицирована как школа: {type_name}")
    
    # Проверяем вузы по ключевым словам
    elif any(x in type_name for x in [
        'вуз', 'университет', 'институт', 'высшее учебное заведение', 'академия',
        'федеральное государственное', 'государственное образовательное',
        'национальный исследовательский', 'технологический университет',
        'высшее образование', 'бюджетное образовательное учреждение',
        'автономное образовательное учреждение', 'государственный университет'
    ]):
        org_type_code = 'higher'
    
    # Проверяем образовательные программы (если переданы)
    elif programs:
        for prog_elem in programs:
            edu_level = safe_text(prog_elem.find('EduLevelName'), '').lower()
            if any(x in edu_level for x in ['бакалавриат', 'магистратура', 'специалитет', 'аспирантура']):
                org_type_code = 'higher'
                logger.debug(f"Организация классифицирована как вуз на основе программы: {edu_level}")
                break
    
    # По умолчанию - среднее профессиональное или общее
    if not org_type_code:
        org_type_code = 'secondary_pro' if 'колледж' in type_name or 'техникум' in type_name else 'secondary'
        logger.debug(f"Неизвестный тип организации: {type_name}, установлен по умолчанию: {org_type_code}")
    
    # Получаем объект OrganizationType из кэша
    org_type_obj = org_types_cache.get(org_type_code)
    if not org_type_obj:
        logger.warning(f"Тип организации '{org_type_code}' не найден в базе")
        return None
    
    # Создаем объект организации
    org = Organization(
        EduOrgShortName=safe_text(org_elem.find('ShortName')),
        EduOrgFullName=safe_text(org_elem.find('FullName')),
        Phone=safe_text(org_elem.find('Phone')),
        Fax=safe_text(org_elem.find('Fax')),
        Email=safe_text(org_elem.find('Email')),
        WebSite=safe_text(org_elem.find('WebSite')),
        PostAddress=safe_text(org_elem.find('PostAddress')),
        INN=safe_text(org_elem.find('INN')),
        KPP=safe_text(org_elem.find('KPP')),
        OGRN=safe_text(org_elem.find('OGRN')),
        HeadPost=safe_text(org_elem.find('HeadPost')),
        HeadName=safe_text(org_elem.find('HeadName')),
        FormName=safe_text(org_elem.find('FormName')),
        KindName=safe_text(org_elem.find('KindName')),
        TypeName=type_name,
        RegionName=safe_text(org_elem.find('RegionName')),
        FederalDistrictName=safe_text(org_elem.find('FederalDistrictName')),
        FederalDistrictShortName=safe_text(org_elem.find('FederalDistrictShortName')),
        IsBranch=is_branch,
        HeadEduOrgId=safe_text(org_elem.find('HeadEduOrgId')),
        type_id=org_type_obj.id
    )
    
    # Устанавливаем объект OrganizationType для связи
    org.org_type = org_type_obj
    
    return org
    
def process_program(prog_elem, session):
    """ Извлекает данные об образовательных программах """
    # Определяем уровень образования
    # Безопасное получение данных с обработкой None
    level_name = safe_text(prog_elem.find('EduLevelName'), '').lower()
    form_name = safe_text(prog_elem.find('EducationForm'), '').lower()
    level_code = None
    if 'бакалавр' in level_name:
        level_code = 'bachelor'
    elif 'магистр' in level_name:
        level_code = 'master'
    elif 'специалист' in level_name or 'аспирант' in level_name:
        level_code = 'specialist'
    
    # Определяем форму обучения
    form_name = safe_text(prog_elem.find('EducationForm'), '').lower()  # Предполагаем, что есть такой тег
    form_code = None
    
    if 'очная' in form_name:
        form_code = 'full_time'
    elif 'заочная' in form_name:
        form_code = 'part_time'
    elif 'очно-заочная' in form_name or 'вечерняя' in form_name:
        form_code = 'mixed'
    else:
        form_code = 'full_time'  # Значение по умолчанию
        
    # Получаем ID уровней и форм из базы
    level_id = None
    if level_code:
        level = session.query(EducationLevel).filter_by(code=level_code).first()
        if level:
            level_id = level.id
            
    form_id = None
    if form_code:
        form = session.query(EducationForm).filter_by(code=form_code).first()
        if form:
            form_id = form.id
    
    program = EducationalProgram (
        TypeName=safe_text(prog_elem.find('TypeName')),
        EduLevelName=safe_text(prog_elem.find('EduLevelName')),
        ProgrammName=safe_text(prog_elem.find('ProgrammName')),
        UGSName=safe_text(prog_elem.find('UGSName')),
        UGSCode=safe_text(prog_elem.find('UGSCode')),
        EduNormativePeriod=safe_text(prog_elem.find('EduNormativePeriod')),
        Qualification=safe_text(prog_elem.find('Qualification')),
        IsAccredited=safe_bool(prog_elem.find('IsAccredited')),
        IsCanceled=safe_bool(prog_elem.find('IsCanceled')),
        IsSuspended=safe_bool(prog_elem.find('IsSuspended')),
        okso_code=safe_text(prog_elem.find('ProgrammCode')),  # Код ОКСО
        level_id=level_id,
        form_id=form_id
    )
    
    return program


def initialize_database(session):
    """Заполняет справочные таблицы начальными данными"""
    # Типы организаций
    org_types = [
        {"name": "высшее образование", "code": "higher"},
        {"name": "среднее профессиональное", "code": "secondary_pro"},
        {"name": "среднее общее", "code": "secondary"}
    ]
    
    # Уровни образования
    edu_levels = [
        {"name": "Бакалавриат", "code": "bachelor"},
        {"name": "Магистратура", "code": "master"},
        {"name": "Специалитет", "code": "specialist"},
        {"name": "Аспирантура", "code": "postgraduate"}
    ]
    
    # Формы обучения
    edu_forms = [
        {"name": "Очная", "code": "full_time"},
        {"name": "Заочная", "code": "part_time"},
        {"name": "Очно-заочная", "code": "mixed"},
        {"name": "Дистанционная", "code": "remote"}
    ]
    
    # Добавляем типы организаций
    for item in org_types:
        session.merge(OrganizationType(**item))
    
    # Добавляем уровни образования
    for item in edu_levels:
        session.merge(EducationLevel(**item))
    
    # Добавляем формы обучения
    for item in edu_forms:
        session.merge(EducationForm(**item))
    
    session.commit()

def main():
    """ Загрузка и распаковка архива """
    try:
        extracted_files = download_and_extract_archive(CONFIG['data_url'])
    except Exception as e:
        logger.error(f"Не удалось загрузить данные: {str(e)}")
        return
    
    """ Поиск XML файла в распакованных данных """
    xml_file = find_xml_file(CONFIG['cache_dir'])
    if not xml_file:
        logger.error("XML файл не найден в распакованных данных")
        return
    
    """ Проверка изменений """
    if not check_for_updates(xml_file):
        logger.info("Данные не изменились, обработка не требуется")
        return
    
    """ Инициализация БД """
    engine = create_engine(f'sqlite:///{CONFIG["db_file"]}', echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Инициализация справочных данных
        initialize_database(session)
        
        logger.info('Начало загрузки данных...')
        start_time = datetime.now()
        
        if parse_xml_to_db(xml_file, session):
            duration = datetime.now() - start_time
            stats = {
                'organizations': session.query(Organization).count(),
                'programs': session.query(EducationalProgram).count(),
                'branches': session.query(Organization).filter(Organization.IsBranch==True).count(),
                'duration_seconds': duration.total_seconds()
            }
            logger.info(f'''
                Загрузка завершена успешно!
                Статистика:
                - Всего организаций: {stats['organizations']}
                - Из них филиалов: {stats['branches']}
                - Образовательных программ: {stats['programs']}
                - Время выполнения: {stats['duration_seconds']:.2f} сек
            ''')
            
        else:
            logger.error('Загрузка завершена с ошибками!')
            
    except Exception as e:
        logger.critical(f'Фатальная ошибка: {str(e)}')
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    main()
    print('Обработка данных завершена!')
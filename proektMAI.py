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



Base = declarative_base() # Создаю базовый класс для всех моделей. Все классы таблиц будут наследоваться от него.

class Organization(Base):
    __tablename__ = 'organizations' # Имя таблицы в БД 
    
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
    
    certificates = relationship('Certificate', back_populates='organization') # Устанавливаю связь 'один-ко-многим': back_populates - обратная ссылка в модели Certificate
    supplements = relationship('Supplement', back_populates='organization')

class Certificate(Base):
    __tablename__ = 'certificates'
    
    id = Column(Integer, primary_key=True) # Идентификатор свидетельства
    organization_id = Column(Integer, ForeignKey('organizations.id')) # Внешний ключ к таблице организаций
    
    IsFederal = Column(Boolean, nullable=True)  # Источник свидетельства (1 - федеральное, 0 - региональное)
    StatusName = Column(String)  # Текущий статус свидетельства
    TypeName = Column(String)  # Вид свидетельства
    RegionName = Column(String)  # Субъект РФ
    RegionCode = Column(String)  # Код субъекта РФ
    FederalDistrictName = Column(String) # Код субъекта рф
    FederalDistrictShortName = Column(String) # Сокращенное наименование Субъекта РФ
    RegNumber = Column(String)  # Регистрационный номер
    SerialNumber = Column(String)  # Серия бланка
    FormNumber = Column(String)  # Номер бланка
    IssueDate = Column(Date)  # Дата выдачи свидетельства 
    EndDate = Column(Date)  # Срок действия свидетельства
    ControlOrgan = Column(String)  # Орган, выдавший свидетельство
    EduOrgINN = Column(String) # ИНН
    EduOrgKPP = Column(String) # КПП
    EduOrgOGRN = Column(String) # ОГРН
    
    organization = relationship('Organization', back_populates='certificates')
    supplements = relationship('Supplement', back_populates='certificate')
    decisions = relationship('Decision', back_populates='certificate')

class Supplement(Base):
    __tablename__ = 'supplements'
    
    id = Column(Integer, primary_key=True)
    certificate_id = Column(Integer, ForeignKey('certificates.id'))
    organization_id = Column(Integer, ForeignKey('organizations.id'))
    
    StatusName = Column(String) # Текущий статус приложения
    StatusCode = Column(String) # Статус приложения
    Number = Column(String) # Номер приложения
    SerialNumber = Column(String) # Серия бланка приложения
    FormNumber = Column(String) # Номер бланка приложения
    IssueDate = Column(Date) # Дата выдачи приложения
    IsForBranch = Column(Boolean, nullable=True) # Выдано филиалу / головной организации (филиалу - 1, головной - 0)
    Note = Column(String) # Примечание
    EduOrgFullName = Column(String) # Полное наименование организации из приложения
    EduOrgShortName = Column(String) # Сокращенное наименование организации из приложения
    EduOrgAddress = Column(String) # Юридический адрес организации из приложения
    EduOrgKPP = Column(String) # КПП организации из приложения
    
    certificate = relationship('Certificate', back_populates='supplements')
    educational_programs = relationship('EducationalProgram', back_populates='supplement')
    organization = relationship('Organization', back_populates='supplements')

class EducationalProgram(Base):
    __tablename__ = 'educational_programs'
    
    id = Column(Integer, primary_key=True) # Идентификатор программы
    supplement_id = Column(Integer, ForeignKey('supplements.id'))
    
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
    
    supplement = relationship('Supplement', back_populates='educational_programs')

class Decision(Base):
    """ Хранит распорядительные документы, связанные с сертификатами. """
    __tablename__ = 'decisions'
    
    id = Column(Integer, primary_key=True) # Идентификатор документа
    certificate_id = Column(Integer, ForeignKey('certificates.id'))
    
    DecisionTypeName = Column(String) # Тип документа распорядительного документа
    OrderDocumentNumber = Column(String) # Номер распорядительного документа
    OrderDocumentKind = Column(String) # Вид документа
    DecisionDate = Column(Date) # Дата распорядительного документа
    
    certificate = relationship('Certificate', back_populates='decisions')

class IndividualEntrepreneur(Base):
    __tablename__ = 'individual_entrepreneurs'
    
    id = Column(Integer, primary_key=True)
    certificate_id = Column(Integer, ForeignKey('certificates.id'))
    
    LastName = Column(String) # Фамилия индивидуального предпринимателя (при наличии)
    FirstName = Column(String) # Имя индивидуального предпринимателя (при наличии)
    MiddleName = Column(String) # Отчество индивидуального предпринимателя (при наличии)
    Address = Column(String) # Юридический адрес индивидуального предпринимателя (при наличии)
    EGRIP = Column(String) # ОГРН индивидуального предпринимателя (при наличии)
    INN = Column(String) # ИНН индивидуального предпринимателя (при наличии)
    
    certificate = relationship('Certificate', backref='entrepreneur')

def safe_text(element, default=None):
    return element.text if element is not None else default

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
        """ Проверка размера файла """
        file_size = os.path.getsize(xml_file) / (1024 * 1024)
        logger.info(f'Начало обработки файла {xml_file} (размер: {file_size:.2f} MB)')
        
        """ Потоковый парсинг для больших файлов """
        for event, elem in ET.iterparse(xml_file, events=('end',)):
            if elem.tag.endswith('Certificate'):
                try:
                    """ Обработка организации """
                    org = process_organization(elem)
                    session.add(org)
                    
                    """ Обработка сертификата """
                    cert = process_certificate(elem, org)
                    session.add(cert)

                    ip_data = {
                        'LastName': safe_text(elem.find('IndividualEntrepreneurLastName')),
                        'FirstName': safe_text(elem.find('IndividualEntrepreneurFirstName')),
                        'MiddleName': safe_text(elem.find('IndividualEntrepreneurMiddleName')),
                        'Address': safe_text(elem.find('IndividualEntrepreneurAddress')),
                        'EGRIP': safe_text(elem.find('IndividualEntrepreneurEGRIP')),
                        'INN': safe_text(elem.find('IndividualEntrepreneurINN'))
                    }
                    if any(value for value in ip_data.values() if value is not None):
                        ip = IndividualEntrepreneur(
                            certificate=cert,
                            **ip_data
                        )
                        session.add(ip)
                        
                    """ Обработка приложений """
                    for supp_elem in elem.findall('.//Supplement'):
                        supp = process_supplement(supp_elem, cert)
                        session.add(supp)
                        
                        """ Обработка программ """
                        for prog_elem in supp_elem.findall('.//EducationalProgram'):
                            prog = process_program(prog_elem, supp)
                            session.add(prog)
                    """ Обработка Decision """
                    for decision_elem in elem.findall('.//Decisions/Decision'):
                        decision = process_decision(decision_elem, cert)
                        session.add(decision)
                    
                    """ Периодический коммит """
                    if len(session.new) % 100 == 0:
                        session.commit()
                        logger.info(f'Обработано {len(session.new)} записей...')
                        
                except Exception as e:
                    logger.error(f'Ошибка обработки сертификата: {str(e)}')
                    session.rollback()
                
                """ Очистка памяти """
                elem.clear()
        
        session.commit()
        return True
        
    except Exception as e:
        logger.error(f'Критическая ошибка: {str(e)}')
        session.rollback()
        return False

def process_organization(cert_elem):
    org_elem = cert_elem.find('ActualEducationOrganization')
    if org_elem is None:
        return None
    return Organization(
        EduOrgShortName=safe_text(org_elem.find('ShortName')),
        EduOrgFullName=safe_text(org_elem.find('FullName')),
        Phone = safe_text(org_elem.find('Phone')),
        Fax = safe_text(org_elem.find('Fax')),
        Email = safe_text(org_elem.find('Email')),
        WebSite = safe_text(org_elem.find('Website')),
        PostAddress = safe_text(org_elem.find('PostAddress')),
        INN = safe_text(org_elem.find('INN')),
        KPP = safe_text(org_elem.find('KPP')),
        OGRN = safe_text(org_elem.find('OGRN')),
        HeadPost = safe_text(org_elem.find('HeadPost')),
        HeadName = safe_text(org_elem.find('HeadName')),
        FormName = safe_text(org_elem.find('FormName')),
        KindName = safe_text(org_elem.find('KindName')),
        TypeName = safe_text(org_elem.find('TypeName')),
        RegionName = safe_text(org_elem.find('RegionName')),
        FederalDistrictName = safe_text(org_elem.find('FederalDistrictName')),
        FederalDistrictShortName = safe_text(org_elem.find('FederalDistrictShortName')),
        IsBranch = safe_bool(org_elem.find('IsBranch')),
        HeadEduOrgId = safe_text(org_elem.find('HeadEduOrgId'))
    )

    
def process_certificate(cert_elem, organization):
    return Certificate(
    organization=organization,
    IsFederal = safe_bool(cert_elem.find('IsFederal')),
    StatusName = safe_text(cert_elem.find('StatusName')),
    TypeName = safe_text(cert_elem.find('TypeName')),
    RegionName = safe_text(cert_elem.find('RegionName')),
    RegionCode = safe_text(cert_elem.find('RegionCode')),
    FederalDistrictName = safe_text(cert_elem.find('FederalDistrictName')),
    FederalDistrictShortName = safe_text(cert_elem.find('FederalDistrictShortName')),
    RegNumber = safe_text(cert_elem.find('RegNumber')),
    SerialNumber = safe_text(cert_elem.find('SerialNumber')),
    FormNumber = safe_text(cert_elem.find('FormNumber')),
    IssueDate = safe_date(cert_elem.find('IssueDate')),
    EndDate = safe_date(cert_elem.find('EndDate')),
    ControlOrgan = safe_text(cert_elem.find('ControlOrgan')),
    EduOrgINN = safe_text(cert_elem.find('EduOrgINN')),
    EduOrgKPP = safe_text(cert_elem.find('EduOrgKPP')),
    EduOrgOGRN = safe_text(cert_elem.find('EduOrgOGRN'))
    )

def process_supplement(supp_elem, cert):
    return Supplement(
    certificate = cert,
    organization = cert.organization,
    StatusName = safe_text(supp_elem.find('StatusName')),
    StatusCode = safe_text(supp_elem.find('StatusCode')),
    Number = safe_text(supp_elem.find('Number')),
    SerialNumber = safe_text(supp_elem.find('SerialNubmer')),
    FormNumber = safe_text(supp_elem.find('FormNumber')),
    IssueDate = safe_date(supp_elem.find('IssueDate')),
    IsForBranch = safe_bool(supp_elem.find('IsForBranch')),
    Note = safe_text(supp_elem.find('Note')),
    EduOrgFullName = safe_text(supp_elem.find('EduOrgFullName')),
    EduOrgShortName = safe_text(supp_elem.find('EduOrgShortName')),
    EduOrgAddress = safe_text(supp_elem.find('EduOrgAddress')),
    EduOrgKPP = safe_text(supp_elem.find('EduOrgKPP'))
    )
    
def process_program(prog_elem, supplement):
    return EducationalProgram(
    supplement = supplement,
    TypeName = safe_text(prog_elem.find('TypeName')),
    EduLevelName = safe_text(prog_elem.find('EduLevelName')),
    ProgrammName = safe_text(prog_elem.find('ProgrammName')),
    ProgrammCode = safe_text(prog_elem.find('ProgrammCode')),
    UGSName = safe_text(prog_elem.find('UGSName')),
    UGSCode = safe_text(prog_elem.find('UGSCode')),
    EduNormativePeriod = safe_text(prog_elem.find('EduNormativePeriod')),
    Qualification = safe_text(prog_elem.find('Qualification')),
    IsAccredited = safe_bool(prog_elem.find('IsAccredited')),
    IsCanceled = safe_bool(prog_elem.find('IsCanceled')),
    IsSuspended = safe_bool(prog_elem.find('IsSuspended')),
    )

def process_decision(decision_elem, certificate):
    return Decision(
        certificate=certificate,
        DecisionTypeName=safe_text(decision_elem.find('DecisionTypeName')),
        OrderDocumentNumber=safe_text(decision_elem.find('OrderDocumentNumber')),
        OrderDocumentKind=safe_text(decision_elem.find('OrderDocumentKind')),
        DecisionDate=safe_date(decision_elem.find('DecisionDate'))
    )
    
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
        logger.info('Начало загрузки данных...')
        start_time = datetime.now()
        
        if parse_xml_to_db(xml_file, session):
            duration = datetime.now() - start_time
            stats = {
                'organizations': session.query(Organization).count(),
                'certificates': session.query(Certificate).count(),
                'decisions': session.query(Decision).count(),
                'entrepreneurs': session.query(IndividualEntrepreneur).count(),
                'duration_seconds': duration.total_seconds()
            }
            logger.info(f'''
                Загрузка завершена успешно!
                Статистика:
                - Организаций: {stats['organizations']}
                - Сертификатов: {stats['certificates']}
                - Решений: {stats['decisions']}
                - Предпринимателей: {stats['entrepreneurs']}
                - Время выполнения: {stats['duration_seconds']:.2f} сек
            ''')
        else:
            logger.error('Загрузка завершена с ошибками!')
            
    except Exception as e:
        logger.critical(f'Фатальная ошибка: {str(e)}')
    finally:
        session.close()

if __name__ == '__main__':
    main()
    print('Обработка данных завершена!')
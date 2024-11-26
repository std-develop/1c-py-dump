import subprocess
import requests
import os
import logging

# Настройка логирования
logging.basicConfig(
    filename='backup.log',  # Укажите имя файла для логов
    filemode='a',           # Режим открытия файла: 'a' - добавление, 'w' - перезапись
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Ваш OAuth-токен и пароль (храните их в переменных окружения)
OAUTH_TOKEN = 'TOKEN'
DB_PASSWORD = "PASSWD"    # Пароль пользователя PostgreSQL

# URL для проверки информации о диске
DISK_URL = "https://cloud-api.yandex.net/v1/disk"


# Остановка сервиса
def stop_service(service_name):
    try:
        subprocess.run(["systemctl", "stop", service_name], check=True)
        logging.info(f"Сервис {service_name} успешно остановлен.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при остановке сервиса {service_name}: {e}")


# Запуск сервиса
def start_service(service_name):
    try:
        subprocess.run(["systemctl", "start", service_name], check=True)
        logging.info(f"Сервис {service_name} успешно запущен.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при запуске сервиса {service_name}: {e}")


# Получение информации о дисковом пространстве
def get_disk_space():
    headers = {
        "Authorization": f"OAuth {OAUTH_TOKEN}"
    }
    response = requests.get(DISK_URL, headers=headers)

    if response.status_code == 200:
        disk_info = response.json()
        total_space = disk_info['total_space']
        used_space = disk_info['used_space']
        free_space = total_space - used_space
        logging.info(f"Общее место: {total_space / (1024**3):.2f} GB")
        logging.info(f"Занятое место: {used_space / (1024**3):.2f} GB")
        logging.info(f"Свободное место: {free_space / (1024**3):.2f} GB")
        return free_space
    else:
        logging.error(f"Ошибка при получении информации о диске: {response.status_code}, {response.text}")
        return None


# Создание дампа базы данных
def create_db_dump(db_name, user, host, output_file):
    try:
        env = os.environ.copy()
        env['PGPASSWORD'] = DB_PASSWORD

        subprocess.run(
            ["pg_dump", "-U", user, "-h", host, "-d", db_name, "-F", "c", "-f", output_file],
            check=True,
            env=env
        )
        logging.info(f"Дамп базы данных {db_name} успешно создан: {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при создании дампа базы данных: {e}")
        return False


# Архивирование и разбивка файла
def archive_and_split(file_path, split_size=100):
    archive_name = f"{file_path}.tar.gz"
    try:
        subprocess.run(["tar", "-czf", archive_name, file_path], check=True)
        logging.info(f"Файл {file_path} успешно заархивирован в {archive_name}.")
        
        # Разбиение архива на части по split_size МБ
        subprocess.run(["split", "-b", f"{split_size}M", archive_name, f"{archive_name}_part_"], check=True)
        logging.info(f"Архив {archive_name} разбит на части по {split_size} МБ.")
        
        # Удаление оригинального архива
        os.remove(archive_name)
        logging.info(f"Удалён оригинальный архив {archive_name}.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при архивировании или разбивке: {e}")


# Загрузка файла на Яндекс.Диск
def upload_to_yandex_disk(file_path, disk_path):
    free_space = get_disk_space()
    if free_space is None:
        logging.error("Не удалось получить информацию о свободном месте.")
        return

    file_size = os.path.getsize(file_path)
    if free_space < file_size:
        logging.error("Недостаточно места на Яндекс.Диске для загрузки файла.")
        return

    file_name = os.path.basename(file_path)
    UPLOAD_URL = "https://cloud-api.yandex.net/v1/disk/resources/upload"
    headers = {
        "Authorization": f"OAuth {OAUTH_TOKEN}"
    }
    params = {
        "path": disk_path + "/" + file_name,
        "overwrite": "true"
    }

    response = requests.get(UPLOAD_URL, headers=headers, params=params)
    if response.status_code == 200:
        upload_link = response.json().get("href")
        logging.info(f"Загрузка файла начата... {upload_link}")

        with open(file_path, "rb") as f:
            upload_response = requests.put(upload_link, files={"file": f})

        if upload_response.status_code == 201:
            logging.info("Файл успешно загружен на Яндекс.Диск")
        else:
            logging.error(f"Ошибка загрузки: {upload_response.status_code}")
    else:
        logging.error(f"Ошибка при получении ссылки на загрузку: {response.status_code}, {response.text}")


# Пример использования
service_name = "srv1cv8-8.3.25.1257@.services"
db_names = ["test11", "test22", "test33"]
user = "postgres"
host = "localhost"
disk_backup_path = "/backups"


# Остановка сервиса перед созданием дампов
stop_service(service_name)

for db_name in db_names:
    dump_file = f"/home/dump/{db_name}.dump"
    logging.info(f'Создание дампа базы данных {db_name}...')
    if create_db_dump(db_name, user, host, dump_file):
        logging.info('Дамп успешно создан.')
        
        # Архивирование и разбивка
        archive_and_split(dump_file)
        
        # Загрузка частей на Яндекс.Диск
        for part in os.listdir('/home/dump'):
            if part.startswith(f"{dump_file}.tar.gz_part_"):
                upload_to_yandex_disk(os.path.join('/home/dump', part), disk_backup_path)

        # Удаление дампа с жесткого диска
        os.remove(dump_file)
        logging.info(f"Файл {dump_file} успешно удален.")


# Запуск сервиса после завершения всех операций
start_service(service_name)

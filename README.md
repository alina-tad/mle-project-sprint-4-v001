# Подготовка виртуальной машины

## Склонируйте репозиторий

Склонируйте репозиторий проекта:

```
git clone https://github.com/yandex-praktikum/mle-project-sprint-4-v001.git
```

## Активируйте виртуальное окружение

Используйте то же самое виртуальное окружение, что и созданное для работы с уроками. Если его не существует, то его следует создать.

Создать новое виртуальное окружение можно командой:

```
python3 -m venv env_recsys_start
```

После его инициализации следующей командой

```
. env_recsys_start/bin/activate
```

установите в него необходимые Python-пакеты следующей командой

```
pip install -r requirements.txt
```

### Скачайте файлы с данными

Для начала работы понадобится три файла с данными:
- [tracks.parquet](https://storage.yandexcloud.net/mle-data/ym/tracks.parquet)
- [catalog_names.parquet](https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet)
- [interactions.parquet](https://storage.yandexcloud.net/mle-data/ym/interactions.parquet)
 
Скачайте их в директорию локального репозитория. Для удобства вы можете воспользоваться командой wget:

```
wget https://storage.yandexcloud.net/mle-data/ym/tracks.parquet

wget https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet

wget https://storage.yandexcloud.net/mle-data/ym/interactions.parquet
```

## Запустите Jupyter Lab

Запустите Jupyter Lab в командной строке

```
jupyter lab --ip=0.0.0.0 --no-browser
```

# Расчёт рекомендаций

Код для выполнения первой части проекта находится в файле `recommendations.ipynb`. Изначально, это шаблон. Используйте его для выполнения первой части проекта.

В рамках ноутбука:
- данные подготавливаются и сохраняются в S3,
- рассчитываются офлайн-рекомендации:
- топ популярных,
- персональные (ALS),
- похожие треки (item-to-item),
- строится ранжирующая модель,
- итоговые рекомендации сохраняются в recommendations.parquet.

# Сервис рекомендаций

Сервис рекомендаций состоит из трёх микросервисов:
1. Feature Store — выдаёт похожие треки (i2i). В файле `features_service.py`
2. Event Store — хранит онлайн-события пользователей. В файле `events_service.py`
3. Recommendation Service — объединяет офлайн и онлайн рекомендации. В файле `recommendations_service.py`

**Запуск сервисов**

Перед запуском убедитесь, что:
- заданы переменные окружения для доступа к S3
(S3_BUCKET_NAME, MLFLOW_S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);
- офлайн-рекомендации и похожие треки уже сохранены в S3.

Запуск сервисов (в разных терминалах):

```bash
uvicorn features_service:app --port 8010
uvicorn events_service:app --port 8020
uvicorn recommendations_service:app --port 8000
```

# Инструкции для тестирования сервиса

Код для тестирования сервиса находится в файле `test_service.py`.
Тестовый скрипт самодостаточен и не требует ручного подбора данных.

    Сценарии по заданию:
    1. пользователь без персональных (упадёт в default)
    2. с персональными, но без онлайн событий
    3. с персональными и с онлайн событиями

python test_service.py | tee test_service.log

Запуск тестов:

```bash
python test_service.py | tee test_service.log
```

В результате:

- вывод тестов сохраняется в файл `test_service.log`,
- в логе видно, что все три сценария корректно отрабатывают,
- для третьего кейса присутствуют онлайн-рекомендации.

## Заключение

В рамках проекта была построена end-to-end рекомендательная система для музыкального сервиса: от анализа данных и обучения моделей (ALS, item-to-item, ранжирование) до реализации микросервисной архитектуры с поддержкой офлайн- и онлайн-рекомендаций. Сервис корректно обрабатывает различные пользовательские сценарии и использует стратегию смешивания рекомендаций, обеспечивая баланс между стабильным качеством и адаптацией к текущему поведению пользователя.
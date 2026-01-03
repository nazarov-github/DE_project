# dbt в проекте Data Engineering

Этот проект включает dbt (Data Build Tool) для трансформации данных в хранилище PostgreSQL. dbt интегрирован в Docker Compose и готов к использованию.

## Структура

- `Dockerfile.dbt` — Docker-образ на основе официального `dbt-postgres:1.7.0`
- `docker-compose.yml` — сервис `dbt` с монтированием папки `./dbt`
- `dbt/` — корневая папка dbt-проекта
  - `dbt_project.yml` — конфигурация проекта
  - `profiles.yml` — профили подключения к базам данных
  - `models/` — SQL-модели (пусто)
  - `seeds/` — CSV-семена (пусто)
  - `tests/` — тесты (пусто)
  - `macros/` — макросы (пусто)
  - `snapshots/` — снапшоты (пусто)
  - `analyses/` — аналитические запросы (пусто)

## Использование

### Запуск контейнера dbt

```bash
docker-compose up -d dbt
```

Контейнер запускается с командой `tail -f /dev/null` и остаётся активным.

### Выполнение команд dbt внутри контейнера

```bash
docker-compose exec dbt dbt <команда>
```

Примеры:

- Проверка подключения: `docker-compose exec dbt dbt debug`
- Запуск моделей: `docker-compose exec dbt dbt run`
- Тестирование: `docker-compose exec dbt dbt test`
- Генерация документации: `docker-compose exec dbt dbt docs generate`
- Загрузка семян: `docker-compose exec dbt dbt seed`

### Создание моделей

Модели размещаются в `dbt/models/`. Пример простой модели `dbt/models/example.sql`:

```sql
{{ config(materialized='table') }}

select
    id,
    name,
    created_at
from {{ source('public', 'raw_table') }}
```

### Источники данных

В `dbt/models/sources.yml` можно определить источники данных из PostgreSQL. Пример:

```yaml
version: 2

sources:
  - name: public
    database: appdb
    schema: public
    tables:
      - name: raw_table
```

### Интеграция с Airflow

В папке `dags/` уже создан DAG `dbt_dag.py`, который запускает dbt run каждый день в 2:00. DAG использует `DockerOperator` для выполнения команд dbt внутри контейнера.

Чтобы запустить DAG вручную:

```bash
docker-compose exec airflow-webserver airflow dags trigger dbt_dag
```

### Подключение к другим базам данных

Профиль `profiles.yml` содержит два подключения:

1. `postgres` — основное подключение к PostgreSQL (сервис `postgres-app`)
2. `clickhouse` — запасное подключение к ClickHouse (сервис `clickhouse-server`)

Для использования ClickHouse необходимо установить адаптер `dbt-clickhouse` в Dockerfile и настроить профиль.

## Разработка

### Локальная разработка моделей

1. Внесите изменения в файлы в папке `dbt/` на хосте.
2. Изменения автоматически отобразятся внутри контейнера благодаря volume.
3. Протестируйте изменения командой `docker-compose exec dbt dbt run --select <модель>`.

### Добавление зависимостей

Если нужны дополнительные Python-пакеты, отредактируйте `Dockerfile.dbt` и добавьте `RUN pip install <пакет>`. Затем пересоберите образ:

```bash
docker-compose build dbt
```

### Расширение проекта

- **Тесты**: создавайте файлы в `dbt/tests/` или используйте `schema.yml` для описания тестов.
- **Макросы**: помещайте Jinja-макросы в `dbt/macros/`.
- **Семена**: CSV-файлы в `dbt/seeds/` загружаются командой `dbt seed`.
- **Документация**: используйте `dbt docs generate` и `dbt docs serve` для генерации документации.

## Устранение неполадок

### Ошибка подключения к PostgreSQL

Убедитесь, что сервис `postgres-app` запущен:

```bash
docker-compose ps postgres-app
```

Проверьте логи PostgreSQL:

```bash
docker-compose logs postgres-app
```

### Ошибка "profile not found"

Убедитесь, что `profiles.yml` находится в `/usr/app/dbt` внутри контейнера (монтируется автоматически). Проверьте содержимое:

```bash
docker-compose exec dbt cat /usr/app/dbt/profiles.yml
```

### Контейнер dbt завершается сразу после запуска

Контейнер настроен на `tail -f /dev/null`. Если он завершается, проверьте логи:

```bash
docker-compose logs dbt
```

Возможно, нужно сбросить ENTRYPOINT в Dockerfile (уже сделано).

## Дальнейшие шаги

1. Создайте первую модель в `dbt/models/`.
2. Настройте тесты для проверки качества данных.
3. Интегрируйте dbt с CI/CD (например, GitHub Actions).
4. Настройте мониторинг выполнения dbt (например, через Airflow или dbt Cloud).

## Ссылки

- [Документация dbt](https://docs.getdbt.com/)
- [dbt-postgres адаптер](https://github.com/dbt-labs/dbt-postgres)
- [dbt с Docker](https://docs.getdbt.com/guides/dbt-in-docker)
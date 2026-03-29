# MaM-Airflow-

Pet-project по автоматизации обработки медиафайлов с использованием **Apache Airflow**.  
Проект реализует пайплайн для отслеживания новых файлов в watchfolder, проверки их готовности к обработке, конвертации видео и запуска DAG'ов через REST API.  
Также проект демонстрирует работу с **кастомным Airflow sensor**, хранением состояния файлов в PostgreSQL и интеграцией с `ffmpeg`.

## Features

- **Custom Watchfolder Sensor** — кастомный сенсор для отслеживания новых файлов в директории
- **File Stability Detection** — проверка, что файл перестал изменяться по размеру и готов к обработке
- **File Lifecycle Management** — статусы файлов: `new`, `growing`, `ready`, `processing`, `done`, `error`
- **PostgreSQL Registry** — хранение состояния файлов и промежуточной информации в базе данных
- **Main/Proxy Pipeline** — копирование оригинала в `main` и транскодирование proxy-версии через `ffmpeg`
- **Conversion Validation** — проверка успешности обработки по факту существования и размера выходных файлов
- **Airflow REST API Integration** — запуск DAG'ов и получение статуса выполнения через HTTP API
- **Flask Wrapper** — отдельный REST-сервис для взаимодействия с Airflow

## Project Structure

- `dags/first_dag.py` — простой тестовый DAG с передачей параметров через `conf`
- `dags/test_conv_supp.py` — основной DAG с сенсором, подготовкой путей и конвертацией файлов
- `airflow_api.py` — модуль для работы с Airflow REST API и JWT-аутентификацией
- `client_rest_api.py` — Flask API для запуска DAG'ов и получения их статуса
- `tests/` — тестовые скрипты и вспомогательные файлы для проверки пайплайна

## Tech Stack

- **Language:** Python
- **Orchestration:** Apache Airflow
- **Backend / API:** Flask, requests
- **Database:** PostgreSQL
- **Media Processing:** ffmpeg
- **Data Handling:** pandas
- **Time Utilities:** pendulum
- **Architecture:** modular workflow-based application

## Project Goal

Цель проекта — попрактиковаться в построении workflow-пайплайнов на Apache Airflow, работе с кастомными сенсорами, автоматизации обработки файлов, интеграции с PostgreSQL и `ffmpeg`, а также в проектировании небольшого REST-слоя поверх Airflow.

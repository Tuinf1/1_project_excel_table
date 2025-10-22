Тестовое задание: реализовать сквозной процесс SQL → Python → Excel
(дедупликация заказов, сбор отчёта и визуализация).

report_task/
├── data/
│   └── generate_data.py        # генерация тестовых CSV
├── sql/
│   └── export.sql              # SQL-дедупликация заказов
├── py/
│   └── build_report.py         # основной Python-скрипт отчёта
├── excel/
│   └── Report.xlsx             # итоговый Excel-файл
├── docs/
│   └── README.md               # документация

запуск на windows 

py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
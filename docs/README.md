Тестовое задание: реализовать сквозной процесс SQL → Python → Excel
(дедупликация заказов, сбор отчёта и визуализация).


запуск на windows 
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

python data/generate_data.py --email you@domain.com --orders 8000 --days 30
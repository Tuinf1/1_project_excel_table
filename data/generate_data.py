#!/usr/bin/env python3  # позволяет запускать скрипт как исполняемый (в Linux/Unix)
import argparse, csv, hashlib, random, sys
from datetime import datetime, timedelta
from pathlib import Path
import subprocess


# Возможные источники заказов и статусы
CHANNELS = ["site", "ozon", "b24"]
STATUSES = ["created", "paid", "prod_started", "shipped", "delivered", "cancelled"]

def seeded_rng(email: str) -> random.Random:
    """
    Создаёт детерминированный генератор случайных чисел,
    одинаковый для одного и того же email (чтобы результаты генерации повторялись).
    """
    h = hashlib.sha256(email.encode("utf-8")).hexdigest()
    seed = int(h[:16], 16)  # берём первые 16 символов хеша как 64-битное число
    return random.Random(seed)

def choose_status(rng: random.Random):
    """
    Выбирает статус заказа с реалистичным распределением вероятностей.
    Больше всего доставленных, меньше — отменённых и т.п.
    """
    r = rng.random()
    if r < 0.72: return "delivered"
    if r < 0.87: return "cancelled"
    if r < 0.90: return "shipped"
    if r < 0.94: return "prod_started"
    if r < 0.98: return "paid"
    return "created"


def run_report(data_dir: Path, days: int):
    project_root = Path(__file__).resolve().parents[1]
    build_script = project_root / "py" / "build_report.py"

    # формируем аргументы для отчёта
    args = [
        sys.executable,                  # тот же Python/venv
        str(build_script),
        "--db", str(project_root / "test.db"),
        "--out", str(project_root / "excel" / "Report.xlsx"),
        "--days", str(days),             # <- ключевое: прокидываем твой --days
    ]

    print("🚀 Запускаю build_report.py с аргументами:", " ".join(args[1:]))
    # если build_report вернёт код >0 (фейл Checks) — получим исключение
    result = subprocess.run(args)
    if result.returncode != 0:
        print("⚠️ Отчёт собран, но есть проблемы (см. Checks в Excel)")
    else:
        print("✅ Отчёт успешно собран без ошибок")

# вызов после генерации



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True, help="используется как seed")
    ap.add_argument("--orders", type=int, default=8000)
    ap.add_argument("--days", type=int, default=90, help="диапазон дат, последние N дней")
    # ap.add_argument("--db", default=":memory:", help="Путь к SQLite базе или :memory:")
    args = ap.parse_args()

    rng = seeded_rng(args.email)
    outdir = Path("data")
    outdir.mkdir(parents=True, exist_ok=True)

    # Sellers
    sellers = []
    nsellers = 30
    for i in range(1, nsellers + 1):
        sellers.append({"id": i, "name": f"Seller {i:03d}"})

    with open(outdir / "sellers.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "name"])
        w.writeheader()
        w.writerows(sellers)

    # Orders + items
    orders = []
    items = []

    # Часть external_id будет дублироваться
    ext_pool = []
    now = datetime.utcnow().replace(microsecond=0)

    def rand_date_within(days_back: int):
        delta = rng.uniform(0, days_back * 86400)
        return now - timedelta(seconds=delta)

    next_id = 1
    for _ in range(args.orders):
        # 5% шанс сделать дубль по external_id
        make_dup = (len(ext_pool) > 0) and (rng.random() < 0.05)
        if make_dup:
            external_id = rng.choice(ext_pool)
        else:
            external_id = f"ORD-{rng.randrange(10**9, 10**10)}"
            ext_pool.append(external_id)

        date = rand_date_within(args.days)
        channel = rng.choices(CHANNELS, weights=[0.5, 0.3, 0.2], k=1)[0]
        seller_id = rng.randint(1, nsellers)
        status = choose_status(rng)
        # 0.5% — отсутствующий продавец (для Checks)
        if rng.random() < 0.005:
            seller_id = nsellers + rng.randint(1, 3)

        # updated_at после date, delivered_at только для delivered
        updated_at = date + timedelta(hours=rng.randint(1, 240))
        delivered_at = None
        if status == "delivered":
            delivered_at = date + timedelta(days=rng.randint(2, 30), hours=rng.randint(1, 12))
            # иногда updated_at позже delivered_at
            if rng.random() < 0.3:
                updated_at = delivered_at + timedelta(hours=rng.randint(1, 48))

        oid = next_id
        next_id += 1
        orders.append(
            {
                "id": oid,
                "external_id": external_id,
                "date": date.isoformat(timespec="seconds"),
                "channel": channel,
                "seller_id": seller_id,
                "status": status,
                "updated_at": updated_at.isoformat(timespec="seconds"),
                "delivered_at": delivered_at.isoformat(timespec="seconds") if delivered_at else "",
            }
        )

        # Позиции заказа
        nitems = rng.randint(1, 5)
        for _i in range(nitems):
            cat = rng.choice(["TB", "ST", "CH", "WD", "DR", "SH"])
            sku = f"{cat}-{rng.randint(1000, 9999)}"
            qty = rng.randint(1, 5)
            # 1% аномалий qty<=0
            if rng.random() < 0.01:
                qty = rng.choice([0, -1])

            base_cost = rng.randint(50, 500) * 1.0
            markup = rng.uniform(0.15, 0.70)
            revenue = round(base_cost * (1 + markup) * qty, 2)
            cost = round(base_cost * qty, 2)
            # 2% отрицательных марж (ошибка ценообразования)
            if rng.random() < 0.02:
                revenue = max(0.0, round(cost * rng.uniform(0.3, 0.9), 2))

            items.append(
                {
                    "order_id": oid,
                    "sku": sku,
                    "qty": qty,
                    "revenue": revenue,
                    "cost": cost,
                }
            )

    # Запись CSV
    with open(outdir / "orders.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "external_id",
                "date",
                "channel",
                "seller_id",
                "status",
                "updated_at",
                "delivered_at",
            ],
        )
        w.writeheader()
        w.writerows(orders)

    with open(outdir / "order_items.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["order_id", "sku", "qty", "revenue", "cost"])
        w.writeheader()
        w.writerows(items)

    print(f"Generated {len(sellers)} sellers, {len(orders)} orders, {len(items)} items into {outdir}/")

    run_report(data_dir=outdir, days=args.days)

    

if __name__ == "__main__":
    main()
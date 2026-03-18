#!/usr/bin/env python3
"""Синхронизация транзакций из Monobank API в DuckDB.

- Первый запуск: загружает за последние 365 дней
- Последующие: досинкивает с последней сохранённой даты
- Поддерживает все счета клиента
"""

import os
import sys
import time
from datetime import datetime, timedelta

import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()

MONO_TOKEN = os.getenv("MONO_TOKEN")
MONO_API_BASE = "https://api.monobank.ua"
DB_PATH = os.getenv("DB_PATH", "/data/db/mono.duckdb")
PARQUET_DIR = os.getenv("PARQUET_DIR", "/data/parquet")

_start_time = time.time()


def log(msg: str):
    elapsed = time.time() - _start_time
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts} +{elapsed:6.1f}s] {msg}", flush=True)


def get_headers():
    return {"X-Token": MONO_TOKEN}


def api_get(path: str) -> requests.Response:
    log(f"API GET {path}")
    t0 = time.time()
    resp = requests.get(f"{MONO_API_BASE}{path}", headers=get_headers())
    if resp.status_code == 429:
        log(f"  429 Rate limit — ожидание 65с...")
        time.sleep(65)
        resp = requests.get(f"{MONO_API_BASE}{path}", headers=get_headers())
    resp.raise_for_status()
    log(f"  ← {resp.status_code} ({time.time() - t0:.1f}с)")
    return resp


def init_db(con: duckdb.DuckDBPyConnection):
    log("Инициализация базы данных...")
    con.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id VARCHAR PRIMARY KEY,
            account_id VARCHAR NOT NULL,
            time TIMESTAMP NOT NULL,
            description VARCHAR,
            mcc INTEGER,
            original_mcc INTEGER,
            amount DOUBLE,
            operation_amount DOUBLE,
            currency_code INTEGER,
            commission_rate DOUBLE,
            cashback_amount DOUBLE,
            balance DOUBLE,
            hold BOOLEAN,
            comment VARCHAR,
            receipt_id VARCHAR,
            invoice_id VARCHAR,
            counter_edrpou VARCHAR,
            counter_iban VARCHAR,
            counter_name VARCHAR,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id VARCHAR PRIMARY KEY,
            send_id VARCHAR,
            currency_code INTEGER,
            cashback_type VARCHAR,
            balance DOUBLE,
            credit_limit DOUBLE,
            masked_pan VARCHAR,
            type VARCHAR,
            iban VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1")
    con.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY DEFAULT nextval('sync_log_seq'),
            account_id VARCHAR,
            synced_from TIMESTAMP,
            synced_to TIMESTAMP,
            tx_count INTEGER,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    log("БД готова")


def sync_accounts(con: duckdb.DuckDBPyConnection) -> list[dict]:
    log("Загрузка информации о счетах...")
    info = api_get("/personal/client-info").json()
    accounts = info.get("accounts", [])

    currency_map = {980: "UAH", 840: "USD", 978: "EUR"}
    for acc in accounts:
        cur = currency_map.get(acc["currencyCode"], str(acc["currencyCode"]))
        bal = acc.get("balance", 0) / 100
        log(f"  Счёт {acc['id'][:8]}... {cur} баланс={bal:,.2f}")

        pans = ",".join(acc.get("maskedPan", []))
        con.execute("""
            INSERT OR REPLACE INTO accounts
            (id, send_id, currency_code, cashback_type, balance, credit_limit, masked_pan, type, iban, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            acc["id"],
            acc.get("sendId", ""),
            acc.get("currencyCode"),
            acc.get("cashbackType", ""),
            bal,
            acc.get("creditLimit", 0) / 100,
            pans,
            acc.get("type", ""),
            acc.get("iban", ""),
        ])

    log(f"Счетов найдено: {len(accounts)}")
    return accounts


def get_last_sync_time(con: duckdb.DuckDBPyConnection, account_id: str) -> int | None:
    result = con.execute(
        "SELECT MAX(time) FROM transactions WHERE account_id = ?",
        [account_id]
    ).fetchone()
    if result and result[0]:
        dt = result[0]
        return int(dt.timestamp())
    return None


def fetch_statement(account_id: str, from_ts: int, to_ts: int) -> list[dict]:
    """Загружает выписку, разбивая на куски по 31 день."""
    max_range = 31 * 24 * 60 * 60
    all_txs = []
    current_from = from_ts

    # Считаем количество чанков
    total_chunks = 0
    t = from_ts
    while t < to_ts:
        total_chunks += 1
        t = min(t + max_range, to_ts) + 1
    chunk_num = 0

    while current_from < to_ts:
        current_to = min(current_from + max_range, to_ts)
        chunk_num += 1
        date_from = datetime.fromtimestamp(current_from).strftime('%d.%m.%Y')
        date_to = datetime.fromtimestamp(current_to).strftime('%d.%m.%Y')
        log(f"  Чанк {chunk_num}/{total_chunks}: {date_from} — {date_to}")

        path = f"/personal/statement/{account_id}/{current_from}/{current_to}"

        for attempt in range(5):
            try:
                t0 = time.time()
                resp = requests.get(
                    f"{MONO_API_BASE}{path}", headers=get_headers()
                )
                elapsed_req = time.time() - t0

                if resp.status_code == 429:
                    log(f"    429 Rate limit (попытка {attempt + 1}/5) — ожидание 65с...")
                    time.sleep(65)
                    continue
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < 4:
                    log(f"    Ошибка (попытка {attempt + 1}/5): {e} — повтор через 10с...")
                    time.sleep(10)
                else:
                    raise

        data = resp.json()
        if isinstance(data, list):
            all_txs.extend(data)
            log(f"    ← {len(data)} транзакций за {elapsed_req:.1f}с (всего: {len(all_txs)})")
        else:
            log(f"    ← Неожиданный ответ: {str(data)[:100]}")

        current_from = current_to + 1

    return all_txs


def save_transactions(
    con: duckdb.DuckDBPyConnection, account_id: str, transactions: list[dict]
):
    if not transactions:
        return 0

    log(f"  Сохранение {len(transactions)} транзакций в БД...")
    t0 = time.time()
    inserted = 0
    for tx in transactions:
        try:
            con.execute("""
                INSERT OR IGNORE INTO transactions
                (id, account_id, time, description, mcc, original_mcc,
                 amount, operation_amount, currency_code, commission_rate,
                 cashback_amount, balance, hold, comment, receipt_id,
                 invoice_id, counter_edrpou, counter_iban, counter_name, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                tx["id"],
                account_id,
                datetime.fromtimestamp(tx["time"]),
                tx.get("description", ""),
                tx.get("mcc", 0),
                tx.get("originalMcc", 0),
                tx.get("amount", 0) / 100,
                tx.get("operationAmount", 0) / 100,
                tx.get("currencyCode", 0),
                tx.get("commissionRate", 0) / 100,
                tx.get("cashbackAmount", 0) / 100,
                tx.get("balance", 0) / 100,
                tx.get("hold", False),
                tx.get("comment", ""),
                tx.get("receiptId", ""),
                tx.get("invoiceId", ""),
                tx.get("counterEdrpou", ""),
                tx.get("counterIban", ""),
                tx.get("counterName", ""),
            ])
            inserted += 1
        except Exception:
            pass  # Duplicate, skip

    log(f"  Сохранено: {inserted} новых, {len(transactions) - inserted} дублей ({time.time() - t0:.1f}с)")
    return inserted


def sync_account(con: duckdb.DuckDBPyConnection, account_id: str, currency: str):
    last_ts = get_last_sync_time(con, account_id)
    now_ts = int(time.time())

    if last_ts:
        from_ts = last_ts
        log(f"Досинк с {datetime.fromtimestamp(from_ts).strftime('%d.%m.%Y %H:%M')}")
    else:
        from_ts = now_ts - 365 * 24 * 60 * 60
        days = (now_ts - from_ts) // 86400
        log(f"Первая загрузка — последние {days} дней")

    transactions = fetch_statement(account_id, from_ts, now_ts)
    inserted = save_transactions(con, account_id, transactions)

    con.execute("""
        INSERT INTO sync_log (account_id, synced_from, synced_to, tx_count, synced_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [account_id, datetime.fromtimestamp(from_ts), datetime.fromtimestamp(now_ts), inserted])

    log(f"Итого по счёту: +{inserted} новых из {len(transactions)} полученных")


def main():
    log("=" * 50)
    log("MONOBANK SYNC START")
    log("=" * 50)

    if not MONO_TOKEN:
        log("ОШИБКА: MONO_TOKEN не задан!")
        sys.exit(1)

    log(f"DB: {DB_PATH}")
    log(f"Parquet: {PARQUET_DIR}")

    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

    con = duckdb.connect(DB_PATH)
    init_db(con)

    accounts = sync_accounts(con)

    currency_map = {980: "UAH", 840: "USD", 978: "EUR"}

    for i, acc in enumerate(accounts, 1):
        currency = currency_map.get(acc["currencyCode"], str(acc["currencyCode"]))
        log("")
        log(f"━━━ Счёт {i}/{len(accounts)}: {acc['id'][:8]}... ({currency}) ━━━")
        try:
            sync_account(con, acc["id"], currency)
        except Exception as e:
            log(f"ОШИБКА: {e}")

    total = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    log("")
    log(f"Всего в базе: {total} транзакций")

    # Экспорт в Parquet для Rill
    log("Экспорт в Parquet...")
    t0 = time.time()
    os.makedirs(PARQUET_DIR, exist_ok=True)
    con.execute(f"COPY transactions TO '{PARQUET_DIR}/transactions.parquet' (FORMAT PARQUET, OVERWRITE)")
    con.execute(f"COPY accounts TO '{PARQUET_DIR}/accounts.parquet' (FORMAT PARQUET, OVERWRITE)")
    log(f"Parquet экспортирован ({time.time() - t0:.1f}с)")

    con.close()

    log("")
    log(f"SYNC ЗАВЕРШЁН за {time.time() - _start_time:.1f}с")
    log("=" * 50)


if __name__ == "__main__":
    main()

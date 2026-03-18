#!/bin/bash
set -e

echo "=== Monobank Sync ==="
echo "Запуск начальной синхронизации..."
python3 /app/sync.py

# Запуск cron для периодической синхронизации
echo "Настройка автосинка каждый час..."
echo "0 * * * * cd /app && python3 sync.py >> /var/log/sync.log 2>&1" | crontab -
cron

echo "Синк-сервис запущен. Следующая синхронизация через час."
echo "Логи: /var/log/sync.log"

# Держим контейнер живым
tail -f /var/log/sync.log 2>/dev/null &
wait

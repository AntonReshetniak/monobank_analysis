SELECT
    t.id,
    t.account_id,
    -- Читаемое имя счёта: "UAH *1234" или "USD *5678"
    CASE
        WHEN a.currency_code = 980 THEN 'UAH'
        WHEN a.currency_code = 840 THEN 'USD'
        WHEN a.currency_code = 978 THEN 'EUR'
        ELSE CAST(a.currency_code AS VARCHAR)
    END || ' ' || COALESCE(a.type, '') ||
    CASE
        WHEN a.masked_pan IS NOT NULL AND a.masked_pan != ''
        THEN ' *' || RIGHT(REPLACE(a.masked_pan, ',', '/'), 4)
        ELSE ''
    END AS account_name,
    t.time,
    t.description,
    t.mcc,
    t.amount,
    t.operation_amount,
    t.currency_code,
    t.cashback_amount,
    t.balance,
    t.hold,
    t.comment,
    t.counter_name,
    -- Категория по MCC
    CASE
        WHEN t.mcc IN (5411, 5412) THEN 'Продукты'
        WHEN t.mcc = 5422 THEN 'Мясо/Рыба'
        WHEN t.mcc = 5441 THEN 'Сладости'
        WHEN t.mcc = 5451 THEN 'Молочные продукты'
        WHEN t.mcc = 5462 THEN 'Пекарни'
        WHEN t.mcc = 5499 THEN 'Продукты (другое)'
        WHEN t.mcc IN (5812) THEN 'Рестораны/Кафе'
        WHEN t.mcc = 5813 THEN 'Бары'
        WHEN t.mcc = 5814 THEN 'Фастфуд'
        WHEN t.mcc IN (4011, 4112) THEN 'Ж/Д билеты'
        WHEN t.mcc IN (4111, 4131) THEN 'Общественный транспорт'
        WHEN t.mcc = 4121 THEN 'Такси'
        WHEN t.mcc IN (5541, 5542, 5983) THEN 'АЗС'
        WHEN t.mcc = 4784 THEN 'Платные дороги'
        WHEN t.mcc IN (5531, 5532, 5533, 7531, 7534, 7535, 7538, 7542) THEN 'Авто'
        WHEN t.mcc IN (5611, 5621, 5631, 5641, 5651, 5691, 5699, 5137, 5139) THEN 'Одежда'
        WHEN t.mcc IN (5661) THEN 'Обувь'
        WHEN t.mcc = 5912 THEN 'Аптеки'
        WHEN t.mcc IN (8011, 8021, 8031, 8041, 8042, 8049, 8050, 8062, 8071, 8099) THEN 'Медицина'
        WHEN t.mcc = 7832 THEN 'Кинотеатры'
        WHEN t.mcc IN (7922, 7929) THEN 'Концерты/Театры'
        WHEN t.mcc = 7997 THEN 'Спортклубы'
        WHEN t.mcc IN (7991, 7992, 7993, 7994, 7996, 7998, 7999) THEN 'Развлечения'
        WHEN t.mcc IN (4812, 4813, 4814) THEN 'Связь'
        WHEN t.mcc IN (4899, 4900) THEN 'Коммуналка'
        WHEN t.mcc IN (5732) THEN 'Электроника'
        WHEN t.mcc IN (5734, 5816, 5817, 5818) THEN 'Цифровые товары'
        WHEN t.mcc IN (5815, 5968) THEN 'Подписки'
        WHEN t.mcc IN (7011) THEN 'Отели'
        WHEN t.mcc = 6513 THEN 'Аренда жилья'
        WHEN t.mcc IN (8211, 8220, 8241, 8244, 8249, 8299) THEN 'Образование'
        WHEN t.mcc IN (7230, 7297, 7298) THEN 'Красота/СПА'
        WHEN t.mcc = 5977 THEN 'Косметика'
        WHEN t.mcc = 4829 THEN 'Переводы/Платежи'
        WHEN t.mcc IN (6010, 6011) THEN 'Снятие наличных'
        WHEN t.mcc = 6211 THEN 'Инвестиции'
        WHEN t.mcc IN (6300, 6399) THEN 'Страхование'
        WHEN t.mcc = 6540 THEN 'Пополнение карт'
        WHEN t.mcc IN (5200, 5211, 5231, 5251, 5261) THEN 'Стройматериалы'
        WHEN t.mcc IN (5712, 5713, 5714, 5718, 5719, 5722) THEN 'Дом/Техника'
        WHEN t.mcc = 5995 THEN 'Зоотовары'
        WHEN t.mcc = 5941 THEN 'Спорттовары'
        WHEN t.mcc = 5942 THEN 'Книги'
        WHEN t.mcc = 5944 THEN 'Ювелирные'
        WHEN t.mcc = 5945 THEN 'Игрушки'
        WHEN t.mcc = 5992 THEN 'Цветы'
        WHEN t.mcc = 5993 THEN 'Сигареты'
        WHEN t.mcc IN (7372, 7379) THEN 'IT-услуги'
        WHEN t.mcc = 8111 THEN 'Юристы'
        WHEN t.mcc = 8398 THEN 'Благотворительность'
        WHEN t.mcc IN (9211, 9222) THEN 'Штрафы'
        WHEN t.mcc IN (9311, 7276) THEN 'Налоги'
        WHEN t.mcc = 9402 THEN 'Почта'
        ELSE 'Другое (MCC: ' || CAST(t.mcc AS VARCHAR) || ')'
    END AS category,
    -- Тип: расход или доход
    CASE WHEN t.amount < 0 THEN 'Расход' ELSE 'Доход' END AS tx_type,
    ABS(t.amount) AS abs_amount,
    -- Валюта
    CASE
        WHEN t.currency_code = 980 THEN 'UAH'
        WHEN t.currency_code = 840 THEN 'USD'
        WHEN t.currency_code = 978 THEN 'EUR'
        ELSE CAST(t.currency_code AS VARCHAR)
    END AS currency,
    -- Время: день недели, месяц
    CASE DAYOFWEEK(t.time)
        WHEN 0 THEN '7 Вс'
        WHEN 1 THEN '1 Пн'
        WHEN 2 THEN '2 Вт'
        WHEN 3 THEN '3 Ср'
        WHEN 4 THEN '4 Чт'
        WHEN 5 THEN '5 Пт'
        WHEN 6 THEN '6 Сб'
    END AS day_of_week,
    STRFTIME(t.time, '%Y-%m') AS month,
    -- Мерчант (первое осмысленное имя)
    COALESCE(NULLIF(t.counter_name, ''), t.description, '(без описания)') AS merchant
FROM read_parquet('/data/parquet/transactions.parquet') t
LEFT JOIN read_parquet('/data/parquet/accounts.parquet') a ON t.account_id = a.id

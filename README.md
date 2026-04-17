# Almaty Traffic Scraper — IMPROVED VERSION

## Что было не так

Старый `config.py` использовал **RGB-диапазоны** для классификации пикселей трафика.
Проблема: диапазоны для оранжевого/красного/тёмно-красного были слишком узкие.

**Результат:** за 11 дней сбора данных:
- `pct_moderate`: 13 ненулевых из 46,945 (0.0%)
- `pct_heavy`: **0** (ноль!)
- `pct_jam`: **0** (ноль!)

Парсер видел только зелёные и жёлтые пиксели → все scores ≈ 1.5.

## Что исправлено

1. **HSV-классификация** (основной метод) — HSV (Hue/Saturation/Value) намного
   надёжнее RGB для определения цвета, потому что Hue напрямую кодирует цвет:
   - Зелёный (free) → H: 80-160°
   - Жёлтый (slow) → H: 35-80°
   - Оранжевый (moderate) → H: 10-35°
   - Красный (heavy) → H: 350-10°
   - Тёмно-красный (jam) → H: 345-15°, V < 50

2. **Порог прозрачности снижен** с 60 до 30 — ловим больше пикселей

3. **RGB-диапазоны расширены** как fallback

4. **Логирование** — после каждого запуска в лог пишется сколько moderate/heavy/jam
   пикселей обнаружено. Если 0 — будет WARNING.

## Пошаговая инструкция (3 недели до сдачи)

### Шаг 1: Диагностика (5 минут)

```bash
# Запустите ДИАГНОСТИКУ во время час-пика (8-9 утра или 18-19 вечера)
cd traffic_scrape
python diagnose_colors.py
```

Это скачает тайлы и покажет реальные цвета пикселей.
Сравните с HSV_RULES в config.py. Если не совпадает — подправьте диапазоны.

### Шаг 2: Замените файлы

Замените в репозитории:
- `config.py` → новый config.py (с HSV_RULES)
- `scraper.py` → новый scraper.py (с HSV-классификацией)
- Добавьте `diagnose_colors.py`

НЕ трогайте: `tiles.py`, `weather.py`, `geocoder.py`, `build_geocache.py`

### Шаг 3: Настройте погоду (2 минуты)

1. Зайдите на https://openweathermap.org/api
2. Зарегистрируйтесь (бесплатно)
3. Скопируйте API key
4. В GitHub → Settings → Secrets → добавьте: `OWM_API_KEY = ваш_ключ`

### Шаг 4: Пересоберите geocache (3 минуты)

```bash
# Удалите старый пустой кеш
rm data/geocode_cache.json

# Запустите заново
python build_geocache.py
```

### Шаг 5: Начните НОВЫЙ сбор данных

```bash
# Создайте новый файл для чистых данных
mv data/traffic_data.csv data/traffic_data_old.csv

# Запустите скрейпер
python scraper.py
```

Проверьте лог — должны появиться строки:
```
Score stats: mean=X.XX, max=Y.YY
pixels_moderate: NNN (detected!)
pixels_heavy: NNN (detected!)
```

Если всё ещё `pixels_heavy: 0` — запустите `diagnose_colors.py` и сообщите результат.

### Шаг 6: Подождите 2 недели

Cron будет собирать данные каждые 30 минут. За 2 недели у вас будет:
- ~1,344 временных точки (48 × 28 дней)
- ~200,000+ строк
- Данные с часами пик, выходными, дождём
- Разнообразие congestion levels

### Шаг 7: Запустите ML-пайплайн

Откройте `Almaty_Traffic_ML_Pipeline.ipynb` в Google Colab,
загрузите новый `traffic_data.csv` — и модели покажут реальные результаты.

## Структура файлов

```
traffic_scrape/
├── config.py           ← ОБНОВЛЁН (HSV + расширенные RGB)
├── scraper.py          ← ОБНОВЛЁН (HSV-классификация)
├── diagnose_colors.py  ← НОВЫЙ (диагностика цветов)
├── tiles.py            ← без изменений
├── weather.py          ← без изменений
├── geocoder.py         ← без изменений
├── build_geocache.py   ← без изменений
├── run_scraper.sh      ← без изменений
└── data/
    ├── traffic_data.csv
    ├── traffic_latest.csv
    └── geocode_cache.json
```

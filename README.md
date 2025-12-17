# Cryptocurrency Data API & ML Predictor
Repozytorium: https://github.com/wojciech70711/data-fetcher-api.git
## Opis Projektu

System do analizy i predykcji cen kryptowalut wykorzystujący Apache Kafka, PySpark oraz uczenia maszynowego. Projekt składa się z trzech głównych komponentów:

1. **FastAPI Service (main.py)** - REST API do zarządzania danymi OHLCV
2. **ML Predictor (ml_predictor.py)** - Serwis PySpark do treningu modeli i predykcji w czasie rzeczywistym
3. **Data Collector (get_data_from_market.py)** - Skrypt do pobierania danych historycznych z giełd kryptowalut

## Architektura

```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│   Data Source   │─────>│  FastAPI     │─────>│  Kafka Broker   │
│ (Market Data)   │      │  (main.py)   │      │   (ohlcv topic) │
└─────────────────┘      └──────────────┘      └────────┬────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  ML Predictor   │
                                                │ (PySpark + ML)  │
                                                │  ml_predictor   │
                                                └─────────────────┘
```

## Funkcjonalności

### API Service (main.py)
- Przyjmowanie danych OHLCV (Open, High, Low, Close, Volume) przez REST API
- Walidacja danych wejściowych
- Publikacja danych do Kafka w czasie rzeczywistym
- Ładowanie i wykorzystanie wytrenowanych modeli ML
- Logowanie operacji do plików
- Dokumentacja API (Swagger/OpenAPI)

### ML Predictor (ml_predictor.py)
- Trening modelu Gradient Boosting Regressor na danych historycznych
- Konsumpcja danych z Kafka w czasie rzeczywistym
- Przetwarzanie strumieniowe z PySpark
- Generowanie predykcji cen
- Zapis wyników predykcji do plików JSON
- Obliczanie wskaźników technicznych

### Data Collector (get_data_from_market.py)
- Pobieranie historycznych danych OHLCV z giełd
- Wsparcie dla wielu par handlowych
- Konfiguracja interwałów czasowych
- Zapis danych treningowych

## Technologie

- **Python 3.11**
- **FastAPI** - framework web do REST API
- **Apache Kafka** - broker komunikatów do streamingu danych
- **PySpark 3.5.0** - przetwarzanie strumieniowe danych
- **scikit-learn** - modele uczenia maszynowego
- **Pandas & NumPy** - analiza danych
- **Docker & Docker Compose** - konteneryzacja i orkiestracja

## Wymagania

- Docker i Docker Compose
- Python 3.11+ (do rozwoju lokalnego)
- 4GB RAM minimum (dla Spark)

## Szybki Start

### Uruchomienie z Docker Compose

1. **Zbuduj i uruchom wszystkie serwisy:**
```bash
docker-compose up --build
```

2. **Sprawdź logi:**
```bash
docker-compose logs -f
```

### Instalacja Lokalna

1. **Utwórz środowisko wirtualne:**
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac
```

2. **Zainstaluj zależności:**
```bash
pip install -r requirements.txt
```

3. **Uruchom Kafka lokalnie** (wymagane Zookeeper i Kafka)

4. **Uruchom serwisy:**
```bash
# Terminal 1 - API
python main.py

# Terminal 2 - ML Predictor
python ml_predictor.py

# Terminal 3 - Pobierz dane treningowe
python get_data_from_market.py --pairs BTC --interval 1h --limit 1000
```

## Struktura Projektu

```
data-api/
├── config.py                 # Konfiguracja (env variables)
├── main.py                   # FastAPI service
├── ml_predictor.py          # PySpark ML service
├── get_data_from_market.py  # Data collector
├── requirements.txt         # Python dependencies
├── Dockerfile              # Docker image definition
├── docker-compose.yml      # Orkiestracja kontenerów
├── DOCKER_SETUP.md        # Dokumentacja Docker
├── data/                  # Dane treningowe
│   └── train_data.csv
├── models/               # Wytrenowane modele
│   ├── gbt_model.pkl
│   └── scaler.pkl
├── results/             # Wyniki predykcji
├── logs/               # Logi aplikacji
└── hadoop/            # Hadoop binaries (Windows)
```

## API Endpoints

Po uruchomieniu API jest dostępne pod adresem: `http://localhost:8000`

### Dokumentacja API
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Przykładowe Endpointy
- `POST /ohlcv/batch` - Wysłanie serii danych OHLCV
- `GET /` - Info o API

## Konfiguracja

Zmienne środowiskowe (plik `.env` lub docker-compose):

```env
# Server
HOST=0.0.0.0
PORT=8000

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=ohlcv

# Data
DATA_DIRECTORY=data
MIN_VOLUME=0
MAX_PRICE=1000000
MIN_PRICE=0.00001

# API
API_TITLE=Cryptocurrency OHLCV Data API
API_VERSION=1.0.0
```

## Model ML

### Gradient Boosting Regressor
- **n_estimators**: 100
- **max_depth**: 5
- **learning_rate**: 0.1
- **Features**: Open, High, Low, Close, Volume + wskaźniki techniczne

### Proces Treningu
1. Wczytanie danych z `data/train_data.csv`
2. Feature engineering (wskaźniki techniczne)
3. Normalizacja danych (StandardScaler)
4. Trening modelu GBT
5. Zapis modelu do `models/`

### Predykcja w Czasie Rzeczywistym
1. Konsumpcja danych z Kafka
2. Agregacja w oknie czasowym
3. Ekstrakcja cech
4. Predykcja
5. Zapis wyników do `results/`

## Kafka Topics

- **Topic**: `ohlcv`
- **Partitions**: 3
- **Replication Factor**: 1

### Testowanie API:
```bash
curl -X POST http://localhost:8000/ohlcv \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "BTCUSDT",
    "timestamp": "2025-12-17T10:00:00",
    "open": 45000.0,
    "high": 45500.0,
    "low": 44800.0,
    "close": 45200.0,
    "volume": 1234.56
  }'
```

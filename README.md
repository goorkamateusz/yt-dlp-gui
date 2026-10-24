# yt-dlp Docker Web UI

Minimalistyczny interfejs webowy do pobierania wideo (YouTube, CDA itp.) z izolacją zadań w Dockerze – każde zlecenie pobierania uruchamia dedykowany, tymczasowy kontener z `yt-dlp`.

## Funkcje

* **Izolacja zadań:** Jeden kontener Docker (`jauderho/yt-dlp`) per pobierany plik.
* **Podgląd na żywo:** Pasek postępu, prędkość, rozmiar, ETA oraz tytuł wideo przesyłane w czasie rzeczywistym przez Server-Sent Events (SSE).
* **Trwałość stanu:** Odświeżenie lub zamknięcie przeglądarki nie przerywa pobierania – po ponownym otwarciu karty aktywne zadania są automatycznie wznawiane w UI.
* **Automatyczne sprzątanie:** Kontenery są usuwane natychmiast po zakończeniu pobierania lub scalania przez FFmpeg.

## Wymagania

* Docker
* Docker Compose

## Szybki start

### 1. Konfiguracja `.env`

Utwórz plik `.env` i wskaż ścieżkę **na hoście**, do której mają trafiać pobrane pliki:

```env
WEB_PORT=8000
HOST_DOWNLOAD_PATH=/bezwzgledna/sciezka/do/folderu/pobrane
```

> **Uwaga:** Ścieżka musi być bezwzględna (np. `/home/user/downloads` na Linux lub `C:\Downloads` na Windows), ponieważ kontener komunikuje się bezpośrednio z demonem Dockera na hoście.

### 2. Uruchomienie

Zbuduj i uruchom serwis:

```bash
docker compose up -d --build
```

Aplikacja dostępna jest pod adresem: `http://localhost:8000` (lub innym zdefiniowanym w `WEB_PORT`).

## Zatrzymanie aplikacji

```bash
docker compose down
```
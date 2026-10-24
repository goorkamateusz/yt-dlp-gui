import os
import re
import json
import uuid
import docker
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI()
client = docker.from_env()

HOST_DOWNLOAD_PATH = os.getenv("HOST_DOWNLOAD_PATH", "/downloads")

# Wyrażenie regularne do wyciągania postępu z yt-dlp
PROGRESS_REGEX = re.compile(
    r"\[download\]\s+([0-9.]+)%\s+of\s+(?:~\s*)?(\S+)\s+at\s+(\S+)\s+ETA\s+(\S+)"
)

HTML = """
<!DOCTYPE html>
<html lang="pl">
<head>
  <meta charset="utf-8">
  <title>Pobieracz Wideo</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 720px; margin: 40px auto; padding: 0 16px; background: #121212; color: #e0e0e0; }
    h2 { font-weight: 600; margin-bottom: 24px; }
    .input-group { display: flex; gap: 8px; margin-bottom: 24px; }
    input[type="text"] { flex-grow: 1; padding: 12px 14px; background: #1e1e1e; border: 1px solid #333; border-radius: 6px; color: #fff; font-size: 14px; outline: none; }
    input[type="text"]:focus { border-color: #0070f3; }
    button { padding: 12px 24px; background: #0070f3; border: none; border-radius: 6px; color: #fff; font-weight: 500; cursor: pointer; transition: background 0.2s; }
    button:hover { background: #005bb5; }
    button:disabled { background: #444; cursor: not-allowed; }

    .card { background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 20px; display: none; margin-bottom: 20px; }
    .job-meta { display: flex; justify-content: space-between; font-size: 13px; color: #888; margin-bottom: 12px; }

    .progress-track { width: 100%; height: 16px; background: #2a2a2a; border-radius: 8px; overflow: hidden; margin-bottom: 16px; position: relative; }
    .progress-fill { width: 0%; height: 100%; background: linear-gradient(90deg, #0070f3, #00c6ff); border-radius: 8px; transition: width 0.3s ease; }

    .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; text-align: center; }
    .stat-box { background: #161616; padding: 10px; border-radius: 6px; border: 1px solid #282828; }
    .stat-label { font-size: 11px; color: #888; text-transform: uppercase; margin-bottom: 4px; }
    .stat-val { font-size: 14px; font-weight: 600; color: #fff; }

    details { margin-top: 16px; }
    summary { font-size: 12px; color: #777; cursor: pointer; user-select: none; }
    #console-log { margin-top: 8px; padding: 10px; background: #0a0a0a; border-radius: 4px; font-family: monospace; font-size: 11px; max-height: 180px; overflow-y: auto; white-space: pre-wrap; color: #a0a0a0; }
  </style>
</head>
<body>
  <h2>yt-dlp Docker Launcher</h2>

  <form id="dlForm" class="input-group">
    <input type="text" id="url" placeholder="Wklej link z YouTube, CDA itp..." required autocomplete="off" />
    <button type="submit" id="submitBtn">Pobierz</button>
  </form>

  <div id="card" class="card">
    <div class="job-meta">
      <span>Kontener: <strong id="container-name" style="color:#00c6ff;">-</strong></span>
      <span id="status-text">Inicjalizacja...</span>
    </div>

    <div class="progress-track">
      <div id="progress-fill" class="progress-fill"></div>
    </div>

    <div class="stats-grid">
      <div class="stat-box"><div class="stat-label">Postęp</div><div class="stat-val" id="stat-percent">0%</div></div>
      <div class="stat-box"><div class="stat-label">Rozmiar</div><div class="stat-val" id="stat-size">--</div></div>
      <div class="stat-box"><div class="stat-label">Prędkość</div><div class="stat-val" id="stat-speed">--</div></div>
      <div class="stat-box"><div class="stat-label">Pozostało</div><div class="stat-val" id="stat-eta">--</div></div>
    </div>

    <details>
      <summary>Pokaż surowe logi</summary>
      <div id="console-log"></div>
    </details>
  </div>

  <script>
    const form = document.getElementById('dlForm');
    const submitBtn = document.getElementById('submitBtn');
    const card = document.getElementById('card');
    const progressFill = document.getElementById('progress-fill');
    const containerNameEl = document.getElementById('container-name');
    const statusText = document.getElementById('status-text');
    const consoleLog = document.getElementById('console-log');

    const statPercent = document.getElementById('stat-percent');
    const statSize = document.getElementById('stat-size');
    const statSpeed = document.getElementById('stat-speed');
    const statEta = document.getElementById('stat-eta');

    form.onsubmit = async (e) => {
      e.preventDefault();
      submitBtn.disabled = true;
      card.style.display = 'block';
      consoleLog.textContent = '';
      progressFill.style.width = '0%';
      statPercent.textContent = '0%';
      statSize.textContent = '--';
      statSpeed.textContent = '--';
      statEta.textContent = '--';
      statusText.textContent = "Startowanie kontenera...";

      const formData = new FormData();
      formData.append("url", document.getElementById('url').value);

      try {
        const res = await fetch("/start-download", { method: "POST", body: formData });
        const data = await res.json();

        containerNameEl.textContent = data.container_name;
        statusText.textContent = "Pobieranie...";

        const evtSource = new EventSource(`/stream/${data.container_id}`);

        evtSource.addEventListener("progress", (event) => {
          const info = JSON.parse(event.data);
          progressFill.style.width = info.percent + '%';
          statPercent.textContent = info.percent + '%';
          statSize.textContent = info.size;
          statSpeed.textContent = info.speed;
          statEta.textContent = info.eta;
        });

        evtSource.addEventListener("status", (event) => {
          const info = JSON.parse(event.data);
          statusText.textContent = info.status;
          if (info.percent) {
            progressFill.style.width = info.percent + '%';
            statPercent.textContent = info.percent + '%';
          }
        });

        evtSource.addEventListener("log", (event) => {
          consoleLog.textContent += event.data + '\\n';
          consoleLog.scrollTop = consoleLog.scrollHeight;
        });

        evtSource.addEventListener("end", () => {
          statusText.textContent = "Zakończono pomyślnie!";
          progressFill.style.width = '100%';
          statPercent.textContent = '100%';
          evtSource.close();
          submitBtn.disabled = false;
        });

        evtSource.onerror = () => {
          statusText.textContent = "Błąd lub rozłączono.";
          evtSource.close();
          submitBtn.disabled = false;
        };

      } catch (err) {
        statusText.textContent = "Błąd żądania.";
        submitBtn.disabled = false;
      }
    };
  </script>
</body>
</html>
"""

@app.get("/")
def index():
    return HTMLResponse(HTML)

@app.post("/start-download")
def start_download(url: str = Form(...)):
    # 1. Unikalna nazwa kontenera per pobieranie
    job_id = uuid.uuid4().hex[:6]
    container_name = f"ytdlp-job-{job_id}"

    # Flagi yt-dlp: --newline (każdy wpis w nowej linii), --no-colors (czysty tekst bez ANSI)
    cmd = f'"{url}" --newline --no-colors -o "/downloads/%(title)s.%(ext)s"'

    container = client.containers.run(
        image="jauderho/yt-dlp:latest",
        name=container_name,
        command=cmd,
        volumes={HOST_DOWNLOAD_PATH: {"bind": "/downloads", "mode": "rw"}},
        detach=True
    )
    return {"container_id": container.id, "container_name": container_name}

@app.get("/stream/{container_id}")
def stream_logs(container_id: str):
    def event_generator():
        container = client.containers.get(container_id)
        try:
            for chunk in container.logs(stdout=True, stderr=True, stream=True, follow=True):
                line = chunk.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue

                # Wysyłanie surowego logu do sekcji "Pokaż surowe logi"
                yield f"event: log\ndata: {line}\n\n"

                # Parsowanie postępu pobierania
                match = PROGRESS_REGEX.search(line)
                if match:
                    payload = json.dumps({
                        "percent": match.group(1),
                        "size": match.group(2),
                        "speed": match.group(3),
                        "eta": match.group(4)
                    })
                    yield f"event: progress\ndata: {payload}\n\n"
                elif "[Merger]" in line or "Merging" in line:
                    payload = json.dumps({
                        "status": "Scalanie FFmpeg...",
                        "percent": "99"
                    })
                    yield f"event: status\ndata: {payload}\n\n"
                elif "100%" in line:
                    payload = json.dumps({
                        "percent": "100",
                        "size": "Gotowe",
                        "speed": "--",
                        "eta": "00:00"
                    })
                    yield f"event: progress\ndata: {payload}\n\n"

            yield "event: end\ndata: done\n\n"
        finally:
            try:
                # Czyszczenie kontenera po odczytaniu strumienia
                container.remove(force=True)
            except Exception:
                pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")
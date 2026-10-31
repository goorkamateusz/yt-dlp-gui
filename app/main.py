import asyncio
from contextlib import asynccontextmanager
import json
import os
import re
from threading import RLock
import time
from urllib.parse import urlsplit
import uuid
import docker
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import FileResponse, StreamingResponse

# --- Konfiguracja ścieżek i środowiska ---
HOST_DOWNLOAD_PATH = os.getenv("HOST_DOWNLOAD_PATH", "/downloads")
DOWNLOAD_DIR = "/downloads" if os.path.exists("/downloads") else HOST_DOWNLOAD_PATH
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

JOBS_FILE = os.getenv("JOBS_FILE", "/data/jobs.json")
CONTAINER_LABEL = "app=ytdlp-job"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 10))
RETRY_DELAYS_MINUTES = [15, 30, 60]

client = docker.from_env()
file_lock = RLock()

retry_event: asyncio.Event | None = None
main_loop: asyncio.AbstractEventLoop | None = None

PROGRESS_REGEX = re.compile(
    r"\[download\]\s+([0-9.]+)%\s+of\s+(?:~\s*)?(\S+)\s+at\s+(\S+)\s+ETA\s+(\S+)"
)


class JobStatus:
    RUNNING = "running"
    INTERRUPTED = "interrupted"
    COMPLETED = "completed"
    FAILED = "failed"
    ABANDONED = "abandoned"


# --- Zarządzanie stanem i plikiem jobs.json (Atomowy zapis) ---

def load_jobs() -> dict:
    with file_lock:
        if not os.path.exists(JOBS_FILE):
            save_jobs({})
            return {}
        try:
            with open(JOBS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}


def save_jobs(jobs: dict):
    """Zapis atomowy: zapobiega uszkodzeniu pliku JSON w razie nagłego wyłączenia."""
    with file_lock:
        tmp_file = f"{JOBS_FILE}.tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        os.replace(tmp_file, JOBS_FILE)


def update_job(job_id: str, **kwargs):
    with file_lock:
        jobs = load_jobs()
        if job_id in jobs:
            jobs[job_id].update(kwargs)
            save_jobs(jobs)


load_jobs()  # Inicjalizacja pliku przy starcie


# --- Narzędzia pomocnicze (DRY) ---

def notify_retry_worker():
    if main_loop and main_loop.is_running() and retry_event:
        main_loop.call_soon_threadsafe(retry_event.set)


def get_retry_delay_seconds(attempt: int) -> int:
    idx = min(max(attempt - 1, 0), len(RETRY_DELAYS_MINUTES) - 1)
    return RETRY_DELAYS_MINUTES[idx] * 60


def remove_part_files(title: str):
    if not title or not os.path.isdir(DOWNLOAD_DIR):
        return
    for fname in os.listdir(DOWNLOAD_DIR):
        if (fname.endswith(".part") or fname.endswith(".ytdl")) and title in fname:
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, fname))
            except OSError:
                pass


def is_valid_url(url: str) -> bool:
    if not url or len(url) > 2048:
        return False
    try:
        p = urlsplit(url.strip())
        return p.scheme in ("http", "https") and bool(p.netloc) and not re.search(r"\s", url)
    except Exception:
        return False


def run_ytdlp_container(job_id: str, url: str):
    container_name = f"ytdlp-job-{job_id}"
    cmd = [
        "--newline", "--no-colors", "--no-exec", "--no-config-locations",
        "--continue", "--part",
        "--retries", "10", "--fragment-retries", "20", "--extractor-retries", "5",
        "--file-access-retries", "5", "--socket-timeout", "20",
        "--retry-sleep", "http:exp=2:2:20", "--retry-sleep", "fragment:exp=1:1.5:10",
        "--http-chunk-size", "10M", "--concurrent-fragments", "1",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "--print", "before_dl:[title] %(title)s",
        "--no-simulate", "--no-quiet", "--windows-filenames",
        "-o", "/downloads/%(title)s.%(ext)s",
        "--", url,
    ]
    try:
        client.containers.get(container_name).remove(force=True)
    except Exception:
        pass

    return client.containers.run(
        image="jauderho/yt-dlp:latest",
        name=container_name,
        command=cmd,
        labels={"app": "ytdlp-job", "job_id": job_id},
        volumes={HOST_DOWNLOAD_PATH: {"bind": "/downloads", "mode": "rw"}},
        detach=True,
    )


def handle_job_failure(job: dict, error_msg: str):
    """Jednolita logika obsługi niepowodzenia i planowania kolejnej próby."""
    attempts = job.get("attempts", 1)
    is_failed = attempts >= MAX_RETRIES
    new_status = JobStatus.FAILED if is_failed else JobStatus.INTERRUPTED
    cooldown = None if is_failed else time.time() + get_retry_delay_seconds(attempts)

    job["status"] = new_status
    job["next_retry_at"] = cooldown
    job["last_error"] = error_msg
    return is_failed


def restart_job(job: dict):
    """Jednolita logika uruchamiania kolejnej próby zadania."""
    container = run_ytdlp_container(job["job_id"], job["url"])
    job["container_id"] = container.id
    job["container_name"] = container.name
    job["status"] = JobStatus.RUNNING
    job["attempts"] = job.get("attempts", 1) + 1
    job["next_retry_at"] = None
    job["last_error"] = None


# --- Parsowanie wyjścia yt-dlp ---

def parse_ytdlp_output(line: str) -> tuple[str | None, dict | str | None, str | None]:
    """Wyodrębnia zdarzenia SSE i wykryty tytuł z pojedynczej linii logu yt-dlp."""
    if line.startswith("[title] "):
        title = line[8:].strip()
        return "title", {"title": title}, title

    if line.startswith("[download] Destination: "):
        dest = line[23:].strip()
        cand = re.sub(r"\.f\d+$", "", os.path.splitext(os.path.basename(dest))[0])
        return "title", {"title": cand}, cand

    prog = PROGRESS_REGEX.search(line)
    if prog:
        return "progress", {
            "percent": prog.group(1),
            "size": prog.group(2),
            "speed": prog.group(3),
            "eta": prog.group(4),
        }, None

    if "[Merger]" in line or "Merging" in line:
        return "status", {"status": "Scalanie FFmpeg...", "percent": "99"}, None

    if "100%" in line and "ETA" not in line:
        return "progress", {"percent": "100", "size": "Gotowe", "speed": "--", "eta": "00:00"}, None

    return None, None, None


# --- Automat w tle ---

async def auto_retry_worker():
    while True:
        try:
            now = time.time()
            # Odczyt nie blokuje event loopa
            jobs = await asyncio.to_thread(load_jobs)
            interrupted = [
                j for j in jobs.values()
                if j.get("status") == JobStatus.INTERRUPTED and j.get("attempts", 1) < MAX_RETRIES
            ]

            if not interrupted:
                retry_event.clear()
                await retry_event.wait()
                continue

            ready = [j for j in interrupted if now >= (j.get("next_retry_at") or 0)]
            if ready:
                for rj in ready:
                    j_id = rj["job_id"]
                    url = rj["url"]

                    # Uruchomienie kontenera w osobnym wątku (bez blokowania event loopa i POZA file_lock)
                    container = await asyncio.to_thread(run_ytdlp_container, j_id, url)

                    with file_lock:
                        current_jobs = load_jobs()
                        job = current_jobs.get(j_id)
                        if job and job.get("status") == JobStatus.INTERRUPTED:
                            job["container_id"] = container.id
                            job["container_name"] = container.name
                            job["status"] = JobStatus.RUNNING
                            job["attempts"] = job.get("attempts", 1) + 1
                            job["next_retry_at"] = None
                            job["last_error"] = None
                            save_jobs(current_jobs)
                continue

            earliest_retry = min(j.get("next_retry_at") or now for j in interrupted)
            retry_event.clear()
            try:
                await asyncio.wait_for(retry_event.wait(), timeout=max(0.5, earliest_retry - now))
            except asyncio.TimeoutError:
                pass

        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop, retry_event
    main_loop = asyncio.get_running_loop()
    retry_event = asyncio.Event()

    task = asyncio.create_task(auto_retry_worker())
    notify_retry_worker()
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


# --- Endpointy ---

@app.get("/")
def index():
    return FileResponse("index.html")


@app.get("/active-jobs")
def get_active_jobs():
    jobs = load_jobs()
    running_jobs = [j for j in jobs.values() if j.get("status") == JobStatus.RUNNING]

    # Sprawdzanie kontenerów wykonujemy BEZ trzymania file_lock
    finished_updates = {}
    for job in running_jobs:
        try:
            c = client.containers.get(job["container_id"])
            if c.status != "running":
                code = c.attrs.get("State", {}).get("ExitCode", 0)
                finished_updates[job["job_id"]] = code
        except Exception:
            pass

    should_notify = False
    if finished_updates:
        with file_lock:
            jobs = load_jobs()
            for j_id, code in finished_updates.items():
                job = jobs.get(j_id)
                if job and job.get("status") == JobStatus.RUNNING:
                    if code == 0:
                        job["status"] = JobStatus.COMPLETED
                    else:
                        is_failed = handle_job_failure(job, f"Zatrzymano (kod {code})")
                        if not is_failed:
                            should_notify = True
            save_jobs(jobs)

    if should_notify:
        notify_retry_worker()

    return list(jobs.values())


@app.post("/start-download")
def start_download(url: str = Form(...)):
    url = url.strip()
    if not is_valid_url(url):
        raise HTTPException(status_code=400, detail="Niepoprawny format adresu URL.")

    job_id = uuid.uuid4().hex[:6]
    container = run_ytdlp_container(job_id, url)

    with file_lock:
        jobs = load_jobs()
        jobs[job_id] = {
            "job_id": job_id,
            "container_id": container.id,
            "container_name": container.name,
            "url": url,
            "title": "",
            "status": JobStatus.RUNNING,
            "attempts": 1,
            "next_retry_at": None,
            "last_error": None,
        }
        save_jobs(jobs)

    return {"job_id": job_id, "container_id": container.id, "container_name": container.name}


@app.post("/retry-job/{job_id}")
def retry_job(job_id: str):
    with file_lock:
        jobs = load_jobs()
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Nie odnaleziono zadania.")

        if job.get("attempts", 1) >= MAX_RETRIES:
            raise HTTPException(status_code=400, detail=f"Osiągnięto limit {MAX_RETRIES} prób.")

        now = time.time()
        next_retry = job.get("next_retry_at") or 0
        if now < next_retry:
            rem = int(next_retry - now)
            raise HTTPException(
                status_code=429,
                detail=f"Cooldown aktywny. Odczekaj jeszcze {rem // 60}m {rem % 60:02d}s.",
            )
        url = job["url"]

    # Wywołanie Dockera POZA blokadą pliku
    container = run_ytdlp_container(job_id, url)

    with file_lock:
        jobs = load_jobs()
        if job_id in jobs:
            job = jobs[job_id]
            job["container_id"] = container.id
            job["container_name"] = container.name
            job["status"] = JobStatus.RUNNING
            job["attempts"] = job.get("attempts", 1) + 1
            job["next_retry_at"] = None
            job["last_error"] = None
            save_jobs(jobs)

    return {"status": "ok", "job_id": job_id}


@app.post("/abandon-job/{job_id}")
def abandon_job(job_id: str):
    with file_lock:
        jobs = load_jobs()
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Nie odnaleziono zadania.")

        try:
            client.containers.get(job["container_id"]).remove(force=True)
        except Exception:
            pass

        remove_part_files(job.get("title", ""))
        job["status"] = JobStatus.ABANDONED
        job["next_retry_at"] = None
        job["last_error"] = "Zadanie porzucone przez użytkownika"
        save_jobs(jobs)

    notify_retry_worker()
    return {"status": "abandoned"}


def generate_sse_stream(job_id: str):
    jobs = load_jobs()
    job = jobs.get(job_id)
    if not job:
        for j in jobs.values():
            if j.get("container_id") == job_id:
                job, job_id = j, j["job_id"]
                break

    if not job:
        yield "event: end\ndata: not_found\n\n"
        return

    try:
        container = client.containers.get(job["container_id"])
    except Exception:
        yield "event: end\ndata: not_found\n\n"
        return

    buffer = ""
    try:
        for chunk in container.logs(stdout=True, stderr=True, stream=True, follow=True):
            buffer += chunk.decode("utf-8", errors="ignore")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                yield f"event: log\ndata: {line}\n\n"

                evt_type, payload, title = parse_ytdlp_output(line)
                if evt_type:
                    if title and not job.get("title"):
                        update_job(job_id, title=title)
                    yield f"event: {evt_type}\ndata: {json.dumps(payload)}\n\n"

        try:
            container.reload()
            exit_code = container.attrs.get("State", {}).get("ExitCode", 0)
        except Exception:
            exit_code = 0

        with file_lock:
            jobs = load_jobs()
            current_job = jobs.get(job_id, {})
            if exit_code != 0:
                err_msg = f"Błąd pobierania (kod wyjścia {exit_code})"
                is_failed = handle_job_failure(current_job, err_msg)
                save_jobs(jobs)
                if not is_failed:
                    notify_retry_worker()
                yield f"event: fail\ndata: {json.dumps({'error': err_msg})}\n\n"
            else:
                current_job["status"] = JobStatus.COMPLETED
                current_job["next_retry_at"] = None
                save_jobs(jobs)
                yield "event: end\ndata: done\n\n"

    finally:
        try:
            c = client.containers.get(job["container_id"])
            if load_jobs().get(job_id, {}).get("status") == JobStatus.COMPLETED:
                c.remove(force=True)
        except Exception:
            pass


@app.get("/stream/{job_id}")
def stream_logs(job_id: str):
    return StreamingResponse(generate_sse_stream(job_id), media_type="text/event-stream")

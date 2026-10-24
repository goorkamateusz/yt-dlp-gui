import os
import re
import json
import uuid
from urllib.parse import urlsplit
import docker
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import FileResponse, StreamingResponse

app = FastAPI()
client = docker.from_env()

HOST_DOWNLOAD_PATH = os.getenv("HOST_DOWNLOAD_PATH", "/downloads")
CONTAINER_LABEL = "app=ytdlp-job"

JOB_TITLES: dict[str, str] = {}

PROGRESS_REGEX = re.compile(
    r"\[download\]\s+([0-9.]+)%\s+of\s+(?:~\s*)?(\S+)\s+at\s+(\S+)\s+ETA\s+(\S+)"
)


def is_valid_url(url: str) -> bool:
    if not url or len(url) > 2048:
        return False
    try:
        parsed = urlsplit(url.strip())
        return (
            parsed.scheme in ("http", "https")
            and bool(parsed.netloc)
            and not any(c in url for c in (" ", "\t", "\r", "\n"))
        )
    except Exception:
        return False


@app.get("/")
def index():
    return FileResponse("index.html")


@app.get("/active-jobs")
def get_active_jobs():
    containers = client.containers.list(all=True, filters={"label": CONTAINER_LABEL})
    active = []

    for c in containers:
        if c.status == "running":
            active.append(
                {
                    "container_id": c.id,
                    "container_name": c.name,
                    "title": JOB_TITLES.get(c.id, ""),
                }
            )
        else:
            JOB_TITLES.pop(c.id, None)
            try:
                c.remove(force=True)
            except Exception:
                pass

    return active


@app.post("/start-download")
def start_download(url: str = Form(...)):
    url = url.strip()
    if not is_valid_url(url):
        raise HTTPException(
            status_code=400,
            detail="Podaj poprawny adres URL (http:// lub https:// bez spacji).",
        )

    job_id = uuid.uuid4().hex[:6]
    container_name = f"ytdlp-job-{job_id}"

    cmd = [
        "--newline",
        "--no-colors",
        "--no-exec",
        "--no-config-locations",
        "--print",
        "before_dl:[title] %(title)s",
        "--no-simulate",
        "--no-quiet",
        "-o",
        "/downloads/%(title)s.%(ext)s",
        "--",
        url,
    ]

    container = client.containers.run(
        image="jauderho/yt-dlp:latest",
        name=container_name,
        command=cmd,
        labels={"app": "ytdlp-job"},
        volumes={HOST_DOWNLOAD_PATH: {"bind": "/downloads", "mode": "rw"}},
        detach=True,
    )
    return {"container_id": container.id, "container_name": container_name}


def generate_sse_stream(container_id: str):
    try:
        container = client.containers.get(container_id)
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

                if line.startswith("[title] "):
                    title = line[8:].strip()
                    JOB_TITLES[container_id] = title
                    yield f"event: title\ndata: {json.dumps({'title': title})}\n\n"
                elif line.startswith("[download] Destination: "):
                    dest = line[23:].strip()
                    base = os.path.basename(dest)
                    title_cand, _ = os.path.splitext(base)
                    title_cand = re.sub(r"\.f\d+$", "", title_cand)
                    if title_cand and container_id not in JOB_TITLES:
                        JOB_TITLES[container_id] = title_cand
                        yield f"event: title\ndata: {json.dumps({'title': title_cand})}\n\n"

                match = PROGRESS_REGEX.search(line)
                if match:
                    payload = json.dumps(
                        {
                            "percent": match.group(1),
                            "size": match.group(2),
                            "speed": match.group(3),
                            "eta": match.group(4),
                        }
                    )
                    yield f"event: progress\ndata: {payload}\n\n"
                elif "[Merger]" in line or "Merging" in line:
                    payload = json.dumps(
                        {"status": "Scalanie FFmpeg...", "percent": "99"}
                    )
                    yield f"event: status\ndata: {payload}\n\n"
                elif "100%" in line and "ETA" not in line:
                    payload = json.dumps(
                        {
                            "percent": "100",
                            "size": "Gotowe",
                            "speed": "--",
                            "eta": "00:00",
                        }
                    )
                    yield f"event: progress\ndata: {payload}\n\n"

        yield "event: end\ndata: done\n\n"
    finally:
        try:
            c = client.containers.get(container_id)
            if c.status == "exited":
                JOB_TITLES.pop(container_id, None)
                c.remove(force=True)
        except Exception:
            pass


@app.get("/stream/{container_id}")
def stream_logs(container_id: str):
    return StreamingResponse(
        generate_sse_stream(container_id), media_type="text/event-stream"
    )
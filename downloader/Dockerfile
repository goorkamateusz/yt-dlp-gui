FROM alpine:latest
RUN apk add --no-cache python3 py3-pip ffmpeg && \
    python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir yt-dlp
ENTRYPOINT ["/opt/venv/bin/yt-dlp"]
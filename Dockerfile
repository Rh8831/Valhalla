FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash netcat-openbsd gcc default-libmysqlclient-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x /app/wait-for-mysql.sh /app/start.sh

CMD ["/bin/bash", "-lc", "/app/start.sh"]

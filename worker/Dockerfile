FROM python:3.12-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

ENV PORT=8001 DATA_DIR=/data
VOLUME /data
EXPOSE 8001

CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port $PORT"]

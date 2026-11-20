FROM python:3.12-slim

WORKDIR /cli
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY oscli.py .

ENV OS_URL=http://controller:8000
WORKDIR /files
ENTRYPOINT ["python", "/cli/oscli.py"]

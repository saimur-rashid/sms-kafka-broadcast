FROM python:3.11-slim

WORKDIR /app

COPY utils/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dlq/dlq.py ./dlq/
COPY utils ./utils/

WORKDIR /app/dlq

CMD ["python", "dlq.py"]
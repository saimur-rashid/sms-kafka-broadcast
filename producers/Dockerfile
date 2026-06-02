FROM python:3.11-slim

WORKDIR /app

COPY utils/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producers/producer.py ./producers/
COPY utils ./utils/

WORKDIR /app/producers

CMD ["python", "producer.py"]
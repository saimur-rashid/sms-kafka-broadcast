FROM python:3.11-slim

WORKDIR /app

COPY utils/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumers/consumer.py ./consumers/
COPY utils ./utils/

WORKDIR /app/consumers

CMD ["python", "consumer.py"]
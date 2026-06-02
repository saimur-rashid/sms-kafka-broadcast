FROM python:3.11-slim

WORKDIR /app

# Copy from root (parent directory)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command (will be overridden in docker-compose)
CMD ["python", "consumer.py"]
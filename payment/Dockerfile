FROM python:3.11-slim

WORKDIR /app

# Copy and install requirements
COPY utils/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create payment directory for state files
RUN mkdir -p /app/payment

# Copy payment executor and utils
COPY payment/ ./payment/
COPY utils/ ./utils/

WORKDIR /app

# Run the payment data executor
CMD ["python", "payment/payment_data_executor.py"]
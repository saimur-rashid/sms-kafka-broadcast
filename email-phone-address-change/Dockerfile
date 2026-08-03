FROM python:3.11-slim

WORKDIR /app

# Copy and install requirements
COPY utils/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create payment directory for state files
RUN mkdir -p /app/email-phone-address-change

# Copy payment executor and utils
COPY email-phone-address-change/ ./email-phone-address-change/
COPY utils/ ./utils/

WORKDIR /app

# Run the payment data executor
CMD ["python", "email-phone-address-change/email_phone_address_change_executor.py"]
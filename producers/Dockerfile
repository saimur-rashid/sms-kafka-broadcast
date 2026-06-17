FROM python:3.11-slim

WORKDIR /app

# Copy and install requirements
COPY utils/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create payment directory for state files
RUN mkdir -p /app/payment

# Copy producers and utils
COPY producers/ ./producers/
COPY utils/ ./utils/

WORKDIR /app

# Default to SMS producer
ENV PRODUCER_TYPE=sms
# Change this: CMD python producers/producer_${PRODUCER_TYPE}.py
# To this:
CMD ["sh", "-c", "python producers/producer_${PRODUCER_TYPE}.py"]
FROM python:3.11-slim

WORKDIR /app

# Copy and install requirements
COPY utils/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy consumers and utils
COPY consumers/ ./consumers/
COPY utils/ ./utils/

WORKDIR /app

# Default to SMS consumer
ENV CONSUMER_TYPE=sms
# Change this: CMD python consumers/consumer_${CONSUMER_TYPE}.py
# To this:
CMD ["sh", "-c", "python consumers/consumer_${CONSUMER_TYPE}.py"]

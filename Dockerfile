FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV MEDIROUTE_DATA_MODE=csv
ENV MEDIROUTE_PROCESSED_DIR=data/processed
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 10000

CMD streamlit run app/streamlit_app.py \
    --server.port=${PORT:-10000} \
    --server.address=0.0.0.0 \
    --server.headless=true
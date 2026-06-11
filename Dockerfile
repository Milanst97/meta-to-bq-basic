FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY meta_to_bq.py .

ENTRYPOINT ["python", "meta_to_bq.py"]

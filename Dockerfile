FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Build v2 - fix fstring
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "pipeline.py", "--schedule"]]
FROM python:3.13.3
WORKDIR /app
EXPOSE 8000

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .


ENV PYTHONPATH=/app

CMD ["python", "srt/main.py"]
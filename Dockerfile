# Base image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Upgrade pip and install torch from local wheel cache (no fallback to PyPI)
COPY wheelhouse/ /wheelhouse/
RUN pip install --upgrade pip && \
    pip install --no-index --find-links=/wheelhouse torch

# Copy dependency file and install remaining dependencies
COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt

# Copy the rest of the application into container
COPY . .

# Load environment variables
ENV PYTHONUNBUFFERED=1

# FastAPI port
EXPOSE 8000

# Command to run the API
CMD ["uvicorn", "scripts.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
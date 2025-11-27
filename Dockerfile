FROM python:3.10-slim

# Install system deps if you need any (optional)
# RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Set app directory
WORKDIR /app

# Copy requirement file and install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# (Optional but nice) ensure /app is on Python path
ENV PYTHONPATH=/app

# Expose Streamlit port
EXPOSE 8080

# IMPORTANT: use python -m streamlit so imports from src work
CMD ["python", "-m", "streamlit", "run", "src/ui/dashboard.py", "--server.port=8080", "--server.address=0.0.0.0"]

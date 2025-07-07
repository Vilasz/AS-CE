# Dockerfile

# 1. Usar uma imagem base oficial e leve do Python.
FROM python:3.11-slim

# 2. Definir o diretório de trabalho dentro do container.
WORKDIR /app

# 3. Instalar o Java Runtime Environment (JRE).
#    - MODIFICADO: Usando openjdk-17-jre-headless, que é o pacote correto para a imagem base (Debian Bookworm)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# 4. Copiar e instalar as dependências Python.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar todo o resto do código do seu projeto para o diretório de trabalho no container.
COPY . .
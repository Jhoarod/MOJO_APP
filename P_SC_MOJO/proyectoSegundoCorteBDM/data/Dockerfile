# --- Base ---
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# --- Instalar dependencias necesarias ---
RUN apt-get update && \
    apt-get install -y \
        tzdata \
        software-properties-common \
        curl \
        wget \
        gnupg \
        perl \
        build-essential \
        libsqlite3-dev \
        sqlite3 \
        libmariadb-dev \
        default-mysql-client \
        netcat && \
    rm -rf /var/lib/apt/lists/*

# --- Instalar herramientas MongoDB ---
RUN wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add - && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list && \
    apt-get update && \
    apt-get install -y mongodb-mongosh mongodb-org-tools && \
    rm -rf /var/lib/apt/lists/*

# --- Instalar Mojolicious y dependencias Perl ---
RUN curl -L https://cpanmin.us | perl - App::cpanminus && \
    cpanm --notest Mojolicious DBD::SQLite MongoDB JSON File::Slurp Log::Log4perl

# --- Directorio de trabajo ---
WORKDIR /app

# --- Copiar la aplicación ---
COPY mojo_app.pl /app/mojo_app.pl

# Crear script de inicio que espera a que MongoDB esté listo antes de ejecutar Mojolicious
RUN printf '#!/bin/bash\n\
echo " Esperando que MongoDB esté disponible..."\n\
for host in personas_db articulos_db ventas_db; do\n\
  echo "Comprobando $host..."\n\
  until mongosh --quiet --host $host --eval "db.runCommand({ ping: 1 })" >/dev/null 2>&1; do\n\
    echo " Esperando que MongoDB en $host responda..."\n\
    sleep 5\n\
  done\n\
  echo " MongoDB en $host listo."\n\
done\n\
# Espera adicional para asegurar estabilidad\n\
sleep 5\n\
echo " Iniciando aplicación Mojolicious..."\n\
exec perl /app/mojo_app.pl daemon -m production -l http://*:8080\n' > /app/start.sh \
  && chmod +x /app/start.sh

# --- Puerto por defecto ---
EXPOSE 8080

# --- Volumen para datos externos ---
VOLUME /app/data

# --- Comando para iniciar Mojolicious ---
CMD ["/app/start.sh"]

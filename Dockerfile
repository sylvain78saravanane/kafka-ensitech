# Version simplifiée du Dockerfile
FROM openjdk:23-jdk-slim

LABEL description="Exchange Rate Kafka Application"

# Variables d'environnement
ENV SPRING_PROFILES_ACTIVE=docker
ENV JAVA_OPTS="-Xms512m -Xmx1024m"

# Installation de curl pour healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Répertoire de travail
WORKDIR /app

# Copie des fichiers Maven wrapper et pom.xml
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./

# Installation des dépendances
RUN chmod +x mvnw && ./mvnw dependency:go-offline -B

# Copie du code source
COPY src ./src

# Construction de l'application
RUN ./mvnw clean package -DskipTests -B

# Copie du JAR
RUN cp target/*.jar app.jar

# Nettoyage
RUN rm -rf target/ src/ .mvn/ mvnw pom.xml

# Port exposé
EXPOSE 8080

# Healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

# Démarrage de l'application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
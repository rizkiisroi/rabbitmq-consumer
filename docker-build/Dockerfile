# Use Amazon Corretto 8 (Java 1.8) base image
FROM amazoncorretto:8

# Set working directory
WORKDIR /app

# Copy the built JAR into the container
COPY RabbitMqConsumer.jar app.jar

# Copy dependencies
COPY dependency/ ./dependency/

# Run the RabbitConsumer class
ENTRYPOINT ["java", "-cp", "app.jar:dependency/*", "com.local.alam.RabbitConsumer"]

# Run with classpath including dependencies
ENTRYPOINT ["java", "-cp", "app.jar:dependency/*", "com.local.alam.RabbitConsumer"]

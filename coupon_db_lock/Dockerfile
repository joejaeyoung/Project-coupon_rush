# Use the official Gradle image to build the project
FROM gradle:8.5-jdk17 AS build

# Set the working directory
WORKDIR /home/gradle/src

# Copy Gradle wrapper and build files
COPY build.gradle settings.gradle gradlew ./
COPY gradle ./gradle

# Grant execute permission to gradlew
RUN chmod +x ./gradlew

# Download dependencies to leverage Docker cache
RUN ./gradlew dependencies

# Copy the source code
COPY src ./src

# Build the project, skipping tests
RUN ./gradlew build --no-daemon -x test

# Use Eclipse Temurin (openjdk 공식 후속 이미지)
FROM eclipse-temurin:17-jre-jammy

# Set the working directory
WORKDIR /app

# Copy the JAR file from the build stage
COPY --from=build /home/gradle/src/build/libs/coupon-0.0.1-SNAPSHOT.jar app.jar

# Expose the application port
EXPOSE 8080

# Set the entrypoint to run the application
ENTRYPOINT ["java","-jar","/app/app.jar"]
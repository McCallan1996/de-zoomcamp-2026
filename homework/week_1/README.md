# Homework Week 1: Docker & SQL

## Question 1. Understanding Docker images
**Command:** `docker run -it --entrypoint bash python:3.13`
**Answer:** 25.3

## Question 2. Understanding Docker networking and docker-compose
**Answer:** db:5432 (or postgres:5432)
**Reasoning:**
- **Hostname:** Within the Docker network, containers communicate using their service name (`db`) or container name (`postgres`).
- **Port** They communicate internally via the container's internal port (`5432`).
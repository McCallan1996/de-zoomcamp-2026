.PHONY: build up down tumbling_job session_job tips_job stop start

build:
	docker compose build

up:
	docker compose up --build --remove-orphans -d

down:
	docker compose down --remove-orphans

tumbling_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/tumbling_window_job.py --pyFiles /opt/src -d

session_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_window_job.py --pyFiles /opt/src -d

tips_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/tips_window_job.py --pyFiles /opt/src -d

stop:
	docker compose stop

start:
	docker compose start

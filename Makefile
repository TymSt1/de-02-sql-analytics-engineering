.PHONY: up down seed clean psql status

up:
	docker compose up -d
	@echo "Waiting for PostgreSQL to be healthy..."
	@echo "pgAdmin: http://localhost:8080 (admin@admin.com / admin)"

down:
	docker compose down

seed:
	docker cp scripts/seed.sh de02-postgres:/tmp/seed.sh
	docker exec de02-postgres bash /tmp/seed.sh

clean:
	docker compose down -v
	@echo "Volumes removed. Fresh start."

psql:
	docker exec -it de02-postgres psql -U dataeng -d ecommerce

status:
	@docker exec de02-postgres psql -U dataeng -d ecommerce -c "\dt raw.*"

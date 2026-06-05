up:
	docker-compose up -d
down:
	docker compose down -v
ps:
	docker compose ps
tidy:
	go mod tidy
	go fmt ./...
	go vet ./...
lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix
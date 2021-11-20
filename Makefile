localdown:

	docker-compose -f docker-compose.yml down -v

streambuild:

	docker-compose -f docker-compose.yml up --build faust

streamrun:
	docker-compose up faust

kafka:

	docker-compose -f docker-compose.yml up -d zookeeper kafka

log:
	docker-compose logs -f stream



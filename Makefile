airflow-deploy:
	docker-compose up

airflow-kill:
	docker-compose down
	docker image prune -f --all
	docker volume prune -f --all
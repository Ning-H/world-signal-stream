.PHONY: demo infra schema dashboard correlate test

infra:
	docker compose up -d

schema:
	docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/schema.sql
	docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/materialized_views.sql

dashboard:
	. .venv/bin/activate && streamlit run dashboard/app.py --server.port $${STREAMLIT_SERVER_PORT:-8501}

correlate:
	. .venv/bin/activate && python -m flink_jobs.cross_source_correlator --hours $${CORRELATION_HOURS:-24} --min-events $${CORRELATION_MIN_EVENTS:-3}

demo: infra schema
	@echo "OpenSignal dashboard: http://localhost:$${STREAMLIT_SERVER_PORT:-8501}"
	. .venv/bin/activate && streamlit run dashboard/app.py --server.port $${STREAMLIT_SERVER_PORT:-8501}

test:
	. .venv/bin/activate && pytest -q

# Build context is the repo root (see docker-compose.yml) so the image can copy
# api/scoring.py - the dashboard derives its rule SQL from the same RULES dict
# the notebook and the FastAPI scorer use, instead of a third copy of the
# thresholds that no test guards.
FROM python:3.12-slim

WORKDIR /app

COPY dashboard/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY dashboard/app.py dashboard/queries.py /app/
COPY api/scoring.py /app/scoring.py

RUN useradd --create-home --uid 10002 dash && chown -R dash:dash /app
USER dash
ENV HOME=/home/dash
COPY --chown=dash:dash dashboard/config.toml /home/dash/.streamlit/config.toml

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]

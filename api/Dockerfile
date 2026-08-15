# python:3.12, not 3.10: scikit-learn 1.9.0 - the version that pickled the
# @champion model - requires Python >=3.11, and 3.12 is what the Databricks
# runtime that created the artifact used, so the unpickle happens on a
# matching interpreter.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Non-root: nothing here needs write access at runtime beyond mlflow's
# artifact cache, which lands in the home dir below.
RUN useradd --create-home --uid 10001 api && chown -R api:api /app
USER api
ENV HOME=/home/api

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

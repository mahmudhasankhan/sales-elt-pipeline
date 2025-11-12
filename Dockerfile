FROM astrocrpublic.azurecr.io/runtime:3.1-4

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

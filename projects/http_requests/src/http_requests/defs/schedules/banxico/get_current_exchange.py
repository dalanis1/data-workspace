import dagster as dg

exchanges_daily_schedule = dg.ScheduleDefinition(
    name="exchanges_daily_schedule",
    job_name="get_current_exchange_job",
    cron_schedule="0 5 * * *",
    execution_timezone="America/Mexico_City"
)

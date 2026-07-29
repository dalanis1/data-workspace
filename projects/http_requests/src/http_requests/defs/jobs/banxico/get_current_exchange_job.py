from dagster import define_asset_job, AssetSelection

get_current_exchange_job = define_asset_job(
    name="get_current_exchange_job",
    selection = AssetSelection.groups("banxico/series")
)

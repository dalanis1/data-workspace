from typing import List, Dict, Any
from datetime import datetime, date
import dagster as dg
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from utils.sources.warehouse import WarehouseResource
from utils.models.warehouse.exchange_rates.banxico import BanxicoModel
from http_requests.defs.resources.banxico_api_resource import BanxicoAPIResource

series = ["SF60653"]

def get_next_fetch_date(session: Session) -> List[Dict[str, Any]]:
    """ Get start date by serie last exchange date. """
    stmt_max_date = select(
        BanxicoModel.serie,
        (func.max(BanxicoModel.gdatu) + 1).label("gdatu")
    ).where(BanxicoModel.serie.in_(series)).group_by(BanxicoModel.serie)
    return session.execute(stmt_max_date).all()

def get_from_date_series(gdatu: date) -> List[Dict[str, Any]]:
    """ Get start date by serie last exchange date. """
    return gdatu if gdatu is not None else date(2000, 1, 1)

def transform_banxico_data(raw_data: List[Dict[str, Any]], custom_data: Dict[str, Any]):
    """ Transform Banxico data to match the BanxicoModel schema. """
    fetched_at = datetime.now()
    for item in raw_data:
        item["gdatu"] = datetime.strptime(item.pop("fecha"), "%d/%m/%Y").date()
        item["ukurs"] = float(item.pop("dato"))
        item["fetched_at"] = fetched_at
        item.update(custom_data)
    return raw_data

def insert_banxico_data(session: Session, data: List[Dict[str, Any]]):
    """ Insert Banxico data into the database. """
    if not data:
        return 0
    session.bulk_insert_mappings(BanxicoModel, data)
    session.commit()
    return len(data)

@dg.asset(
    group_name= f"banxico/series",
)
def get_current_exchange(
    context: dg.AssetExecutionContext, 
    whse_resource: WarehouseResource, 
    banxico_resource: BanxicoAPIResource
):
    """ Fetches the current exchange rates for the specified series from Banxico and
        inserts them into the database.
    """
    with whse_resource.get_session() as session:
        try:
            start_dates = get_next_fetch_date(session)
            for row in start_dates:
                from_date = get_from_date_series(row.gdatu)
                context.log.info(f"Fetching data for series {row.serie} starting from {from_date}.")
                series_raw_data = banxico_resource.get_series(row.serie, from_date, date.today())
                raw_data = series_raw_data.get(row.serie, [])
                if not raw_data:
                    context.log.info(f"No new data found for series {row.serie}.")
                    continue
                transformed_data = transform_banxico_data(raw_data, {
                    "kurst": "M",
                    "fcurr": "USD",
                    "tcurr": "MXN",
                    "ffact": 1,
                    "tfact": 1,
                    "serie": row.serie
                })
                inserted_rows = insert_banxico_data(session, transformed_data)
                if inserted_rows > 0:
                    context.log.info(f"Successfully inserted {inserted_rows} rows for series {row.serie}.")
                else:
                    context.log.info(f"No new data to insert for series {row.serie}.")
        except Exception as e:
            session.rollback()
            context.log.error(f"Asset failed: {str(e)}")
            raise e

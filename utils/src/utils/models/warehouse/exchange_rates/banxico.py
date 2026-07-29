from datetime import datetime, date
from sqlalchemy import Date, VARCHAR, Integer, Numeric, Text, TIMESTAMP, func
from sqlalchemy import orm
from utils.database.base import WarehouseBase

class BanxicoModel(WarehouseBase):
    __table_args__ = {"schema": "exchange_rates"}
    __tablename__ = "banxico_source"

    kurst:orm.Mapped[str] = orm.mapped_column(VARCHAR(length = 4), primary_key = True, nullable = False)
    fcurr:orm.Mapped[str] = orm.mapped_column(VARCHAR(length = 5), primary_key = True, nullable = False)
    tcurr:orm.Mapped[str] = orm.mapped_column(VARCHAR(length = 5), primary_key = True, nullable = False)
    gdatu:orm.Mapped[date] = orm.mapped_column(Date, primary_key = True, nullable = False)
    ukurs:orm.Mapped[float] = orm.mapped_column(Numeric(18, 6), nullable = False)
    ffact:orm.Mapped[int] = orm.mapped_column(Integer, nullable = False)
    tfact:orm.Mapped[int] = orm.mapped_column(Integer, nullable = False)
    serie:orm.Mapped[str] = orm.mapped_column(Text, nullable = False)
    fetched_at: orm.Mapped[datetime] = orm.mapped_column(
        TIMESTAMP, 
        nullable=False,
        server_default=func.now()
    )

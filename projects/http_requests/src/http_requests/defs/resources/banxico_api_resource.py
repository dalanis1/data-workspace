from typing import Literal
from datetime import date
import httpx
import dagster as dg

class BanxicoAPIResource(dg.ConfigurableResource):
    token:str
    accept_data: str = "application/json"
    metadata: Literal["datos", "datos/oportuno"] = "datos"
    base_url: str = "https://www.banxico.org.mx/SieAPIRest/service/v1"

    def get_series(self, serie_id: str, date_start:date = None, date_end: date = None):
        """ Fetch series data from Banxico API. """
        url = f"{self.base_url}/series/{serie_id}/{self.metadata}"
        if (
            self.metadata == "datos" and
            isinstance(date_start, date) and
            isinstance(date_end, date)
        ):
            url += f"/{self.__parse_format(date_start)}/{self.__parse_format(date_end)}"
        resp = httpx.get(url, headers = self.__get_headers())
        resp.raise_for_status()
        return {
            serie["idSerie"]: serie["datos"]
            for serie in resp.json()["bmx"]["series"]
        }

    def __get_headers(self):
        return { "Bmx-Token": self.token, "Accept": self.accept_data }

    @staticmethod
    def __parse_format(date_current: date):
        return date_current.strftime("%Y-%m-%d")

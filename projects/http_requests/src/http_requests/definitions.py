from pathlib import Path
import dagster as dg
from utils.sources.warehouse import WarehouseResource
from http_requests.defs.resources.banxico_api_resource import BanxicoAPIResource

@dg.definitions
def defs():
    def_automate = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    manual_def = dg.Definitions(
        resources = {
            "whse_resource": WarehouseResource(),
            "banxico_resource": BanxicoAPIResource(
                token = dg.EnvVar("BANXICO_API_KEY").get_value("")
            )
        }
    )
    return dg.Definitions.merge(def_automate, manual_def)

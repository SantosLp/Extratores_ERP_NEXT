from common import Common
from typing import Dict
api = Common()


def normalize_product(product: Dict, countries_set: set) -> Dict:
    code = str(product.get("id")).strip()
    if not code: return {}
    status = (product.get("status") or "").strip().lower()
    disabled = 0
    group = product.get("grupo")
    uom = product.get("unidadeMedida")

    return {
        "item_code": code,
        "item_name": (product.get("nomeProduto") or "").strip()[:140],
        "item_group": group,
        "stock_uom": uom,
        "disabled": disabled
    }

todos_produtos = api.get_all_ongsys("produtos")

## faça um apply para todos os produtos
produtos_normalizados = [normalize_product(produto, set()) for produto in todos_produtos]



##TODO FALTA FAZER O UPDATE 
for produto in produtos_normalizados:
    resp = api.erp_request("GET", f"api/resource/Item/{produto['item_code']}")
    if resp.status_code == 404:
        resp = api.erp_request("POST","Item", produto["item_code"], produto)
    print(resp.status_code)







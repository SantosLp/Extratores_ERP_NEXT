
from common import Common
import json 
api = Common()
todos_produtos = api.get_all_ongsys("produtos")
# Pegar todos os grupos distintos
grupos_distintos = set()
for produto in todos_produtos:
    grupo = produto.get("grupo")
    if grupo:
        grupos_distintos.add(grupo)

grupos_erp = api.erp_request("GET", "Item Group", params={"fields": '["item_group_name"]', "limit_page_length": 1000})
grupos_erp_json =  json.loads(grupos_erp.text)
grupos_erp_nomes = {grupo["item_group_name"] for grupo in grupos_erp_json.get("data", [])}

grupos_para_criar = grupos_distintos - grupos_erp_nomes
for grupo in grupos_para_criar:
    payload_grupo = {
        "item_group_name": grupo,
        "is_group": 1,
    }
    resp = api.erp_request("POST","Item Group", grupo, payload_grupo)

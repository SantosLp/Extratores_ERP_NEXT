import os
import json
import requests
import time
from typing import List, Dict, Optional
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# URLs e credenciais do ERPNext e ONGSYS
ERP_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")

ONGSYS_URL = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

# Cabeçalhos para ERPNext
HEADERS_ERP = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Content-Type": "application/json"
}

# Configurações de sincronização
SYNC_ACTIVE_ONLY = False
DISABLE_INACTIVE = True
DEFAULT_GROUP = "Todos os Grupos de Itens"
DEFAULT_UOM = "Unidade"
MAX_WAIT_CREATE = 60  # Espera máxima para confirmação de criação
VERIFY_INTERVAL = 3  # Intervalo para verificar a criação

# Mapeamento de países
COUNTRY_MAP = {
    "nacional": "Brazil", "brasil": "Brazil", "brasileiro": "Brazil",
    "importado": None, "": None, None: None,
}

# Mapeamento de campos ONGSYS -> ERPNext
FIELD_MAP = {
    "nomeProduto": ("item_name", lambda v: (v or "").strip()[:140] if v else None),
    "descricaoProduto": ("description", lambda v: (v or "").strip()[:1000] if v else None),
    "fabricante": ("manufacturer", lambda v: (v or "").strip() or None),
    "valorCustoBase": ("standard_rate", lambda v: float(v) if v not in (None, "", "null") else 0.0),
    "contaPadraoPlanoFinanceiro": ("default_expense_account", lambda v: (v or "").strip() or None),
}

# Função genérica para fazer requisições ao ERPNext
def erp_request(method: str, path: str, params: Dict = None, payload: Dict = None, timeout=60) -> requests.Response:
    url = f"{ERP_URL.rstrip('/')}/{path.lstrip('/')}"
    if not path.startswith("api/resource/"):
        url = f"{ERP_URL.rstrip('/')}/api/resource/{path.lstrip('/')}"
    try:
        return requests.request(method, url, headers=HEADERS_ERP, params=params, json=payload, timeout=timeout)
    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão com o ERPNext: {e}")
        response = requests.Response()
        response.status_code = 503
        return response

# Verifica se o documento já existe no ERPNext
def doc_exists(doctype: str, docname: str, filters: List = None) -> bool:
    path = f"api/resource/{doctype}"
    if filters:
        params = {"filters": json.dumps(filters), "limit_page_length": 1}
        r = erp_request("GET", path, params=params)
        return r.status_code == 200 and bool(r.json().get("data"))
    else:
        path = f"api/resource/{doctype}/{docname}"
        r = erp_request("GET", path)
        return r.status_code == 200

# Garante que o documento existe no ERPNext (cria se necessário)
def ensure_doc_with_verification(doctype: str, docname: str, payload: Dict, filters: List) -> bool:
    if doc_exists(doctype, docname, filters): 
        return True
    print(f"Criando {doctype}: '{docname}'")
    create_r = erp_request("POST", f"api/resource/{doctype}", payload=payload)
    if create_r.status_code not in [200, 201, 409]: 
        print(f"Falha ao criar {doctype}: {create_r.status_code} {create_r.text}")
        return False

    print(f"Aguardando confirmação de '{docname}'...")
    t0 = time.time()
    while time.time() - t0 < MAX_WAIT_CREATE:
        time.sleep(VERIFY_INTERVAL)
        if doc_exists(doctype, docname, filters):
            print("Confirmado!")
            return True
    print(f"Timeout! {doctype} '{docname}' não apareceu em {MAX_WAIT_CREATE}s.")
    return False

# Função para garantir as dependências base no ERPNext
def ensure_base_dependencies():
    print("Garantindo dependências base...")
    if not ensure_doc_with_verification("Item Group", DEFAULT_GROUP, {"item_group_name": DEFAULT_GROUP}, [["item_group_name", "=", DEFAULT_GROUP]]):
        print("Erro crítico! Falha ao garantir grupo de itens.")
        exit(1)
    if not ensure_doc_with_verification("UOM", DEFAULT_UOM, {"uom_name": DEFAULT_UOM}, [["uom_name", "=", DEFAULT_UOM]]):
        print("Erro crítico! Falha ao garantir UOM.")
        exit(1)
    print("Dependências base OK.")

# Função para obter países do ERPNext
def get_countries() -> set:
    countries = set()
    try:
        resp = erp_request("GET", "api/resource/Country", params={"fields": '["name"]', "limit_page_length": 999})
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            countries.update(c["name"] for c in data if c.get("name"))
        else:
            print(f"Erro ao buscar países: {resp.status_code}")
    except Exception as e:
        print(f"Erro ao buscar países: {e}")
    return countries

# Mapeia o país de origem para o formato do ERPNext
def map_country(origem_val: Optional[str], countries_set: set) -> Optional[str]:
    if origem_val is None: return None
    key = str(origem_val).strip().lower()
    mapped = COUNTRY_MAP.get(key)
    if mapped is None and key:
        candidate = origem_val.strip()
        if candidate in countries_set: return candidate
        candidate2 = candidate.capitalize()
        if candidate2 in countries_set: return candidate2
    return mapped if mapped in countries_set or mapped is None else None

# Função para extrair produtos da API ONGSYS
def extract_products_from_api() -> Tuple[List[Dict], int]:
    print("Extraindo produtos da API ONGSYS...")
    page = 1
    all_products = []
    while True:
        print(f"Buscando página {page}...")
        try:
            resp = requests.get(f"{ONGSYS_URL.rstrip('/')}/produtos", auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params={"pageNumber": page}, timeout=30)
            if resp.status_code == 422: 
                print("Fim dos dados (422).")
                break
            resp.raise_for_status()
            data = resp.json()
            total_records = int(data.get("totalRecords", 0))
            products = data.get("data", [])
            if not products:
                print("Página vazia.")
                break
            all_products.extend(products)
            print(f"Página {page}: {len(products)} itens extraídos.")
            if len(all_products) >= total_records:
                break
            page += 1
        except Exception as e:
            print(f"Erro na extração: {e}")
            break
    return all_products, len(all_products)

# Função para normalizar os dados do produto
def normalize_product(product: Dict, countries_set: set) -> Dict:
    code = str(product.get("id")).strip()
    if not code: return {}
    status = (product.get("status") or "").strip().lower()
    disabled = 1 if (DISABLE_INACTIVE and status == "inativo") else 0
    group = (product.get("grupo") or "").strip() or DEFAULT_GROUP
    uom = (product.get("unidadeMedida") or "").strip() or DEFAULT_UOM
    country = map_country(product.get("origem"), countries_set)

    return {
        "item_code": code,
        "item_name": (product.get("nomeProduto") or "").strip()[:140],
        "item_group": group,
        "stock_uom": uom,
        "disabled": disabled,
        "country_of_origin": country if country else None
    }

# Função para comparar produtos
def compare_fields(new: Dict, old: Dict) -> Dict:
    changes = {}
    for field in ["item_name", "item_group", "stock_uom", "description", "manufacturer", "standard_rate", "country_of_origin", "disabled", "default_expense_account"]:
        new_val = new.get(field)
        old_val = old.get(field)
        if new_val != old_val:
            changes[field] = new_val
    return changes

# Função para criar produto no ERPNext
def create_item(payload: Dict):
    if not ensure_doc_with_verification("Item", payload["item_code"], payload, []):
        print(f"Falha ao criar o item {payload['item_code']}")
        return False
    return True

# Função para atualizar produto no ERPNext
def update_item(item_code: str, changes: Dict):
    response = erp_request("PUT", f"api/resource/Item/{item_code}", payload=changes)
    if response.status_code != 200:
        print(f"Falha ao atualizar item {item_code}: {response.status_code} {response.text}")
        return False
    return True

# Função principal de sincronização
def sync():
    ensure_base_dependencies()
    countries_set = get_countries()
    products, total_declared = extract_products_from_api()

    if not products:
        print("Nenhum produto para processar.")
        return

    created, updated, unchanged, skipped, errors = 0, 0, 0, 0, 0
    for i, product in enumerate(products):
        print(f"Processando produto {i + 1}/{len(products)}...")
        normalized_product = normalize_product(product, countries_set)

        if not normalized_product.get("item_code"):
            errors += 1
            continue

        if normalized_product.pop("disabled", False):
            skipped += 1
            continue

        existing_item = erp_request("GET", f"api/resource/Item/{normalized_product['item_code']}")
        if existing_item.status_code == 200:
            changes = compare_fields(normalized_product, existing_item.json().get("data", {}))
            if changes:
                updated += 1
                update_item(normalized_product["item_code"], changes)
            else:
                unchanged += 1
        else:
            created += 1
            create_item(normalized_product)

    print(f"Resumo: Criados: {created}, Atualizados: {updated}, Iguais: {unchanged}, Pulados: {skipped}, Erros: {errors}")

# Execução
if __name__ == "__main__":
    sync()

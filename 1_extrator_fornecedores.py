import os
import json
import requests
from typing import List, Dict
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

ERP_URL = os.getenv("ERP_URL")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

ONGSYS_URL = os.getenv("ONGSYS_URL")
ONGSYS_USER = os.getenv("ONGSYS_USER")
ONGSYS_PASS = os.getenv("ONGSYS_PASS")

HEADERS = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Content-Type": "application/json"
}

# Definir valores padrões
SYNC_ACTIVE_ONLY = False
DISABLE_INACTIVE = True
DEFAULT_GROUP = "Local"
DEFAULT_TYPE = "Company"

# Função para checar variáveis de ambiente
def check_env():
    missing = []
    if not all([ERP_URL, API_KEY, API_SECRET]):
        missing.append("ERP_URL/API_KEY/API_SECRET")
    if not all([ONGSYS_URL, ONGSYS_USER, ONGSYS_PASS]):
        missing.append("ONGSYS_URL/ONGSYS_USER/ONGSYS_PASS")
    if missing:
        print(f"ERROR: Missing env variables: {', '.join(missing)}")
        exit(1)

# Funções para chamadas HTTP ao ERP
def erp_get(path: str, params: Dict = None):
    url = f"{ERP_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_post(path: str, payload: Dict):
    url = f"{ERP_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_put(path: str, payload: Dict):
    url = f"{ERP_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.put(url, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# Criar grupo de fornecedor no ERP, se não existir
def create_supplier_group(group_name: str):
    group_name = group_name or DEFAULT_GROUP
    try:
        erp_get(f"api/resource/Supplier Group/{group_name}")
        return
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 404:
            raise
    payload = {"supplier_group_name": group_name, "parent_supplier_group": "All Supplier Groups"}
    erp_post("api/resource/Supplier Group", payload)
    print(f" -> Supplier Group '{group_name}' created in ERP.")

# Coletar fornecedores do ONGSYS
def get_suppliers_ongsys() -> List[Dict]:
    print("--- Step 1: Getting Suppliers from ONGSYS ---")
    page = 1
    all_suppliers = []
    while True:
        print(f"Getting page {page}...")
        try:
            params = {"pageNumber": page}
            resp = requests.get(f"{ONGSYS_URL.rstrip('/')}/fornecedores", auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("data"):
                print("No more data.")
                break
            all_suppliers.extend(data.get("data", []))
            page += 1
        except Exception as e:
            print(f"ERROR on page {page}: {e}")
            break
    print(f"\n-> Extracted {len(all_suppliers)} suppliers.")
    return all_suppliers

# Normalizar dados do fornecedor
def normalize_supplier(src: Dict) -> Dict:
    name = src.get("nomeEmpresa") or src.get("razaoSocial")
    if not name: return {}
    tax_id = src.get("documento") or src.get("cnpj") or src.get("cpf")
    status = (src.get("status") or "").strip().lower()
    disabled = 1 if (DISABLE_INACTIVE and status == "inativo") else 0
    if SYNC_ACTIVE_ONLY and status == "inativo":
        return {"_skip_due_to_status": True, "supplier_name": name}
    return {
        "supplier_name": name[:140],
        "supplier_group": DEFAULT_GROUP,
        "supplier_type": DEFAULT_TYPE,
        "tax_id": tax_id,
        "disabled": disabled
    }

# Verificar fornecedor pelo CNPJ/CPF ou nome
def find_supplier_by_tax_id(tax_id: str) -> Dict:
    if not tax_id: return None
    params = {"filters": json.dumps([["tax_id", "=", tax_id]])}
    data = erp_get("api/resource/Supplier", params=params)
    suppliers = data.get("data", [])
    return suppliers[0] if suppliers else None

def find_supplier_by_name(name: str) -> Dict:
    if not name: return None
    params = {"filters": json.dumps([["supplier_name", "=", name]])}
    data = erp_get("api/resource/Supplier", params=params)
    suppliers = data.get("data", [])
    return suppliers[0] if suppliers else None

# Comparar dados de fornecedor
def compare_supplier_fields(new: Dict, old: Dict) -> Dict:
    changes = {}
    fields = ["supplier_name", "supplier_group", "supplier_type", "tax_id", "disabled"]
    for field in fields:
        if str(new.get(field, "")).strip() != str(old.get(field, "")).strip():
            changes[field] = new.get(field)
    return changes

# Funções para criar ou atualizar fornecedor no ERP
def create_supplier(payload: Dict):
    create_supplier_group(payload["supplier_group"])
    erp_post("api/resource/Supplier", payload)

def update_supplier(docname: str, changes: Dict):
    erp_put(f"api/resource/Supplier/{docname}", changes)

# Função de sincronização
def sync_suppliers():
    print("\n===== Starting Supplier Sync =====")
    create_supplier_group(DEFAULT_GROUP)
    suppliers_ongsys = get_suppliers_ongsys()
    if not suppliers_ongsys:
        print("Nothing to sync.")
        return
    created, updated, unchanged, skipped, errors = 0, 0, 0, 0, 0
    for supplier in suppliers_ongsys:
        target = normalize_supplier(supplier)
        if not target: continue
        if target.pop("_skip_due_to_status", False):
            print(f"[SKIPPED] Supplier '{target.get('supplier_name')}' is inactive.")
            skipped += 1
            continue
        name = target["supplier_name"]
        tax_id = target.get("tax_id")
        try:
            current = find_supplier_by_tax_id(tax_id) or find_supplier_by_name(name)
            if not current:
                print(f"[CREATE] {name} (CNPJ/CPF: {tax_id or 'N/A'})")
                create_supplier(target)
                created += 1
                continue
            changes = compare_supplier_fields(target, current)
            if not changes:
                unchanged += 1
                continue
            print(f"[UPDATE] {name} -> {list(changes.keys())}")
            update_supplier(current["name"], changes)
            updated += 1
        except Exception as e:
            print(f"ERROR processing '{name}': {e}")
            errors += 1
    print(f"\n===== SUMMARY =====")
    print(f"Suppliers from ONGSYS: {len(suppliers_ongsys)}")
    print(f"Created: {created} | Updated: {updated} | Unchanged: {unchanged} | Skipped: {skipped} | Errors: {errors}")
    print("\n===== END =====")

# Execução
if __name__ == "__main__":
    check_env()
    sync_suppliers()

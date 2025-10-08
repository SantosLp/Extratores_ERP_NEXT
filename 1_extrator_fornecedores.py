import os
import json
import requests
from typing import List, Dict, Tuple, Optional
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# ========== CONFIG ========== #
load_dotenv()

ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")

ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

HEADERS_ERP = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Content-Type": "application/json"
}

# === Comportamento ===
SYNC_ONLY_ACTIVE = False
DISABLE_INACTIVE = True
DEFAULT_SUPPLIER_GROUP = "Local"
DEFAULT_SUPPLIER_TYPE = "Company"

# ========== Validação de ambiente ========== #
def _assert_env():
    faltam = []
    if not all([ERPNext_URL, API_KEY, API_SECRET]):
        faltam.append("ERPNext_URL/API_KEY/API_SECRET")
    if not all([ONGSYS_URL_BASE, ONGSYS_USER, ONGSYS_PASS]):
        faltam.append("ONGSYS_URL_BASE/ONGSYS_USERNAME/ONGSYS_PASSWORD")
    if faltam:
        print(f"!!! ERRO CRÍTICO: Variáveis ausentes no arquivo .env: {', '.join(faltam)}")
        exit(1)

# ========== Helpers HTTP ERPNext ========== #
def erp_get(path: str, params: Dict = None):
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, headers=HEADERS_ERP, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_post(path: str, payload: Dict):
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.post(url, headers=HEADERS_ERP, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_put(path: str, payload: Dict):
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.put(url, headers=HEADERS_ERP, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# ========== Garantir Dependências no ERPNext ========== #
def ensure_supplier_group(name: str):
    name = name or DEFAULT_SUPPLIER_GROUP
    try:
        erp_get(f"api/resource/Supplier Group/{name}")
        return
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 404:
            raise
    payload = {"supplier_group_name": name, "parent_supplier_group": "All Supplier Groups"}
    erp_post("api/resource/Supplier Group", payload)
    print(f" -> Grupo de Fornecedor '{name}' criado no ERPNext.")

# ========== Funções para Coletar e Normalizar Dados do ONGSYS ========== #
def extrair_fornecedores_ongsys() -> Tuple[List[Dict], int]:
    print("--- ETAPA 1: Extraindo Fornecedores do ONGSYS ---")
    pagina_atual = 1
    todos: List[Dict] = []
    total_records_api = None
    endpoint = f"{ONGSYS_URL_BASE.rstrip('/')}/fornecedores"
    vistos = set()
    while True:
        print(f"Buscando página {pagina_atual}...")
        try:
            params = {"pageNumber": pagina_atual}
            resp = requests.get(endpoint, auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params=params, timeout=30)
            if resp.status_code == 422:
                print("-> Fim dos dados (422).")
                break
            resp.raise_for_status()
            dados = resp.json()
            if 'errors' in dados and isinstance(dados['errors'], list) and dados['errors']:
                if "Não existe registros de fornecedores" in dados['errors'][0].get('message', ''):
                    print("-> Fim dos dados (mensagem da API).")
                    break
            if total_records_api is None:
                total_records_api = int(dados.get("totalRecords") or 0)
            page = dados.get("data", []) or []
            if not page:
                print("-> Página vazia. Fim da extração.")
                break
            for fornecedor in page:
                fid = fornecedor.get("id") or fornecedor.get("documento") or fornecedor.get("nomeEmpresa")
                if fid in vistos: continue
                vistos.add(fid)
                todos.append(fornecedor)
            print(f" -> {len(page)} nesta página | Coletados (únicos): {len(todos)}" + (f" de {total_records_api}" if total_records_api else ""))
            if total_records_api and len(todos) >= total_records_api:
                break
            pagina_atual += 1
        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}")
            break
    print(f"\n-> Extração concluída: {len(todos)} itens (declarados: {total_records_api}).")
    return todos, total_records_api or len(todos)

def normalizar_fornecedor_ongsys(src: Dict) -> Dict:
    nome = src.get("nomeEmpresa") or src.get("razaoSocial")
    if not nome: return {}
    tax_id = src.get("documento") or src.get("cnpj") or src.get("cpf")
    status = (src.get("status") or "").strip().lower()
    disabled = 1 if (DISABLE_INACTIVE and status == "inativo") else 0
    if SYNC_ONLY_ACTIVE and status == "inativo":
        return {"_ignorar_por_status": True, "supplier_name": nome}
    return {
        "supplier_name": nome[:140],
        "supplier_group": DEFAULT_SUPPLIER_GROUP,
        "supplier_type": DEFAULT_SUPPLIER_TYPE,
        "tax_id": tax_id,
        "disabled": disabled
    }

# ========== Funções para Verificar Fornecedor no ERPNext ========== #
def find_supplier_by_tax_id(tax_id: str) -> Optional[Dict]:
    if not tax_id: return None
    params = {"filters": json.dumps([["tax_id", "=", tax_id]])}
    data = erp_get("api/resource/Supplier", params=params)
    # <<<--- CORREÇÃO APLICADA AQUI ---<<<
    suppliers = data.get("data", [])
    return suppliers[0] if suppliers else None

def find_supplier_by_name(name: str) -> Optional[Dict]:
    if not name: return None
    params = {"filters": json.dumps([["supplier_name", "=", name]])}
    data = erp_get("api/resource/Supplier", params=params)
    # <<<--- CORREÇÃO APLICADA AQUI ---<<<
    suppliers = data.get("data", [])
    return suppliers[0] if suppliers else None

# ========== Funções de Comparação e Carga ========== #
def diff_campos_supplier(alvo: Dict, atual: Dict) -> Dict:
    mudancas = {}
    campos_para_comparar = ["supplier_name", "supplier_group", "supplier_type", "tax_id", "disabled"]
    for campo in campos_para_comparar:
        a = str(alvo.get(campo, "")).strip()
        b = str(atual.get(campo, "")).strip()
        if a != b:
            mudancas[campo] = a
    return mudancas

def criar_supplier(payload: Dict):
    ensure_supplier_group(payload["supplier_group"])
    erp_post("api/resource/Supplier", payload)

def atualizar_supplier(docname: str, mudancas: Dict):
    erp_put(f"api/resource/Supplier/{docname}", mudancas)

# ========== Função Orquestradora de Sincronização ========== #
def sincronizar_fornecedores():
    print("\n====== INICIANDO SINCRONIZAÇÃO DE FORNECEDORES ======")
    ensure_supplier_group(DEFAULT_SUPPLIER_GROUP)
    fornecedores_ongsys, total_decl = extrair_fornecedores_ongsys()
    if not fornecedores_ongsys:
        print("Nada para processar.")
        return
    criar_ct = atualizar_ct = iguais_ct = pulados_ct = falhas_ct = 0
    for f in fornecedores_ongsys:
        alvo = normalizar_fornecedor_ongsys(f)
        if not alvo: continue
        if alvo.pop("_ignorar_por_status", False):
            print(f"[PULADO] Fornecedor '{alvo.get('supplier_name')}' está inativo.")
            pulados_ct += 1
            continue
        name = alvo["supplier_name"]
        tax_id = alvo.get("tax_id")
        try:
            atual = find_supplier_by_tax_id(tax_id) or find_supplier_by_name(name)
            if not atual:
                print(f"[CRIAR] {name} (CNPJ/CPF: {tax_id or 'N/A'})")
                criar_supplier(alvo)
                criar_ct += 1
                continue
            mudancas = diff_campos_supplier(alvo, atual)
            if not mudancas:
                iguais_ct += 1
                continue
            print(f"[ATUALIZAR] {name} -> {list(mudancas.keys())}")
            atualizar_supplier(atual["name"], mudancas)
            atualizar_ct += 1
        except Exception as e:
            print(f" -> FALHA GERAL no processamento de '{name}': {e}")
            if hasattr(e, 'response'): print(f" -> Resposta do Servidor: {e.response.text}")
            falhas_ct += 1
    print(f"\n====== RESUMO ======")
    print(f"Declarados pelo ONGSYS: {total_decl}")
    print(f"Criados: {criar_ct} | Atualizados: {atualizar_ct} | Iguais: {iguais_ct} | Pulados por status: {pulados_ct} | Falhas: {falhas_ct}")
    print("\n====== FIM ======")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    _assert_env()
    sincronizar_fornecedores()
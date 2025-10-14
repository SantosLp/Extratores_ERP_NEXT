import os
import json
import requests
from typing import List, Dict, Tuple, Optional
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# ========== CONFIG ========== #
load_dotenv()

ERPNext_URL   = os.getenv("ERPNext_URL")
API_KEY       = os.getenv("ERPNext_API_KEY")
API_SECRET    = os.getenv("ERPNext_API_SECRET")

ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER     = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS     = os.getenv("ONGSYS_PASSWORD")

HEADERS_ERPNext = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Content-Type": "application/json"
}

# === Comportamento ===
SYNC_ONLY_ACTIVE = False
DISABLE_INACTIVE = True
# <<<--- AJUSTE APLICADO AQUI ---<<<
DEFAULT_ITEM_GROUP = "SEM GRUPO" # Usado APENAS se o grupo do ONGSYS for vazio
DEFAULT_UOM = "Unidade"

ORIGEM_COUNTRY_MAP = {
    "nacional": "Brazil", "brasil": "Brazil", "brasileiro": "Brazil",
    "importado": None, "": None, None: None,
}

MAPA_CAMPOS_ONGSYS_ERP = {
    "nomeProduto": ("item_name", lambda v: (v or "").strip()[:140] if v else None),
    "descricaoProduto": ("description", lambda v: (v or "").strip()[:1000] if v else None),
    "fabricante": ("manufacturer", lambda v: (v or "").strip() or None),
    "valorCustoBase": ("standard_rate", lambda v: float(v) if v not in (None, "", "null") else None),
    "contaPadraoPlanoFinanceiro": ("default_expense_account", lambda v: (v or "").strip() or None),
}

# ========== Helpers HTTP ERPNext ========== #
def erp_get(path: str, params: Dict = None):
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, headers=HEADERS_ERPNext, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_post(path: str, payload: Dict):
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.post(url, headers=HEADERS_ERPNext, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_put(path: str, payload: Dict):
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.put(url, headers=HEADERS_ERPNext, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# ========== Validação de ambiente ========== #
def _assert_env():
    faltam = []
    if not all([ERPNext_URL, API_KEY, API_SECRET]): faltam.append("ERPNext")
    if not all([ONGSYS_URL_BASE, ONGSYS_USER, ONGSYS_PASS]): faltam.append("ONGSYS")
    if faltam:
        print(f"!!! ERRO CRÍTICO: Variáveis ausentes para: {', '.join(faltam)}")
        exit(1)

# ========== Garantias de dependências no ERPNext ========== #
def ensure_item_group(name: str):
    try:
        erp_get(f'api/resource/Item Group/{name}')
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
        print(f" -> Grupo de Itens '{name}' não encontrado. Criando...")
        payload = {"item_group_name": name, "parent_item_group": "All Item Groups", "is_group": 0}
        erp_post("api/resource/Item Group", payload)
        print(f" -> Grupo '{name}' criado com sucesso.")

def ensure_uom(name: str):
    try:
        erp_get(f'api/resource/UOM/{name}')
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
        payload = {"uom_name": name, "enabled": 1}
        erp_post("api/resource/UOM", payload)

def ensure_dependencies_base():
    print("--- PRÉ-ETAPA: Garantindo dependências base ---")
    ensure_item_group(DEFAULT_ITEM_GROUP)
    ensure_uom(DEFAULT_UOM)
    print("-> OK dependências base.")

# ========== Países do ERPNext ========== #
def get_countries_set() -> set:
    countries = set()
    try:
        data = erp_get("api/resource/Country", params={"fields": '["name"]', "limit": 999}).get("data", [])
        countries.update(c["name"] for c in data if c.get("name"))
    except Exception: pass
    return countries

def map_country(origem_val: Optional[str], countries_set: set) -> Optional[str]:
    if origem_val is None: return None
    key = str(origem_val).strip().lower()
    mapped = ORIGEM_COUNTRY_MAP.get(key)
    if mapped is None and key:
        candidate = origem_val.strip()
        if candidate in countries_set: return candidate
        candidate2 = candidate.capitalize()
        if candidate2 in countries_set: return candidate2
        return None
    return mapped if (mapped in countries_set or mapped is None) else None

# ========== Coleta ONGSYS com totalRecords ========== #
def extrair_produtos_da_api() -> Tuple[List[Dict], int]:
    print("--- ETAPA 1: Extraindo dados de Produtos do ONGSYS ---")
    pagina_atual = 1
    todos, vistos = [], set()
    total_records_api = None
    endpoint = ONGSYS_URL_BASE.rstrip("/") + "/produtos"
    while True:
        print(f"Buscando página {pagina_atual}...")
        try:
            resp = requests.get(endpoint, auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params={"pageNumber": pagina_atual}, timeout=30)
            if resp.status_code == 422: print("-> Fim dos dados (422)."); break
            resp.raise_for_status()
            dados = resp.json()
            if total_records_api is None: total_records_api = int(dados.get("totalRecords") or 0)
            pagina = dados.get("data", []) or []
            if not pagina: print("-> Página vazia -> fim."); break
            for it in pagina:
                _id = it.get("id")
                if _id in vistos: continue
                vistos.add(_id)
                todos.append(it)
            print(f" -> {len(pagina)} itens | Coletados: {len(todos)}" + (f" de {total_records_api}" if total_records_api else ""))
            if total_records_api and len(todos) >= total_records_api: break
            pagina_atual += 1
        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}"); break
    print(f"\n-> Extração concluída: {len(todos)} itens (declarados: {total_records_api}).")
    return todos, (total_records_api or len(todos))

# ========== GET de existência item-a-item ========== #
ITEM_FIELDS = ["name","item_code","item_name","item_group","stock_uom","description","manufacturer","standard_rate","country_of_origin","disabled","default_expense_account"]

def get_item_by_code(item_code: str) -> Optional[Dict]:
    try:
        data = erp_get(f"api/resource/Item/{item_code}", params={"fields": json.dumps(ITEM_FIELDS)})
        return data.get("data", data) # Compatibilidade com diferentes versões do Frappe
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404: return None
        raise

# ========== Mapeamento ONGSYS -> ERPNext ========== #
def normalizar_item_ongsys(prod: Dict, countries_set: set) -> Dict:
    codigo = prod.get("id")
    if not str(codigo or "").strip(): return {}
    try: item_code = str(int(str(codigo).strip()))
    except (ValueError, TypeError): item_code = str(codigo).strip()
    status = (prod.get("status") or "").strip().lower()
    disabled = 1 if (DISABLE_INACTIVE and status == "inativo") else 0
    
    grupo_ongsys = (prod.get("grupo") or "").strip()
    item_group = grupo_ongsys or DEFAULT_ITEM_GROUP # Usa grupo do ONGSYS, ou o padrão se for vazio
    
    stock_uom = (prod.get("unidadeMedida") or "").strip() or DEFAULT_UOM

    alvo = {
        "item_code": item_code,
        "item_group": item_group,
        "stock_uom": stock_uom,
        "is_stock_item": 1,
        "disabled": disabled
    }

    for k_src, (k_dst, fn) in MAPA_CAMPOS_ONGSYS_ERP.items():
        valor_src = prod.get(k_src)
        try: valor_dst = fn(valor_src)
        except Exception: valor_dst = None
        if valor_dst is not None: alvo[k_dst] = valor_dst

    country = map_country(prod.get("origem"), countries_set)
    if country: alvo["country_of_origin"] = country
    if SYNC_ONLY_ACTIVE and status == "inativo": alvo["_ignorar_por_status"] = True
    return alvo

# ========== Comparação ========== #
CAMPOS_COMPARADOS = ["item_name","item_group","stock_uom","description","manufacturer","standard_rate","country_of_origin","disabled","default_expense_account"]

def diff_campos(alvo: Dict, atual: Dict) -> Dict:
    mudancas = {}
    for campo in CAMPOS_COMPARADOS:
        a = alvo.get(campo)
        b = atual.get(campo)
        if isinstance(a, str): a = a.strip()
        if isinstance(b, str): b = b.strip()
        if a != b:
            if a is None and campo not in ("standard_rate", "disabled", "country_of_origin"): continue
            mudancas[campo] = a
    return mudancas

# ========== Criar / Atualizar ========== #
def criar_item(payload: Dict):
    ensure_item_group(payload["item_group"])
    ensure_uom(payload["stock_uom"])
    erp_post("api/resource/Item", payload)

def atualizar_item(docname: str, mudancas: Dict):
    if "item_group" in mudancas: ensure_item_group(mudancas["item_group"])
    if "stock_uom" in mudancas: ensure_uom(mudancas["stock_uom"])
    erp_put(f"api/resource/Item/{docname}", mudancas)

# ========== Orquestração ========== #
def sincronizar():
    _assert_env()
    print("\n====== INICIANDO SINCRONIZAÇÃO DE PRODUTOS ======")
    ensure_dependencies_base()
    countries_set = get_countries_set()
    origem, total_decl = extrair_produtos_da_api()
    if not origem: print("Nada para processar."); return
    criar_ct, atualizar_ct, iguais_ct, pulados_ct, falhas_ct = 0, 0, 0, 0, 0

    for prod in origem:
        alvo = normalizar_item_ongsys(prod, countries_set)
        if not alvo: continue
        if alvo.pop("_ignorar_por_status", False): pulados_ct += 1; continue
        code = alvo["item_code"]
        
        try:
            atual = get_item_by_code(code)
            if not atual:
                print(f"[CRIAR] code={code}, nome={alvo.get('item_name')}, grupo={alvo.get('item_group')}")
                criar_item(alvo)
                criar_ct += 1
            else:
                mudancas = diff_campos(alvo, atual)
                if not mudancas:
                    iguais_ct += 1
                else:
                    print(f"[ATUALIZAR] code={code} -> {list(mudancas.keys())}")
                    atualizar_item(atual["name"], mudancas)
                    atualizar_ct += 1
        except requests.HTTPError as e:
            body = e.response.text if e.response is not None else str(e)
            print(f" -> FALHA DE API em '{code}': {body}")
            falhas_ct += 1
        except Exception as e:
            print(f" -> FALHA GERAL em '{code}': {e}")
            falhas_ct += 1
            
    print(f"\n====== RESUMO ======")
    print(f"Declarados: {total_decl}")
    print(f"Criados: {criar_ct} | Atualizados: {atualizar_ct} | Iguais: {iguais_ct} | Pulados: {pulados_ct} | Falhas: {falhas_ct}")
    print("\n====== FIM ======")

if __name__ == "__main__":
    sincronizar()
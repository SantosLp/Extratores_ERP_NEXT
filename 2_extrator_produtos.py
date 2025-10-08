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
SYNC_ONLY_ACTIVE = False    # True: ignora itens "inativo" do ONGSYS
DISABLE_INACTIVE = True     # True: se inativo no ONGSYS, marca disabled=1 no ERPNext
DEFAULT_ITEM_GROUP = "Produtos"
DEFAULT_UOM = "Unidade"     # fallback quando unidadeMedida não vier

# Mapa de origem (ONGSYS) -> Country.name (ERPNext)
ORIGEM_COUNTRY_MAP = {
    "nacional": "Brazil",
    "brasil": "Brazil",
    "brasileiro": "Brazil",
    "importado": None,  # sem país específico -> não setamos
    "": None,
    None: None,
}

# === Mapeamento de campos ONGSYS -> ERPNext ===
MAPA_CAMPOS_ONGSYS_ERP = {
    "nomeProduto": ("item_name", lambda v: (v or "").strip()[:140] if v else None),
    "descricaoProduto": ("description", lambda v: (v or "").strip()[:1000] if v else None),
    "fabricante": ("manufacturer", lambda v: (v or "").strip() or None),
    "valorCustoBase": ("standard_rate", lambda v: float(v) if v not in (None, "", "null") else None),
    # origem tratado separadamente
    # precisa existir no seu plano de contas:
    "contaPadraoPlanoFinanceiro": ("default_expense_account", lambda v: (v or "").strip() or None),
}

# ========== Helpers HTTP ERPNext ==========

def erp_get(path: str, params: Dict = None):
    """Função GET para interagir com a API do ERPNext."""
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, headers=HEADERS_ERPNext, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_post(path: str, payload: Dict):
    """Função POST para interagir com a API do ERPNext."""
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.post(url, headers=HEADERS_ERPNext, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def erp_put(path: str, payload: Dict):
    """Função PUT para interagir com a API do ERPNext."""
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.put(url, headers=HEADERS_ERPNext, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# ========== Validação de ambiente ==========

def _assert_env():
    faltam = []
    if not all([ERPNext_URL, API_KEY, API_SECRET]):
        faltam.append("ERPNext_URL/API_KEY/API_SECRET")
    if not all([ONGSYS_URL_BASE, ONGSYS_USER, ONGSYS_PASS]):
        faltam.append("ONGSYS_URL_BASE/ONGSYS_USERNAME/ONGSYS_PASSWORD")
    if faltam:
        print(f"!!! ERRO CRÍTICO: Variáveis ausentes: {', '.join(faltam)}")
        exit(1)

# ========== Garantias de dependências no ERPNext ==========

def ensure_item_group(name: str):
    """Garantir que o Item Group esteja disponível no ERPNext."""
    name = name or DEFAULT_ITEM_GROUP
    try:
        erp_get(f'api/resource/Item Group/{name}')
        return
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 404:
            raise
    payload = {"item_group_name": name, "parent_item_group": "All Item Groups", "is_group": 0}
    erp_post("api/resource/Item Group", payload)

def ensure_uom(name: str):
    """Garantir que a unidade de medida (UOM) esteja disponível no ERPNext."""
    name = name or DEFAULT_UOM
    try:
        erp_get(f'api/resource/UOM/{name}')
        return
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 404:
            raise
    payload = {"uom_name": name, "enabled": 1}
    erp_post("api/resource/UOM", payload)

def ensure_dependencies_base():
    print("--- PRÉ-ETAPA: Garantindo dependências base ---")
    ensure_item_group(DEFAULT_ITEM_GROUP)
    ensure_uom(DEFAULT_UOM)
    print("-> OK dependências base.")

# ========== Países do ERPNext ==========

def get_countries_set() -> set:
    """
    Busca lista de países válidos (Country.name) para validar country_of_origin.
    """
    countries = set()
    start = 0
    page_len = 500
    fields = ["name"]
    while True:
        try:
            params = {"fields": json.dumps(fields), "limit_start": start, "limit_page_length": page_len}
            data = erp_get("api/resource/Country", params=params).get("data", [])
            if not data:
                break
            for c in data:
                if c.get("name"):
                    countries.add(c["name"])
            if len(data) < page_len:
                break
            start += page_len
        except Exception:
            break
    return countries

def map_country(origem_val: Optional[str], countries_set: set) -> Optional[str]:
    if origem_val is None:
        return None
    key = str(origem_val).strip().lower()
    mapped = ORIGEM_COUNTRY_MAP.get(key, None)
    # se não mapeou e veio string não vazia, tente usar como veio (se existir)
    if mapped is None and key:
        candidate = origem_val.strip()
        if candidate in countries_set:
            return candidate
        # fallback: tente capitalizar
        candidate2 = candidate.capitalize()
        if candidate2 in countries_set:
            return candidate2
        return None
    return mapped if (mapped in countries_set or mapped is None) else None

# ========== Coleta ONGSYS com totalRecords ==========

def extrair_produtos_da_api() -> Tuple[List[Dict], int]:
    print("--- ETAPA 1: Extraindo dados de Produtos do ONGSYS ---")
    pagina_atual = 1
    todos: List[Dict] = []
    total_records_api = None
    endpoint = ONGSYS_URL_BASE.rstrip("/") + "/produtos"
    vistos = set()  # evita duplicidade caso a API repita registros em páginas finais

    while True:
        print(f"Buscando página {pagina_atual}...")
        params = {"pageNumber": pagina_atual}
        resp = requests.get(
            endpoint,
            auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS),
            params=params,
            timeout=30
        )

        if resp.status_code == 422:
            print("-> Fim dos dados (422).")
            break

        resp.raise_for_status()
        dados = resp.json()

        if total_records_api is None:
            try:
                total_records_api = int(dados.get("totalRecords") or 0)
            except Exception:
                total_records_api = 0

        pagina = dados.get("data", []) or []
        if not pagina:
            print("-> Página vazia -> fim.")
            break

        for it in pagina:
            _id = it.get("id")
            if _id in vistos:
                continue
            vistos.add(_id)
            todos.append(it)

        print(f" -> {len(pagina)} itens nesta página | Coletados (únicos): {len(todos)}"
              + (f" de {total_records_api}" if total_records_api else ""))

        if total_records_api and len(todos) >= total_records_api:
            break

        pagina_atual += 1

    print(f"\n-> Extração concluída: {len(todos)} itens únicos (declarados: {total_records_api}).")
    return todos, (total_records_api or len(todos))

# ========== GET de existência item-a-item ==========

ITEM_FIELDS = [
    "name","item_code","item_name","item_group","stock_uom",
    "description","manufacturer","standard_rate",
    "country_of_origin","disabled","default_expense_account"
]

def get_item_by_code(item_code: str) -> Optional[Dict]:
    """
    Retorna o doc do item (com campos de interesse) ou None se não existir.
    Usa GET direto por nome (mais robusto que listar em massa).
    """
    try:
        params = {"fields": json.dumps(ITEM_FIELDS)}
        data = erp_get(f"api/resource/Item/{item_code}", params=params)
        # Algumas versões retornam {"data": {...}}; outras retornam o doc direto.
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            return data["data"]
        return data
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return None
        raise

# ========== Mapeamento ONGSYS -> ERPNext ==========

def normalizar_item_ongsys(prod: Dict, countries_set: set) -> Dict:
    """
    Retorna payload-alvo p/ ERPNext a partir de um item ONGSYS com todos os campos úteis.
    """
    codigo = prod.get("id")
    if codigo is None or str(codigo).strip() == "":
        return {}
    try:
        item_code = str(int(str(codigo).strip()))
    except (ValueError, TypeError):
        item_code = str(codigo).strip()

    status = (prod.get("status") or "").strip().lower()
    disabled = 1 if (DISABLE_INACTIVE and status == "inativo") else 0

    item_group = (prod.get("grupo") or "").strip() or DEFAULT_ITEM_GROUP
    stock_uom = (prod.get("unidadeMedida") or "").strip() or DEFAULT_UOM

    alvo = {
        "item_code": item_code,
        "item_group": item_group,
        "stock_uom": stock_uom,
        "is_stock_item": 1,
        "has_variants": 0,
        "disabled": disabled
    }

    # campos diretos
    for k_src, (k_dst, fn) in MAPA_CAMPOS_ONGSYS_ERP.items():
        valor_src = prod.get(k_src)
        try:
            valor_dst = fn(valor_src)
        except Exception:
            valor_dst = None
        if valor_dst is not None:
            alvo[k_dst] = valor_dst

    # country_of_origin mapeado/validado
    origem = prod.get("origem")
    country = map_country(origem, countries_set)
    if country:
        alvo["country_of_origin"] = country
    # Se SYNC_ONLY_ACTIVE, ignorar inativos
    if SYNC_ONLY_ACTIVE and status == "inativo":
        alvo["_ignorar_por_status"] = True

    return alvo

# ========== Comparação ==========

CAMPOS_COMPARADOS = [
    "item_name","item_group","stock_uom","description","manufacturer",
    "standard_rate","country_of_origin","disabled","default_expense_account"
]

def diff_campos(alvo: Dict, atual: Dict) -> Dict:
    """
    Compara subset de campos. Retorna somente os que mudaram.
    """
    mudancas = {}
    for campo in CAMPOS_COMPARADOS:
        a = (alvo.get(campo) if alvo.get(campo) is not None else None)
        b = (atual.get(campo) if atual and atual.get(campo) is not None else None)
        if isinstance(a, str): a = a.strip()
        if isinstance(b, str): b = b.strip()
        if a != b:
            if a is None and campo not in ("standard_rate", "disabled"):
                continue
            mudancas[campo] = a
    return mudancas

# ========== Criar / Atualizar ==========

def criar_item(payload: Dict):
    ensure_item_group(payload.get("item_group") or DEFAULT_ITEM_GROUP)
    ensure_uom(payload.get("stock_uom") or DEFAULT_UOM)
    erp_post("api/resource/Item", payload)

def atualizar_item(docname: str, mudancas: Dict):
    if "item_group" in mudancas:
        ensure_item_group(mudancas["item_group"] or DEFAULT_ITEM_GROUP)
    if "stock_uom" in mudancas:
        ensure_uom(mudancas["stock_uom"] or DEFAULT_UOM)
    erp_put(f"api/resource/Item/{docname}", mudancas)

# ========== Orquestração ==========

def sincronizar():
    _assert_env()
    print("\n====== INICIANDO SINCRONIZAÇÃO DE PRODUTOS (ONGSYS → ERPNext) ======")
    ensure_dependencies_base()

    # Países válidos no ERPNext (para validar country_of_origin)
    countries_set = get_countries_set()

    origem, total_decl = extrair_produtos_da_api()
    if not origem:
        print("Nada para processar.")
        return

    criar_ct, atualizar_ct, iguais_ct, pulados_status_ct, falhas_ct = 0, 0, 0, 0, 0

    for prod in origem:
        alvo = normalizar_item_ongsys(prod, countries_set)
        if not alvo:
            continue

        if alvo.pop("_ignorar_por_status", False):
            pulados_status_ct += 1
            continue

        code = alvo["item_code"]

        # Checa existência item-a-item (robusto contra 417 na listagem)
        try:
            atual = get_item_by_code(code)
        except Exception as e:
            print(f"[WARN] Falha ao consultar existência do code={code}: {e}")
            atual = None

        # Garante dependências deste item
        try:
            ensure_item_group(alvo.get("item_group"))
            ensure_uom(alvo.get("stock_uom"))
        except Exception as e:
            print(f"[WARN] Dependência não garantida para code={code}: {e}")

        if not atual:
            print(f"[CRIAR] code={code} | nome={alvo.get('item_name')}")
            try:
                criar_item(alvo)
                criar_ct += 1
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else "N/A"
                body = e.response.text if e.response is not None else str(e)
                # Se der Duplicate (409) por corrida, tente atualizar
                if status == 409:
                    try:
                        atual2 = get_item_by_code(code)
                        if atual2:
                            mud = diff_campos(alvo, atual2)
                            if mud:
                                atualizar_item(atual2["name"], mud)
                                atualizar_ct += 1
                                print(" -> Existia. Atualizado após 409.")
                            else:
                                iguais_ct += 1
                                print(" -> Existia. Sem mudanças após 409.")
                        else:
                            print(f" -> 409 mas não consegui ler o item '{code}'.")
                            falhas_ct += 1
                    except Exception as e2:
                        print(f" -> Falha ao tratar 409 (read/update): {e2}")
                        falhas_ct += 1
                else:
                    print(f" -> FALHA CRIAR [{status}]: {body}")
                    falhas_ct += 1
            except Exception as e:
                print(f" -> FALHA CRIAR: {e}")
                falhas_ct += 1
            continue

        # Existe -> comparar e atualizar se necessário
        mud = diff_campos(alvo, atual)
        if not mud:
            iguais_ct += 1
            continue

        print(f"[ATUALIZAR] code={code} -> {list(mud.keys())}")
        try:
            atualizar_item(atual["name"], mud)
            atualizar_ct += 1
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "N/A"
            print(f" -> FALHA ATUALIZAR [{status}]: {e.response.text if e.response is not None else str(e)}")
            falhas_ct += 1
        except Exception as e:
            print(f" -> FALHA ATUALIZAR: {e}")
            falhas_ct += 1

    print(f"\n====== RESUMO ======")
    print(f"Declarados pelo ONGSYS: {total_decl}")
    print(f"Criados: {criar_ct} | Atualizados: {atualizar_ct} | Iguais: {iguais_ct} | Pulados por status: {pulados_status_ct} | Falhas: {falhas_ct}")
    print("\n====== FIM ======")

if __name__ == "__main__":
    sincronizar()

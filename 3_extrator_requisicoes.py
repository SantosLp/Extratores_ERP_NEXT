import os
import json
import time
import logging
from typing import Dict, List, Optional, Set
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# =================================================================================
# LOG
# =================================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="log_criador_lancamentos_final.txt",
    filemode="w",
)
console = logging.getLogger("console_lanc_final")
console.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))
if not console.handlers:
    console.addHandler(console_handler)

# =================================================================================
# CONFIG
# =================================================================================
load_dotenv()
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")
ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

HEADERS_ERPNext = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Content-Type": "application/json", "Accept": "application/json",
    "Connection": "keep-alive", "User-Agent": "CDC-StockEntry-Final/1.2", # Versão
}
HEADERS_ONGSYS = {'User-Agent': 'CDC-StockEntry-Final/1.2'}

COMPANY_NAME = "CDC"
DEFAULT_SUPPLIER_GROUP = "Local"
ARQUIVO_MAPEAMENTO = r"C:\Users\Santos\Desktop\DataPath\EXTRATOR - ERP NEXT\centro_de_custo_armazen.csv"

MAX_WAIT_CREATE_SUPPLIER = 60
VERIFY_INTERVAL = 3
MAX_TENTATIVAS_LANCAMENTO = 3
ESPERA_ENTRE_TENTATIVAS = 5

# =================================================================================
# UTILS E CHAMADAS HTTP
# =================================================================================
def validar_variaveis_ambiente():
    console.info("--- Validando variáveis de ambiente (ERPNext & ONGSYS) ---")
    faltando = []
    if not all([ERPNext_URL, API_KEY, API_SECRET]): faltando.append("ERPNext")
    if not all([ONGSYS_URL_BASE, ONGSYS_USER, ONGSYS_PASS]): faltando.append("ONGSYS")
    if faltando: msg = f"ERRO CRÍTICO: Variáveis ({', '.join(faltando)}) ausentes!"; console.error(msg); logging.critical(msg); raise SystemExit(1)
    console.info("-> Variáveis OK.")

def carregar_mapeamento() -> Dict[str, str]:
    console.info(f"--- Carregando mapeamento CC->Armazém de '{os.path.basename(ARQUIVO_MAPEAMENTO)}' ---")
    try:
        if not os.path.exists(ARQUIVO_MAPEAMENTO): raise FileNotFoundError(f"Arquivo não encontrado: {ARQUIVO_MAPEAMENTO}")
        df = pd.read_csv(ARQUIVO_MAPEAMENTO, sep=";", dtype=str, encoding="latin-1")
        df.columns = ["centro_custo", "armazem"]
        df = df.dropna(subset=["centro_custo", "armazem"])
        df["centro_custo"] = df["centro_custo"].str.strip(); df["armazem"] = df["armazem"].str.strip()
        df = df[df["centro_custo"] != ""]; df = df[df["armazem"] != ""]
        mapeamento_dict = dict(zip(df["centro_custo"], df["armazem"]))
        console.info(f"-> {len(mapeamento_dict)} regras carregadas.")
        return mapeamento_dict
    except Exception as e: msg = f"ERRO CRÍTICO ao ler mapeamento: {e}"; logging.critical(msg); console.error(msg); raise SystemExit(1)

def erp_request(method: str, path: str, params: Dict = None, payload: Dict = None, timeout=60) -> requests.Response:
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    if not path.startswith("api/resource/"): url = f"{ERPNext_URL.rstrip('/')}/api/resource/{path.lstrip('/')}"
    try: return requests.request(method, url, headers=HEADERS_ERPNext, params=params, json=payload, timeout=timeout)
    except requests.exceptions.RequestException as e:
        msg = f"ERRO DE CONEXÃO ERPNext: {e}"; console.error(f"!!! {msg}"); logging.error(msg);
        r = requests.Response(); r.status_code = 503; return r

def erp_doc_exists(doctype: str, docname: str, filters: List = None) -> bool:
    path = f"api/resource/{doctype}"; params = {"limit_page_length": 1}
    if filters: params["filters"] = json.dumps(filters)
    else: path = f"api/resource/{doctype}/{docname}"
    r = erp_request("GET", path, params=params if filters else None)
    if r.status_code == 200: return bool(r.json().get("data")) if filters else True
    elif r.status_code == 404: return False
    else:
        if not filters:
            filters_alt = [["name", "=", docname]]; params_alt = {"filters": json.dumps(filters_alt), "limit_page_length": 1}
            r_filter = erp_request("GET", f"api/resource/{doctype}", params=params_alt)
            if r_filter.status_code == 200 and r_filter.json().get("data"): return True
        logging.warning(f"Erro ao verificar {doctype} '{docname}': {r.status_code}"); return False

def ensure_erp_doc_com_verificacao_otimista(doctype: str, docname: str, payload: Dict, filters: List) -> bool:
    if erp_doc_exists(doctype, docname, filters): return True
    console.info(f"    -> Criando {doctype}: '{docname}'")
    create_r = erp_request("POST", f"api/resource/{doctype}", payload=payload)
    if create_r.status_code in [200, 201, 409, 417]:
        console.info(f"       ... Comando POST enviado (status {create_r.status_code}). Verificando brevemente...")
        t0 = time.time(); verified = False
        while time.time() - t0 < 15:
            time.sleep(VERIFY_INTERVAL)
            if erp_doc_exists(doctype, docname, filters): verified = True; break
        if not verified: msg = f"AVISO: Confirmação {doctype} '{docname}' demorou. Assumindo sucesso."; console.warning(f"       ... {msg}"); logging.warning(msg)
        return True
    else: msg = f"Falha POST {doctype} '{docname}': {create_r.status_code} {create_r.text[:300]}"; console.error(f"    !!! {msg}"); logging.error(msg); return False

def erp_list_one(doctype: str, filters: List) -> Optional[Dict]:
    params = {"fields": json.dumps(["name"]), "limit_page_length": 1, "filters": json.dumps(filters)}
    r = erp_request("GET", f"api/resource/{doctype}", params=params)
    if r.status_code == 200: data = r.json().get("data", []); return data[0] if data else None
    return None

# =================================================================================
# LÓGICA DE SINCRONIZAÇÃO DE FORNECEDOR (com correção)
# =================================================================================
def ensure_supplier(pedido: Dict) -> bool:
    fornecedor_data = pedido.get("fornecedor")
    if not fornecedor_data or not fornecedor_data.get("nome"):
        # console.warning(f"Pedido {pedido.get('idPedido')} sem dados de fornecedor válidos.")
        return True # Permite continuar

    nome = fornecedor_data["nome"].strip()
    doc = (fornecedor_data.get("documento") or "").strip()
    id_pedido = pedido.get("idPedido", "Desconhecido")

    console.info(f"    - Verificando Fornecedor: {nome} (Doc: {doc or 'N/A'})")
    filtros = [["tax_id", "=", doc]] if doc else [["supplier_name", "=", nome]]
    existente = erp_list_one("Supplier", filtros) # Retorna {'name': 'ID_REAL'} ou None

    if existente:
        erp_supplier_name = existente['name'] # O ID real no ERPNext
        if doc:
             get_r = erp_request("GET", f"api/resource/Supplier/{erp_supplier_name}")
             # >>> CORREÇÃO APLICADA AQUI <<<
             if get_r.status_code == 200: # Verifica se a busca foi bem-sucedida
                  try:
                      supplier_details = get_r.json().get("data", {})
                      current_tax_id = supplier_details.get("tax_id")
                      if current_tax_id != doc:
                           console.info(f"      -> Atualizando Tax ID do fornecedor '{nome}' para '{doc}'")
                           update_r = erp_request("PUT", f"api/resource/Supplier/{erp_supplier_name}", payload={"tax_id": doc})
                           if update_r.status_code != 200: msg = f"Falha ao atualizar Tax ID de '{nome}': {update_r.text}"; console.warning(f"      !!! {msg}"); logging.warning(msg)
                  except json.JSONDecodeError: logging.warning(f"Resposta inválida ao buscar detalhes do fornecedor {erp_supplier_name}")
             # <<< FIM DA CORREÇÃO >>>
             else: logging.warning(f"Não foi possível buscar detalhes do fornecedor {erp_supplier_name} para verificar tax_id (status: {get_r.status_code}).")
        return True # Fornecedor já existe
    else:
        console.info(f"      -> Fornecedor '{nome}' não encontrado. Criando...")
        payload_sg = {"supplier_group_name": DEFAULT_SUPPLIER_GROUP}; filters_sg = [["supplier_group_name", "=", DEFAULT_SUPPLIER_GROUP]]
        if not ensure_erp_doc_com_verificacao_otimista("Supplier Group", DEFAULT_SUPPLIER_GROUP, payload_sg, filters_sg):
            console.error(f"      !!! Falha Grupo de Fornecedor. Abortando fornecedor."); return False
        payload_supplier = {"supplier_name": nome, "supplier_group": DEFAULT_SUPPLIER_GROUP, **({"tax_id": doc} if doc else {})}
        filters_supplier = [["supplier_name", "=", nome]]
        if not ensure_erp_doc_com_verificacao_otimista("Supplier", nome, payload_supplier, filters_supplier):
            console.error(f"      !!! Falha ao criar/verificar Fornecedor '{nome}'."); return False
        return True

# =================================================================================
# LÓGICA PRINCIPAL - PEDIDOS E LANÇAMENTOS
# =================================================================================
def extrair_pedidos_ongsys() -> List[Dict]:
    console.info("--- Etapa 1: Extraindo Pedidos do ONGSYS ---"); pagina, filtrados = 1, []
    while True:
        console.info(f"Buscando página de pedidos: {pagina}...");
        try:
            response = requests.get(f"{ONGSYS_URL_BASE.rstrip('/')}/pedidos", auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params={'pageNumber': pagina}, headers=HEADERS_ONGSYS, timeout=30)
            if response.status_code == 422: console.info("-> Fim dos dados (422)."); break
            response.raise_for_status()
            pedidos_da_pagina = response.json().get('data', [])
            if not pedidos_da_pagina: console.info("-> Página vazia, fim."); break
            pedidos_produto = [p for p in pedidos_da_pagina if p.get("tipoPedido") == "Produto"]
            pedidos_finalizados = [p for p in pedidos_produto if p.get("statusPedido") == "Ordem finalizada"]
            filtrados.extend(pedidos_finalizados)
            console.info(f" -> {len(pedidos_da_pagina)} registros | {len(pedidos_finalizados)} finalizados adicionados.")
            pagina += 1; time.sleep(0.1)
        except requests.exceptions.RequestException as e: msg = f"!!! FALHA conexão na extração p.{pagina}: {e}"; console.error(msg); logging.error(msg); break
        except Exception as e: msg = f"!!! FALHA inesperada na extração p.{pagina}: {e}"; console.error(msg); logging.exception(msg); break
    console.info(f"\n-> Extração concluída: {len(filtrados)} pedidos encontrados.")
    return filtrados

def get_lancamentos_existentes_erpnext() -> Set[str]:
    console.info("--- ETAPA 2: Verificando Lançamentos existentes ---")
    ids_existentes: Set[str] = set(); limit = 500; start = 0
    while True:
         console.info(f"   Buscando lote (a partir de {start})...")
         params = {"fields": json.dumps(["name", "custom_id_ongsys"]), "filters": json.dumps([["custom_id_ongsys", "is", "set"]]), "limit_page_length": limit, "limit_start": start}
         response = erp_request("GET", "api/resource/Stock Entry", params=params)
         if response.status_code == 200:
             data = response.json().get('data', [])
             if not data: console.info("   -> Fim dos lotes."); break
             novos_ids = {str(req['custom_id_ongsys']) for req in data if req.get('custom_id_ongsys')}
             ids_existentes.update(novos_ids); console.info(f"   -> Lote ok. Total IDs: {len(ids_existentes)}"); start += limit
         else:
             if response.status_code in [403, 417] and "Field not permitted" in response.text: msg = ("AVISO: Campo 'custom_id_ongsys' não é filtrável ('In List View'). Verificação de duplicados inativa."); console.warning(f"!!! {msg}"); logging.warning(msg); return set()
             else: msg = f"AVISO: Não foi possível buscar lançamentos: {response.status_code} {response.text}"; console.warning(f"!!! {msg}"); logging.warning(msg); return set()
    console.info(f"-> {len(ids_existentes)} lançamentos encontrados com ID ONGSYS.")
    return ids_existentes

def criar_lancamentos(pedidos: List[Dict], mapa_armazens: Dict[str, str], ids_ja_importados: Set[str]):
    console.info(f"\n--- Iniciando processamento de {len(pedidos)} pedidos ---")
    sucesso_count = 0; falha_count = 0; pulado_count = 0; total_pedidos = len(pedidos)

    for i, pedido in enumerate(pedidos):
        id_pedido_str = str(pedido.get("idPedido"))
        console.info(f"\n({i+1}/{total_pedidos}) Pedido ID: {id_pedido_str}")

        if id_pedido_str in ids_ja_importados: console.info("    -> Já existe. Pulando."); pulado_count += 1; continue
        if not ensure_supplier(pedido): console.info("    !!! Falha Fornecedor. Pulando."); falha_count += 1; continue
        itens_do_pedido = pedido.get("itensPedido", [])
        if not itens_do_pedido: console.info("    -> Sem itens. Pulando."); pulado_count += 1; continue

        console.info("    - Montando payload...")
        payload_lancamento = {"doctype": "Stock Entry", "stock_entry_type": "Material Receipt", "posting_date": pedido.get("dataPedido"), "docstatus": 1, "company": COMPANY_NAME, "custom_id_ongsys": id_pedido_str, "items": []}
        itens_validos = True
        for item_pedido in itens_do_pedido:
            try:
                qty_str = item_pedido.get("quantidade", "0") or "0"; quantidade = float(qty_str)
                prod_id_raw = item_pedido.get("idProduto")
                if quantidade <= 0 or prod_id_raw is None: continue
                try: item_code = str(int(float(prod_id_raw)))
                except (ValueError, TypeError): item_code = str(prod_id_raw).strip()
                cc_codigo = item_pedido.get("centroCusto")
                if not cc_codigo: msg = f"Item {item_code} sem CC. Pulando item."; console.warning(f"      !!! {msg}"); logging.warning(msg); continue
                armazem_nome = mapa_armazens.get(cc_codigo)
                if not armazem_nome: msg = f"CC '{cc_codigo}' (Item {item_code}) sem mapeamento. Pulando item."; console.warning(f"      !!! {msg}"); logging.warning(msg); continue
                cc_nome_erpnext = f"{cc_codigo} - {armazem_nome}"

                if not erp_doc_exists("Item", item_code): msg = f"Item '{item_code}' não encontrado! Execute script produtos."; console.error(f"   !!! {msg}"); logging.error(msg); itens_validos = False; break
                if not erp_doc_exists("Cost Center", cc_nome_erpnext): msg = f"CC '{cc_nome_erpnext}' não encontrado! Execute script CCs."; console.error(f"   !!! {msg}"); logging.error(msg); itens_validos = False; break
                if not erp_doc_exists("Warehouse", armazem_nome): msg = f"Armazém '{armazem_nome}' não encontrado! Execute script Armazéns."; console.error(f"   !!! {msg}"); logging.error(msg); itens_validos = False; break

                payload_lancamento["items"].append({"item_code": item_code, "qty": quantidade, "t_warehouse": armazem_nome, "cost_center": cc_nome_erpnext})
            except ValueError: msg = f"Qtd inválida ('{qty_str}') item {prod_id_raw}. Ignorando."; console.warning(f" -> {msg}"); logging.warning(msg); continue
            except Exception as e: msg = f"Erro item {item_pedido.get('idProduto')}: {e}"; console.error(f" -> {msg}"); logging.exception(msg); itens_validos = False; break
        
        if not itens_validos: console.error(f"    !!! Falha crítica itens. Pulando."); falha_count += 1; continue
        if not payload_lancamento["items"]: console.info(f"    -> Nenhum item válido. Pulando."); pulado_count += 1; continue

        console.info(f"    - Enviando Lançamento ({len(payload_lancamento['items'])} itens)...")
        sucesso_envio = False
        for tentativa in range(1, MAX_TENTATIVAS_LANCAMENTO + 1):
            console.info(f"      -> Tentativa {tentativa}/{MAX_TENTATIVAS_LANCAMENTO}...")
            response = erp_request("POST", "api/resource/Stock Entry", payload=payload_lancamento)
            if response.status_code == 200: console.info("      -> SUCESSO!"); sucesso_envio = True; sucesso_count += 1; break
            response_text = response.text
            if "LinkValidationError" in response_text and tentativa < MAX_TENTATIVAS_LANCAMENTO: console.warning(f"      -> Falha de 'timing'. Aguardando {ESPERA_ENTRE_TENTATIVAS}s..."); time.sleep(ESPERA_ENTRE_TENTATIVAS)
            elif response.status_code in [409, 417] or "already exists" in response_text.lower(): console.info("      -> Duplicidade detectada."); pulado_count += 1; sucesso_envio = True; break
            else: msg = f"FALHA PEDIDO {id_pedido_str}: {response.status_code} {response_text[:300]}"; console.error(f"      !!! {msg}"); logging.error(msg); break
        if not sucesso_envio: falha_count += 1

    console.info("\n--- Resumo Final ---")
    console.info(f"Lançamentos criados: {sucesso_count}")
    console.info(f"Falhas: {falha_count}")
    console.info(f"Pulados: {pulado_count}")
    console.info(f"Total: {sucesso_count + falha_count + pulado_count} de {total_pedidos}")

# =================================================================================
# MAIN
# =================================================================================
if __name__ == "__main__":
    console.info("\n====== INICIANDO CRIADOR DE LANÇAMENTOS ERPNEXT (v1.2 - Corrigido) ======")
    validar_variaveis_ambiente()
    mapa_cc_armazem = carregar_mapeamento()
    iniciar = time.time()
    try:
        pedidos_ongsys = extrair_pedidos_ongsys()
        if pedidos_ongsys:
            ids_existentes = get_lancamentos_existentes_erpnext()
            criar_lancamentos(pedidos_ongsys, mapa_cc_armazem, ids_existentes)
        else: console.info("\nNenhum pedido encontrado.")
    except SystemExit: console.error("\n!!! Execução abortada.")
    except Exception as e: console.error(f"\n!!! Erro inesperado: {e}"); logging.exception("Erro inesperado")
    finally: console.info(f"\n====== PROCESSAMENTO FINALIZADO em {time.time() - iniciar:.1f}s ======")
import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
from typing import List, Dict, Set

# --- CONFIGURAÇÕES ---
load_dotenv()
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")

ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

WAREHOUSE_ALVO = os.getenv("ERPNext_WAREHOUSE") 

if not all([ERPNext_URL, API_KEY, API_SECRET, ONGSYS_URL_BASE, ONGSYS_USER, ONGSYS_PASS, WAREHOUSE_ALVO]):
    print("!!! ERRO CRÍTICO: Verifique TODAS as variáveis de ambiente no arquivo .env (incluindo ERPNext_WAREHOUSE) !!!")
    exit()

HEADERS_ERPNext = {"Authorization": f"token {API_KEY}:{API_SECRET}"}
HEADERS_ONGSYS = {'User-Agent': 'Mozilla/5.0'}

# --- FUNÇÕES DO EXTRATOR ---

def extrair_requisicoes_ongsys() -> List[Dict]:
    print("--- ETAPA 1: Extraindo PEDIDOS da API do ONGSYS ---")
    endpoint = f"{ONGSYS_URL_BASE.rstrip('/')}/pedidos"
    pagina_atual = 1
    todos_pedidos = []
    
    while True:
        print(f"Buscando dados da página: {pagina_atual}...")
        try:
            params = {'pageNumber': pagina_atual}
            response = requests.get(endpoint, auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params=params, headers=HEADERS_ONGSYS, timeout=30)
            if response.status_code == 422:
                print("-> Fim dos dados (422 recebido)."); break
            response.raise_for_status()
            dados = response.json()
            pedidos_da_pagina = dados.get('data', [])
            if not pedidos_da_pagina:
                print("-> Fim dos dados. Página vazia recebida."); break
            
            pedidos_de_produto = [p for p in pedidos_da_pagina if p.get("tipoPedido") == "Produto"]
            todos_pedidos.extend(pedidos_de_produto)
            
            print(f" -> {len(pedidos_da_pagina)} registros encontrados, {len(pedidos_de_produto)} são de produtos.")
            pagina_atual += 1
        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}"); break
            
    print(f"\n-> Extração do ONGSYS concluída. Total de {len(todos_pedidos)} pedidos de PRODUTO encontrados.")
    return todos_pedidos

def get_lancamentos_existentes_erpnext() -> Set[str]:
    print("--- ETAPA 2: Verificando Lançamentos de Estoque já existentes no ERPNext ---")
    ids_existentes = set()
    try:
        url = f"{ERPNext_URL}/api/resource/Stock Entry?fields=[\"custom_id_ongsys\"]&limit=9999"
        response = requests.get(url, headers=HEADERS_ERPNext, timeout=120)
        response.raise_for_status()
        dados = response.json()
        ids_existentes = {str(req['custom_id_ongsys']) for req in dados.get('data', []) if req.get('custom_id_ongsys')}
        print(f"-> {len(ids_existentes)} lançamentos do ONGSYS encontrados no ERPNext.")
    except Exception as e:
        print(f"!!! AVISO: Não foi possível buscar lançamentos existentes. Pode haver duplicatas. Erro: {e}")
    return ids_existentes

def transformar_requisicoes_para_erpnext(pedidos_origem: List[Dict], ids_existentes: Set[str]) -> List[Dict]:
    print("--- ETAPA 3: Transformando PEDIDOS para o formato de LANÇAMENTO DE ESTOQUE ---")
    lancamentos_finais = []
    for pedido in pedidos_origem:
        id_pedido_str = str(pedido.get("idPedido"))

        if id_pedido_str in ids_existentes:
            continue
        
        novo_lancamento = {
            "doctype": "Stock Entry",
            "stock_entry_type": "Material Receipt",
            "posting_date": pedido.get("dataPedido"),
            "docstatus": 1,
            "custom_id_ongsys": id_pedido_str,
            "items": []
        }

        for item_pedido in pedido.get("itensPedido", []):
            try:
                # <<<--- AJUSTE FINAL DE ROBUSTEZ ---<<<
                # Tenta converter a quantidade para número e verifica se é maior que zero
                quantidade = float(item_pedido.get("quantidade", 0))
                if quantidade <= 0:
                    print(f" -> AVISO: Item '{item_pedido.get('idProduto')}' no pedido '{id_pedido_str}' tem quantidade zero ou inválida e será ignorado.")
                    continue

                id_original = int(item_pedido.get("idProduto"))
                item_code_final = str(id_original)

                item_formatado = {
                    "item_code": item_code_final,
                    "qty": quantidade, # Usa a quantidade validada
                    "rate": 1,
                    "t_warehouse": WAREHOUSE_ALVO
                }
                novo_lancamento["items"].append(item_formatado)
            except (TypeError, ValueError, AttributeError):
                print(f" -> AVISO: Item com dados inválidos no pedido '{id_pedido_str}' será ignorado. Dados: {item_pedido}")
                continue
        
        if novo_lancamento["items"]:
            lancamentos_finais.append(novo_lancamento)
    
    print(f"-> {len(lancamentos_finais)} novos Lançamentos de Estoque prontos para serem enviados.")
    return lancamentos_finais

def carregar_lancamentos_erpnext(lista_de_lancamentos: List[Dict]):
    if not lista_de_lancamentos:
        print("--- ETAPA 4: Nenhum Lançamento de Estoque para carregar. ---")
        return
        
    print(f"--- ETAPA 4: Carregando {len(lista_de_lancamentos)} LANÇAMENTOS DE ESTOQUE no ERPNext ---")
    url = f"{ERPNext_URL}/api/resource/Stock Entry"

    for lancamento in lista_de_lancamentos:
        id_original = lancamento.get("custom_id_ongsys")
        print(f"Enviando Lançamento (ID ONGsys: {id_original})...")
        try:
            response = requests.post(url, headers=HEADERS_ERPNext, json=lancamento, timeout=120)
            response.raise_for_status()
            print(f" -> SUCESSO!")
        except requests.exceptions.HTTPError as e:
            print(f" -> FALHA DE API: {e}")
            print(f" -> Resposta do Servidor: {e.response.text}")
        except requests.exceptions.RequestException as e:
            print(f" -> FALHA DE CONEXÃO: {e}")
        except Exception as e:
             print(f" -> ERRO INESPERADO: {e}")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"\n====== INICIANDO EXTRATOR DE REQUISIÇÕES ======")
    pedidos_ongsys = extrair_requisicoes_ongsys()
    if pedidos_ongsys:
        ids_ja_importados = get_lancamentos_existentes_erpnext()
        lancamentos_para_erpnext = transformar_requisicoes_para_erpnext(pedidos_ongsys, ids_ja_importados)
        carregar_lancamentos_erpnext(lancamentos_para_erpnext)
    print("\n====== EXTRATOR DE REQUISIÇÕES FINALIZADO ======")
import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv

# --- CONFIGURAÇÕES ---
load_dotenv()
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")

ONGSYS_URL_PRODUTOS = os.getenv("ONGSYS_URL_BASE") + "/produtos"
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

if not all([ERPNext_URL, API_KEY, API_SECRET]):
    print("!!! ERRO CRÍTICO: Verifique as variáveis do ERPNext no arquivo .env !!!")
    exit()

HEADERS_ERPNext = {"Authorization": f"token {API_KEY}:{API_SECRET}"}

# --- FUNÇÕES DO EXTRATOR DE PRODUTOS ---

def extrair_produtos_da_api():
    print("--- ETAPA 1: Extraindo dados de Produtos do ONGSYS ---")
    pagina_atual = 1
    todos_produtos = []
    while True:
        print(f"Buscando dados da página: {pagina_atual}...")
        try:
            params = {'pageNumber': pagina_atual}
            response = requests.get(
                ONGSYS_URL_PRODUTOS, 
                auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), 
                params=params, 
                timeout=30
            )
            response.raise_for_status()
            dados = response.json()
            
            produtos_da_pagina = dados.get('data', [])
            if not produtos_da_pagina:
                print("-> Fim dos dados. Página vazia recebida.")
                break 
            
            todos_produtos.extend(produtos_da_pagina)
            print(f" -> {len(produtos_da_pagina)} registros encontrados nesta página.")
            pagina_atual += 1
        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}")
            break
    print(f"\n-> Extração do ONGSYS concluída. Total de {len(todos_produtos)} produtos encontrados.")
    return todos_produtos

def get_produtos_existentes_do_erpnext():
    print("--- ETAPA 2: Verificando produtos que já existem no ERPNext ---")
    try:
        url = f"{ERPNext_URL}/api/resource/Item?fields=[\"item_code\"]&limit=9999"
        response = requests.get(url, headers=HEADERS_ERPNext, timeout=30)
        response.raise_for_status()
        dados = response.json()
        codigos_existentes = {item['item_code'] for item in dados.get('data', [])}
        print(f"-> {len(codigos_existentes)} produtos encontrados no ERPNext.")
        return codigos_existentes
    except Exception as e:
        print(f"!!! FALHA ao buscar produtos do ERPNext: {e}")
        return set()

def transformar_e_filtrar_produtos(produtos_origem, codigos_existentes):
    print("--- ETAPA 3: Comparando e transformando apenas os dados novos ---")
    novos_produtos = []
    for produto in produtos_origem:
        codigo_origem_str = produto.get('id')
        nome_produto = produto.get('nomeProduto')
        
        if not (codigo_origem_str and nome_produto):
            continue

        try:
            novo_codigo_str = str(int(codigo_origem_str))

            if novo_codigo_str not in codigos_existentes:
                produto_formatado = {
                    "item_code": novo_codigo_str,
                    "item_name": nome_produto,
                    "item_group": produto.get("grupo", "Produtos"),
                    "stock_uom": "Unidade",
                    "description": produto.get("descricaoProduto")
                }
                
                # Garante que o item_group não seja nulo, se não vier da API
                if not produto_formatado["item_group"]:
                    produto_formatado["item_group"] = "Produtos"

                novos_produtos.append(produto_formatado)

        except (ValueError, TypeError):
            print(f" -> AVISO: O produto '{nome_produto}' com ID '{codigo_origem_str}' será ignorado.")
            continue
            
    print(f"-> {len(novos_produtos)} novos produtos para criar.")
    return novos_produtos

def carregar_novos_produtos(lista_para_criar):
    if not lista_para_criar:
        print("--- ETAPA 4: Nenhum produto novo para carregar. ---")
        return

    print(f"--- ETAPA 4: Carregando {len(lista_para_criar)} novos PRODUTOS no ERPNext ---")
    url = f"{ERPNext_URL}/api/resource/Item"
    
    for produto in lista_para_criar:
        print(f"Enviando produto '{produto.get('item_name')}' (Código Novo: {produto.get('item_code')})...")
        try:
            response = requests.post(url, headers=HEADERS_ERPNext, json=produto, timeout=30)
            response.raise_for_status()
            print(f" -> SUCESSO!")
        except Exception as e:
            print(f" -> FALHA: {e}")
            if hasattr(e, 'response'): print(f" -> Resposta do Servidor: {e.response.text}")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"\n====== INICIANDO EXTRATOR DE PRODUTOS ======")
    produtos_ongsys = extrair_produtos_da_api()
    if produtos_ongsys:
        produtos_erpnext = get_produtos_existentes_do_erpnext()
        produtos_novos = transformar_e_filtrar_produtos(produtos_ongsys, produtos_erpnext)
        carregar_novos_produtos(produtos_novos)
    print("\n====== EXTRATOR DE PRODUTOS FINALIZADO ======")
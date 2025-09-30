import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
from datetime import datetime
import json

# --- CONFIGURAÇÕES ---
load_dotenv()
ERPNext_URL = os.getenv("ERPNext_URL")
ERPNext_API_KEY = os.getenv("ERPNext_API_KEY")
ERPNext_API_SECRET = os.getenv("ERPNext_API_SECRET")
ONGSYS_URL_BASE = "https://www.ongsys.com.br/app/index.php/api/v2"
ONGSYS_USER = "03970166000129"
ONGSYS_PASS = "fa009965195f9770db49a9111570b531"

if not all([ERPNext_URL, ERPNext_API_KEY, ERPNext_API_SECRET]):
    print("!!! ERRO CRÍTICO: Verifique as variáveis do ERPNext no arquivo .env !!!")
    exit()

HEADERS_ERPNext = {"Authorization": f"token {ERPNext_API_KEY}:{ERPNext_API_SECRET}"}
AUTH_ONGSYS = HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS)

erros_encontrados = []

def extrair_pedidos_ongsys():
    """Extrai todos os pedidos da API do ONGsys, lidando com a paginação."""
    print("--- ETAPA 1: Extraindo PEDIDOS da API REAL do ONGSYS ---")
    endpoint = f"{ONGSYS_URL_BASE}/pedidos"
    pagina_atual = 1
    todos_pedidos = []
    
    while True:
        print(f"Buscando dados da página: {pagina_atual}...")
        try:
            params = {'pageNumber': pagina_atual}
            response = requests.get(endpoint, auth=AUTH_ONGSYS, params=params, timeout=30)
            response.raise_for_status()
            dados = response.json()
            
            pedidos_da_pagina = dados.get('data', [])
            if not pedidos_da_pagina:
                print("-> Fim dos dados. Página vazia recebida.")
                break
            
            todos_pedidos.extend(pedidos_da_pagina)
            print(f" -> {len(pedidos_da_pagina)} registros encontrados nesta página.")
            pagina_atual += 1
        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}")
            break
            
    print(f"\n-> Extração do ONGSYS concluída. Total de {len(todos_pedidos)} pedidos encontrados.")
    return todos_pedidos

def transformar_pedidos_para_erpnext(pedidos_origem):
    """Converte a estrutura de Pedido do ONGsys para Nota de Recebimento do ERPNext."""
    print("--- ETAPA 2: Transformando PEDIDOS para o formato do ERPNext ---")
    requisicoes_finais = []
    for pedido in pedidos_origem:
        id_pedido = pedido.get('idPedido')
        fornecedor_info = pedido.get('fornecedor', {})
        nome_fornecedor = fornecedor_info.get("nome")
        
        if not fornecedor_info or not nome_fornecedor:
            erros_encontrados.append({'pedido_id': id_pedido, 'fornecedor': 'N/A', 'motivo': 'Dados Inválidos', 'detalhe': 'Pedido ignorado por não ter um fornecedor válido.'})
            continue

        nova_requisicao = {
            "doctype": "Purchase Receipt",
            "supplier": nome_fornecedor,
            "posting_date": pedido.get("dataPedido"),
            "docstatus": 0,
            "items": [],
            "_id_original": id_pedido
        }
        
        for item_pedido in pedido.get("itensPedido", []):
            try:
                # Tenta converter a quantidade para float e depois arredonda para o inteiro mais próximo.
                quantidade_original = item_pedido.get("quantidade", 0)
                quantidade_arredondada = round(float(quantidade_original))

                if quantidade_arredondada == 0:
                    erros_encontrados.append({'pedido_id': id_pedido, 'fornecedor': nome_fornecedor, 'motivo': 'Item Ignorado', 'detalhe': f"Item com ID '{item_pedido.get('idProduto')}' ignorado (quantidade é zero ou arredondada para zero)."})
                    continue

                id_original = int(item_pedido.get("idProduto"))
                item_code_final = str(id_original)

                item_formatado = { "item_code": item_code_final, "qty": quantidade_arredondada, "rate": 1 }
                nova_requisicao["items"].append(item_formatado)
            except (TypeError, ValueError):
                erros_encontrados.append({'pedido_id': id_pedido, 'fornecedor': nome_fornecedor, 'motivo': 'Item Ignorado', 'detalhe': f"Item com ID '{item_pedido.get('idProduto')}' não é um número e será ignorado."})
                continue
        
        if nova_requisicao["items"]:
            requisicoes_finais.append(nova_requisicao)
        else:
            erros_encontrados.append({'pedido_id': id_pedido, 'fornecedor': nome_fornecedor, 'motivo': 'Pedido Ignorado', 'detalhe': 'Pedido ignorado por não conter itens válidos após a limpeza.'})

    print(f"-> {len(requisicoes_finais)} requisições prontas para serem enviadas.")
    return requisicoes_finais

def carregar_requisicoes_erpnext(lista_de_requisicoes):
    """Envia as Notas de Recebimento formatadas para a API do ERPNext."""
    if not lista_de_requisicoes:
        print("--- ETAPA 3: Nenhuma requisição nova para carregar. ---")
        return
        
    print(f"--- ETAPA 3: Carregando {len(lista_de_requisicoes)} REQUISIÇÕES no ERPNext ---")
    url = f"{ERPNext_URL}/api/resource/Purchase Receipt"

    for requisicao in lista_de_requisicoes:
        fornecedor = requisicao.get("supplier")
        id_original = requisicao.pop("_id_original", "N/A")

        print(f"Enviando Requisição (Pedido Original ID: {id_original}) do fornecedor '{fornecedor}'...")
        try:
            response = requests.post(url, headers=HEADERS_ERPNext, json=requisicao, timeout=60)
            response.raise_for_status()
            print(f" -> SUCESSO!")
        except Exception as e:
            print(f" -> FALHA!")
            resposta_servidor = str(e)
            if hasattr(e, 'response'): 
                resposta_servidor = e.response.text
            erros_encontrados.append({'pedido_id': id_original, 'fornecedor': fornecedor, 'motivo': 'Falha no Carregamento (API)', 'detalhe': resposta_servidor})

def salvar_log_de_erros(lista_de_erros):
    """Salva um relatório de todos os erros encontrados em um arquivo na Área de Trabalho."""
    if not lista_de_erros:
        print("\nNenhum erro documentado. Execução limpa!")
        return

    caminho_desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nome_arquivo = f"log_erros_requisicoes_{timestamp}.txt"
    caminho_completo = os.path.join(caminho_desktop, nome_arquivo)

    print(f"\nDocumentando {len(lista_de_erros)} erros no arquivo: {nome_arquivo}")

    with open(caminho_completo, 'w', encoding='utf-8') as f:
        f.write(f"Relatório de Erros - Execução de {timestamp}\n")
        f.write("="*50 + "\n\n")
        for i, erro in enumerate(lista_de_erros):
            f.write(f"--- Erro #{i+1} ---\n")
            f.write(f"Pedido Original ID: {erro.get('pedido_id', 'N/A')}\n")
            f.write(f"Fornecedor: {erro.get('fornecedor', 'N/A')}\n")
            f.write(f"Motivo: {erro.get('motivo')}\n")
            f.write(f"Detalhe: {erro.get('detalhe')}\n")
            f.write("-" * 20 + "\n\n")
    
    print(f"SUCESSO! Arquivo de log de erros foi salvo na sua Área de Trabalho.")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"\n====== INICIANDO EXTRATOR DE REQUISIÇÕES (VIDA REAL) ======")
    pedidos_ongsys = extrair_pedidos_ongsys()
    if pedidos_ongsys:
        requisicoes_para_erpnext = transformar_pedidos_para_erpnext(pedidos_ongsys)
        carregar_requisicoes_erpnext(requisicoes_para_erpnext)
    
    salvar_log_de_erros(erros_encontrados)
    
    print("\n====== EXTRATOR DE REQUISIÇÕES FINALIZADO ======")


#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import requests
from typing import Any, Dict, List, Optional
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Carregar variáveis do .env (mantido como no seu padrão)
load_dotenv()


class Common:
    # Configurações de sincronização (mantidas)
    SYNC_ACTIVE_ONLY: bool = False
    DISABLE_INACTIVE: bool = True
    DEFAULT_GROUP: str = "Todos os Grupos de Itens"
    DEFAULT_UOM: str = "Unidade"
    MAX_WAIT_CREATE: int = 60  # Espera máxima para confirmação de criação
    VERIFY_INTERVAL: int = 3   # Intervalo para verificar a criação

    def __init__(self) -> None:
        # URLs e credenciais do ERPNext e ONGSYS (mantidas)
        self.ERP_URL: str = (os.getenv("ERPNext_URL") or "").rstrip("/")
        self.API_KEY: Optional[str] = os.getenv("ERPNext_API_KEY")
        self.API_SECRET: Optional[str] = os.getenv("ERPNext_API_SECRET")

        self.ONGSYS_URL: str = (os.getenv("ONGSYS_URL_BASE") or "").rstrip("/")
        self.ONGSYS_USER: Optional[str] = os.getenv("ONGSYS_USERNAME")
        self.ONGSYS_PASS: Optional[str] = os.getenv("ONGSYS_PASSWORD")

        # Cabeçalhos para ERPNext (mantidos)
        self.HEADERS_ERP: Dict[str, str] = {
            "Authorization": f"token {self.API_KEY}:{self.API_SECRET}",
            "Content-Type": "application/json",
        }

        # Cabeçalhos para ONGSYS (adicionado)
        self.HEADERS_ONGSYS: Dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self._logger = logging.getLogger(__name__)

    # ------- MESMA LÓGICA: erp_request -------
    def erp_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
    ) -> requests.Response:
        base = self.ERP_URL.rstrip("/")
        url = f"{base}/{path.lstrip('/')}"
        if not path.startswith("api/resource/"):
            url = f"{base}/api/resource/{path.lstrip('/')}"

        try:
            return requests.request(
                method=method,
                url=url,
                headers=self.HEADERS_ERP,
                params=params,
                json=payload,
                timeout=timeout,
            )
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão com o ERPNext: {e}")
            response = requests.Response()
            response.status_code = 503
            return response

    # ------- ONGSYS: agora DENTRO da classe e com nomes corretos -------
    def ongsys_request(
        self,
        method: str,
        path: str,
        *,
        page_number: Optional[int] = None,  # controla apenas pageNumber (opcional)
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
    ) -> requests.Response:
        """
        Requisição genérica ao ONGSYS.
        Controla somente o 'pageNumber' via parâmetro opcional.
        Ex.: self.ongsys_request("GET", "/pedidos", page_number=1)
        """
        base = self.ONGSYS_URL.rstrip("/")
        url = f"{base}/{path.lstrip('/')}"

        params = {"pageNumber": int(page_number)} if page_number is not None else None

        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=self.HEADERS_ONGSYS,
                params=params,
                json=payload,
                timeout=timeout,
                auth=HTTPBasicAuth(self.ONGSYS_USER or "", self.ONGSYS_PASS or ""),
            )
            return resp
        except requests.exceptions.RequestException as e:
            msg = f"!!! FALHA conexão ONGSYS: {e}"
            print(msg)
            self._logger.error(msg)
            r = requests.Response()
            r.status_code = 503
            return r

    def get_all_ongsys(self, doctype: str) -> List[Dict[str, Any]]:
        pagina = 1
        all_records = []
        while True:
            resp = self.ongsys_request("GET", doctype, page_number=pagina)
            if resp.status_code == 422:
                break
            if resp.status_code != 200:
                self._logger.error(f"Failed to fetch records for {doctype}: {resp.status_code}")
                break
            data = resp.json()
            records = data.get('data', [])
            if not records:
                break
            all_records.extend(records)
            pagina += 1
        return all_records

            
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - FIPE
Client otimizado para enviar dados da FIPE ao Supabase
"""

import os
import time
import requests
from typing import Dict, List, Any
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class FipeSupabaseClient:
    """Cliente otimizado para Supabase - dados FIPE"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no .env")
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        
        # Session com connection pooling e retry
        self.session = self._create_session()
        
        # Cache de disponibilidade da função RPC
        self._rpc_available = None
        self._check_rpc_availability()
    
    def _create_session(self) -> requests.Session:
        """Cria session com connection pooling e retry automático"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET", "PATCH"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def _check_rpc_availability(self) -> bool:
        """Verifica se função RPC está disponível"""
        if self._rpc_available is not None:
            return self._rpc_available
        
        try:
            url = f"{self.url}/rest/v1/rpc/upsert_fipe_vehicles"
            r = self.session.post(
                url,
                headers=self.headers,
                json={'items': []},
                timeout=5
            )
            self._rpc_available = r.status_code in (200, 201)
            
            if self._rpc_available:
                print("✅ RPC upsert_fipe_vehicles disponível (modo otimizado)")
            else:
                print("⚠️  RPC não disponível - execute o SQL de instalação")
                
        except Exception as e:
            print(f"⚠️  Erro ao verificar RPC: {e}")
            self._rpc_available = False
        
        return self._rpc_available
    
    def upsert_vehicles(self, items: List[Dict]) -> Dict[str, int]:
        """
        UPSERT otimizado com batching inteligente
        Retorna: {'inserted': X, 'updated': Y, 'errors': Z, 'time_ms': T}
        """
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'time_ms': 0}
        
        start_time = time.time()
        
        # Se RPC disponível, usar (mais rápido)
        if self._rpc_available:
            stats = self._upsert_via_rpc(items)
        else:
            print("⚠️  RPC indisponível! Execute o SQL para melhor performance!")
            stats = self._upsert_fallback(items)
        
        stats['time_ms'] = int((time.time() - start_time) * 1000)
        
        return stats
    
    def _upsert_via_rpc(self, items: List[Dict]) -> Dict[str, int]:
        """Método otimizado usando RPC"""
        url = f"{self.url}/rest/v1/rpc/upsert_fipe_vehicles"
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        total_batches = (len(items) + batch_size - 1) // batch_size
        
        print(f"📦 Processando {len(items)} veículos em {total_batches} batches")
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                r = self.session.post(
                    url,
                    headers=self.headers,
                    json={'items': batch},
                    timeout=120
                )
                
                if r.status_code == 200:
                    result = r.json()
                    stats['inserted'] += result.get('inserted', 0)
                    stats['updated'] += result.get('updated', 0)
                    stats['errors'] += result.get('errors', 0)
                    
                    progress = (batch_num / total_batches) * 100
                    print(f"   ✅ [{progress:3.0f}%] Batch {batch_num}/{total_batches}: "
                          f"+{result.get('inserted', 0)} novos, "
                          f"~{result.get('updated', 0)} atualizados")
                else:
                    print(f"   ❌ Batch {batch_num}: HTTP {r.status_code}")
                    stats['errors'] += len(batch)
                    
            except Exception as e:
                print(f"   ❌ Batch {batch_num}: {str(e)[:100]}")
                stats['errors'] += len(batch)
        
        total = stats['inserted'] + stats['updated'] + stats['errors']
        success_rate = ((stats['inserted'] + stats['updated']) / total * 100) if total > 0 else 0
        print(f"\n📊 RESULTADO: {stats['inserted']} novos | "
              f"{stats['updated']} atualizados | "
              f"{stats['errors']} erros | "
              f"{success_rate:.1f}% sucesso")
        
        return stats
    
    def _upsert_fallback(self, items: List[Dict]) -> Dict[str, int]:
        """Fallback sem RPC (mais lento)"""
        url = f"{self.url}/rest/v1/fipe_vehicles"
        
        upsert_headers = self.headers.copy()
        upsert_headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 200
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            
            try:
                r = self.session.post(
                    url,
                    headers=upsert_headers,
                    json=batch,
                    timeout=30
                )
                
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"   ✅ Batch {i//batch_size + 1}: {len(batch)} processados")
                else:
                    print(f"   ⚠️  Batch {i//batch_size + 1}: Status {r.status_code}")
                    stats['errors'] += len(batch)
                    
            except Exception as e:
                print(f"   ❌ Erro: {str(e)[:100]}")
                stats['errors'] += len(batch)
        
        return stats
    
    def get_stats(self, tipo: str = None) -> Dict[str, Any]:
        """
        Retorna estatísticas da base FIPE
        tipo: 'carros', 'motos', 'caminhoes' ou None (todos)
        """
        url = f"{self.url}/rest/v1/fipe_vehicles"
        
        params = {'select': 'tipo,marca,count'}
        if tipo:
            params['tipo'] = f'eq.{tipo}'
        
        try:
            r = self.session.get(url, headers=self.headers, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"❌ Erro ao buscar stats: {e}")
        
        return {}
    
    def __del__(self):
        """Fecha session ao destruir objeto"""
        if hasattr(self, 'session'):
            self.session.close()


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    # Teste básico
    import json
    from pathlib import Path
    
    client = FipeSupabaseClient()
    
    # Carrega JSON de exemplo
    json_file = Path("fipe_caminhoes.json")
    
    if json_file.exists():
        print(f"📁 Carregando {json_file}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"📋 {len(data)} veículos encontrados")
        
        # Envia para Supabase
        result = client.upsert_vehicles(data)
        
        print(f"\n✅ Concluído em {result['time_ms']}ms")
    else:
        print(f"❌ Arquivo {json_file} não encontrado")
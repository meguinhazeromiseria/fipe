#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MARKET PRICE SUPABASE CLIENT
Cliente para buscar e atualizar preços de mercado
"""

import os
import requests
from datetime import datetime
from typing import List, Dict, Optional


class MarketPriceSupabaseClient:
    """Cliente Supabase para operações de Market Price"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def fetch_vehicles_without_price(
        self, 
        table: str = 'veiculos',
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict]:
        """
        Busca veículos sem market_price
        
        Args:
            table: Nome da tabela
            limit: Quantidade de registros
            offset: Offset para paginação
        
        Returns:
            Lista de veículos
        """
        try:
            url = f"{self.url}/rest/v1/{table}"
            
            params = {
                'select': '*',
                'market_price': 'is.null',
                'is_active': 'eq.true',
                'limit': limit,
                'offset': offset,
                'order': 'created_at.desc'
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                return r.json()
            else:
                print(f"❌ Erro {r.status_code}: {r.text[:200]}")
                return []
        
        except Exception as e:
            print(f"❌ Erro ao buscar veículos: {e}")
            return []
    
    def update_market_price(
        self,
        table: str,
        vehicle_id: str,
        price_data: Dict
    ) -> bool:
        """
        Atualiza market_price de um veículo
        
        Args:
            table: Nome da tabela
            vehicle_id: ID do veículo
            price_data: {
                'market_price': float,
                'market_price_source': str,
                'market_price_confidence': str,
                'market_price_metadata': dict,
                'vehicle_type': str
            }
        
        Returns:
            True se sucesso
        """
        try:
            url = f"{self.url}/rest/v1/{table}"
            
            update_data = {
                'market_price': price_data.get('market_price'),
                'market_price_source': price_data.get('market_price_source'),
                'market_price_updated_at': datetime.now().isoformat(),
                'market_price_confidence': price_data.get('market_price_confidence', 'medium'),
                'market_price_metadata': price_data.get('market_price_metadata', {}),
            }
            
            # Adiciona vehicle_type se fornecido
            if 'vehicle_type' in price_data:
                update_data['vehicle_type'] = price_data['vehicle_type']
            
            params = {'id': f'eq.{vehicle_id}'}
            
            r = self.session.patch(
                url,
                json=update_data,
                params=params,
                timeout=30
            )
            
            if r.status_code in (200, 204):
                return True
            else:
                print(f"❌ Erro ao atualizar {vehicle_id}: {r.status_code}")
                return False
        
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False
    
    def batch_update_market_prices(
        self,
        table: str,
        updates: List[Dict]
    ) -> Dict:
        """
        Atualiza múltiplos preços em batch
        
        Args:
            table: Nome da tabela
            updates: Lista de {
                'id': str,
                'market_price': float,
                'market_price_source': str,
                'vehicle_type': str,
                ...
            }
        
        Returns:
            {'success': int, 'errors': int}
        """
        stats = {'success': 0, 'errors': 0}
        
        for update in updates:
            vehicle_id = update.pop('id')
            
            if self.update_market_price(table, vehicle_id, update):
                stats['success'] += 1
            else:
                stats['errors'] += 1
        
        return stats
    
    def get_stats(self, table: str = 'veiculos') -> Dict:
        """Retorna estatísticas da tabela"""
        try:
            url = f"{self.url}/rest/v1/{table}"
            
            # Total
            r_total = self.session.get(
                url,
                params={'select': 'count', 'is_active': 'eq.true'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            total = 0
            if r_total.status_code == 200:
                total = int(r_total.headers.get('Content-Range', '0').split('/')[-1])
            
            # Com preço
            r_with_price = self.session.get(
                url,
                params={'select': 'count', 'is_active': 'eq.true', 'market_price': 'not.is.null'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            with_price = 0
            if r_with_price.status_code == 200:
                with_price = int(r_with_price.headers.get('Content-Range', '0').split('/')[-1])
            
            return {
                'total': total,
                'with_market_price': with_price,
                'without_market_price': total - with_price,
                'percentage_complete': round(with_price / total * 100, 2) if total > 0 else 0
            }
        
        except Exception as e:
            print(f"❌ Erro ao buscar stats: {e}")
            return {'total': 0, 'with_market_price': 0, 'without_market_price': 0}
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


if __name__ == "__main__":
    print("="*60)
    print("🧪 TESTE MARKET PRICE CLIENT")
    print("="*60)
    
    try:
        client = MarketPriceSupabaseClient()
        
        print("\n📊 Estatísticas:")
        stats = client.get_stats('veiculos')
        print(f"   • Total: {stats['total']}")
        print(f"   • Com preço: {stats['with_market_price']}")
        print(f"   • Sem preço: {stats['without_market_price']}")
        print(f"   • Progresso: {stats['percentage_complete']}%")
        
        print("\n🔍 Buscando 5 veículos sem preço:")
        vehicles = client.fetch_vehicles_without_price(limit=5)
        print(f"   ✅ {len(vehicles)} veículos encontrados")
        
        for v in vehicles[:3]:
            print(f"   • {v.get('title', 'Sem título')[:60]}")
        
        print("\n" + "="*60)
        print("✅ Testes concluídos!")
        print("="*60)
    
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        print("="*60)
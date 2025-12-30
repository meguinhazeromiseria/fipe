#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MARKET PRICE SCRAPER - VEÍCULOS
Extrai preços FIPE para veículos no banco de dados
"""

import json
import time
import random
import requests
from datetime import datetime
from typing import Dict, Optional, List

from market_price_supabase_client import MarketPriceSupabaseClient
from vehicle_analyzer import VehicleAnalyzer


class FipeAPI:
    """Cliente para API FIPE"""
    
    BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
    
    HEADERS = {
        "Referer": "https://veiculos.fipe.org.br/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://veiculos.fipe.org.br"
    }
    
    # Mapa de tipos
    TIPO_VEICULO = {
        'carros': 1,
        'motos': 2,
        'caminhoes': 3,
        'onibus': 3  # Mesmo código de caminhões
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.ref_table = None
        self.delay = 0.5
    
    def _request(self, endpoint: str, data: dict, retries: int = 3) -> Optional[dict]:
        """Faz request na API com retry"""
        for attempt in range(retries):
            try:
                if attempt > 0:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait)
                
                r = self.session.post(
                    f"{self.BASE_URL}/{endpoint}",
                    json=data,
                    timeout=30
                )
                
                if r.status_code == 429:
                    time.sleep(5)
                    continue
                
                if r.status_code == 200:
                    time.sleep(self.delay)
                    return r.json()
                
            except Exception:
                pass
        
        return None
    
    def get_reference_table(self) -> Optional[int]:
        """Pega tabela de referência atual"""
        if self.ref_table:
            return self.ref_table
        
        data = self._request("ConsultarTabelaDeReferencia", {})
        if data and len(data) > 0:
            self.ref_table = int(data[0]['Codigo'])
            return self.ref_table
        
        return None
    
    def find_brand_code(self, brand_name: str, vehicle_type: str) -> Optional[str]:
        """Encontra código da marca"""
        tipo_cod = self.TIPO_VEICULO.get(vehicle_type, 1)
        ref = self.get_reference_table()
        
        if not ref:
            return None
        
        brands = self._request("ConsultarMarcas", {
            "codigoTabelaReferencia": ref,
            "codigoTipoVeiculo": tipo_cod
        })
        
        if not brands:
            return None
        
        brand_upper = brand_name.upper().strip()
        
        # Busca exata
        for brand in brands:
            if brand['Label'].upper() == brand_upper:
                return brand['Value']
        
        # Busca parcial
        for brand in brands:
            if brand_upper in brand['Label'].upper():
                return brand['Value']
        
        return None
    
    def find_model_code(
        self, 
        brand_code: str, 
        model_name: str, 
        vehicle_type: str
    ) -> Optional[str]:
        """Encontra código do modelo"""
        tipo_cod = self.TIPO_VEICULO.get(vehicle_type, 1)
        ref = self.get_reference_table()
        
        if not ref:
            return None
        
        data = self._request("ConsultarModelos", {
            "codigoTipoVeiculo": tipo_cod,
            "codigoTabelaReferencia": ref,
            "codigoMarca": brand_code
        })
        
        models = data.get('Modelos', []) if data else []
        
        if not models:
            return None
        
        model_upper = model_name.upper().strip() if model_name else ""
        
        # Busca exata
        for model in models:
            if model['Label'].upper() == model_upper:
                return model['Value']
        
        # Busca parcial (pelo menos 2 palavras em comum)
        model_words = set(model_upper.split())
        
        best_match = None
        best_score = 0
        
        for model in models:
            label_words = set(model['Label'].upper().split())
            common_words = model_words & label_words
            
            if len(common_words) > best_score:
                best_score = len(common_words)
                best_match = model['Value']
        
        if best_score >= 2:
            return best_match
        
        return None
    
    def find_year_code(
        self,
        brand_code: str,
        model_code: str,
        year: int,
        vehicle_type: str
    ) -> Optional[str]:
        """Encontra código do ano"""
        tipo_cod = self.TIPO_VEICULO.get(vehicle_type, 1)
        ref = self.get_reference_table()
        
        if not ref:
            return None
        
        years = self._request("ConsultarAnoModelo", {
            "codigoTipoVeiculo": tipo_cod,
            "codigoTabelaReferencia": ref,
            "codigoMarca": brand_code,
            "codigoModelo": model_code
        })
        
        if not years:
            return None
        
        year_str = str(year)
        
        # Busca exata
        for y in years:
            if year_str in y['Label']:
                return y['Value']
        
        return None
    
    def get_price(
        self,
        brand_code: str,
        model_code: str,
        year_code: str,
        vehicle_type: str
    ) -> Optional[Dict]:
        """Busca preço FIPE"""
        tipo_cod = self.TIPO_VEICULO.get(vehicle_type, 1)
        ref = self.get_reference_table()
        
        if not ref:
            return None
        
        # Separa ano e combustível
        if '-' in year_code:
            ano, comb = year_code.split('-')
        else:
            ano = year_code
            comb = '1'
        
        data = self._request("ConsultarValorComTodosParametros", {
            "codigoTipoVeiculo": tipo_cod,
            "codigoTabelaReferencia": ref,
            "codigoMarca": brand_code,
            "codigoModelo": model_code,
            "anoModelo": ano,
            "codigoTipoCombustivel": comb,
            "tipoConsulta": "tradicional"
        })
        
        if not data:
            return None
        
        # Converte valor
        valor_text = data.get('Valor', '')
        valor = None
        
        if valor_text:
            try:
                valor = float(
                    valor_text.replace('R$', '')
                    .replace('.', '')
                    .replace(',', '.')
                    .strip()
                )
            except:
                pass
        
        return {
            'valor': valor,
            'valor_texto': valor_text,
            'marca': data.get('Marca'),
            'modelo': data.get('Modelo'),
            'ano': int(ano) if ano.isdigit() else None,
            'combustivel': data.get('Combustivel'),
            'codigo_fipe': data.get('CodigoFipe'),
            'mes_referencia': data.get('MesReferencia')
        }
    
    def search_vehicle_price(
        self,
        brand: str,
        model: str,
        year: int,
        vehicle_type: str
    ) -> Optional[Dict]:
        """
        Busca completa de preço (marca -> modelo -> ano -> preço)
        
        Returns:
            {
                'valor': float,
                'codigo_fipe': str,
                'marca': str,
                'modelo': str,
                'ano': int,
                'mes_referencia': str
            }
        """
        # 1. Busca código da marca
        brand_code = self.find_brand_code(brand, vehicle_type)
        if not brand_code:
            return None
        
        # 2. Busca código do modelo
        model_code = self.find_model_code(brand_code, model, vehicle_type)
        if not model_code:
            return None
        
        # 3. Busca código do ano
        year_code = self.find_year_code(brand_code, model_code, year, vehicle_type)
        if not year_code:
            return None
        
        # 4. Busca preço
        return self.get_price(brand_code, model_code, year_code, vehicle_type)


class MarketPriceScraper:
    """Scraper principal de market price"""
    
    def __init__(self):
        self.db_client = MarketPriceSupabaseClient()
        self.analyzer = VehicleAnalyzer()
        self.fipe = FipeAPI()
        
        self.stats = {
            'processed': 0,
            'success': 0,
            'not_found': 0,
            'errors': 0,
            'by_type': {}
        }
    
    def process_batch(self, batch_size: int = 50, offset: int = 0) -> bool:
        """
        Processa um batch de veículos
        
        Returns:
            True se encontrou veículos, False se acabou
        """
        print(f"\n{'='*60}")
        print(f"📦 PROCESSANDO BATCH (offset: {offset})")
        print(f"{'='*60}")
        
        # Busca veículos sem preço
        vehicles = self.db_client.fetch_vehicles_without_price(
            limit=batch_size,
            offset=offset
        )
        
        if not vehicles:
            print("✅ Nenhum veículo sem preço encontrado")
            return False
        
        print(f"📋 {len(vehicles)} veículos carregados\n")
        
        for idx, vehicle in enumerate(vehicles, 1):
            self.stats['processed'] += 1
            vehicle_id = vehicle.get('id')
            title = vehicle.get('title', '')[:60]
            
            print(f"[{idx}/{len(vehicles)}] {title}...")
            
            try:
                # Analisa veículo
                analysis = self.analyzer.analyze(vehicle)
                
                vehicle_type = analysis.get('vehicle_type')
                brand = analysis.get('brand')
                model = analysis.get('model')
                year = analysis.get('year_model')
                
                print(f"   🔍 {vehicle_type} | {brand} {model} {year}")
                
                # Só busca FIPE se tiver dados mínimos
                if not brand or not year:
                    print(f"   ⚠️  Dados insuficientes")
                    self.stats['not_found'] += 1
                    continue
                
                # Busca na FIPE
                fipe_data = self.fipe.search_vehicle_price(
                    brand=brand,
                    model=model or "",
                    year=year,
                    vehicle_type=vehicle_type
                )
                
                if fipe_data and fipe_data.get('valor'):
                    # Atualiza DB
                    price_data = {
                        'market_price': fipe_data['valor'],
                        'market_price_source': 'fipe',
                        'market_price_confidence': analysis.get('confidence', 'medium'),
                        'vehicle_type': vehicle_type,
                        'market_price_metadata': {
                            'codigo_fipe': fipe_data.get('codigo_fipe'),
                            'mes_referencia': fipe_data.get('mes_referencia'),
                            'combustivel': fipe_data.get('combustivel'),
                            'marca_fipe': fipe_data.get('marca'),
                            'modelo_fipe': fipe_data.get('modelo'),
                            'ano_fipe': fipe_data.get('ano')
                        }
                    }
                    
                    if self.db_client.update_market_price('veiculos', vehicle_id, price_data):
                        valor_fmt = f"R$ {fipe_data['valor']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                        print(f"   ✅ {valor_fmt}")
                        self.stats['success'] += 1
                        
                        # Contabiliza por tipo
                        self.stats['by_type'][vehicle_type] = self.stats['by_type'].get(vehicle_type, 0) + 1
                    else:
                        print(f"   ❌ Erro ao atualizar DB")
                        self.stats['errors'] += 1
                else:
                    print(f"   ⚠️  Não encontrado na FIPE")
                    self.stats['not_found'] += 1
                
            except Exception as e:
                print(f"   ❌ Erro: {str(e)[:50]}")
                self.stats['errors'] += 1
            
            # Delay entre veículos
            time.sleep(random.uniform(0.5, 1.0))
        
        return True
    
    def run(self, max_batches: int = 10, batch_size: int = 50):
        """Executa scraping completo"""
        print("="*60)
        print("🚗 MARKET PRICE SCRAPER - VEÍCULOS")
        print("="*60)
        
        start_time = time.time()
        
        # Mostra estatísticas iniciais
        stats = self.db_client.get_stats('veiculos')
        print(f"\n📊 ESTATÍSTICAS INICIAIS:")
        print(f"   • Total: {stats['total']}")
        print(f"   • Com preço: {stats['with_market_price']}")
        print(f"   • Sem preço: {stats['without_market_price']}")
        print(f"   • Progresso: {stats['percentage_complete']}%")
        
        # Inicializa FIPE
        print(f"\n🔄 Inicializando API FIPE...")
        ref = self.fipe.get_reference_table()
        if ref:
            print(f"   ✅ Referência: {ref}")
        else:
            print(f"   ❌ Erro ao conectar com FIPE")
            return
        
        # Processa batches
        offset = 0
        batch_num = 0
        
        while batch_num < max_batches:
            has_more = self.process_batch(batch_size, offset)
            
            if not has_more:
                break
            
            offset += batch_size
            batch_num += 1
            
            # Delay entre batches
            if batch_num < max_batches:
                print(f"\n⏳ Aguardando 5s antes do próximo batch...")
                time.sleep(5)
        
        # Estatísticas finais
        elapsed = time.time() - start_time
        
        print(f"\n{'='*60}")
        print(f"✅ SCRAPING CONCLUÍDO")
        print(f"{'='*60}")
        print(f"   • Processados: {self.stats['processed']}")
        print(f"   • Sucesso: {self.stats['success']}")
        print(f"   • Não encontrados: {self.stats['not_found']}")
        print(f"   • Erros: {self.stats['errors']}")
        
        if self.stats['by_type']:
            print(f"\n   📊 Por tipo:")
            for vtype, count in self.stats['by_type'].items():
                print(f"      • {vtype}: {count}")
        
        print(f"\n   ⏱️  Tempo: {elapsed/60:.1f}min")
        
        # Estatísticas finais do DB
        final_stats = self.db_client.get_stats('veiculos')
        print(f"\n   📊 PROGRESSO FINAL:")
        print(f"      • Com preço: {final_stats['with_market_price']}")
        print(f"      • Sem preço: {final_stats['without_market_price']}")
        print(f"      • Completo: {final_stats['percentage_complete']}%")
        
        print(f"{'='*60}")


if __name__ == "__main__":
    print("="*60)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    scraper = MarketPriceScraper()
    
    # Processa até 10 batches de 50 veículos (500 total)
    scraper.run(max_batches=10, batch_size=50)
    
    print(f"\n📅 Término: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
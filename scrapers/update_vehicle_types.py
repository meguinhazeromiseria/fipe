#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UPDATE VEHICLE TYPES
Atualiza vehicle_type para registros existentes
"""

import time
import random
from datetime import datetime
from market_price_supabase_client import MarketPriceSupabaseClient
from vehicle_analyzer import VehicleAnalyzer


def update_vehicle_types_batch(batch_size: int = 100, max_batches: int = 50):
    """Atualiza vehicle_type em batches"""
    print("="*60)
    print("🔄 ATUALIZAR VEHICLE_TYPE - VEÍCULOS EXISTENTES")
    print("="*60)
    
    client = MarketPriceSupabaseClient()
    analyzer = VehicleAnalyzer()
    
    stats = {
        'processed': 0,
        'updated': 0,
        'errors': 0,
        'by_type': {}
    }
    
    offset = 0
    batch_num = 0
    
    while batch_num < max_batches:
        print(f"\n{'='*60}")
        print(f"📦 BATCH {batch_num + 1} (offset: {offset})")
        print(f"{'='*60}")
        
        # Busca veículos ativos (independente de market_price)
        try:
            url = f"{client.url}/rest/v1/veiculos"
            params = {
                'select': 'id,title,normalized_title,description,metadata,vehicle_type',
                'is_active': 'eq.true',
                'limit': batch_size,
                'offset': offset,
                'order': 'created_at.desc'
            }
            
            r = client.session.get(url, params=params, timeout=30)
            
            if r.status_code != 200:
                print(f"❌ Erro ao buscar: {r.status_code}")
                break
            
            vehicles = r.json()
            
            if not vehicles:
                print("✅ Fim dos registros")
                break
            
            print(f"📋 {len(vehicles)} veículos carregados\n")
            
            # Processa cada veículo
            for idx, vehicle in enumerate(vehicles, 1):
                stats['processed'] += 1
                vehicle_id = vehicle.get('id')
                current_type = vehicle.get('vehicle_type')
                title = vehicle.get('title', '')[:50]
                
                # Pula se já tem tipo
                if current_type:
                    continue
                
                print(f"[{idx}/{len(vehicles)}] {title}...")
                
                try:
                    # Analisa veículo
                    analysis = analyzer.analyze(vehicle)
                    vehicle_type = analysis.get('vehicle_type')
                    
                    if vehicle_type:
                        # Atualiza apenas vehicle_type
                        update_url = f"{client.url}/rest/v1/veiculos"
                        update_data = {'vehicle_type': vehicle_type}
                        update_params = {'id': f'eq.{vehicle_id}'}
                        
                        r_update = client.session.patch(
                            update_url,
                            json=update_data,
                            params=update_params,
                            timeout=30
                        )
                        
                        if r_update.status_code in (200, 204):
                            print(f"   ✅ {vehicle_type}")
                            stats['updated'] += 1
                            stats['by_type'][vehicle_type] = stats['by_type'].get(vehicle_type, 0) + 1
                        else:
                            print(f"   ❌ Erro: {r_update.status_code}")
                            stats['errors'] += 1
                    else:
                        print(f"   ⚠️  Tipo não identificado")
                
                except Exception as e:
                    print(f"   ❌ Erro: {str(e)[:40]}")
                    stats['errors'] += 1
                
                # Delay
                time.sleep(random.uniform(0.1, 0.3))
            
        except Exception as e:
            print(f"❌ Erro no batch: {e}")
            break
        
        offset += batch_size
        batch_num += 1
        
        # Delay entre batches
        if batch_num < max_batches:
            time.sleep(2)
    
    # Resumo
    print(f"\n{'='*60}")
    print(f"✅ ATUALIZAÇÃO CONCLUÍDA")
    print(f"{'='*60}")
    print(f"   • Processados: {stats['processed']}")
    print(f"   • Atualizados: {stats['updated']}")
    print(f"   • Erros: {stats['errors']}")
    
    if stats['by_type']:
        print(f"\n   📊 Por tipo:")
        for vtype, count in stats['by_type'].items():
            print(f"      • {vtype}: {count}")
    
    print(f"{'='*60}")


if __name__ == "__main__":
    print("="*60)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Atualiza até 50 batches de 100 (5000 total)
    update_vehicle_types_batch(batch_size=100, max_batches=50)
    
    print(f"\n📅 Término: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
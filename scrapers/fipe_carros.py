#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FIPE SCRAPER - CARROS (VERSÃO ROBUSTA COM UPLOAD INCREMENTAL)
Extrai tabela FIPE e faz upload a cada marca processada
"""

import json
import requests
import time
import random
from datetime import datetime
from pathlib import Path
from fipe_supabase_client import FipeSupabaseClient

# Config
OUTPUT_FILE = Path("fipe_carros.json")
FIPE_API = "https://veiculos.fipe.org.br/api/veiculos"

HEADERS = {
    "Referer": "https://veiculos.fipe.org.br/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://veiculos.fipe.org.br"
}

# Delays configuráveis
DELAY_BETWEEN_REQUESTS = 0.5
DELAY_BETWEEN_MODELS = 0.3
DELAY_BETWEEN_BRANDS = 2.0
MAX_RETRIES = 5


def request_api(endpoint, data, retries=MAX_RETRIES):
    """Faz request na API FIPE com retry exponencial"""
    for attempt in range(retries):
        try:
            if attempt > 0:
                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"      ⏳ Retry {attempt}/{retries} após {wait:.1f}s...")
                time.sleep(wait)
            
            r = requests.post(
                f"{FIPE_API}/{endpoint}", 
                json=data, 
                headers=HEADERS, 
                timeout=30
            )
            
            if r.status_code == 429:
                print(f"      ⚠️  Rate limit detectado (429)")
                time.sleep(5 + random.uniform(0, 2))
                continue
            
            if r.status_code == 500:
                print(f"      ⚠️  Erro do servidor (500)")
                time.sleep(3)
                continue
            
            if r.status_code == 200:
                time.sleep(DELAY_BETWEEN_REQUESTS + random.uniform(0, 0.2))
                return r.json()
            
            print(f"      ⚠️  Status {r.status_code}: {r.text[:100]}")
            
        except requests.exceptions.Timeout:
            print(f"      ⏱️  Timeout na tentativa {attempt+1}")
        except requests.exceptions.ConnectionError:
            print(f"      🔌 Erro de conexão na tentativa {attempt+1}")
        except Exception as e:
            print(f"      ❌ Erro: {str(e)[:80]}")
    
    print(f"      ❌ Falhou após {retries} tentativas")
    return None


def get_ref_table():
    """Pega mês de referência atual"""
    data = request_api("ConsultarTabelaDeReferencia", {})
    return int(data[0]['Codigo']) if data else None


def get_marcas(ref):
    """Lista marcas de carros"""
    data = request_api("ConsultarMarcas", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo": 1
    })
    
    if not data:
        print(f"   ⚠️  API retornou vazio para carros")
    
    return data or []


def get_modelos(marca, ref):
    """Lista modelos da marca"""
    data = request_api("ConsultarModelos", {
        "codigoTipoVeiculo": 1,
        "codigoTabelaReferencia": ref,
        "codigoMarca": marca
    })
    
    modelos = data.get('Modelos', []) if data else []
    
    if not modelos:
        print(f"      ⚠️  Nenhum modelo encontrado")
    
    return modelos


def get_anos(marca, modelo, ref):
    """Lista anos do modelo"""
    anos = request_api("ConsultarAnoModelo", {
        "codigoTipoVeiculo": 1,
        "codigoTabelaReferencia": ref,
        "codigoMarca": marca,
        "codigoModelo": modelo
    }) or []
    
    time.sleep(DELAY_BETWEEN_MODELS + random.uniform(0, 0.1))
    
    return anos


def get_preco(marca, modelo, ano_cod, ref):
    """Pega preço do carro"""
    ano, comb = ano_cod.split('-') if '-' in ano_cod else (ano_cod, '1')
    
    data = request_api("ConsultarValorComTodosParametros", {
        "codigoTipoVeiculo": 1,
        "codigoTabelaReferencia": ref,
        "codigoMarca": marca,
        "codigoModelo": modelo,
        "anoModelo": ano,
        "codigoTipoCombustivel": comb,
        "tipoConsulta": "tradicional"
    })
    
    if not data:
        return None
    
    valor_text = data.get('Valor', '')
    valor = None
    if valor_text:
        try:
            valor = float(valor_text.replace('R$','').replace('.','').replace(',','.').strip())
        except:
            pass
    
    return {
        'tipo': 'carros',
        'marca': data.get('Marca'),
        'modelo': data.get('Modelo'),
        'ano': int(ano) if ano.isdigit() else None,
        'combustivel': data.get('Combustivel'),
        'codigo_fipe': data.get('CodigoFipe'),
        'valor': valor,
        'valor_texto': valor_text,
        'mes_referencia': data.get('MesReferencia'),
        'coletado_em': datetime.now().isoformat()
    }


def salvar_json(veiculos):
    """Salva lista em JSON"""
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(veiculos, f, ensure_ascii=False, indent=2)


def upload_para_supabase(veiculos):
    """Faz upload incremental para Supabase"""
    if not veiculos:
        return {'inserted': 0, 'updated': 0, 'errors': 0}
    
    try:
        client = FipeSupabaseClient()
        result = client.upsert_vehicles(veiculos)
        return result
    except Exception as e:
        print(f"      ❌ Erro no upload: {str(e)[:100]}")
        return {'inserted': 0, 'updated': 0, 'errors': len(veiculos)}


def process_modelo_sequencial(marca_cod, modelo, ref):
    """Processa um modelo completo"""
    modelo_nome = modelo['Label']
    modelo_cod = modelo['Value']
    
    anos = get_anos(marca_cod, modelo_cod, ref)
    
    if not anos:
        return []
    
    veiculos_modelo = []
    
    for ano in anos:
        resultado = get_preco(marca_cod, modelo_cod, ano['Value'], ref)
        if resultado:
            veiculos_modelo.append(resultado)
    
    return veiculos_modelo


def main():
    print("="*60)
    print("🚗 FIPE SCRAPER - CARROS (UPLOAD INCREMENTAL)")
    print("="*60)
    
    print(f"\n⚙️  CONFIGURAÇÃO:")
    print(f"   • Delay entre requests: {DELAY_BETWEEN_REQUESTS}s")
    print(f"   • Delay entre modelos: {DELAY_BETWEEN_MODELS}s")
    print(f"   • Delay entre marcas: {DELAY_BETWEEN_BRANDS}s")
    print(f"   • Upload: A CADA MARCA processada")
    print()
    
    ref = get_ref_table()
    if not ref:
        print("❌ Erro ao buscar referência")
        return
    
    print(f"📅 Referência: {ref}")
    
    marcas = get_marcas(ref)
    print(f"✅ {len(marcas)} marcas de carros\n")
    
    veiculos_total = []
    total_marcas = len(marcas)
    start_time = time.time()
    
    stats_upload = {'inserted': 0, 'updated': 0, 'errors': 0}
    
    for idx_marca, marca in enumerate(marcas, 1):
        marca_nome = marca['Label']
        marca_cod = marca['Value']
        
        elapsed = time.time() - start_time
        eta_per_brand = elapsed / idx_marca if idx_marca > 0 else 0
        eta_remaining = eta_per_brand * (total_marcas - idx_marca)
        
        print(f"{'='*60}")
        print(f"[{idx_marca}/{total_marcas}] 🏭 {marca_nome}")
        print(f"⏱️  {elapsed/60:.1f}min | ETA: {eta_remaining/60:.1f}min")
        print(f"{'='*60}")
        
        modelos = get_modelos(marca_cod, ref)
        print(f"   📋 {len(modelos)} modelos")
        
        if not modelos:
            print(f"   ⚠️  Pulando marca\n")
            time.sleep(DELAY_BETWEEN_BRANDS)
            continue
        
        veiculos_marca = []
        
        for idx_modelo, modelo in enumerate(modelos, 1):
            modelo_nome = modelo['Label']
            print(f"      [{idx_modelo}/{len(modelos)}] {modelo_nome}...", end=" ")
            
            try:
                veiculos_modelo = process_modelo_sequencial(marca_cod, modelo, ref)
                
                if veiculos_modelo:
                    veiculos_marca.extend(veiculos_modelo)
                    print(f"✅ {len(veiculos_modelo)}")
                else:
                    print(f"⚠️  0")
                    
            except Exception as e:
                print(f"❌ {str(e)[:40]}")
        
        # Upload da marca para Supabase
        if veiculos_marca:
            print(f"\n   📤 Enviando {len(veiculos_marca)} veículos para Supabase...")
            result = upload_para_supabase(veiculos_marca)
            
            stats_upload['inserted'] += result.get('inserted', 0)
            stats_upload['updated'] += result.get('updated', 0)
            stats_upload['errors'] += result.get('errors', 0)
            
            print(f"   ✅ Upload: +{result.get('inserted', 0)} novos, "
                  f"~{result.get('updated', 0)} atualizados")
            
            # Adiciona ao total para backup JSON
            veiculos_total.extend(veiculos_marca)
        
        print(f"   📊 Marca: {len(veiculos_marca)} | Total: {len(veiculos_total)}")
        
        # Backup JSON local
        salvar_json(veiculos_total)
        print(f"   💾 Backup JSON: {len(veiculos_total)} carros\n")
        
        if idx_marca < total_marcas:
            time.sleep(DELAY_BETWEEN_BRANDS)
    
    total_time = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"✅ SCRAPING CONCLUÍDO")
    print(f"   • Veículos coletados: {len(veiculos_total)}")
    print(f"   • Enviados ao Supabase:")
    print(f"     - Novos: {stats_upload['inserted']}")
    print(f"     - Atualizados: {stats_upload['updated']}")
    print(f"     - Erros: {stats_upload['errors']}")
    print(f"   • Tempo total: {total_time/60:.1f}min")
    print(f"   • Média por marca: {total_time/total_marcas:.1f}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
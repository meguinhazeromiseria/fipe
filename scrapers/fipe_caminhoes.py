#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FIPE SCRAPER - CAMINHÕES
Extrai tabela FIPE de caminhões e envia para Supabase
"""

import json
import requests
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from fipe_supabase_client import FipeSupabaseClient

# Config
OUTPUT_FILE = Path("fipe_caminhoes.json")
FIPE_API = "https://veiculos.fipe.org.br/api/veiculos"

HEADERS = {
    "Referer": "https://veiculos.fipe.org.br/",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json"
}


def request_api(endpoint, data):
    """Faz request na API FIPE"""
    try:
        r = requests.post(f"{FIPE_API}/{endpoint}", json=data, headers=HEADERS, timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None


def get_ref_table():
    """Pega mês de referência atual"""
    data = request_api("ConsultarTabelaDeReferencia", {})
    return int(data[0]['Codigo']) if data else None


def get_marcas(ref):
    """Lista marcas de caminhões"""
    data = request_api("ConsultarMarcas", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo": 3  # Caminhões
    })
    
    if not data:
        print(f"   ⚠️  API retornou vazio para caminhões")
        print(f"   Ref: {ref}")
    
    return data or []


def get_modelos(marca, ref):
    """Lista modelos da marca"""
    data = request_api("ConsultarModelos", {
        "codigoTipoVeiculo": 3,
        "codigoTabelaReferencia": ref,
        "codigoMarca": marca
    })
    return data.get('Modelos', []) if data else []


def get_anos(marca, modelo, ref):
    """Lista anos do modelo"""
    return request_api("ConsultarAnoModelo", {
        "codigoTipoVeiculo": 3,
        "codigoTabelaReferencia": ref,
        "codigoMarca": marca,
        "codigoModelo": modelo
    }) or []


def get_preco(marca, modelo, ano_cod, ref):
    """Pega preço do caminhão"""
    ano, comb = ano_cod.split('-') if '-' in ano_cod else (ano_cod, '1')
    
    data = request_api("ConsultarValorComTodosParametros", {
        "codigoTipoVeiculo": 3,
        "codigoTabelaReferencia": ref,
        "codigoMarca": marca,
        "codigoModelo": modelo,
        "anoModelo": ano,
        "codigoTipoCombustivel": comb,
        "tipoConsulta": "tradicional"
    })
    
    if not data:
        return None
    
    # Parse do valor
    valor_text = data.get('Valor', '')
    valor = None
    if valor_text:
        try:
            valor = float(valor_text.replace('R$','').replace('.','').replace(',','.').strip())
        except:
            pass
    
    return {
        'tipo': 'caminhoes',
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


def process_ano(args):
    """Processa um ano (paralelo)"""
    marca_cod, modelo_cod, ano, ref = args
    return get_preco(marca_cod, modelo_cod, ano['Value'], ref)


def main():
    print("="*60)
    print("🚚 FIPE SCRAPER - CAMINHÕES")
    print("="*60)
    
    ref = get_ref_table()
    if not ref:
        print("❌ Erro ao buscar referência")
        return
    
    print(f"📅 Referência: {ref}")
    
    marcas = get_marcas(ref)
    print(f"✅ {len(marcas)} marcas de caminhões")
    
    veiculos = []
    total_marcas = len(marcas)
    
    for idx_marca, marca in enumerate(marcas, 1):
        marca_nome = marca['Label']
        marca_cod = marca['Value']
        
        print(f"\n[{idx_marca}/{total_marcas}] 🏭 {marca_nome}")
        
        modelos = get_modelos(marca_cod, ref)
        print(f"   📋 {len(modelos)} modelos")
        
        veiculos_marca = []  # Contador por marca
        
        for modelo in modelos:
            modelo_nome = modelo['Label']
            modelo_cod = modelo['Value']
            
            anos = get_anos(marca_cod, modelo_cod, ref)
            
            if not anos:
                continue
            
            # Processa anos em paralelo
            with ThreadPoolExecutor(max_workers=5) as executor:
                tasks = [
                    (marca_cod, modelo_cod, ano, ref)
                    for ano in anos
                ]
                
                futures = [executor.submit(process_ano, task) for task in tasks]
                
                for future in as_completed(futures):
                    try:
                        resultado = future.result()
                        if resultado:
                            veiculos.append(resultado)
                            veiculos_marca.append(resultado)
                    except:
                        pass
            
            print(f"      ✅ {modelo_nome}: {len(anos)} anos")
        
        print(f"   📊 {marca_nome}: +{len(veiculos_marca)} veículos | Total geral: {len(veiculos)}")
        
        # Salva a cada marca
        salvar_json(veiculos)
        print(f"   💾 Salvou {len(veiculos)} caminhões em {OUTPUT_FILE}")
    
    print(f"\n{'='*60}")
    print(f"✅ SCRAPING CONCLUÍDO: {len(veiculos)} caminhões")
    print(f"💾 JSON salvo em: {OUTPUT_FILE}")
    print(f"{'='*60}")
    
    # Envia para Supabase
    print(f"\n{'='*60}")
    print("🚀 ENVIANDO PARA SUPABASE...")
    print(f"{'='*60}")
    
    try:
        client = FipeSupabaseClient()
        result = client.upsert_vehicles(veiculos)
        
        print(f"\n{'='*60}")
        print(f"✅ UPLOAD CONCLUÍDO")
        print(f"   • {result['inserted']} novos registros")
        print(f"   • {result['updated']} atualizados")
        print(f"   • {result['errors']} erros")
        print(f"   • Tempo: {result['time_ms']}ms")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"❌ Erro ao enviar para Supabase: {e}")
        print("   (JSON salvo localmente)")


if __name__ == "__main__":
    main()
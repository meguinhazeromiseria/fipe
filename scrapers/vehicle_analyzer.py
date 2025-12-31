#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FIPE SMART SEARCHER V2
Sistema inteligente de busca na FIPE com múltiplas estratégias de fallback
"""

import re
from typing import Dict, Optional, List, Tuple
from difflib import SequenceMatcher


class FIPESmartSearcher:
    """Busca inteligente na FIPE com normalização e fallback strategies"""
    
    # Mapeamento de versões comuns para simplificação
    VERSION_SIMPLIFICATIONS = {
        # Remove sufixos técnicos comuns
        r'\s+(AT|MT|CVT|AUT|MAN|MANUAL|AUTOMATICO|AUTOMATICA)\b': '',
        r'\s+\d+\.\d+[BGL]?\b': '',  # Remove "1.0B", "1.6L", etc
        r'\s+(4X2|4X4|6X2|6X4)\b': '',  # Tração
        r'\s+(FLEX|GAS|DIESEL|GASOLINA)\b': '',
        r'\s+(ABS|CBS|EBS)\b': '',  # Sistemas de freio
        r'\s+(CD|CS|CE|CABINE)\s*(DUPLA|SIMPLES|ESTENDIDA)?\b': '',
        # Códigos específicos de versão
        r'\s+[A-Z]{2,}\d+[A-Z]\d+[A-Z]\b': '',  # Ex: XLSCD4A22C
        r'\s+(EX2?|LX|LTZ?|SL|SR|SE|ST|S|LIFE|JOY)\s*(OFFG4)?\b': '',  # Versões
    }
    
    # Normalizações de modelos conhecidos
    MODEL_NORMALIZATIONS = {
        # Motos Honda
        r'CB\s*(\d+)F?': r'CB \1F',
        r'CG\s*(\d+)': r'CG \1',
        r'BIZ\s*(\d+)': r'BIZ \1',
        r'POP\s*(\d+)I?': r'POP \1',
        r'FAN\s*(\d+)': r'FAN \1',
        
        # Motos Yamaha
        r'YBR\s*(\d+)': r'YBR \1',
        r'YS\s*(\d+)': r'YS \1',
        r'FZ\s*(\d+)': r'FZ \1',
        r'MT[\-\s]*(\d+)': r'MT-\1',
        r'XTZ\s*(\d+)': r'XTZ \1',
        
        # Motos Kawasaki
        r'Z\s*(\d+)': r'Z \1',
        
        # Carros comuns
        r'KA\s+SE': 'KA',
        r'ECOSPORT\s+SE': 'ECOSPORT',
        r'ONIX\s+\d+MT': 'ONIX',
        r'CORSA\s+(HATCH|SEDAN)': r'CORSA \1',
        
        # Caminhões/Pickups
        r'HILUX\s+[A-Z]{4,}': 'HILUX',
        r'RANGER\s+[A-Z]{4,}': 'RANGER',
        r'S10\s+[A-Z]{3,}': 'S10',
        r'STRADA\s+(HD|ENDURANCE)': r'STRADA \1',
    }
    
    def __init__(self):
        self.compiled_simplifications = [
            (re.compile(pattern, re.IGNORECASE), repl) 
            for pattern, repl in self.VERSION_SIMPLIFICATIONS.items()
        ]
        self.compiled_normalizations = [
            (re.compile(pattern, re.IGNORECASE), repl)
            for pattern, repl in self.MODEL_NORMALIZATIONS.items()
        ]
    
    def normalize_vehicle_name(self, brand: str, model: str, year: int) -> List[str]:
        """
        Gera múltiplas variações normalizadas do nome do veículo
        Retorna lista ordenada por probabilidade de sucesso
        """
        variations = []
        
        # 1. Normaliza marca
        brand_clean = self._normalize_brand(brand)
        
        # 2. Normaliza modelo
        model_clean = self._normalize_model(model)
        
        # 3. Variação COMPLETA (como está)
        variations.append(f"{brand_clean} {model_clean} {year}")
        
        # 4. Remove versões específicas
        model_simple = self._simplify_version(model_clean)
        if model_simple != model_clean:
            variations.append(f"{brand_clean} {model_simple} {year}")
        
        # 5. Apenas primeiras palavras do modelo (mais genérico)
        model_words = model_simple.split()
        if len(model_words) > 2:
            model_basic = ' '.join(model_words[:2])
            variations.append(f"{brand_clean} {model_basic} {year}")
        
        if len(model_words) > 1:
            model_minimal = model_words[0]
            variations.append(f"{brand_clean} {model_minimal} {year}")
        
        # Remove duplicatas mantendo ordem
        seen = set()
        unique_variations = []
        for v in variations:
            v_upper = v.upper()
            if v_upper not in seen:
                seen.add(v_upper)
                unique_variations.append(v)
        
        return unique_variations
    
    def _normalize_brand(self, brand: str) -> str:
        """Normaliza nome da marca"""
        brand_upper = brand.upper().strip()
        
        # Mapeamento de marcas
        brand_map = {
            'VW': 'VOLKSWAGEN',
            'MERCEDES': 'MERCEDES-BENZ',
            'BMW MOTORRAD': 'BMW',
            'HARLEY': 'HARLEY-DAVIDSON',
            'ROYAL': 'ROYAL ENFIELD',
        }
        
        return brand_map.get(brand_upper, brand_upper)
    
    def _normalize_model(self, model: str) -> str:
        """Normaliza modelo aplicando regras conhecidas"""
        model_clean = model.upper().strip()
        
        # Aplica normalizações específicas
        for pattern, replacement in self.compiled_normalizations:
            model_clean = pattern.sub(replacement, model_clean)
        
        # Capitaliza corretamente
        model_clean = self._capitalize_model(model_clean)
        
        return model_clean.strip()
    
    def _simplify_version(self, model: str) -> str:
        """Remove versões e códigos específicos"""
        simplified = model
        
        for pattern, replacement in self.compiled_simplifications:
            simplified = pattern.sub(replacement, simplified)
        
        return simplified.strip()
    
    def _capitalize_model(self, model: str) -> str:
        """
        Capitaliza modelo mantendo siglas em maiúsculo
        Ex: "CB 300F" mantém assim, "FACTOR ED" vira "Factor Ed"
        """
        words = []
        for word in model.split():
            # Mantém números e códigos curtos em maiúsculo
            if word.isdigit() or len(word) <= 3 or word.isupper():
                words.append(word)
            else:
                words.append(word.capitalize())
        
        return ' '.join(words)
    
    def fuzzy_match(self, query: str, candidates: List[str], threshold: float = 0.8) -> Optional[Tuple[str, float]]:
        """
        Encontra melhor match usando fuzzy matching
        Retorna (melhor_candidato, score) ou None
        """
        if not candidates:
            return None
        
        query_clean = self._clean_for_comparison(query)
        best_match = None
        best_score = 0.0
        
        for candidate in candidates:
            candidate_clean = self._clean_for_comparison(candidate)
            
            # Calcula similaridade
            score = SequenceMatcher(None, query_clean, candidate_clean).ratio()
            
            # Bônus se palavras-chave coincidem
            query_words = set(query_clean.split())
            candidate_words = set(candidate_clean.split())
            word_overlap = len(query_words & candidate_words) / max(len(query_words), 1)
            
            final_score = (score * 0.7) + (word_overlap * 0.3)
            
            if final_score > best_score:
                best_score = final_score
                best_match = candidate
        
        if best_score >= threshold:
            return (best_match, best_score)
        
        return None
    
    def _clean_for_comparison(self, text: str) -> str:
        """Limpa texto para comparação fuzzy"""
        # Remove pontuação e caracteres especiais
        clean = re.sub(r'[^\w\s]', '', text.upper())
        # Remove espaços múltiplos
        clean = re.sub(r'\s+', ' ', clean)
        return clean.strip()
    
    def search_with_fallback(self, brand: str, model: str, year: int, 
                           fipe_search_func) -> Optional[Dict]:
        """
        Busca com múltiplas estratégias de fallback
        
        Args:
            brand: Marca do veículo
            model: Modelo do veículo
            year: Ano do veículo
            fipe_search_func: Função que recebe (vehicle_name) e retorna resultado FIPE
            
        Returns:
            Resultado da FIPE ou None
        """
        # Gera variações do nome
        variations = self.normalize_vehicle_name(brand, model, year)
        
        print(f"   🔍 Tentando {len(variations)} variações:")
        
        # Tenta cada variação em ordem
        for i, variation in enumerate(variations, 1):
            print(f"      [{i}] {variation}")
            
            result = fipe_search_func(variation)
            
            if result:
                print(f"      ✅ Encontrado com variação #{i}")
                return result
        
        print(f"      ❌ Nenhuma variação encontrou resultado")
        return None


class ImprovedVehicleAnalyzer:
    """
    Analyzer melhorado que integra com FIPESmartSearcher
    """
    
    def __init__(self):
        self.base_analyzer = VehicleAnalyzer()  # Seu analyzer original
        self.fipe_searcher = FIPESmartSearcher()
    
    def analyze_for_fipe(self, vehicle: Dict) -> Dict:
        """Analisa veículo e prepara para busca FIPE"""
        # Usa analyzer base
        analysis = self.base_analyzer.analyze(vehicle)
        
        brand = analysis.get('brand')
        model = analysis.get('model')
        year = analysis.get('year_model')
        
        if not all([brand, model, year]):
            return {
                **analysis,
                'fipe_ready': False,
                'fipe_variations': []
            }
        
        # Gera variações para FIPE
        variations = self.fipe_searcher.normalize_vehicle_name(brand, model, year)
        
        return {
            **analysis,
            'fipe_ready': True,
            'fipe_variations': variations,
            'fipe_primary': variations[0] if variations else None
        }
    
    def search_fipe(self, vehicle: Dict, fipe_api_func) -> Optional[Dict]:
        """Busca na FIPE com fallback inteligente"""
        analysis = self.analyze_for_fipe(vehicle)
        
        if not analysis['fipe_ready']:
            print(f"   ⚠️  Dados insuficientes: {analysis}")
            return None
        
        return self.fipe_searcher.search_with_fallback(
            brand=analysis['brand'],
            model=analysis['model'],
            year=analysis['year_model'],
            fipe_search_func=fipe_api_func
        )


# ============================================================================
# EXEMPLO DE USO INTEGRADO
# ============================================================================

def example_integration():
    """Exemplo de como integrar no seu script de scraping"""
    
    print("="*60)
    print("🧪 TESTE INTEGRAÇÃO FIPE SMART SEARCHER")
    print("="*60)
    
    # Simula função de busca FIPE
    def mock_fipe_search(vehicle_name: str) -> Optional[Dict]:
        """Mock da API FIPE"""
        # Simula base de dados FIPE (simplificada)
        fipe_database = [
            "FIAT Uno Vivace 1.0 2016",
            "HONDA CB 300F Twister 2024",
            "HONDA BIZ 125 2020",
            "YAMAHA YBR 150 Factor 2022",
            "FORD KA 2017",
            "VOLKSWAGEN Gol 2012",
        ]
        
        # Busca exata
        for entry in fipe_database:
            if entry.upper() == vehicle_name.upper():
                return {"name": entry, "price": "R$ 50.000,00"}
        
        # Busca aproximada
        searcher = FIPESmartSearcher()
        match = searcher.fuzzy_match(vehicle_name, fipe_database, threshold=0.75)
        
        if match:
            matched_name, score = match
            return {"name": matched_name, "price": "R$ 45.000,00", "fuzzy": True, "score": score}
        
        return None
    
    # Casos de teste do seu log
    test_vehicles = [
        {
            'title': 'yamaha - ybr150 factor ed',
            'normalized_title': 'YAMAHA YBR150 FACTOR ED 2022',
            'description': 'Moto Yamaha YBR 150 Factor ED ano 2022',
        },
        {
            'title': 'honda - cb300f twister abs',
            'normalized_title': 'HONDA CB300F TWISTER ABS 2024',
            'description': 'Moto Honda CB 300F Twister ABS',
        },
        {
            'title': 'ford - ka se 1.0 ha b',
            'normalized_title': 'FORD KA SE 1.0 HA B 2017',
            'description': 'Ford Ka SE 1.0',
        },
        {
            'title': 'fiat - uno vivace 1.0',
            'normalized_title': 'FIAT UNO VIVACE 1.0 2016',
            'description': 'Fiat Uno Vivace',
        },
    ]
    
    analyzer = ImprovedVehicleAnalyzer()
    
    print("\n")
    for i, vehicle in enumerate(test_vehicles, 1):
        print(f"[{i}/{len(test_vehicles)}] {vehicle['title']}...")
        
        # Analisa veículo
        analysis = analyzer.analyze_for_fipe(vehicle)
        print(f"   📋 Tipo: {analysis['vehicle_type']}")
        print(f"   🏭 Marca: {analysis['brand']}")
        print(f"   🚗 Modelo: {analysis['model']}")
        print(f"   📅 Ano: {analysis['year']}")
        
        if analysis['fipe_ready']:
            # Busca na FIPE
            result = analyzer.search_fipe(vehicle, mock_fipe_search)
            
            if result:
                fuzzy_label = f" (match {result.get('score', 0):.0%})" if result.get('fuzzy') else ""
                print(f"   ✅ {result['price']} - {result['name']}{fuzzy_label}")
            else:
                print(f"   ⚠️  Não encontrado na FIPE")
        else:
            print(f"   ⚠️  Dados insuficientes")
        
        print()
    
    print("="*60)


if __name__ == "__main__":
    # Teste standalone
    example_integration()
    
    print("\n" + "="*60)
    print("💡 DICAS DE INTEGRAÇÃO:")
    print("="*60)
    print("""
1. Substitua seu VehicleAnalyzer por ImprovedVehicleAnalyzer
2. Use search_fipe() passando sua função real de API FIPE
3. O sistema tentará múltiplas variações automaticamente
4. Fuzzy matching encontra nomes aproximados (>75% similar)
5. Logs detalhados mostram qual variação funcionou
    """)
    print("="*60)
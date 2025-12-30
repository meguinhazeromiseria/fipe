#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VEHICLE ANALYZER
Extrai tipo, marca, modelo e ano de veículos
"""

import re
from typing import Dict, Optional, Tuple


class VehicleAnalyzer:
    """Analisa títulos de veículos e extrai informações estruturadas"""
    
    # Tipos de veículos
    VEHICLE_TYPES = {
        'carros': [
            'carro', 'automovel', 'automóvel', 'sedan', 'hatch', 'suv', 
            'pickup', 'crossover', 'station', 'wagon', 'van'
        ],
        'motos': [
            'moto', 'motocicleta', 'scooter', 'ciclomotor', 'triciclo motorizado'
        ],
        'caminhoes': [
            'caminhao', 'caminhão', 'truck', 'bitruck', 'rodotrem'
        ],
        'onibus': [
            'onibus', 'ônibus', 'micro-onibus', 'microônibus', 'bus'
        ],
        'embarcacoes': [
            'barco', 'lancha', 'jet ski', 'jetski', 'moto aquatica', 
            'moto aquática', 'embarcacao', 'embarcação', 'iate', 'veleiro'
        ],
        'aeronaves': [
            'aviao', 'avião', 'aeronave', 'helicóptero', 'helicoptero', 'ultraleve'
        ],
        'implementos': [
            'reboque', 'carreta', 'semirreboque', 'implemento', 'trailer'
        ]
    }
    
    # Marcas conhecidas por tipo
    BRANDS = {
        'carros': [
            'AUDI', 'BMW', 'BYD', 'CAOA', 'CHEVROLET', 'CHERY', 'CITROEN',
            'FIAT', 'FORD', 'GWM', 'HONDA', 'HYUNDAI', 'JEEP', 'KIA',
            'LAND ROVER', 'MAZDA', 'MERCEDES', 'MERCEDES-BENZ', 'MITSUBISHI',
            'NISSAN', 'PEUGEOT', 'PORSCHE', 'RENAULT', 'SUBARU', 'SUZUKI',
            'TOYOTA', 'VOLKSWAGEN', 'VW', 'VOLVO', 'JAC', 'LIFAN'
        ],
        'motos': [
            'HONDA', 'YAMAHA', 'SUZUKI', 'KAWASAKI', 'HARLEY-DAVIDSON',
            'HARLEY', 'BMW', 'DUCATI', 'TRIUMPH', 'KTM', 'ROYAL ENFIELD'
        ],
        'caminhoes': [
            'VOLVO', 'SCANIA', 'MERCEDES', 'MERCEDES-BENZ', 'IVECO',
            'DAF', 'MAN', 'FORD', 'VOLKSWAGEN', 'VW'
        ]
    }
    
    def __init__(self):
        # Compila patterns para melhor performance
        self.year_pattern = re.compile(r'\b(\d{4})[/\-](\d{4})\b')
        self.single_year_pattern = re.compile(r'\b(20\d{2}|19\d{2})\b')
    
    def analyze(self, vehicle: Dict) -> Dict:
        """
        Analisa um veículo e retorna informações estruturadas
        
        Args:
            vehicle: {
                'title': str,
                'normalized_title': str,
                'description': str,
                'metadata': dict
            }
        
        Returns:
            {
                'vehicle_type': str,
                'brand': str,
                'model': str,
                'year': str,
                'year_model': int,
                'confidence': str
            }
        """
        title = vehicle.get('title', '')
        normalized = vehicle.get('normalized_title', '')
        description = vehicle.get('description', '')
        metadata = vehicle.get('metadata', {})
        
        # Tenta extrair do metadata primeiro (mais confiável)
        if isinstance(metadata, dict) and 'veiculo' in metadata:
            veiculo_meta = metadata['veiculo']
            return {
                'vehicle_type': self._detect_type(title, description),
                'brand': veiculo_meta.get('marca', '').upper().strip(),
                'model': veiculo_meta.get('modelo', '').strip(),
                'year': veiculo_meta.get('ano'),
                'year_model': self._extract_year_model(str(veiculo_meta.get('ano', ''))),
                'confidence': 'high'
            }
        
        # Caso contrário, extrai do texto
        text = f"{title} {normalized} {description}"
        
        vehicle_type = self._detect_type(title, description)
        brand = self._extract_brand(text, vehicle_type)
        model = self._extract_model(text, brand)
        year, year_model = self._extract_year(text)
        
        confidence = 'low'
        if brand and year:
            confidence = 'medium'
        if brand and model and year:
            confidence = 'high'
        
        return {
            'vehicle_type': vehicle_type,
            'brand': brand,
            'model': model,
            'year': year,
            'year_model': year_model,
            'confidence': confidence
        }
    
    def _detect_type(self, title: str, description: str) -> str:
        """Detecta tipo de veículo"""
        text = f"{title} {description}".lower()
        
        for vehicle_type, keywords in self.VEHICLE_TYPES.items():
            for keyword in keywords:
                if keyword in text:
                    return vehicle_type
        
        # Default: carros
        return 'carros'
    
    def _extract_brand(self, text: str, vehicle_type: str) -> Optional[str]:
        """Extrai marca do veículo"""
        text_upper = text.upper()
        
        # Busca marcas específicas do tipo
        brands = self.BRANDS.get(vehicle_type, self.BRANDS['carros'])
        
        for brand in brands:
            if brand in text_upper:
                return brand
        
        # Busca em todas as marcas
        for brands_list in self.BRANDS.values():
            for brand in brands_list:
                if brand in text_upper:
                    return brand
        
        return None
    
    def _extract_model(self, text: str, brand: Optional[str]) -> Optional[str]:
        """Extrai modelo do veículo"""
        if not brand:
            return None
        
        text_upper = text.upper()
        
        # Remove a marca do texto
        text_without_brand = text_upper.replace(brand, '', 1)
        
        # Tenta pegar as próximas 3-5 palavras após a marca
        words = text_without_brand.split()
        model_words = []
        
        for word in words[:5]:
            # Pula palavras comuns
            if word.lower() in ['carro', 'moto', 'caminhao', 'caminhão', 'ano', 'modelo']:
                continue
            # Para se encontrar ano
            if re.match(r'^\d{4}$', word):
                break
            model_words.append(word)
        
        if model_words:
            return ' '.join(model_words[:3]).strip().title()
        
        return None
    
    def _extract_year(self, text: str) -> Tuple[Optional[str], Optional[int]]:
        """
        Extrai ano do veículo
        
        Returns:
            (ano_string, ano_modelo_int)
            Ex: ("2020/2021", 2021) ou ("2020", 2020)
        """
        # Procura formato YYYY/YYYY
        match = self.year_pattern.search(text)
        if match:
            year1 = match.group(1)
            year2 = match.group(2)
            return f"{year1}/{year2}", int(year2)
        
        # Procura ano único
        match = self.single_year_pattern.search(text)
        if match:
            year = match.group(1)
            return year, int(year)
        
        return None, None
    
    def _extract_year_model(self, year_str: str) -> Optional[int]:
        """Extrai ano modelo de string"""
        if not year_str:
            return None
        
        # Se já é int
        if isinstance(year_str, int):
            return year_str
        
        # Tenta extrair
        match = re.search(r'(\d{4})', str(year_str))
        if match:
            return int(match.group(1))
        
        return None


if __name__ == "__main__":
    print("="*60)
    print("🧪 TESTE VEHICLE ANALYZER")
    print("="*60)
    
    analyzer = VehicleAnalyzer()
    
    # Casos de teste
    test_cases = [
        {
            'title': 'MMC/Pajero TR4, flex HP, 11/12',
            'normalized_title': 'mmc pajero tr4 flex hp 11 12',
            'description': 'Veículo marca MMC/Pajero TR4, flex HP, ano 2011, mod. 2012',
            'metadata': {}
        },
        {
            'title': 'Honda CG 160 Fan 2020',
            'normalized_title': 'honda cg 160 fan 2020',
            'description': 'Moto Honda CG 160 Fan, ano 2020',
            'metadata': {}
        },
        {
            'title': 'Volkswagen Gol 1.6 2018/2019',
            'normalized_title': 'volkswagen gol 1 6 2018 2019',
            'description': 'Carro Volkswagen Gol 1.6',
            'metadata': {}
        }
    ]
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n{'='*60}")
        print(f"TESTE {i}: {test['title']}")
        print(f"{'='*60}")
        
        result = analyzer.analyze(test)
        
        print(f"   • Tipo: {result['vehicle_type']}")
        print(f"   • Marca: {result['brand']}")
        print(f"   • Modelo: {result['model']}")
        print(f"   • Ano: {result['year']}")
        print(f"   • Ano Modelo: {result['year_model']}")
        print(f"   • Confiança: {result['confidence']}")
    
    print("\n" + "="*60)
    print("✅ Testes concluídos!")
    print("="*60)
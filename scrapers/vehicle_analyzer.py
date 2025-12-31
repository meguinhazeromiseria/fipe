#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VEHICLE ANALYZER V4 - ULTRA CORRIGIDO
Extrai tipo, marca, modelo e ano de veículos com MÁXIMA PRECISÃO
"""

import re
from typing import Dict, Optional, Tuple


class VehicleAnalyzer:
    """Analisa títulos de veículos e extrai informações estruturadas"""
    
    # MARCAS EXCLUSIVAS (prioridade máxima)
    EXCLUSIVE_BRANDS = {
        'motos': [
            'YAMAHA', 'KAWASAKI', 'HARLEY-DAVIDSON', 'HARLEY',
            'DUCATI', 'TRIUMPH', 'KTM', 'ROYAL ENFIELD', 'ROYAL', 'BAJAJ',
            'DAFRA', 'KASINSKI', 'SHINERAY', 'TRAXX', 'SUNDOWN', 'JTZ'
        ],
        'caminhoes': [
            'SCANIA', 'IVECO', 'DAF', 'MAN'
        ],
        'onibus': [
            'MARCOPOLO', 'COMIL', 'BUSSCAR', 'CAIO', 'NEOBUS'
        ],
        'implementos': [
            'RODOFORT', 'LIBRELATO', 'RANDON', 'GUERRA', 'FACCHINI'
        ]
    }
    
    # MODELOS CONHECIDOS DE MOTOS (EXPANDIDO E PRIORIZADO)
    MOTO_MODELS = {
        # Honda motos (ALTA PRIORIDADE)
        'BIZ', 'POP', 'CG', 'TITAN', 'FAN', 'BROS', 'XRE', 'CB', 'CBR',
        'PCX', 'ADV', 'ELITE', 'CBX', 'TWISTER', 'NXR', 'SH', 'LEAD',
        'SHADOW', 'HORNET', 'TRANSALP',
        # Yamaha
        'FACTOR', 'YBR', 'FAZER', 'XTZ', 'LANDER', 'CROSSER', 'MT', 'MT-',
        'R1', 'R3', 'R6', 'FZ', 'YZF', 'XMAX', 'NMAX', 'XJ6', 'YS', 'CRYPTON',
        'NEO', 'DRAG STAR', 'V-MAX',
        # Suzuki
        'YES', 'INTRUDER', 'VSTROM', 'V-STROM', 'BURGMAN', 'GSXR', 'GSX-R',
        'BANDIT', 'HAYABUSA', 'GSX', 'DL', 'BOULEVARD',
        # Kawasaki
        'NINJA', 'VERSYS', 'VULCAN', 'ER-6', 'ER6', 'ZX', 'Z1000', 'Z800',
        'Z650', 'Z400', 'Z300',
        # Harley
        'STREET', 'ROAD', 'SOFTAIL', 'SPORTSTER', 'ELECTRA', 'HERITAGE',
        'FAT BOY', 'IRON', 'FORTY-EIGHT', 'ULTRA', 'GLIDE', 'FLSTN',
        # Ducati
        'MONSTER', 'DIAVEL', 'SCRAMBLER', 'PANIGALE', 'MULTISTRADA', 'MTS',
        'HYPERMOTARD', 'SUPERSPORT',
        # Triumph
        'TIGER', 'BONNEVILLE', 'STREET TWIN', 'SPEED TWIN', 'ROCKET',
        # BMW Motos (IMPORTANTE!)
        'F800', 'F850', 'F700', 'R1200', 'R1250', 'S1000', 'G310', 'C650',
        'K1600', 'F900',
        # Outras
        'DOMINAR', 'PULSAR', 'DUKE', 'RC', 'ADVENTURE', 'MAXSYM', 'NEXT',
        'NH', 'KANSAS', 'DR',
    }
    
    # MODELOS DE CARROS (ALTA PRIORIDADE)
    CAR_MODELS = {
        # VW carros
        'GOL', 'VOYAGE', 'POLO', 'FOX', 'UP', 'UP!', 'JETTA', 'PASSAT', 'TIGUAN',
        'T-CROSS', 'NIVUS', 'TAOS', 'FUSCA', 'VARIANT', 'BRASILIA', 'SANTANA',
        'QUANTUM', 'VIRTUS', 'SPACEFOX', 'PARATI',
        # Ford carros
        'KA', 'FIESTA', 'FOCUS', 'FUSION', 'ECOSPORT', 'EDGE',
        'ESCORT', 'BELINA', 'DEL REY',
        # Fiat carros
        'UNO', 'PALIO', 'SIENA', 'ARGO', 'CRONOS', 'MOBI',
        'PULSE', 'FASTBACK', 'MAREA', 'TIPO', 'TEMPRA', 'ELBA',
        'PUNTO', 'LINEA',
        # Chevrolet carros
        'ONIX', 'PRISMA', 'CRUZE', 'SPIN', 'COBALT', 'MONTANA',
        'TRACKER', 'TRAILBLAZER', 'CELTA', 'CORSA', 'ASTRA', 'VECTRA',
        'ZAFIRA', 'MERIVA', 'CLASSIC', 'AGILE', 'OMEGA', 'SONIC',
        # Honda carros
        'CIVIC', 'FIT', 'CITY', 'ACCORD', 'HR-V', 'WR-V', 'CR-V',
        # Toyota carros
        'COROLLA', 'SW4', 'RAV4', 'ETIOS', 'YARIS', 'FIELDER',
        'PRIUS', 'LAND CRUISER', 'CAMRY',
        # Hyundai
        'HB20', 'CRETA', 'TUCSON', 'SANTA FE', 'ELANTRA', 'AZERA', 'IX35',
        'I30', 'VELOSTER',
        # Nissan
        'MARCH', 'VERSA', 'KICKS', 'SENTRA', 'LIVINA', 'TIIDA',
        # Renault
        'KWID', 'SANDERO', 'LOGAN', 'DUSTER', 'CAPTUR', 'OROCH', 'FLUENCE',
        'CLIO', 'MEGANE', 'SCENIC',
        # Jeep
        'RENEGADE', 'COMPASS', 'COMMANDER', 'WRANGLER', 'GRAND CHEROKEE',
        # Peugeot
        '206', '207', '208', '2008', '3008', '308', '408', '508', '306', '307', '406',
        # Citroën
        'C3', 'C4', 'AIRCROSS', 'PICASSO', 'XSARA', 'BERLINGO', 'CACTUS',
        # Mercedes carros
        'C180', 'C200', 'C250', 'E200', 'E250', 'GLA', 'GLC', 'CLASSE A', 'CLASSE C',
        'CLASSE E',
        # BMW carros
        '118I', '120I', '320I', '328I', 'X1', 'X2', 'X3', 'X5', 'X6', 'SERIE 1',
        'SERIE 3', 'SERIE 5',
        # Mitsubishi
        'OUTLANDER', 'PAJERO', 'ASX',
        # Land Rover
        'FREELANDER', 'DISCOVERY', 'RANGE ROVER', 'DEFENDER', 'EVOQUE',
        # Kia
        'SPORTAGE', 'SORENTO', 'SOUL', 'CERATO', 'RIO',
        # Outros
        'QQ3', 'CELER', 'CAYENNE', 'A3', 'A4', 'A5', 'A6', 'Q3', 'Q5',
    }
    
    # MODELOS DE CAMINHÕES/PICKUPS
    TRUCK_MODELS = {
        'TECTOR', 'STRALIS', 'ATEGO', 'ACCELO', 'FH', 'FM', 'VM',
        'CONSTELLATION', 'METEOR', 'CARGO', 'WORKER', 'DELIVERY',
        'R440', 'R450', 'R500', 'R540', 'P360', 'G420', 'G480', 'G500',
        'AXOR', 'ATRON', 'DAILY', 'SPRINTER',
        # Pickups
        'HILUX', 'RANGER', 'S10', 'AMAROK', 'FRONTIER', 'L200', 'TRITON',
        'TORO', 'STRADA', 'SAVEIRO', 'MONTANA',
    }
    
    # Marcas ambíguas (fazem carros E motos E caminhões)
    AMBIGUOUS_BRANDS = {
        'HONDA': ['carros', 'motos'],
        'SUZUKI': ['carros', 'motos'],
        'BMW': ['carros', 'motos'],
        'VOLKSWAGEN': ['carros', 'caminhoes'],
        'VW': ['carros', 'caminhoes'],
        'FORD': ['carros', 'caminhoes'],
        'MERCEDES-BENZ': ['carros', 'caminhoes'],
        'MERCEDES': ['carros', 'caminhoes'],
        'VOLVO': ['carros', 'caminhoes'],
        'FIAT': ['carros', 'caminhoes'],
        'TOYOTA': ['carros', 'caminhoes'],
        'NISSAN': ['carros', 'caminhoes'],
        'RENAULT': ['carros', 'caminhoes'],
        'CHEVROLET': ['carros', 'caminhoes'],
        'HYUNDAI': ['carros', 'caminhoes'],
    }
    
    # KEYWORDS ESPECÍFICAS
    VEHICLE_KEYWORDS = {
        'motos': [
            r'\b(110|125|150|160|190|200|250|300|400|500|600|650|750|800|1000|1100|1200|1300)CC?\b',
            r'\bCILINDRADA\b',
            r'\bMOTO(CICLETA)?\b',
            r'\bSCOOTER\b',
            r'\bCICLOMOTOR\b',
        ],
        'caminhoes': [
            r'\bCAMINH[AÃ]O\b',
            r'\bTRUCK\b',
            r'\bBITRUCK\b',
            r'\bRODOTREM\b',
            r'\bTRATOR\b',
            r'\bCAVALO\s+MEC[AÂ]NICO\b',
            r'\bBASCULANTE\b',
            r'\bBAU\b',
            r'\bBAÚ\b',
            r'\bTANQUE\b',
            r'\bCOMPACTADOR\b',
            r'\bGUINDAUTO\b',
            r'\bMECANISMO\s+OPERACIONAL\b',
            r'\b(4X2|6X2|6X4|8X2|8X4)\b',
        ],
        'onibus': [
            r'\bÔNIBUS\b',
            r'\bMICRO[\s\-]*ÔNIBUS\b',
            r'\bURBANO\b',
            r'\bESCOLAR\b',
            r'\bRODOVIARIO\b',
        ],
        'implementos': [
            r'\bREBOQUE\b',
            r'\bCARRETA\b',
            r'\bSEMI[\s\-]*REBOQUE\b',
            r'\bTRAILER\b',
            r'\bIMPLEMENTO\b',
            r'\bSIDER\b',
            r'\bDOLLY\b',
            r'\bCARROCERIA(?!\s+CARGA)\b',  # Carroceria sozinha, não "carroceria carga seca"
        ],
        'embarcacoes': [
            r'\bBARCO\b',
            r'\bLANCHA\b',
            r'\bJET[\s\-]*SKI\b',
            r'\bMOTO\s*AQU[AÁ]TICA\b',
            r'\bEMBARCA[ÇC][AÃ]O\b',
            r'\bIATE\b',
            r'\bVELEIRO\b',
        ],
        'aeronaves': [
            r'\bAVI[AÃ]O\b',
            r'\bAERONAVE\b',
            r'\bHELIC[ÓO]PTERO\b',
            r'\bULTRALEVE\b',
            r'\bFAROL.*LANDING\b',
        ],
        'outros': [
            r'\bBICICLETA\b',
            r'\bBIKE\b',
            r'\bTÍTULO\b',
            r'\bMOTOR\s+DE\s+BICICLETA\b',
            r'\bPLATAFORMA\s+PETROBRAS\b',
        ]
    }
    
    def __init__(self):
        self.year_pattern = re.compile(r'\b(\d{4})[/\-](\d{4})\b')
        self.single_year_pattern = re.compile(r'\b(20\d{2}|19\d{2})\b')
        # Compila patterns de keywords
        self.compiled_keywords = {}
        for vtype, patterns in self.VEHICLE_KEYWORDS.items():
            self.compiled_keywords[vtype] = [
                re.compile(p, re.IGNORECASE) for p in patterns
            ]
    
    def analyze(self, vehicle: Dict) -> Dict:
        """Analisa um veículo e retorna informações estruturadas"""
        title = vehicle.get('title', '')
        normalized = vehicle.get('normalized_title', '')
        description = vehicle.get('description', '')
        metadata = vehicle.get('metadata', {})
        
        # Extrai do texto
        text = f"{title} {normalized} {description}"
        
        vehicle_type = self._detect_type(text)
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
    
    def _detect_type(self, text: str) -> str:
        """
        Detecta tipo de veículo com MÁXIMA PRECISÃO
        
        ORDEM DE PRIORIDADE (CRÍTICA!):
        1. Filtrar não-veículos (bicicletas, títulos, etc)
        2. Keywords específicas fortes (embarcações, aeronaves)
        3. Cilindradas (forte indicador de moto)
        4. Marcas exclusivas
        5. Modelos conhecidos (PRIORIDADE MÁXIMA)
        6. Marcas ambíguas com desambiguação
        7. Keywords de tipo
        8. Default: carros
        """
        text_upper = text.upper()
        
        # 0. FILTRAR NÃO-VEÍCULOS
        if 'outros' in self.compiled_keywords:
            for pattern in self.compiled_keywords['outros']:
                if pattern.search(text_upper):
                    return 'outros'
        
        # 1. KEYWORDS SUPER ESPECÍFICAS (máxima prioridade)
        for vtype in ['embarcacoes', 'aeronaves']:
            if vtype in self.compiled_keywords:
                for pattern in self.compiled_keywords[vtype]:
                    if pattern.search(text_upper):
                        return vtype
        
        # 2. CILINDRADAS (forte indicador de moto) - ANTES de tudo
        if re.search(r'\b(110|125|150|160|190|200|250|300|400|500|600|650|750|800|1000|1100|1200|1300)CC?\b', text_upper):
            return 'motos'
        
        # 3. MARCAS EXCLUSIVAS
        for vtype, brands in self.EXCLUSIVE_BRANDS.items():
            for brand in brands:
                if re.search(rf'\b{re.escape(brand)}\b', text_upper):
                    return vtype
        
        # 4. MODELOS CONHECIDOS (PRIORIDADE MÁXIMA - antes de marcas ambíguas)
        # Verifica motos primeiro (mais específico)
        if self._has_exact_moto_model(text_upper):
            return 'motos'
        
        # Depois caminhões
        if self._has_exact_truck_model(text_upper):
            return 'caminhoes'
        
        # Por último carros
        if self._has_exact_car_model(text_upper):
            return 'carros'
        
        # 5. KEYWORDS DE IMPLEMENTOS (antes de caminhões)
        if 'implementos' in self.compiled_keywords:
            for pattern in self.compiled_keywords['implementos']:
                if pattern.search(text_upper):
                    return 'implementos'
        
        # 6. KEYWORDS DE ÔNIBUS
        if 'onibus' in self.compiled_keywords:
            for pattern in self.compiled_keywords['onibus']:
                if pattern.search(text_upper):
                    return 'onibus'
        
        # 7. KEYWORDS DE CAMINHÕES (forte)
        if 'caminhoes' in self.compiled_keywords:
            truck_matches = 0
            for pattern in self.compiled_keywords['caminhoes']:
                if pattern.search(text_upper):
                    truck_matches += 1
            # Precisa de pelo menos 1 keyword forte de caminhão
            if truck_matches >= 1:
                return 'caminhoes'
        
        # 8. KEYWORDS DE MOTOS
        if 'motos' in self.compiled_keywords:
            for pattern in self.compiled_keywords['motos']:
                if pattern.search(text_upper):
                    return 'motos'
        
        # 9. MARCAS AMBÍGUAS - última tentativa
        for brand in self.AMBIGUOUS_BRANDS.keys():
            if re.search(rf'\b{re.escape(brand)}\b', text_upper):
                # Se chegou aqui e é Honda/Suzuki/BMW, assume carro por ser mais comum
                if brand in ['HONDA', 'SUZUKI', 'BMW']:
                    return 'carros'
                # VW, Ford, etc -> carros por padrão
                return 'carros'
        
        # 10. DEFAULT: carros
        return 'carros'
    
    def _has_exact_moto_model(self, text: str) -> bool:
        """Verifica modelo de moto com word boundary"""
        for model in self.MOTO_MODELS:
            # Word boundary mais flexível para modelos como "CB" e "MT-"
            if re.search(rf'\b{re.escape(model)}[\s\-]?', text):
                return True
        return False
    
    def _has_exact_car_model(self, text: str) -> bool:
        """Verifica modelo de carro com word boundary"""
        for model in self.CAR_MODELS:
            if re.search(rf'\b{re.escape(model)}\b', text):
                return True
        return False
    
    def _has_exact_truck_model(self, text: str) -> bool:
        """Verifica modelo de caminhão com word boundary"""
        for model in self.TRUCK_MODELS:
            if re.search(rf'\b{re.escape(model)}\b', text):
                return True
        return False
    
    def _extract_brand(self, text: str, vehicle_type: str) -> Optional[str]:
        """Extrai marca do veículo"""
        text_upper = text.upper()
        
        # Busca marcas exclusivas do tipo
        if vehicle_type in self.EXCLUSIVE_BRANDS:
            for brand in self.EXCLUSIVE_BRANDS[vehicle_type]:
                if re.search(rf'\b{re.escape(brand)}\b', text_upper):
                    return brand
        
        # Busca marcas ambíguas
        for brand in self.AMBIGUOUS_BRANDS.keys():
            if re.search(rf'\b{re.escape(brand)}\b', text_upper):
                return brand
        
        # Busca em todas as marcas exclusivas
        for brands_list in self.EXCLUSIVE_BRANDS.values():
            for brand in brands_list:
                if re.search(rf'\b{re.escape(brand)}\b', text_upper):
                    return brand
        
        # Lista adicional de marcas
        other_brands = [
            'CHERY', 'CAOA', 'JAC', 'LIFAN', 'BYD', 'GWM',
            'PEUGEOT', 'CITROEN', 'JEEP', 'MITSUBISHI', 'KIA',
            'LAND ROVER', 'AUDI', 'PORSCHE', 'MAZDA', 'SUBARU'
        ]
        
        for brand in other_brands:
            if re.search(rf'\b{re.escape(brand)}\b', text_upper):
                return brand
        
        return None
    
    def _extract_model(self, text: str, brand: Optional[str]) -> Optional[str]:
        """Extrai modelo do veículo"""
        if not brand:
            return None
        
        text_upper = text.upper()
        # Remove a marca para extrair o modelo
        text_without_brand = re.sub(rf'\b{re.escape(brand)}\b', '', text_upper, count=1)
        
        words = text_without_brand.split()
        model_words = []
        
        for word in words[:5]:
            clean_word = word.strip(',-/()[]')
            if not clean_word:
                continue
            if clean_word.lower() in ['carro', 'moto', 'caminhao', 'caminhão', 'ano', 'modelo']:
                continue
            if re.match(r'^\d{4}$', clean_word):
                break
            model_words.append(clean_word)
        
        if model_words:
            return ' '.join(model_words[:3]).strip().title()
        
        return None
    
    def _extract_year(self, text: str) -> Tuple[Optional[str], Optional[int]]:
        """Extrai ano do veículo"""
        match = self.year_pattern.search(text)
        if match:
            year1 = match.group(1)
            year2 = match.group(2)
            return f"{year1}/{year2}", int(year2)
        
        match = self.single_year_pattern.search(text)
        if match:
            year = match.group(1)
            return year, int(year)
        
        return None, None


if __name__ == "__main__":
    print("="*60)
    print("🧪 TESTE VEHICLE ANALYZER V4 - ULTRA CORRIGIDO")
    print("="*60)
    
    analyzer = VehicleAnalyzer()
    
    # Casos PROBLEMÁTICOS do log + outros
    test_cases = [
        # CASOS PROBLEMÁTICOS DO LOG (devem estar corretos!)
        {'title': 'honda pop 110i', 'expected': 'motos'},
        {'title': 'honda cb 300f twister cbs', 'expected': 'motos'},
        {'title': 'honda cb 250f twister abs', 'expected': 'motos'},
        {'title': 'fiat uno vivace 1.0', 'expected': 'carros'},
        {'title': 'volkswagen polo 1.6', 'expected': 'carros'},
        {'title': 'volkswagen fox 1.0', 'expected': 'carros'},
        {'title': 'chevrolet corsa sedan premium', 'expected': 'carros'},
        
        # Outros testes
        {'title': 'Honda Civic EXR 2014', 'expected': 'carros'},
        {'title': 'Honda Biz 125 2020', 'expected': 'motos'},
        {'title': 'Toyota Hilux CD 2021', 'expected': 'caminhoes'},
        {'title': 'Scania R450 6x4', 'expected': 'caminhoes'},
        {'title': 'Yamaha Factor 150', 'expected': 'motos'},
        {'title': 'BMW S1000 RR', 'expected': 'motos'},
        {'title': 'BMW 320i', 'expected': 'carros'},
        {'title': 'Bajaj Dominar D400', 'expected': 'motos'},
        {'title': 'Bicicleta Caloi Aro 29', 'expected': 'outros'},
        {'title': 'Jet Ski Yamaha', 'expected': 'embarcacoes'},
    ]
    
    print("\n")
    errors = 0
    for i, test in enumerate(test_cases, 1):
        result = analyzer.analyze(test)
        title = test['title']
        vtype = result['vehicle_type']
        expected = test['expected']
        
        # Emoji map
        emoji_map = {
            'carros': '🚗',
            'motos': '🏍️',
            'caminhoes': '🚚',
            'implementos': '🚛',
            'onibus': '🚌',
            'embarcacoes': '🚤',
            'aeronaves': '✈️',
            'outros': '🚫'
        }
        emoji = emoji_map.get(vtype, '🚙')
        
        status = '✅' if vtype == expected else '❌'
        if vtype != expected:
            errors += 1
            print(f"{status} [{i:2d}] {emoji} {title:40s} → {vtype:15s} (esperado: {expected})")
        else:
            print(f"{status} [{i:2d}] {emoji} {title:40s} → {vtype}")
    
    print("\n" + "="*60)
    if errors == 0:
        print(f"✅ PERFEITO! Todos os {len(test_cases)} casos corretos!")
    else:
        print(f"❌ {errors} erro(s) de {len(test_cases)} casos")
    print("="*60)
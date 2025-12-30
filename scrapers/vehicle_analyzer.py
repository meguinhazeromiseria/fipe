#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VEHICLE ANALYZER V2 - MELHORADO
Extrai tipo, marca, modelo e ano de veículos com ALTA PRECISÃO
"""

import re
from typing import Dict, Optional, Tuple


class VehicleAnalyzer:
    """Analisa títulos de veículos e extrai informações estruturadas"""
    
    # MARCAS EXCLUSIVAS (prioridade máxima)
    EXCLUSIVE_BRANDS = {
        'motos': [
            'YAMAHA', 'KAWASAKI', 'HARLEY-DAVIDSON', 'HARLEY',
            'DUCATI', 'TRIUMPH', 'KTM', 'ROYAL ENFIELD', 'BAJAJ',
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
    
    # MODELOS CONHECIDOS DE CARROS
    CAR_MODELS = [
        # VW
        'GOL', 'VOYAGE', 'POLO', 'FOX', 'UP', 'JETTA', 'PASSAT', 'TIGUAN',
        'T-CROSS', 'NIVUS', 'TAOS', 'FUSCA', 'KOMBI', 'SAVEIRO', 'AMAROK',
        'VARIANT', 'BRASILIA', 'SANTANA', 'QUANTUM',
        # Ford
        'KA', 'FIESTA', 'FOCUS', 'FUSION', 'ECOSPORT', 'EDGE', 'RANGER',
        'ESCORT', 'COURIER', 'BELINA', 'DEL REY', 'PAMPA',
        # Fiat
        'UNO', 'PALIO', 'SIENA', 'STRADA', 'TORO', 'ARGO', 'CRONOS', 'MOBI',
        'PULSE', 'FASTBACK', 'DUCATO', 'MAREA', 'TIPO', 'TEMPRA', 'ELBA',
        # Chevrolet
        'ONIX', 'PRISMA', 'CRUZE', 'SPIN', 'COBALT', 'S10', 'MONTANA',
        'TRACKER', 'TRAILBLAZER', 'CELTA', 'CORSA', 'ASTRA', 'VECTRA',
        'ZAFIRA', 'MERIVA', 'CLASSIC', 'AGILE', 'OMEGA',
        # Honda
        'CIVIC', 'FIT', 'CITY', 'ACCORD', 'HR-V', 'WR-V', 'CR-V',
        # Toyota
        'COROLLA', 'HILUX', 'SW4', 'RAV4', 'ETIOS', 'YARIS', 'FIELDER',
        'PRIUS', 'LAND CRUISER', 'CAMRY', 'BANDEIRANTE',
        # Hyundai
        'HB20', 'CRETA', 'TUCSON', 'SANTA FE', 'ELANTRA', 'AZERA', 'IX35',
        'I30', 'VELOSTER', 'HR',
        # Nissan
        'MARCH', 'VERSA', 'KICKS', 'SENTRA', 'FRONTIER', 'LIVINA', 'TIIDA',
        # Renault
        'KWID', 'SANDERO', 'LOGAN', 'DUSTER', 'CAPTUR', 'OROCH', 'FLUENCE',
        'CLIO', 'MEGANE', 'SCENIC',
        # Jeep
        'RENEGADE', 'COMPASS', 'COMMANDER', 'WRANGLER', 'GRAND CHEROKEE',
        # Peugeot
        '206', '207', '208', '2008', '3008', '308', '408', '508', '306', '307', '406',
        # Citroën
        'C3', 'C4', 'AIRCROSS', 'PICASSO', 'XSARA', 'BERLINGO',
        # Mercedes
        'C180', 'C200', 'C250', 'E200', 'E250', 'GLA', 'GLC', 'CLASSE', 'ACCELO',
        # BMW
        '320I', '328I', 'X1', 'X3', 'X5', 'X6', 'SERIE',
        # Mitsubishi
        'OUTLANDER', 'PAJERO', 'ASX', 'L200', 'TRITON',
        # Outros
        'FREELANDER', 'DISCOVERY', 'RANGE ROVER', 'DEFENDER',
        'SPORTAGE', 'SORENTO', 'SOUL', 'CERATO',
    ]
    
    # MODELOS CONHECIDOS DE MOTOS
    MOTO_MODELS = [
        # Honda
        'BIZ', 'POP', 'CG', 'TITAN', 'FAN', 'BROS', 'XRE', 'CB', 'CBR',
        'PCX', 'ADV', 'ELITE', 'CBX', 'TWISTER', 'NXR', 'SH', 'LEAD',
        'SHADOW', 'HORNET', 'TRANSALP',
        # Yamaha
        'FACTOR', 'YBR', 'FAZER', 'XTZ', 'LANDER', 'CROSSER', 'MT', 'R1',
        'R3', 'R6', 'FZ', 'YZF', 'XMAX', 'NMAX', 'XJ6', 'YS', 'CRYPTON',
        'NEO', 'DRAG STAR', 'V-MAX',
        # Suzuki
        'YES', 'INTRUDER', 'VSTROM', 'V-STROM', 'BURGMAN', 'GSXR', 'GSX-R',
        'BANDIT', 'HAYABUSA', 'GSX', 'DL', 'BOULEVARD',
        # Kawasaki
        'NINJA', 'Z', 'VERSYS', 'VULCAN', 'ER-6', 'ER6', 'ZX', 'Z1000', 'Z800',
        'Z650', 'Z400', 'Z300',
        # Harley
        'STREET', 'ROAD', 'SOFTAIL', 'SPORTSTER', 'ELECTRA', 'HERITAGE',
        'FAT BOY', 'IRON', 'FORTY-EIGHT', 'ULTRA', 'GLIDE',
        # Ducati
        'MONSTER', 'DIAVEL', 'SCRAMBLER', 'PANIGALE', 'MULTISTRADA', 'MTS',
        'HYPERMOTARD', 'SUPERSPORT',
        # Triumph
        'TIGER', 'BONNEVILLE', 'STREET TWIN', 'SPEED TWIN', 'ROCKET',
        # Outras
        'DOMINAR', 'PULSAR', 'DUKE', 'RC', 'ADVENTURE', 'MAXSYM', 'NEXT',
        'NH', 'STRADA'
    ]
    
    # MODELOS DE CAMINHÕES
    TRUCK_MODELS = [
        'TECTOR', 'STRALIS', 'ATEGO', 'ACCELO', 'FH', 'FM', 'VM',
        'CONSTELLATION', 'METEOR', 'CARGO', 'WORKER', 'DELIVERY',
        'R440', 'R450', 'R500', 'P360', 'G420', 'EURO',
    ]
    
    # PALAVRAS-CHAVE ESPECÍFICAS (ordem importa!)
    VEHICLE_KEYWORDS = {
        'motos': [
            # Cilindradas (forte indicador)
            r'\b(125|150|160|190|200|250|300|400|500|600|650|750|800|1000|1100|1200|1300)CC?\b',
            r'\bCILINDRADA\b',
            # Termos exclusivos de motos
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
            r'\bCAMINHONETE\b',
            r'\b(4X2|6X2|6X4|8X2|8X4)\b',
        ],
        'onibus': [
            r'\b[ÔO]NIBUS\b',
            r'\bMICRO[\s\-]*[ÔO]NIBUS\b',
        ],
        'implementos': [
            r'\bREBOQUE\b',
            r'\bCARRETA\b',
            r'\bSEMI[\s\-]*REBOQUE\b',
            r'\bTRAILER\b',
            r'\bIMPLEMENTO\b',
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
        ],
        'carros': [
            r'\bAUTOM[ÓO]VEL\b',
            r'\bSEDAN\b',
            r'\bHATCH\b',
            r'\bSUV\b',
            r'\bPICKUP\b',
            r'\bCROSSOVER\b',
        ]
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
        
        # Tenta extrair do metadata primeiro
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
        
        # Extrai do texto
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
        """
        Detecta tipo de veículo com ALTA PRECISÃO
        
        ORDEM DE PRIORIDADE:
        1. Marcas exclusivas
        2. Modelos conhecidos
        3. Cilindradas (motos)
        4. Keywords específicas
        5. Default: carros
        """
        text = f"{title} {description}".upper()
        
        # 1. MARCAS EXCLUSIVAS (máxima prioridade)
        for vtype, brands in self.EXCLUSIVE_BRANDS.items():
            for brand in brands:
                if brand in text:
                    return vtype
        
        # 2. MARCAS AMBÍGUAS - precisa desambiguar
        detected_ambiguous = None
        for brand, vtypes in self.AMBIGUOUS_BRANDS.items():
            if brand in text:
                detected_ambiguous = brand
                break
        
        if detected_ambiguous:
            # Verifica modelos para desambiguar
            if self._has_moto_model(text):
                return 'motos'
            elif self._has_car_model(text):
                return 'carros'
            elif self._has_truck_model(text):
                return 'caminhoes'
        
        # 3. MODELOS CONHECIDOS
        if self._has_moto_model(text):
            return 'motos'
        if self._has_car_model(text):
            return 'carros'
        if self._has_truck_model(text):
            return 'caminhoes'
        
        # 4. CILINDRADAS (forte indicador de moto)
        if re.search(r'\b(125|150|160|190|200|250|300|400|500|600|650|750|800|1000|1100|1200|1300)CC?\b', text):
            return 'motos'
        
        # 5. KEYWORDS ESPECÍFICAS (ordem importa!)
        # Checa tipos mais específicos primeiro
        for vtype in ['embarcacoes', 'aeronaves', 'implementos', 'onibus', 'caminhoes', 'motos']:
            if vtype in self.compiled_keywords:
                for pattern in self.compiled_keywords[vtype]:
                    if pattern.search(text):
                        return vtype
        
        # 6. Checa keywords de carros
        if 'carros' in self.compiled_keywords:
            for pattern in self.compiled_keywords['carros']:
                if pattern.search(text):
                    return 'carros'
        
        # 7. DEFAULT: carros
        return 'carros'
    
    def _has_moto_model(self, text: str) -> bool:
        """Verifica se tem modelo conhecido de moto"""
        text_upper = text.upper()
        for model in self.MOTO_MODELS:
            # Busca com word boundary
            if re.search(rf'\b{re.escape(model)}\b', text_upper):
                return True
        return False
    
    def _has_car_model(self, text: str) -> bool:
        """Verifica se tem modelo conhecido de carro"""
        text_upper = text.upper()
        for model in self.CAR_MODELS:
            # Busca com word boundary
            if re.search(rf'\b{re.escape(model)}\b', text_upper):
                return True
        return False
    
    def _has_truck_model(self, text: str) -> bool:
        """Verifica se tem modelo conhecido de caminhão"""
        text_upper = text.upper()
        for model in self.TRUCK_MODELS:
            if re.search(rf'\b{re.escape(model)}\b', text_upper):
                return True
        return False
    
    def _extract_brand(self, text: str, vehicle_type: str) -> Optional[str]:
        """Extrai marca do veículo"""
        text_upper = text.upper()
        
        # Busca marcas exclusivas do tipo
        if vehicle_type in self.EXCLUSIVE_BRANDS:
            for brand in self.EXCLUSIVE_BRANDS[vehicle_type]:
                if brand in text_upper:
                    return brand
        
        # Busca marcas ambíguas
        for brand in self.AMBIGUOUS_BRANDS.keys():
            if brand in text_upper:
                return brand
        
        # Busca em todas as marcas exclusivas
        for brands_list in self.EXCLUSIVE_BRANDS.values():
            for brand in brands_list:
                if brand in text_upper:
                    return brand
        
        # Lista adicional de marcas de carros
        car_brands = [
            'CHERY', 'CAOA', 'JAC', 'LIFAN', 'BYD', 'GWM',
            'NISSAN', 'RENAULT', 'PEUGEOT', 'CITROEN',
            'JEEP', 'MITSUBISHI', 'KIA',
            'LAND ROVER', 'AUDI', 'PORSCHE', 'MAZDA', 'SUBARU',
            'CHEVROLET', 'FIAT', 'TOYOTA', 'HYUNDAI'
        ]
        
        for brand in car_brands:
            if brand in text_upper:
                return brand
        
        return None
    
    def _extract_model(self, text: str, brand: Optional[str]) -> Optional[str]:
        """Extrai modelo do veículo"""
        if not brand:
            return None
        
        text_upper = text.upper()
        text_without_brand = text_upper.replace(brand, '', 1)
        
        words = text_without_brand.split()
        model_words = []
        
        for word in words[:5]:
            if word.lower() in ['carro', 'moto', 'caminhao', 'caminhão', 'ano', 'modelo']:
                continue
            if re.match(r'^\d{4}$', word):
                break
            model_words.append(word)
        
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
    
    def _extract_year_model(self, year_str: str) -> Optional[int]:
        """Extrai ano modelo de string"""
        if not year_str:
            return None
        
        if isinstance(year_str, int):
            return year_str
        
        match = re.search(r'(\d{4})', str(year_str))
        if match:
            return int(match.group(1))
        
        return None


if __name__ == "__main__":
    print("="*60)
    print("🧪 TESTE VEHICLE ANALYZER V2 - MELHORADO")
    print("="*60)
    
    analyzer = VehicleAnalyzer()
    
    # Casos de teste (incluindo os problemáticos)
    test_cases = [
        {'title': 'Ford Ka Flex', 'description': 'Carro Ford Ka 2015'},
        {'title': 'Volkswagen Gol 1.6 Power', 'description': 'Gol 2012'},
        {'title': 'Fiat Strada Endurance CS', 'description': 'Pickup Strada'},
        {'title': 'Volkswagen Fusca', 'description': 'Fusca 1978 azul'},
        {'title': 'Bajaj Dominar NS 160', 'description': 'Moto 160cc'},
        {'title': 'Ducati MTS 1200 S', 'description': 'Multistrada 1200'},
        {'title': 'Honda Pop 110i', 'description': 'Moto Honda Pop'},
        {'title': 'Honda Civic EXR', 'description': 'Civic sedan'},
        {'title': 'Yamaha Factor 150', 'description': 'Moto 150cc'},
        {'title': 'Mercedes-Benz Accelo 1117', 'description': 'Caminhão baú'},
        {'title': 'Scania R450', 'description': 'Caminhão trucado'},
        {'title': 'Honda Biz 125', 'description': 'Scooter 125cc'},
        {'title': 'BMW F800 GS', 'description': 'Moto BMW adventure'},
        {'title': 'BMW 328i Active Flex', 'description': 'Sedan BMW'},
        {'title': 'Iveco Daily 35s14', 'description': 'Van'},
        {'title': 'Volvo FH 460', 'description': 'Caminhão'},
        {'title': 'Rodofort SA SRPC 3E', 'description': 'Carreta'},
        {'title': 'Chevrolet Onix', 'description': 'Hatch'},
        {'title': 'Hyundai Creta', 'description': 'SUV'},
    ]
    
    print("\n")
    errors = 0
    for i, test in enumerate(test_cases, 1):
        result = analyzer.analyze(test)
        title = test['title']
        vtype = result['vehicle_type']
        
        # Determina tipo esperado
        title_lower = title.lower()
        if 'bajaj' in title_lower or 'ducati' in title_lower or 'factor' in title_lower or 'pop' in title_lower or 'biz' in title_lower or 'f800' in title_lower:
            expected = 'motos'
        elif 'accelo' in title_lower or 'scania' in title_lower or 'fh 460' in title_lower or 'daily' in title_lower:
            expected = 'caminhoes'
        elif 'rodofort' in title_lower:
            expected = 'implementos'
        else:
            expected = 'carros'
        
        # Emoji
        if vtype == 'carros':
            emoji = '🚗'
        elif vtype == 'motos':
            emoji = '🏍️'
        elif vtype == 'caminhoes':
            emoji = '🚚'
        elif vtype == 'implementos':
            emoji = '🚛'
        else:
            emoji = '🚙'
        
        status = '✅' if vtype == expected else '❌'
        if vtype != expected:
            errors += 1
        
        print(f"{emoji} {status} [{i:2d}] {title:40s} → {vtype}")
    
    print("\n" + "="*60)
    print(f"{'✅ PERFEITO!' if errors == 0 else f'❌ {errors} erros encontrados'}")
    print("="*60)
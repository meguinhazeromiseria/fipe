#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VEHICLE ANALYZER V3 - CORRIGIDO
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
    
    # MODELOS CONHECIDOS DE CARROS (EXPANDIDO)
    CAR_MODELS = [
        # VW
        'GOL', 'VOYAGE', 'POLO', 'FOX', 'UP', 'JETTA', 'PASSAT', 'TIGUAN',
        'T-CROSS', 'NIVUS', 'TAOS', 'FUSCA', 'KOMBI', 'SAVEIRO', 'AMAROK',
        'VARIANT', 'BRASILIA', 'SANTANA', 'QUANTUM', 'VIRTUS', 'SPACEFOX',
        # Ford
        'KA', 'FIESTA', 'FOCUS', 'FUSION', 'ECOSPORT', 'EDGE', 'RANGER',
        'ESCORT', 'COURIER', 'BELINA', 'DEL REY', 'PAMPA',
        # Fiat
        'UNO', 'PALIO', 'SIENA', 'STRADA', 'TORO', 'ARGO', 'CRONOS', 'MOBI',
        'PULSE', 'FASTBACK', 'DUCATO', 'MAREA', 'TIPO', 'TEMPRA', 'ELBA',
        'PUNTO', 'TITANO',
        # Chevrolet
        'ONIX', 'PRISMA', 'CRUZE', 'SPIN', 'COBALT', 'S10', 'MONTANA',
        'TRACKER', 'TRAILBLAZER', 'CELTA', 'CORSA', 'ASTRA', 'VECTRA',
        'ZAFIRA', 'MERIVA', 'CLASSIC', 'AGILE', 'OMEGA', 'SONIC',
        # Honda
        'CIVIC', 'FIT', 'CITY', 'ACCORD', 'HR-V', 'WR-V', 'CR-V',
        # Toyota
        'COROLLA', 'HILUX', 'SW4', 'RAV4', 'ETIOS', 'YARIS', 'FIELDER',
        'PRIUS', 'LAND CRUISER', 'CAMRY', 'BANDEIRANTE',
        # Hyundai
        'HB20', 'CRETA', 'TUCSON', 'SANTA FE', 'ELANTRA', 'AZERA', 'IX35',
        'I30', 'VELOSTER',
        # Nissan
        'MARCH', 'VERSA', 'KICKS', 'SENTRA', 'FRONTIER', 'LIVINA', 'TIIDA',
        # Renault
        'KWID', 'SANDERO', 'LOGAN', 'DUSTER', 'CAPTUR', 'OROCH', 'FLUENCE',
        'CLIO', 'MEGANE', 'SCENIC', 'MASTER',
        # Jeep
        'RENEGADE', 'COMPASS', 'COMMANDER', 'WRANGLER', 'GRAND CHEROKEE',
        # Peugeot
        '206', '207', '208', '2008', '3008', '308', '408', '508', '306', '307', '406',
        # Citroën
        'C3', 'C4', 'AIRCROSS', 'PICASSO', 'XSARA', 'BERLINGO', 'CACTUS',
        # Mercedes
        'C180', 'C200', 'C250', 'E200', 'E250', 'GLA', 'GLC', 'CLASSE', 'ACCELO',
        # BMW
        '320I', '328I', 'X1', 'X2', 'X3', 'X5', 'X6', 'SERIE',
        # Mitsubishi
        'OUTLANDER', 'PAJERO', 'ASX', 'L200', 'TRITON',
        # Outros
        'FREELANDER', 'DISCOVERY', 'RANGE ROVER', 'DEFENDER',
        'SPORTAGE', 'SORENTO', 'SOUL', 'CERATO', 'RIO',
        'QQ3', 'CELER', 'CAYENNE', 'A4',
    ]
    
    # MODELOS CONHECIDOS DE MOTOS (EXPANDIDO)
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
        'FAT BOY', 'IRON', 'FORTY-EIGHT', 'ULTRA', 'GLIDE', 'FLSTN',
        # Ducati
        'MONSTER', 'DIAVEL', 'SCRAMBLER', 'PANIGALE', 'MULTISTRADA', 'MTS',
        'HYPERMOTARD', 'SUPERSPORT',
        # Triumph
        'TIGER', 'BONNEVILLE', 'STREET TWIN', 'SPEED TWIN', 'ROCKET',
        # BMW Motos (IMPORTANTE!)
        'F800', 'F850', 'R1200', 'R1250', 'S1000', 'G310', 'C650',
        # Outras
        'DOMINAR', 'PULSAR', 'DUKE', 'RC', 'ADVENTURE', 'MAXSYM', 'NEXT',
        'NH', 'KANSAS', 'DR',
    ]
    
    # MODELOS DE CAMINHÕES/PICKUPS
    TRUCK_MODELS = [
        'TECTOR', 'STRALIS', 'ATEGO', 'ACCELO', 'FH', 'FM', 'VM',
        'CONSTELLATION', 'METEOR', 'CARGO', 'WORKER', 'DELIVERY',
        'R440', 'R450', 'R500', 'R540', 'P360', 'G420', 'G480', 'G500', 'EURO',
        'AXOR', 'ATRON', 'DAILY', 'SPRINTER',
    ]
    
    # PALAVRAS-CHAVE ESPECÍFICAS (ordem importa!)
    VEHICLE_KEYWORDS = {
        'motos': [
            # Cilindradas (forte indicador)
            r'\b(110|125|150|160|190|200|250|300|400|500|600|650|750|800|1000|1100|1200|1300)CC?\b',
            r'\bCILINDRADA\b',
            # Termos exclusivos de motos
            r'\bMOTO(CICLETA)?\b',
            r'\bSCOOTER\b',
            r'\bCICLOMOTOR\b',
        ],
        'caminhoes': [
            r'\bCAMINH[AÃƒ]O\b',
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
            r'\bGUREIRA\b',
            r'\b(4X2|6X2|6X4|8X2|8X4)\b',
        ],
        'onibus': [
            r'\b[Ã"O]NIBUS\b',
            r'\bMICRO[\s\-]*[Ã"O]NIBUS\b',
            r'\bURBANO\b',
            r'\bESCOLAR\b',
        ],
        'implementos': [
            r'\bREBOQUE\b',
            r'\bCARRETA\b',
            r'\bSEMI[\s\-]*REBOQUE\b',
            r'\bTRAILER\b',
            r'\bIMPLEMENTO\b',
            r'\bSIDER\b',
        ],
        'embarcacoes': [
            r'\bBARCO\b',
            r'\bLANCHA\b',
            r'\bJET[\s\-]*SKI\b',
            r'\bMOTO\s*AQU[AÃ]TICA\b',
            r'\bEMBARCA[Ã‡C][AÃƒ]O\b',
            r'\bIATE\b',
            r'\bVELEIRO\b',
        ],
        'aeronaves': [
            r'\bAVI[AÃƒ]O\b',
            r'\bAERONAVE\b',
            r'\bHELIC[Ã"O]PTERO\b',
            r'\bULTRALEVE\b',
            r'\bFAROL.*LANDING\b',
            r'\bANEL\s+DE\s+AERONAVE\b',
        ],
        'carros': [
            r'\bAUTOM[Ã"O]VEL\b',
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
        'FIAT': ['carros', 'caminhoes'],
        'TOYOTA': ['carros', 'caminhoes'],
        'NISSAN': ['carros', 'caminhoes'],
        'RENAULT': ['carros', 'caminhoes'],
        'CHEVROLET': ['carros', 'caminhoes'],
        'HYUNDAI': ['carros', 'caminhoes'],
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
        1. Keywords específicas mais fortes (embarcações, aeronaves, implementos, ônibus)
        2. Marcas exclusivas
        3. Cilindradas (motos)
        4. Modelos conhecidos
        5. Keywords de caminhões
        6. Keywords de carros/motos
        7. Default: carros
        """
        text = f"{title} {description}".upper()
        
        # 1. KEYWORDS SUPER ESPECÍFICAS (máxima prioridade)
        for vtype in ['embarcacoes', 'aeronaves', 'implementos', 'onibus']:
            if vtype in self.compiled_keywords:
                for pattern in self.compiled_keywords[vtype]:
                    if pattern.search(text):
                        return vtype
        
        # 2. MARCAS EXCLUSIVAS
        for vtype, brands in self.EXCLUSIVE_BRANDS.items():
            for brand in brands:
                if brand in text:
                    return vtype
        
        # 3. CILINDRADAS (forte indicador de moto) - ANTES de marcas ambíguas
        if re.search(r'\b(110|125|150|160|190|200|250|300|400|500|600|650|750|800|1000|1100|1200|1300)CC?\b', text):
            return 'motos'
        
        # 4. MARCAS AMBÍGUAS - desambiguar por modelo
        detected_ambiguous = None
        for brand, vtypes in self.AMBIGUOUS_BRANDS.items():
            if brand in text:
                detected_ambiguous = brand
                break
        
        if detected_ambiguous:
            # Verifica modelos para desambiguar
            if self._has_moto_model(text):
                return 'motos'
            elif self._has_truck_model(text):
                return 'caminhoes'
            elif self._has_car_model(text):
                return 'carros'
        
        # 5. MODELOS CONHECIDOS (sem marca ambígua detectada)
        if self._has_moto_model(text):
            return 'motos'
        if self._has_truck_model(text):
            return 'caminhoes'
        if self._has_car_model(text):
            return 'carros'
        
        # 6. KEYWORDS DE CAMINHÕES (antes de carros)
        if 'caminhoes' in self.compiled_keywords:
            for pattern in self.compiled_keywords['caminhoes']:
                if pattern.search(text):
                    return 'caminhoes'
        
        # 7. KEYWORDS DE MOTOS
        if 'motos' in self.compiled_keywords:
            for pattern in self.compiled_keywords['motos']:
                if pattern.search(text):
                    return 'motos'
        
        # 8. KEYWORDS DE CARROS
        if 'carros' in self.compiled_keywords:
            for pattern in self.compiled_keywords['carros']:
                if pattern.search(text):
                    return 'carros'
        
        # 9. DEFAULT: carros
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
            'JEEP', 'MITSUBISHI', 'KIA', 'KIA MOTORS',
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
    print("🧪 TESTE VEHICLE ANALYZER V3 - CORRIGIDO")
    print("="*60)
    
    analyzer = VehicleAnalyzer()
    
    # Casos de teste (incluindo os problemáticos do log)
    test_cases = [
        {'title': 'BMW F800 R', 'description': 'Moto BMW'},
        {'title': 'BMW F800 GS', 'description': 'Moto adventure'},
        {'title': 'BMW S1000 RR', 'description': 'Moto esportiva'},
        {'title': 'BMW R1200 GS', 'description': 'Moto BMW adventure'},
        {'title': 'BMW 328i Active Flex', 'description': 'Sedan BMW'},
        {'title': 'BMW 320i', 'description': 'Carro'},
        {'title': 'Honda Biz 125', 'description': 'Moto 125cc'},
        {'title': 'Honda Pop 110i', 'description': 'Moto Honda'},
        {'title': 'Honda Civic EXR', 'description': 'Sedan'},
        {'title': 'Ford Ka SE 1.5', 'description': 'Hatch'},
        {'title': 'VW Polo 1.6', 'description': 'Carro'},
        {'title': 'VW Fox 1.0', 'description': 'Hatch'},
        {'title': 'Fiat Uno Vivace', 'description': 'Carro'},
        {'title': 'Chevrolet Celta 4p', 'description': 'Hatch'},
        {'title': 'Chevrolet Onix 1.0', 'description': 'Carro'},
        {'title': 'Chevrolet Corsa Sedan', 'description': 'Sedan'},
        {'title': 'Fiat Siena', 'description': 'Sedan'},
        {'title': 'Fiat Strada Endurance', 'description': 'Pickup'},
        {'title': 'Toyota Hilux CD', 'description': 'Pickup'},
        {'title': 'Hyundai Creta 1.6', 'description': 'SUV'},
        {'title': 'Renault Megane GT', 'description': 'Sedan'},
        {'title': 'Citroen C3 Excl', 'description': 'Hatch'},
        {'title': 'Mercedes-Benz Accelo 1117', 'description': 'Caminhão baú'},
        {'title': 'Scania R450', 'description': 'Caminhão'},
        {'title': 'Volvo FH 460 6x2', 'description': 'Caminhão'},
        {'title': 'Yamaha Factor 150', 'description': 'Moto 150cc'},
        {'title': 'Bajaj Dominar D400', 'description': 'Moto'},
        {'title': 'Ducati MTS 1200', 'description': 'Moto'},
    ]
    
    print("\n")
    errors = 0
    for i, test in enumerate(test_cases, 1):
        result = analyzer.analyze(test)
        title = test['title']
        vtype = result['vehicle_type']
        
        # Determina tipo esperado
        title_lower = title.lower()
        desc_lower = test['description'].lower()
        
        if any(x in title_lower + desc_lower for x in ['biz', 'pop', 'factor', 'bajaj', 'ducati', 'yamaha', 'f800', 'r1200', 's1000', 'moto']):
            expected = 'motos'
        elif any(x in title_lower + desc_lower for x in ['accelo', 'scania', 'volvo fh', 'caminhão', 'caminhao']):
            expected = 'caminhoes'
        else:
            expected = 'carros'
        
        # Emoji
        emoji_map = {
            'carros': '🚗',
            'motos': '🏍️',
            'caminhoes': '🚚',
            'implementos': '🚛',
            'onibus': '🚌',
        }
        emoji = emoji_map.get(vtype, '🚙')
        
        status = '✅' if vtype == expected else '❌'
        if vtype != expected:
            errors += 1
            print(f"{emoji} {status} [{i:2d}] {title:40s} → {vtype} (esperado: {expected})")
        else:
            print(f"{emoji} {status} [{i:2d}] {title:40s} → {vtype}")
    
    print("\n" + "="*60)
    print(f"{'✅ PERFEITO!' if errors == 0 else f'❌ {errors} erros encontrados'}")
    print("="*60)
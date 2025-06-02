import os
from dotenv import load_dotenv
import logging

# 환경 변수 로드
load_dotenv()

# API 키 설정
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_SECRET = os.getenv('BINANCE_SECRET')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# 노션 설정
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID')
TRADING_AGENTS_DB_ID = os.getenv('TRADING_AGENTS_DB_ID')
TRADING_DECISIONS_DB_ID = os.getenv('TRADING_DECISIONS_DB_ID')

# 데이터베이스 설정
DATABASE_PATH = "./data/trading_bot.db"

# 트레이딩 설정
DEFAULT_SYMBOL = "SOL/USDT"
TIMEFRAMES = ["5m", "15m", "1h"]

# 실시간 업데이트 간격 (초)
UPDATE_INTERVALS = {
    "price": 30,      # 현재가 30초마다
    "5m": 300,        # 5분봉 5분마다
    "15m": 900,       # 15분봉 15분마다
    "1h": 3600,       # 1시간봉 1시간마다
}

# 스케줄러 설정
SCHEDULER_INTERVAL_MINUTES = int(os.getenv('SCHEDULER_INTERVAL_MINUTES', '15'))  # 기본 15분

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./data/bot.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def get_symbol_display_name(symbol: str) -> str:
    """심볼의 표시 이름 반환"""
    if "/" in symbol:
        base_currency = symbol.split("/")[0]
        
        symbol_names = {
            "SOL": "Solana",
            "BTC": "Bitcoin", 
            "ETH": "Ethereum",
            "BNB": "Binance Coin",
            "ADA": "Cardano",
            "DOT": "Polkadot",
            "AVAX": "Avalanche",
            "MATIC": "Polygon",
            "LINK": "Chainlink",
            "UNI": "Uniswap",
            "DOGE": "Dogecoin",
            "XRP": "Ripple",
            "LTC": "Litecoin",
            "ATOM": "Cosmos",
            "NEAR": "NEAR Protocol",
            "SHIB": "Shiba Inu",
            "PEPE": "Pepe"
        }
        
        return symbol_names.get(base_currency, base_currency)
    
    return symbol

def normalize_symbol(symbol: str) -> str:
    """심볼을 표준 형식으로 정규화"""
    if not symbol:
        return DEFAULT_SYMBOL
    
    symbol = symbol.upper().strip()
    
    # 이미 정확한 형식인지 확인
    if "/" in symbol and symbol.endswith("USDT"):
        return symbol
    
    # USDT가 없으면 추가
    if not symbol.endswith("USDT"):
        if "/" in symbol:
            base = symbol.split("/")[0]
            symbol = f"{base}/USDT"
        else:
            symbol = f"{symbol}/USDT"
    
    # "BTCUSDT" 형식을 "BTC/USDT"로 변환
    if "/" not in symbol and symbol.endswith("USDT"):
        base = symbol[:-4]  # USDT 제거
        symbol = f"{base}/USDT"
    
    return symbol

def get_popular_symbols() -> list:
    """인기 있는 트레이딩 심볼들 반환"""
    return [
        "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
        "ADA/USDT", "DOT/USDT", "AVAX/USDT", "MATIC/USDT",
        "LINK/USDT", "UNI/USDT", "DOGE/USDT", "XRP/USDT",
        "LTC/USDT", "ATOM/USDT", "NEAR/USDT", "SHIB/USDT"
    ]

# 필수 환경변수 확인
required_vars = ['BINANCE_API_KEY', 'BINANCE_SECRET', 'GEMINI_API_KEY']
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    logger.error(f"필수 환경변수 누락: {missing_vars}")
    raise ValueError(f"환경변수를 설정해주세요: {missing_vars}")

# 노션 설정 확인 (선택사항)
notion_vars = ['NOTION_API_KEY', 'NOTION_DATABASE_ID', 'TRADING_AGENTS_DB_ID', 'TRADING_DECISIONS_DB_ID']
missing_notion_vars = [var for var in notion_vars if not os.getenv(var)]

if missing_notion_vars:
    logger.warning(f"노션 연동 환경변수 누락 (선택사항): {missing_notion_vars}")
    if 'TRADING_DECISIONS_DB_ID' in missing_notion_vars:
        logger.info("총괄 에이전트 매매 결정은 기존 분석 DB에 저장됩니다")
    logger.info("노션 분석 로깅 기능이 부분적으로 비활성화됩니다")
else:
    logger.info("노션 연동 설정 완료 (분석 결과 + 매매 결정)")

logger.info("설정 로드 완료 - 멀티 심볼 지원 활성화")
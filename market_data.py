import requests
import time
from typing import Dict, Optional
from datetime import datetime, timedelta
from config import logger
from database import db

class MarketDataCollector:
    """시장 데이터 수집 클래스"""
    
    def __init__(self):
        self.fear_greed_cache = None
        self.fear_greed_last_update = None
        self.cache_duration_minutes = 60  # 1시간 캐시
        logger.info("시장 데이터 수집기 초기화 완료")
    
    def get_fear_greed_index(self) -> Dict:
        """공포탐욕지수 조회 (캐시 사용)"""
        try:
            # 캐시 확인
            if (self.fear_greed_cache and self.fear_greed_last_update and 
                datetime.now() - self.fear_greed_last_update < timedelta(minutes=self.cache_duration_minutes)):
                return self.fear_greed_cache
            
            # API 호출
            url = "https://api.alternative.me/fng/"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and len(data['data']) > 0:
                    latest = data['data'][0]
                    
                    fear_greed_data = {
                        'value': int(latest['value']),
                        'value_classification': latest['value_classification'],
                        'timestamp': latest['timestamp'],
                        'updated_at': datetime.now().isoformat()
                    }
                    
                    # 캐시 업데이트
                    self.fear_greed_cache = fear_greed_data
                    self.fear_greed_last_update = datetime.now()
                    
                    logger.info(f"공포탐욕지수 업데이트: {fear_greed_data['value']} ({fear_greed_data['value_classification']})")
                    return fear_greed_data
            
            logger.warning(f"공포탐욕지수 API 호출 실패: {response.status_code}")
            return self._get_default_fear_greed()
            
        except Exception as e:
            logger.error(f"공포탐욕지수 조회 실패: {e}")
            return self._get_default_fear_greed()
    
    def _get_default_fear_greed(self) -> Dict:
        """기본 공포탐욕지수 (API 실패시)"""
        return {
            'value': 50,
            'value_classification': 'Neutral',
            'timestamp': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'is_default': True
        }
    
    def calculate_volatility(self, symbol: str, timeframe: str = "1h", periods: int = 24) -> Dict:
        """변동성 계산 (표준편차 기반)"""
        try:
            # 최근 데이터 조회
            candles_df = db.get_candles(symbol, timeframe, limit=periods + 10)
            
            if candles_df.empty or len(candles_df) < periods:
                logger.warning(f"{symbol} 변동성 계산을 위한 데이터 부족")
                return self._get_default_volatility()
            
            # 최근 periods개 데이터로 변동성 계산
            recent_prices = candles_df['close'].tail(periods)
            price_changes = recent_prices.pct_change().dropna()
            
            if len(price_changes) == 0:
                return self._get_default_volatility()
            
            # 변동성 지표들
            volatility = price_changes.std() * 100  # 표준편차 (%)
            avg_change = abs(price_changes.mean()) * 100  # 평균 변화율
            max_change = abs(price_changes).max() * 100  # 최대 변화율
            
            # 변동성 분류
            if volatility > 5.0:
                classification = "Very High"
            elif volatility > 3.0:
                classification = "High"
            elif volatility > 1.5:
                classification = "Medium"
            elif volatility > 0.5:
                classification = "Low"
            else:
                classification = "Very Low"
            
            volatility_data = {
                'symbol': symbol,
                'timeframe': timeframe,
                'periods': periods,
                'volatility': round(volatility, 4),
                'avg_change': round(avg_change, 4),
                'max_change': round(max_change, 4),
                'classification': classification,
                'current_price': float(recent_prices.iloc[-1]),
                'updated_at': datetime.now().isoformat()
            }
            
            logger.debug(f"{symbol} 변동성: {volatility:.2f}% ({classification})")
            return volatility_data
            
        except Exception as e:
            logger.error(f"{symbol} 변동성 계산 실패: {e}")
            return self._get_default_volatility()
    
    def _get_default_volatility(self) -> Dict:
        """기본 변동성 데이터"""
        return {
            'symbol': 'UNKNOWN',
            'timeframe': '1h',
            'periods': 24,
            'volatility': 2.0,
            'avg_change': 1.0,
            'max_change': 3.0,
            'classification': 'Medium',
            'current_price': 0.0,
            'updated_at': datetime.now().isoformat(),
            'is_default': True
        }
    
    def get_market_sentiment(self, symbol: str) -> Dict:
        """시장 센티먼트 종합"""
        try:
            # 공포탐욕지수
            fear_greed = self.get_fear_greed_index()
            
            # 변동성
            volatility = self.calculate_volatility(symbol)
            
            # 센티먼트 점수 계산 (0-100)
            fear_greed_score = fear_greed['value']
            
            # 변동성을 센티먼트로 변환 (높은 변동성 = 낮은 센티먼트)
            if volatility['volatility'] > 5.0:
                volatility_sentiment = 20  # 매우 높은 변동성 = 낮은 센티먼트
            elif volatility['volatility'] > 3.0:
                volatility_sentiment = 35
            elif volatility['volatility'] > 1.5:
                volatility_sentiment = 50
            elif volatility['volatility'] > 0.5:
                volatility_sentiment = 70
            else:
                volatility_sentiment = 85  # 낮은 변동성 = 높은 센티먼트
            
            # 가중 평균 (공포탐욕지수 70%, 변동성 30%)
            combined_sentiment = (fear_greed_score * 0.7) + (volatility_sentiment * 0.3)
            
            # 센티먼트 분류
            if combined_sentiment >= 75:
                sentiment_label = "Extreme Greed"
            elif combined_sentiment >= 55:
                sentiment_label = "Greed"
            elif combined_sentiment >= 45:
                sentiment_label = "Neutral"
            elif combined_sentiment >= 25:
                sentiment_label = "Fear"
            else:
                sentiment_label = "Extreme Fear"
            
            market_sentiment = {
                'symbol': symbol,
                'combined_sentiment': round(combined_sentiment, 1),
                'sentiment_label': sentiment_label,
                'fear_greed_index': fear_greed,
                'volatility_data': volatility,
                'recommendation': self._get_sentiment_recommendation(combined_sentiment),
                'updated_at': datetime.now().isoformat()
            }
            
            logger.info(f"{symbol} 시장 센티먼트: {combined_sentiment:.1f} ({sentiment_label})")
            return market_sentiment
            
        except Exception as e:
            logger.error(f"{symbol} 시장 센티먼트 계산 실패: {e}")
            return self._get_default_sentiment(symbol)
    
    def _get_sentiment_recommendation(self, sentiment_score: float) -> str:
        """센티먼트 기반 추천"""
        if sentiment_score >= 80:
            return "Be Cautious - Extreme Greed"
        elif sentiment_score >= 60:
            return "Consider Taking Profits"
        elif sentiment_score >= 40:
            return "Neutral Market"
        elif sentiment_score >= 20:
            return "Good Buying Opportunity"
        else:
            return "Excellent Buying Opportunity - Extreme Fear"
    
    def _get_default_sentiment(self, symbol: str) -> Dict:
        """기본 센티먼트 데이터"""
        return {
            'symbol': symbol,
            'combined_sentiment': 50.0,
            'sentiment_label': 'Neutral',
            'fear_greed_index': self._get_default_fear_greed(),
            'volatility_data': self._get_default_volatility(),
            'recommendation': 'Neutral Market',
            'updated_at': datetime.now().isoformat(),
            'is_default': True
        }


# 전역 인스턴스
market_data_collector = MarketDataCollector()

if __name__ == "__main__":
    # 테스트
    logger.info("시장 데이터 수집기 테스트 시작")
    
    # 공포탐욕지수 테스트
    fear_greed = market_data_collector.get_fear_greed_index()
    logger.info(f"공포탐욕지수: {fear_greed}")
    
    # 변동성 테스트
    volatility = market_data_collector.calculate_volatility("BTC/USDT")
    logger.info(f"BTC 변동성: {volatility}")
    
    # 시장 센티먼트 테스트
    sentiment = market_data_collector.get_market_sentiment("BTC/USDT")
    logger.info(f"BTC 시장 센티먼트: {sentiment}")
    
    logger.info("시장 데이터 수집기 테스트 완료")
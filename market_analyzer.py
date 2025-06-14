import ccxt
import threading
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from config import (BINANCE_API_KEY, BINANCE_SECRET, DEFAULT_SYMBOL, TIMEFRAMES, 
                   UPDATE_INTERVALS, normalize_symbol, get_symbol_display_name, logger)
from database import db

# market_analyzer.py에 추가할 SignalDetector 클래스

class SignalDetector:
    """기술적 지표 기반 시그널 감지 클래스"""
    
    def __init__(self):
        self.signal_history = {}  # 시그널 중복 방지용
        self.signal_cooldown_minutes = 30  # 같은 시그널 재발생 최소 간격 (분)
        logger.info("시그널 감지기 초기화 완료")
    
    def detect_signals_for_symbol(self, symbol: str, timeframe: str = "5m") -> List[Dict]:
        """특정 심볼의 시그널 감지"""
        try:
            symbol = normalize_symbol(symbol)
            
            # 캔들 데이터 조회 (최근 100개 정도)
            df = db.get_candles(symbol, timeframe, limit=100)
            if df.empty or len(df) < 50:
                logger.debug(f"{symbol} {timeframe}: 시그널 분석을 위한 데이터 부족")
                return []
            
            # 기술적 지표 계산
            analyzer = TechnicalAnalyzer()
            indicators_data = analyzer.calculate_all_indicators_timeseries(df, periods=50)
            if not indicators_data or not indicators_data.get('current'):
                logger.debug(f"{symbol} {timeframe}: 기술적 지표 계산 실패")
                return []
            
            current_indicators = indicators_data['current']
            current_price = df['close'].iloc[-1]
            
            # 시그널 감지
            detected_signals = []
            
            # 1. RSI 시그널
            rsi_signals = self._detect_rsi_signals(current_indicators, symbol)
            detected_signals.extend(rsi_signals)
            
            # 2. MACD 시그널
            macd_signals = self._detect_macd_signals(current_indicators, df, symbol)
            detected_signals.extend(macd_signals)
            
            # 3. 볼린저 밴드 시그널
            bb_signals = self._detect_bollinger_signals(current_indicators, current_price, symbol)
            detected_signals.extend(bb_signals)
            
            # 4. 이동평균 시그널
            ma_signals = self._detect_moving_average_signals(current_indicators, current_price, symbol)
            detected_signals.extend(ma_signals)
            
            # 5. 거래량 시그널
            volume_signals = self._detect_volume_signals(df, symbol)
            detected_signals.extend(volume_signals)
            
            # 6. CCI 시그널
            cci_signals = self._detect_cci_signals(current_indicators, symbol)
            detected_signals.extend(cci_signals)
            
            # 중복 제거 및 쿨다운 체크
            valid_signals = self._filter_valid_signals(detected_signals, symbol)
            
            if valid_signals:
                logger.info(f"🚨 {symbol} 시그널 감지: {[s['type'] for s in valid_signals]}")
            
            return valid_signals
            
        except Exception as e:
            logger.error(f"{symbol} 시그널 감지 실패: {e}")
            return []
    
    def _detect_rsi_signals(self, indicators: Dict, symbol: str) -> List[Dict]:
        """RSI 기반 시그널 감지"""
        signals = []
        rsi = indicators.get('rsi_14')
        
        if rsi is None:
            return signals
        
        # RSI 과매도 (30 이하)
        if rsi <= 30:
            signals.append({
                'symbol': symbol,
                'type': 'RSI_OVERSOLD',
                'strength': 'HIGH' if rsi <= 25 else 'MEDIUM',
                'value': rsi,
                'direction': 'BUY',
                'description': f'RSI 과매도 ({rsi:.1f})',
                'priority': 3 if rsi <= 25 else 2
            })
        
        # RSI 과매수 (70 이상)
        elif rsi >= 70:
            signals.append({
                'symbol': symbol,
                'type': 'RSI_OVERBOUGHT', 
                'strength': 'HIGH' if rsi >= 75 else 'MEDIUM',
                'value': rsi,
                'direction': 'SELL',
                'description': f'RSI 과매수 ({rsi:.1f})',
                'priority': 3 if rsi >= 75 else 2
            })
        
        return signals
    
    def _detect_macd_signals(self, indicators: Dict, df: pd.DataFrame, symbol: str) -> List[Dict]:
        """MACD 크로스오버 시그널 감지"""
        signals = []
        
        if len(df) < 3:
            return signals
        
        # 최근 3개 데이터에서 MACD 크로스오버 확인
        macd_series = indicators.get('macd')
        signal_series = indicators.get('macd_signal')
        
        if macd_series is None or signal_series is None:
            return signals
        
        # 현재와 이전 값 비교
        try:
            # 간단한 크로스오버 감지 로직 (실제로는 시계열 데이터로 더 정확하게 해야 함)
            current_macd = indicators.get('macd')
            current_signal = indicators.get('macd_signal')
            
            if current_macd and current_signal:
                # MACD가 시그널 라인 위에 있고 상승 추세
                if current_macd > current_signal:
                    signals.append({
                        'symbol': symbol,
                        'type': 'MACD_BULLISH',
                        'strength': 'MEDIUM',
                        'value': current_macd - current_signal,
                        'direction': 'BUY',
                        'description': f'MACD 강세 신호 (MACD: {current_macd:.4f})',
                        'priority': 2
                    })
                # MACD가 시그널 라인 아래에 있고 하락 추세
                elif current_macd < current_signal:
                    signals.append({
                        'symbol': symbol,
                        'type': 'MACD_BEARISH',
                        'strength': 'MEDIUM', 
                        'value': current_signal - current_macd,
                        'direction': 'SELL',
                        'description': f'MACD 약세 신호 (MACD: {current_macd:.4f})',
                        'priority': 2
                    })
        except Exception as e:
            logger.debug(f"MACD 시그널 감지 중 오류: {e}")
        
        return signals
    
    def _detect_bollinger_signals(self, indicators: Dict, current_price: float, symbol: str) -> List[Dict]:
        """볼린저 밴드 시그널 감지"""
        signals = []
        
        bb_upper = indicators.get('bb_upper')
        bb_lower = indicators.get('bb_lower')
        bb_middle = indicators.get('bb_middle')
        
        if not all([bb_upper, bb_lower, bb_middle]):
            return signals
        
        # 볼린저 밴드 하단 터치 (과매도)
        if current_price <= bb_lower:
            signals.append({
                'symbol': symbol,
                'type': 'BB_OVERSOLD',
                'strength': 'HIGH',
                'value': (bb_lower - current_price) / bb_lower * 100,
                'direction': 'BUY',
                'description': f'볼린저 밴드 하단 터치 (${current_price:.4f} <= ${bb_lower:.4f})',
                'priority': 3
            })
        
        # 볼린저 밴드 상단 터치 (과매수)
        elif current_price >= bb_upper:
            signals.append({
                'symbol': symbol,
                'type': 'BB_OVERBOUGHT',
                'strength': 'HIGH',
                'value': (current_price - bb_upper) / bb_upper * 100,
                'direction': 'SELL', 
                'description': f'볼린저 밴드 상단 터치 (${current_price:.4f} >= ${bb_upper:.4f})',
                'priority': 3
            })
        
        return signals
    
    def _detect_moving_average_signals(self, indicators: Dict, current_price: float, symbol: str) -> List[Dict]:
        """이동평균 크로스오버 시그널 감지"""
        signals = []
        
        ma_20 = indicators.get('ma_20')
        ma_50 = indicators.get('ma_50')
        
        if not all([ma_20, ma_50]):
            return signals
        
        # 골든 크로스 (MA20 > MA50 and 가격 > MA20)
        if ma_20 > ma_50 and current_price > ma_20:
            strength = 'HIGH' if (ma_20 - ma_50) / ma_50 > 0.02 else 'MEDIUM'  # 2% 이상 차이면 강한 신호
            signals.append({
                'symbol': symbol,
                'type': 'GOLDEN_CROSS',
                'strength': strength,
                'value': (ma_20 - ma_50) / ma_50 * 100,
                'direction': 'BUY',
                'description': f'골든 크로스 (MA20: ${ma_20:.4f} > MA50: ${ma_50:.4f})',
                'priority': 3 if strength == 'HIGH' else 2
            })
        
        # 데드 크로스 (MA20 < MA50 and 가격 < MA20)
        elif ma_20 < ma_50 and current_price < ma_20:
            strength = 'HIGH' if (ma_50 - ma_20) / ma_20 > 0.02 else 'MEDIUM'
            signals.append({
                'symbol': symbol,
                'type': 'DEAD_CROSS',
                'strength': strength,
                'value': (ma_50 - ma_20) / ma_20 * 100,
                'direction': 'SELL',
                'description': f'데드 크로스 (MA20: ${ma_20:.4f} < MA50: ${ma_50:.4f})',
                'priority': 3 if strength == 'HIGH' else 2
            })
        
        return signals
    
    def _detect_volume_signals(self, df: pd.DataFrame, symbol: str) -> List[Dict]:
        """거래량 급증 시그널 감지"""
        signals = []
        
        if len(df) < 20:
            return signals
        
        try:
            # 최근 20개 평균 거래량과 현재 거래량 비교
            recent_volumes = df['volume'].tail(20)
            current_volume = df['volume'].iloc[-1]
            avg_volume = recent_volumes.mean()
            
            # 거래량이 평균의 2배 이상
            if current_volume > avg_volume * 2:
                strength = 'HIGH' if current_volume > avg_volume * 3 else 'MEDIUM'
                signals.append({
                    'symbol': symbol,
                    'type': 'VOLUME_SPIKE',
                    'strength': strength,
                    'value': current_volume / avg_volume,
                    'direction': 'NEUTRAL',  # 거래량 자체는 방향성이 없음
                    'description': f'거래량 급증 ({current_volume/avg_volume:.1f}배)',
                    'priority': 2
                })
        except Exception as e:
            logger.debug(f"거래량 시그널 감지 중 오류: {e}")
        
        return signals
    
    def _detect_cci_signals(self, indicators: Dict, symbol: str) -> List[Dict]:
        """CCI 시그널 감지"""
        signals = []
        cci = indicators.get('cci_20')
        
        if cci is None:
            return signals
        
        # CCI 과매도 (-100 이하)
        if cci <= -100:
            signals.append({
                'symbol': symbol,
                'type': 'CCI_OVERSOLD',
                'strength': 'HIGH' if cci <= -150 else 'MEDIUM',
                'value': cci,
                'direction': 'BUY',
                'description': f'CCI 과매도 ({cci:.1f})',
                'priority': 2
            })
        
        # CCI 과매수 (100 이상)
        elif cci >= 100:
            signals.append({
                'symbol': symbol,
                'type': 'CCI_OVERBOUGHT',
                'strength': 'HIGH' if cci >= 150 else 'MEDIUM',
                'value': cci,
                'direction': 'SELL',
                'description': f'CCI 과매수 ({cci:.1f})',
                'priority': 2
            })
        
        return signals
    
    def _filter_valid_signals(self, signals: List[Dict], symbol: str) -> List[Dict]:
        """시그널 중복 제거 및 쿨다운 체크"""
        if not signals:
            return []
        
        current_time = datetime.now()
        valid_signals = []
        
        for signal in signals:
            signal_key = f"{symbol}_{signal['type']}"
            
            # 이전 시그널 시간 확인
            if signal_key in self.signal_history:
                last_time = self.signal_history[signal_key]
                time_diff = (current_time - last_time).total_seconds() / 60  # 분 단위
                
                if time_diff < self.signal_cooldown_minutes:
                    logger.debug(f"{signal_key} 시그널 쿨다운 중 ({time_diff:.1f}분 < {self.signal_cooldown_minutes}분)")
                    continue
            
            # 유효한 시그널로 판정
            valid_signals.append(signal)
            self.signal_history[signal_key] = current_time
        
        # 우선순위별 정렬 (높은 우선순위 먼저)
        valid_signals.sort(key=lambda x: x.get('priority', 1), reverse=True)
        
        return valid_signals
    
    def detect_signals_for_all_symbols(self, symbols: List[str], timeframe: str = "5m") -> Dict[str, List[Dict]]:
        """모든 심볼의 시그널 감지"""
        all_signals = {}
        
        for symbol in symbols:
            try:
                signals = self.detect_signals_for_symbol(symbol, timeframe)
                if signals:
                    all_signals[symbol] = signals
            except Exception as e:
                logger.error(f"{symbol} 시그널 감지 실패: {e}")
        
        return all_signals
    
    def get_signal_summary(self, all_signals: Dict[str, List[Dict]]) -> Dict:
        """시그널 요약 정보"""
        total_signals = sum(len(signals) for signals in all_signals.values())
        signal_types = {}
        high_priority_count = 0
        
        for symbol, signals in all_signals.items():
            for signal in signals:
                signal_type = signal['type']
                signal_types[signal_type] = signal_types.get(signal_type, 0) + 1
                
                if signal.get('priority', 1) >= 3:
                    high_priority_count += 1
        
        return {
            'total_signals': total_signals,
            'symbols_with_signals': len(all_signals),
            'signal_types': signal_types,
            'high_priority_signals': high_priority_count,
            'timestamp': datetime.now().isoformat()
        }

class DataCollector:
    """개선된 데이터 수집 클래스 - 시간 동기화 기반"""
    
    def __init__(self):
        self.exchange = ccxt.binance({
            'apiKey': BINANCE_API_KEY,
            'secret': BINANCE_SECRET,
            'sandbox': False,
            'enableRateLimit': True,
            'adjustForTimeDifference': True,
            'recvWindow': 10000,
        })
        self.running = False
        self.threads = []
        self.active_symbols = set([DEFAULT_SYMBOL])
        self._symbol_lock = threading.Lock()
        
        # 정각 기준 실행 시간 설정
        self.sync_minutes = {
            '5m': [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56],  # 5분마다 (스케줄러와 동일)
            '15m': [1, 16, 31, 46],  # 15분마다 (스케줄러와 동일)
            '1h': [1]  # 매시 1분 (스케줄러와 동일)
        }
        
        # 데이터 수집 통계
        self.collection_stats = {
            'total_collections': 0,
            'successful_collections': 0,
            'failed_collections': 0,
            'last_collection_time': None,
            'symbols_collected': {},
            'timeframe_stats': {}
        }
        
        logger.info("개선된 데이터 수집기 초기화 완료 - 시간 동기화 지원")

    def ensure_recent_data_for_symbol(self, symbol: str, hours_back: int = 2) -> bool:
        """특정 심볼의 최신 데이터 확보 - 개선된 버전"""
        try:
            symbol = normalize_symbol(symbol)
            logger.debug(f"🔄 {symbol} 최신 {hours_back}시간 데이터 확보 시작...")
            
            success_count = 0
            total_timeframes = len(TIMEFRAMES)
            
            for timeframe in TIMEFRAMES:
                try:
                    # 현재 데이터 상태 확인
                    candles_df = db.get_candles(symbol, timeframe, limit=20)
                    
                    if candles_df.empty:
                        logger.debug(f"📥 {symbol} {timeframe}: 데이터 없음 - 긴급 수집")
                        success = self._emergency_data_collection(symbol, timeframe)
                    else:
                        latest_time = candles_df['timestamp'].iloc[-1]
                        time_diff = datetime.now() - latest_time.to_pydatetime()
                        hours_old = time_diff.total_seconds() / 3600
                        
                        if hours_old > hours_back:
                            logger.debug(f"📥 {symbol} {timeframe}: 데이터가 {hours_old:.1f}시간 오래됨 - 업데이트")
                            success = self._emergency_data_collection(symbol, timeframe)
                        else:
                            logger.debug(f"✅ {symbol} {timeframe}: 최신 데이터 확인 ({hours_old:.1f}시간 전)")
                            success = True
                    
                    if success:
                        success_count += 1
                        
                except Exception as e:
                    logger.warning(f"❌ {symbol} {timeframe} 데이터 확보 실패: {e}")
            
            final_success = success_count >= (total_timeframes * 0.7)  # 70% 이상 성공시 OK
            
            if final_success:
                logger.debug(f"📊 {symbol} 데이터 확보 완료: {success_count}/{total_timeframes}")
            else:
                logger.warning(f"📊 {symbol} 데이터 확보 부족: {success_count}/{total_timeframes}")
            
            # 통계 업데이트
            if symbol not in self.collection_stats['symbols_collected']:
                self.collection_stats['symbols_collected'][symbol] = 0
            self.collection_stats['symbols_collected'][symbol] += success_count
            
            return final_success
            
        except Exception as e:
            logger.error(f"{symbol} 최신 데이터 확보 실패: {e}")
            return False
    
    def update_active_symbols(self, symbols: List[str]):
        """활성 심볼 목록 업데이트"""
        with self._symbol_lock:
            normalized_symbols = set()
            for symbol in symbols:
                try:
                    normalized = normalize_symbol(symbol)
                    normalized_symbols.add(normalized)
                except Exception as e:
                    logger.warning(f"심볼 정규화 실패: {symbol} - {e}")
            
            # 기본 심볼은 항상 포함
            normalized_symbols.add(DEFAULT_SYMBOL)
            
            if normalized_symbols != self.active_symbols:
                old_symbols = self.active_symbols.copy()
                self.active_symbols = normalized_symbols
                
                added = normalized_symbols - old_symbols
                removed = old_symbols - normalized_symbols
                
                if added:
                    logger.info(f"새로 추가된 수집 대상 심볼: {list(added)}")
                if removed:
                    logger.info(f"제거된 수집 대상 심볼: {list(removed)}")
                
                logger.info(f"현재 활성 심볼 {len(self.active_symbols)}개: {list(self.active_symbols)}")
    
    def get_active_symbols(self) -> List[str]:
        """현재 활성 심볼 목록 반환"""
        with self._symbol_lock:
            return list(self.active_symbols)
    
    def start_collection(self):
        """개선된 실시간 데이터 수집 시작"""
        if self.running:
            logger.warning("데이터 수집이 이미 실행 중입니다.")
            return
        
        self.running = True
        logger.info("개선된 멀티 심볼 실시간 데이터 수집 시작")
        
        # 현재가 수집 스레드 (더 빈번하게)
        price_thread = threading.Thread(target=self._collect_current_prices_loop_improved, daemon=True)
        price_thread.start()
        self.threads.append(price_thread)
        
        # 각 시간봉별 수집 스레드 (정각 기준)
        for timeframe in TIMEFRAMES:
            candle_thread = threading.Thread(
                target=self._collect_candles_loop_improved, 
                args=(timeframe,), 
                daemon=True
            )
            candle_thread.start()
            self.threads.append(candle_thread)
        
        logger.info(f"총 {len(self.threads)}개 개선된 수집 스레드 시작")
    
    def stop_collection(self):
        """데이터 수집 중지"""
        self.running = False
        logger.info("데이터 수집 중지 신호 전송")
        
        # 모든 스레드가 종료될 때까지 잠시 대기
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=3)
        
        self.threads.clear()
        logger.info("데이터 수집 완전 중지")
    
    def _collect_current_prices_loop_improved(self):
        """개선된 현재가 수집 루프 - 더 정확한 타이밍"""
        logger.info("개선된 현재가 수집 루프 시작")
        
        while self.running:
            try:
                symbols = self.get_active_symbols()
                collection_start = datetime.now()
                
                successful_updates = 0
                failed_updates = 0
                
                for symbol in symbols:
                    try:
                        ticker = self.exchange.fetch_ticker(symbol)
                        
                        price_data = {
                            'price': ticker['last'],
                            'volume_24h': ticker['quoteVolume'],
                            'change_24h': ticker['percentage']
                        }
                        
                        if db.insert_current_price(symbol, price_data):
                            successful_updates += 1
                            logger.debug(f"{symbol} 현재가 업데이트: ${ticker['last']:.4f}")
                        else:
                            failed_updates += 1
                            logger.warning(f"{symbol} 현재가 저장 실패")
                        
                        time.sleep(0.2)  # 심볼 간 간격 단축
                        
                    except Exception as e:
                        failed_updates += 1
                        logger.warning(f"{symbol} 현재가 수집 실패: {str(e)[:100]}")
                
                collection_time = (datetime.now() - collection_start).total_seconds()
                
                if successful_updates > 0:
                    logger.debug(f"현재가 수집 완료: {successful_updates}개 성공, {failed_updates}개 실패 ({collection_time:.1f}초)")
                
                # 통계 업데이트
                self.collection_stats['total_collections'] += 1
                self.collection_stats['successful_collections'] += successful_updates
                self.collection_stats['failed_collections'] += failed_updates
                self.collection_stats['last_collection_time'] = datetime.now().isoformat()
                
            except Exception as e:
                logger.error(f"현재가 수집 루프 오류: {e}")
                time.sleep(10)
            
            # 더 빈번한 업데이트 (20초마다)
            time.sleep(20)

    def _collect_candles_loop_improved(self, timeframe: str):
        """개선된 캔들 데이터 수집 루프 - 정각 기준"""
        logger.info(f"{timeframe} 개선된 캔들 수집 루프 시작")
        
        target_minutes = self.sync_minutes.get(timeframe, [0])
        
        while self.running:
            try:
                current_time = datetime.now()
                current_minute = current_time.minute
                
                # 정각 기준 실행 시간 체크
                should_collect = current_minute in target_minutes
                
                # 추가 조건: 정확한 시점에서 45초 이내 (더 넉넉하게)
                should_collect = should_collect and current_time.second <= 45
                
                if should_collect:
                    logger.info(f"🕒 {current_time.strftime('%H:%M')} {timeframe} 정각 기준 캔들 수집 시작")
                    
                    collection_start = datetime.now()
                    symbols = self.get_active_symbols()
                    
                    successful_symbols = 0
                    failed_symbols = 0
                    
                    for symbol in symbols:
                        try:
                            success = self._collect_symbol_candles(symbol, timeframe)
                            if success:
                                successful_symbols += 1
                                logger.debug(f"✅ {symbol} {timeframe} 정각 캔들 수집 성공")
                            else:
                                failed_symbols += 1
                                logger.warning(f"❌ {symbol} {timeframe} 정각 캔들 수집 실패")
                            
                            time.sleep(0.3)  # 심볼 간 간격
                            
                        except Exception as e:
                            failed_symbols += 1
                            logger.warning(f"{symbol} {timeframe} 정각 수집 실패: {str(e)[:100]}")
                    
                    collection_time = (datetime.now() - collection_start).total_seconds()
                    
                    logger.info(f"🕒 {timeframe} 정각 기준 수집 완료: {successful_symbols}개 성공, {failed_symbols}개 실패 ({collection_time:.1f}초)")
                    
                    # 통계 업데이트
                    if timeframe not in self.collection_stats['timeframe_stats']:
                        self.collection_stats['timeframe_stats'][timeframe] = {
                            'collections': 0,
                            'successful_symbols': 0,
                            'failed_symbols': 0,
                            'last_collection': None
                        }
                    
                    stats = self.collection_stats['timeframe_stats'][timeframe]
                    stats['collections'] += 1
                    stats['successful_symbols'] += successful_symbols
                    stats['failed_symbols'] += failed_symbols
                    stats['last_collection'] = datetime.now().isoformat()
                    
                    # 다음 정각 시간까지 대기
                    next_collection_time = self._get_next_collection_time(target_minutes)
                    wait_seconds = (next_collection_time - datetime.now()).total_seconds()
                    
                    if wait_seconds > 0:
                        logger.debug(f"{timeframe} 다음 수집 시간까지 대기: {next_collection_time.strftime('%H:%M')} ({wait_seconds:.0f}초)")
                        time.sleep(min(wait_seconds, 300))  # 최대 5분만 대기
                    else:
                        time.sleep(60)  # 기본 1분 대기
                else:
                    # 정각이 아니면 짧게 대기
                    time.sleep(30)
                    
            except Exception as e:
                logger.error(f"{timeframe} 캔들 수집 루프 오류: {e}")
                time.sleep(60)

    def _collect_symbol_candles(self, symbol: str, timeframe: str) -> bool:
        """단일 심볼의 캔들 데이터 수집"""
        try:
            # 최신 캔들 데이터 가져오기 (더 많은 데이터로 안정성 확보)
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=50)
            
            if len(ohlcv) >= 1:
                saved_count = 0
                # 최근 10개 캔들만 처리 (중복 방지)
                for candle in ohlcv[-10:]:
                    timestamp = datetime.fromtimestamp(candle[0] / 1000)
                    
                    # 현재 시간보다 미래 데이터는 제외
                    if timestamp > datetime.now():
                        continue
                    
                    # 너무 오래된 데이터도 제외 (최근 24시간 이내만)
                    if (datetime.now() - timestamp).total_seconds() > 86400:
                        continue
                    
                    candle_data = {
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5])
                    }
                    
                    if db.insert_candle(symbol, timestamp, timeframe, candle_data):
                        saved_count += 1
                
                if saved_count > 0:
                    logger.debug(f"✅ {symbol} {timeframe} 정각 캔들 {saved_count}개 저장")
                
                return saved_count > 0
            else:
                logger.warning(f"{symbol} {timeframe} 캔들 데이터가 없습니다")
                return False
                
        except Exception as e:
            logger.warning(f"{symbol} {timeframe} 캔들 수집 실패: {e}")
            return False

    def _get_next_collection_time(self, target_minutes: list) -> datetime:
        """다음 수집 시간 계산"""
        current_time = datetime.now()
        current_minute = current_time.minute
        
        # 현재 분 이후의 다음 수집 분 찾기
        next_minute = None
        for minute in sorted(target_minutes):
            if minute > current_minute:
                next_minute = minute
                break
        
        if next_minute is None:
            # 다음 시간의 첫 번째 수집 분
            next_minute = min(target_minutes)
            next_time = current_time.replace(minute=next_minute, second=0, microsecond=0) + timedelta(hours=1)
        else:
            next_time = current_time.replace(minute=next_minute, second=0, microsecond=0)
        
        return next_time

    def _emergency_data_collection(self, symbol: str, timeframe: str) -> bool:
        """긴급 데이터 수집 - 최소한의 최신 데이터만"""
        try:
            logger.debug(f"🚨 {symbol} {timeframe} 긴급 데이터 수집")
            
            # 최근 2시간 분량만 수집 (빠른 처리)
            since = int((datetime.now() - timedelta(hours=2)).timestamp() * 1000)
            
            ohlcv = self.exchange.fetch_ohlcv(
                symbol, 
                timeframe, 
                since=since, 
                limit=200  # 최대 200개로 제한
            )
            
            if not ohlcv:
                logger.warning(f"{symbol} {timeframe} 긴급 수집 - 데이터 없음")
                return False
            
            # 최신 데이터만 저장
            saved_count = 0
            for candle in ohlcv[-50:]:  # 최근 50개만
                try:
                    timestamp = datetime.fromtimestamp(candle[0] / 1000)
                    
                    # 미래 데이터 제외
                    if timestamp > datetime.now():
                        continue
                    
                    candle_data = {
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5])
                    }
                    
                    if db.insert_candle(symbol, timestamp, timeframe, candle_data):
                        saved_count += 1
                        
                except Exception as e:
                    logger.debug(f"캔들 저장 실패: {e}")
                    continue
            
            logger.debug(f"🚨 {symbol} {timeframe} 긴급 수집 완료: {saved_count}개")
            return saved_count > 0
            
        except Exception as e:
            logger.warning(f"{symbol} {timeframe} 긴급 수집 실패: {e}")
            return False
    
    def fetch_historical_data(self, symbol: str, timeframe: str, days: int = 5) -> bool:
        """특정 심볼의 과거 데이터 수집 - 개선된 버전"""
        try:
            symbol = normalize_symbol(symbol)
            symbol_display = get_symbol_display_name(symbol)
            
            # 시간봉별로 충분한 기간 설정
            timeframe_days = {
                '5m': max(days, 1),    # 1일 = 약 288개 캔들
                '15m': max(days, 2),   # 2일 = 약 192개 캔들  
                '1h': max(days, 5),    # 5일 = 약 120개 캔들
            }
            
            actual_days = timeframe_days.get(timeframe, days)
            logger.info(f"{symbol} ({symbol_display}) {timeframe} 과거 {actual_days}일 데이터 수집 시작")
            
            # 기존 데이터 확인 - 더 엄격하게 체크
            existing_data = db.get_candles(symbol, timeframe, limit=200)
            should_collect = True
            
            if not existing_data.empty:
                latest_time = existing_data['timestamp'].max()
                time_diff = datetime.now() - latest_time.to_pydatetime()
                data_count = len(existing_data)
                
                logger.info(f"{symbol} {timeframe} 기존 데이터: {data_count}개, 최신: {latest_time}")
                
                # 데이터 수와 최신성 모두 체크
                min_required_count = {
                    '5m': 200,   # 최소 200개
                    '15m': 150,  # 최소 150개
                    '1h': 100    # 최소 100개
                }
                
                required_count = min_required_count.get(timeframe, 100)
                max_age_hours = {
                    '5m': 0.5,   # 30분 이내
                    '15m': 1,    # 1시간 이내
                    '1h': 2      # 2시간 이내
                }
                
                max_age = max_age_hours.get(timeframe, 1)
                
                if data_count >= required_count and time_diff.total_seconds() < (max_age * 3600):
                    logger.info(f"{symbol} {timeframe} 충분한 데이터 존재 - 수집 건너뛰기")
                    should_collect = False
            
            if not should_collect:
                return True
            
            # 시작 시간 계산 - 더 넉넉하게
            since = int((datetime.now() - timedelta(days=actual_days * 2)).timestamp() * 1000)  # 2배 여유
            
            # 전체 데이터 수집
            all_ohlcv = []
            current_since = since
            batch_count = 0
            max_batches = 15
            
            logger.info(f"{symbol} {timeframe} 데이터 수집 시작 (목표: {actual_days}일)")
            
            while current_since < int(datetime.now().timestamp() * 1000) and batch_count < max_batches:
                try:
                    # 1000개씩 배치로 가져오기
                    ohlcv = self.exchange.fetch_ohlcv(
                        symbol, 
                        timeframe, 
                        since=current_since, 
                        limit=1000
                    )
                    
                    if not ohlcv or len(ohlcv) == 0:
                        logger.warning(f"{symbol} {timeframe} 배치 {batch_count}: 데이터 없음")
                        break
                    
                    all_ohlcv.extend(ohlcv)
                    
                    # 다음 배치 시작점 설정
                    last_timestamp = ohlcv[-1][0]
                    current_since = last_timestamp + 1
                    batch_count += 1
                    
                    logger.info(f"{symbol} {timeframe} 배치 {batch_count}: {len(ohlcv)}개 수집 (총 {len(all_ohlcv)}개)")
                    
                    # Rate limiting
                    time.sleep(0.1)
                    
                    # 중복 방지 - 같은 타임스탬프면 중단
                    if len(ohlcv) < 1000:  # 마지막 배치인 경우
                        break
                        
                except Exception as e:
                    logger.error(f"{symbol} {timeframe} 배치 {batch_count} 수집 실패: {e}")
                    time.sleep(1)
                    break
            
            if not all_ohlcv:
                logger.error(f"{symbol} {timeframe} 수집된 데이터가 없습니다")
                return False
            
            # 중복 제거 (타임스탬프 기준)
            unique_ohlcv = {}
            for candle in all_ohlcv:
                timestamp = candle[0]
                unique_ohlcv[timestamp] = candle
            
            final_ohlcv = list(unique_ohlcv.values())
            final_ohlcv.sort(key=lambda x: x[0])  # 시간순 정렬
            
            logger.info(f"{symbol} {timeframe} 중복 제거 후: {len(final_ohlcv)}개 캔들")
            
            # 데이터베이스에 저장
            saved_count = 0
            error_count = 0
            
            for candle in final_ohlcv:
                try:
                    timestamp = datetime.fromtimestamp(candle[0] / 1000)
                    candle_data = {
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5])
                    }
                    
                    if db.insert_candle(symbol, timestamp, timeframe, candle_data):
                        saved_count += 1
                        
                except Exception as e:
                    error_count += 1
                    if error_count < 5:  # 처음 5개 에러만 로깅
                        logger.warning(f"캔들 저장 실패: {e}")
            
            logger.info(f"{symbol} {timeframe} 저장 완료: {saved_count}개 성공, {error_count}개 실패")
            
            # 최종 확인
            final_check = db.get_candles(symbol, timeframe, limit=300)
            logger.info(f"{symbol} {timeframe} 최종 DB 확인: {len(final_check)}개 사용 가능")
            
            if len(final_check) < 50:
                logger.warning(f"{symbol} {timeframe} 저장된 데이터가 부족합니다: {len(final_check)}개")
            
            return saved_count > 0
            
        except Exception as e:
            logger.error(f"{symbol} {timeframe} 과거 데이터 수집 실패: {e}")
            return False
    
    def get_current_market_data(self, symbol: str = None) -> Dict:
        """특정 심볼의 현재 시장 데이터 조회"""
        if symbol is None:
            symbol = DEFAULT_SYMBOL
        
        try:
            symbol = normalize_symbol(symbol)
            ticker = self.exchange.fetch_ticker(symbol)
            
            return {
                'symbol': symbol,
                'symbol_display': get_symbol_display_name(symbol),
                'price': ticker['last'],
                'bid': ticker['bid'],
                'ask': ticker['ask'],
                'volume_24h': ticker['quoteVolume'],
                'change_24h': ticker['percentage'],
                'high_24h': ticker['high'],
                'low_24h': ticker['low'],
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"{symbol} 시장 데이터 조회 실패: {e}")
            return {}
    
    def check_connection(self) -> bool:
        """거래소 연결 상태 확인"""
        try:
            markets = self.exchange.load_markets()
            logger.info(f"거래소 연결 정상 - 지원 마켓: {len(markets)}개")
            return True
        except Exception as e:
            logger.error(f"거래소 연결 실패: {e}")
            return False
    
    def get_collection_statistics(self) -> Dict:
        """데이터 수집 통계 조회"""
        try:
            return {
                'running': self.running,
                'active_symbols': list(self.active_symbols),
                'active_symbol_count': len(self.active_symbols),
                'total_collections': self.collection_stats['total_collections'],
                'successful_collections': self.collection_stats['successful_collections'],
                'failed_collections': self.collection_stats['failed_collections'],
                'success_rate': (self.collection_stats['successful_collections'] / 
                               max(self.collection_stats['total_collections'], 1)) * 100,
                'last_collection_time': self.collection_stats['last_collection_time'],
                'symbols_collected': self.collection_stats['symbols_collected'],
                'timeframe_stats': self.collection_stats['timeframe_stats'],
                'sync_minutes': self.sync_minutes,
                'thread_count': len(self.threads),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"수집 통계 조회 실패: {e}")
            return {
                'running': self.running,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def force_symbol_collection(self, symbol: str, timeframes: List[str] = None) -> Dict:
        """특정 심볼의 강제 데이터 수집"""
        if timeframes is None:
            timeframes = TIMEFRAMES
        
        symbol = normalize_symbol(symbol)
        logger.info(f"🔄 {symbol} 강제 데이터 수집 시작: {timeframes}")
        
        results = {}
        
        for timeframe in timeframes:
            try:
                success = self._emergency_data_collection(symbol, timeframe)
                results[timeframe] = {
                    'success': success,
                    'message': '수집 완료' if success else '수집 실패'
                }
                
                time.sleep(0.5)  # 시간봉 간 간격
                
            except Exception as e:
                results[timeframe] = {
                    'success': False,
                    'error': str(e)
                }
        
        logger.info(f"✅ {symbol} 강제 수집 완료: {results}")
        
        return {
            'symbol': symbol,
            'symbol_display': get_symbol_display_name(symbol),
            'timeframes': timeframes,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_data_freshness(self, symbol: str = None) -> Dict:
        """데이터 신선도 확인"""
        try:
            if symbol:
                symbols = [normalize_symbol(symbol)]
            else:
                symbols = self.get_active_symbols()
            
            freshness_info = {}
            
            for sym in symbols:
                sym_info = {}
                for timeframe in TIMEFRAMES:
                    candles_df = db.get_candles(sym, timeframe, limit=1)
                    
                    if candles_df.empty:
                        sym_info[timeframe] = {
                            'status': 'NO_DATA',
                            'last_update': None,
                            'age_minutes': None
                        }
                    else:
                        latest_time = candles_df['timestamp'].iloc[-1]
                        age = datetime.now() - latest_time.to_pydatetime()
                        age_minutes = age.total_seconds() / 60
                        
                        if age_minutes < 60:
                            status = 'FRESH'
                        elif age_minutes < 180:
                            status = 'STALE'
                        else:
                            status = 'OLD'
                        
                        sym_info[timeframe] = {
                            'status': status,
                            'last_update': latest_time.isoformat(),
                            'age_minutes': round(age_minutes, 1)
                        }
                
                freshness_info[sym] = sym_info
            
            return {
                'symbols': freshness_info,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"데이터 신선도 확인 실패: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

class TechnicalAnalyzer:
    """기술적 분석 클래스 (기존 technical_analysis.py의 TechnicalAnalyzer)"""
    
    def __init__(self):
        pass
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """RSI 계산"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_ma(self, prices: pd.Series, period: int) -> pd.Series:
        """이동평균 계산"""
        return prices.rolling(window=period).mean()
    
    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """지수이동평균 계산"""
        return prices.ewm(span=period).mean()
    
    def calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
        """MACD 계산"""
        ema_fast = self.calculate_ema(prices, fast)
        ema_slow = self.calculate_ema(prices, slow)
        macd = ema_fast - ema_slow
        signal_line = self.calculate_ema(macd, signal)
        histogram = macd - signal_line
        
        return {
            'macd': macd,
            'signal': signal_line,
            'histogram': histogram
        }
    
    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_dev: int = 2) -> Dict:
        """볼린저 밴드 계산"""
        middle = self.calculate_ma(prices, period)
        std = prices.rolling(window=period).std()
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        
        return {
            'upper': upper,
            'middle': middle,
            'lower': lower
        }
    
    def calculate_cci(self, df: pd.DataFrame, period: int = 20) -> pd.Series:
        """CCI (Commodity Channel Index) 계산"""
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        sma_tp = typical_price.rolling(window=period).mean()
        mad = typical_price.rolling(window=period).apply(lambda x: np.mean(np.abs(x - x.mean())))
        cci = (typical_price - sma_tp) / (0.015 * mad)
        return cci
    
    def calculate_all_indicators_timeseries(self, df: pd.DataFrame, periods: int = 50) -> Dict:
        """모든 기술적 지표를 시계열로 계산"""
        if df.empty:
            logger.warning("기술적 지표 계산을 위한 데이터가 없습니다")
            return {}
        
        min_required = max(50, periods)  # 최소 필요 데이터
        
        if len(df) < min_required:
            logger.warning(f"데이터가 부족합니다. 현재: {len(df)}개, 권장: {min_required}개")
        
        close_prices = df['close']
        
        try:
            # 모든 지표 계산
            rsi_14 = self.calculate_rsi(close_prices, 14)
            ma_20 = self.calculate_ma(close_prices, 20)
            ma_50 = self.calculate_ma(close_prices, 50)
            macd_data = self.calculate_macd(close_prices)
            bb_data = self.calculate_bollinger_bands(close_prices)
            cci_20 = self.calculate_cci(df, 20)
            
            # 최근 N개 기간만 추출
            def safe_extract_series(series, periods):
                if series is None or series.empty:
                    return [None] * periods
                
                # NaN이 아닌 유효한 데이터만 추출
                valid_data = series.dropna()
                if valid_data.empty:
                    return [None] * periods
                
                # 최근 periods개 데이터 추출
                recent_data = valid_data.tail(periods)
                result = []
                for val in recent_data:
                    if pd.isna(val) or np.isinf(val):
                        result.append(None)
                    else:
                        result.append(round(float(val), 4))
                
                # 부족한 부분은 None으로 채움
                while len(result) < periods:
                    result.insert(0, None)
                
                return result[-periods:]
            
            indicators_timeseries = {
                'rsi_14': safe_extract_series(rsi_14, periods),
                'ma_20': safe_extract_series(ma_20, periods),
                'ma_50': safe_extract_series(ma_50, periods),
                'macd': safe_extract_series(macd_data['macd'], periods),
                'macd_signal': safe_extract_series(macd_data['signal'], periods),
                'macd_histogram': safe_extract_series(macd_data['histogram'], periods),
                'bb_upper': safe_extract_series(bb_data['upper'], periods),
                'bb_middle': safe_extract_series(bb_data['middle'], periods),
                'bb_lower': safe_extract_series(bb_data['lower'], periods),
                'cci_20': safe_extract_series(cci_20, periods),
            }
            
            # 현재값도 함께 반환
            def safe_get_last_value(series_list):
                if not series_list:
                    return None
                for val in reversed(series_list):
                    if val is not None:
                        return val
                return None
            
            current_indicators = {
                'rsi_14': safe_get_last_value(indicators_timeseries['rsi_14']),
                'ma_20': safe_get_last_value(indicators_timeseries['ma_20']),
                'ma_50': safe_get_last_value(indicators_timeseries['ma_50']),
                'macd': safe_get_last_value(indicators_timeseries['macd']),
                'macd_signal': safe_get_last_value(indicators_timeseries['macd_signal']),
                'bb_upper': safe_get_last_value(indicators_timeseries['bb_upper']),
                'bb_middle': safe_get_last_value(indicators_timeseries['bb_middle']),
                'bb_lower': safe_get_last_value(indicators_timeseries['bb_lower']),
                'cci_20': safe_get_last_value(indicators_timeseries['cci_20']),
            }
            
            return {
                'timeseries': indicators_timeseries,
                'current': current_indicators
            }
            
        except Exception as e:
            logger.error(f"기술적 지표 계산 실패: {e}")
            return {}
    
    def get_trading_signals(self, symbol: str, timeframe: str, analysis_periods: int = 50) -> Dict:
        """트레이딩 신호 생성"""
        try:
            # 심볼 정규화
            symbol = normalize_symbol(symbol)
            symbol_display = get_symbol_display_name(symbol)
            
            # 캔들 데이터 조회
            required_candles = max(100, analysis_periods * 2)
            df = db.get_candles(symbol, timeframe, limit=required_candles)
            
            if df.empty:
                logger.warning(f"{symbol} ({symbol_display}) {timeframe} 캔들 데이터가 없습니다")
                return {}
            
            logger.info(f"{symbol} ({symbol_display}) {timeframe}: {len(df)}개 캔들 데이터로 분석 시작")
            
            # 기술적 지표 시계열 계산
            indicators_data = self.calculate_all_indicators_timeseries(df, analysis_periods)
            if not indicators_data:
                logger.error(f"{symbol} ({symbol_display}) {timeframe} 기술적 지표 계산 실패")
                return {}
            
            current_indicators = indicators_data['current']
            timeseries_indicators = indicators_data['timeseries']
            
            # 최신 데이터 저장
            if not df.empty and current_indicators:
                latest_timestamp = df.iloc[-1]['timestamp']
                if hasattr(latest_timestamp, 'to_pydatetime'):
                    timestamp = latest_timestamp.to_pydatetime()
                else:
                    timestamp = pd.to_datetime(latest_timestamp).to_pydatetime()
                
                success = db.insert_technical_indicators(symbol, timestamp, timeframe, current_indicators)
                if success:
                    logger.debug(f"{symbol} {timeframe} 기술적 지표 저장 완료")
            
            # 기본 신호 생성
            signals = self._generate_signals(current_indicators, df)
            
            # 최근 캔들 데이터
            recent_candles = self._format_candles_for_api(df.tail(analysis_periods))
            
            # 최근 거래량 데이터
            recent_volumes = df.tail(analysis_periods)['volume'].tolist()
            recent_volumes = [round(float(vol), 2) for vol in recent_volumes if not pd.isna(vol)]
            
            logger.info(f"{symbol} ({symbol_display}) {timeframe} 신호 생성 완료: {signals.get('overall', 'N/A')}")
            
            return {
                'symbol': symbol,
                'symbol_display': symbol_display,
                'timeframe': timeframe,
                'analysis_periods': analysis_periods,
                'current_indicators': current_indicators,
                'indicators_timeseries': timeseries_indicators,
                'recent_candles': recent_candles,
                'recent_volumes': recent_volumes,
                'signals': signals,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"{symbol} {timeframe} 신호 생성 실패: {e}")
            return {}
    
    def _format_candles_for_api(self, df: pd.DataFrame) -> list:
        """캔들 데이터를 API 응답용으로 포맷팅"""
        candles_data = []
        for _, row in df.iterrows():
            try:
                candle = {
                    "timestamp": row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp']),
                    "open": float(row['open']),
                    "high": float(row['high']),
                    "low": float(row['low']),
                    "close": float(row['close']),
                    "volume": float(row['volume'])
                }
                candles_data.append(candle)
            except Exception as e:
                logger.warning(f"캔들 데이터 포맷팅 실패: {e}")
                continue
        
        return candles_data
    
    def _generate_signals(self, indicators: Dict, df: pd.DataFrame) -> Dict:
        """기술적 지표 기반 신호 생성"""
        signals = {}
        
        try:
            current_price = df['close'].iloc[-1]
            
            # RSI 신호
            if indicators.get('rsi_14') is not None:
                rsi = indicators['rsi_14']
                if rsi > 70:
                    signals['rsi'] = 'OVERBOUGHT'
                elif rsi < 30:
                    signals['rsi'] = 'OVERSOLD'
                else:
                    signals['rsi'] = 'NEUTRAL'
            
            # MACD 신호
            if indicators.get('macd') is not None and indicators.get('macd_signal') is not None:
                if indicators['macd'] > indicators['macd_signal']:
                    signals['macd'] = 'BULLISH'
                else:
                    signals['macd'] = 'BEARISH'
            
            # 볼린저 밴드 신호
            if all(k in indicators and indicators[k] is not None for k in ['bb_upper', 'bb_lower', 'bb_middle']):
                if current_price > indicators['bb_upper']:
                    signals['bollinger'] = 'OVERBOUGHT'
                elif current_price < indicators['bb_lower']:
                    signals['bollinger'] = 'OVERSOLD'
                else:
                    signals['bollinger'] = 'NEUTRAL'
            
            # CCI 신호
            if indicators.get('cci_20') is not None:
                cci = indicators['cci_20']
                if cci > 100:
                    signals['cci'] = 'OVERBOUGHT'
                elif cci < -100:
                    signals['cci'] = 'OVERSOLD'
                else:
                    signals['cci'] = 'NEUTRAL'
            
            # 이동평균 신호
            if indicators.get('ma_20') is not None and indicators.get('ma_50') is not None:
                if indicators['ma_20'] > indicators['ma_50']:
                    signals['ma_trend'] = 'BULLISH'
                else:
                    signals['ma_trend'] = 'BEARISH'
            
            # 종합 신호 계산
            signals['overall'] = self._calculate_overall_signal(signals)
            
        except Exception as e:
            logger.error(f"신호 생성 중 오류: {e}")
        
        return signals
    
    def _calculate_overall_signal(self, signals: Dict) -> str:
        """종합 신호 계산"""
        signal_weights = {
            'rsi': {'OVERSOLD': 1, 'OVERBOUGHT': -1, 'NEUTRAL': 0},
            'macd': {'BULLISH': 1, 'BEARISH': -1},
            'bollinger': {'OVERSOLD': 1, 'OVERBOUGHT': -1, 'NEUTRAL': 0},
            'cci': {'OVERSOLD': 1, 'OVERBOUGHT': -1, 'NEUTRAL': 0},
            'ma_trend': {'BULLISH': 1, 'BEARISH': -1}
        }
        
        total_score = 0
        signal_count = 0
        
        for signal_type, value in signals.items():
            if signal_type in signal_weights and value in signal_weights[signal_type]:
                total_score += signal_weights[signal_type][value]
                signal_count += 1
        
        if signal_count == 0:
            return 'HOLD'
        
        if signal_count < 3:
            if total_score >= 2:
                return 'BUY'
            elif total_score <= -2:
                return 'SELL'
            else:
                return 'HOLD'
        else:
            if total_score >= 3:
                return 'STRONG_BUY'
            elif total_score >= 1:
                return 'BUY'
            elif total_score <= -3:
                return 'STRONG_SELL'
            elif total_score <= -1:
                return 'SELL'
            else:
                return 'HOLD'


class MultiTimeframeAnalyzer:
    """멀티 타임프레임 분석 클래스 (기존 multi_timeframe_analyzer.py의 MultiTimeframeAnalyzer)"""
    
    def __init__(self):
        self.supported_timeframes = ["5m", "15m", "1h"]
        self.technical_analyzer = TechnicalAnalyzer()
    
    def collect_multi_timeframe_data(self, symbol: str, timeframes: List[str], analysis_periods: int = 50) -> Dict:
        """여러 시간봉의 데이터를 수집하고 기술적 지표 계산"""
        try:
            # 심볼 정규화
            symbol = normalize_symbol(symbol)
            symbol_display = get_symbol_display_name(symbol)
            
            # 요청된 시간봉이 지원되는지 확인
            invalid_timeframes = [tf for tf in timeframes if tf not in self.supported_timeframes]
            if invalid_timeframes:
                logger.error(f"지원하지 않는 시간봉: {invalid_timeframes}")
                return {}
            
            logger.info(f"{symbol} ({symbol_display}) 멀티 타임프레임 데이터 수집 시작: {timeframes}")
            
            # 🔥 각 시간봉별 최신 데이터 시간 확인 및 로깅
            logger.info(f"📊 === {symbol} 시간봉별 최신 데이터 상태 확인 ===")
            data_issues = []
            for tf in timeframes:
                candles_df = db.get_candles(symbol, tf, limit=5)
                if candles_df.empty:
                    logger.warning(f"❌ {tf}: 데이터 없음")
                    data_issues.append(tf)
                else:
                    latest_time = candles_df['timestamp'].iloc[-1]
                    latest_time_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(latest_time, 'strftime') else str(latest_time)
                    time_diff = datetime.now() - latest_time.to_pydatetime()
                    logger.info(f"✅ {tf}: 최신 데이터 {latest_time_str} ({time_diff.total_seconds()/60:.1f}분 전)")
            
            multi_data = {
                "symbol": symbol,
                "symbol_display": symbol_display,
                "timeframes_used": timeframes,
                "analysis_periods": analysis_periods,
                "timestamp": datetime.now().isoformat(),
                "timeframe_data": {}
            }
            
            successful_timeframes = []
            failed_timeframes = []
            
            # 각 시간봉별로 데이터 수집
            for timeframe in timeframes:
                try:
                    logger.debug(f"{symbol} {timeframe} 데이터 수집 시작...")
                    timeframe_info = self._collect_single_timeframe(symbol, timeframe, analysis_periods)
                    
                    if timeframe_info:
                        multi_data["timeframe_data"][timeframe] = timeframe_info
                        successful_timeframes.append(timeframe)
                        logger.info(f"✅ {symbol} {timeframe} 데이터 수집 성공")
                    else:
                        failed_timeframes.append(timeframe)
                        logger.warning(f"❌ {symbol} {timeframe} 데이터 수집 실패")
                        
                except Exception as e:
                    failed_timeframes.append(timeframe)
                    logger.error(f"❌ {symbol} {timeframe} 데이터 수집 중 오류: {e}")
            
            # 성공적으로 수집된 시간봉이 있는지 확인
            if not multi_data["timeframe_data"]:
                logger.error(f"{symbol} 모든 시간봉 데이터 수집 실패 ({failed_timeframes})")
                return {}
            
            # 부분적으로라도 성공한 경우 진행
            if failed_timeframes:
                logger.warning(f"{symbol} 일부 시간봉 실패: {failed_timeframes}, 성공: {successful_timeframes}")
                multi_data["timeframes_used"] = successful_timeframes
            
            logger.info(f"📊 === {symbol} ({symbol_display}) 멀티 타임프레임 수집 최종 완료: {successful_timeframes} ===")
            return multi_data
            
        except Exception as e:
            logger.error(f"{symbol} 멀티 타임프레임 데이터 수집 실패: {e}")
            return {}
    
    def _collect_single_timeframe(self, symbol: str, timeframe: str, analysis_periods: int) -> Optional[Dict]:
        """단일 시간봉 데이터 수집"""
        try:
            # 먼저 기본 캔들 데이터가 있는지 확인
            candles_df = db.get_candles(symbol, timeframe, limit=max(100, analysis_periods + 50))
            if candles_df.empty:
                logger.warning(f"{symbol} {timeframe} 캔들 데이터가 데이터베이스에 없습니다")
                return None
            
            logger.debug(f"{symbol} {timeframe}: 데이터베이스에서 {len(candles_df)}개 캔들 발견")
            
            # 기존 analyzer를 사용하여 기술적 지표 계산
            signals_data = self.technical_analyzer.get_trading_signals(symbol, timeframe, analysis_periods)
            
            if not signals_data:
                logger.warning(f"{symbol} {timeframe} 기술적 분석 실패")
                return None
            
            # 현재가 정보
            current_price = db.get_current_price(symbol)
            if not current_price:
                current_price = {
                    'symbol': symbol,
                    'price': candles_df.iloc[-1]['close'],
                    'timestamp': datetime.now().isoformat()
                }
            
            # 시계열 데이터를 배열로 정리 (최근 analysis_periods개)
            def extract_array_data(timeseries_data, key, periods):
                """시계열 데이터에서 배열 추출"""
                if not timeseries_data or key not in timeseries_data:
                    return []
                
                data_list = timeseries_data[key]
                if not isinstance(data_list, list):
                    return []
                
                # None 값을 제거하고 최근 데이터만 추출
                valid_data = [x for x in data_list if x is not None]
                return valid_data[-periods:] if len(valid_data) > periods else valid_data
            
            indicators_timeseries = signals_data.get("indicators_timeseries", {})
            recent_candles = signals_data.get("recent_candles", [])
            
            # 가격 및 볼륨 배열 추출
            prices = [float(candle["close"]) for candle in recent_candles[-analysis_periods:]]
            volumes = [float(candle["volume"]) for candle in recent_candles[-analysis_periods:]]
            
            timeframe_data = {
                "symbol": symbol,
                "timeframe": timeframe,
                "current_price": current_price['price'],
                "data_arrays": {
                    "prices": prices,
                    "volumes": volumes,
                    "rsi": extract_array_data(indicators_timeseries, "rsi_14", analysis_periods),
                    "macd": extract_array_data(indicators_timeseries, "macd", analysis_periods),
                    "macd_signal": extract_array_data(indicators_timeseries, "macd_signal", analysis_periods),
                    "ma_20": extract_array_data(indicators_timeseries, "ma_20", analysis_periods),
                    "ma_50": extract_array_data(indicators_timeseries, "ma_50", analysis_periods),
                    "bb_upper": extract_array_data(indicators_timeseries, "bb_upper", analysis_periods),
                    "bb_lower": extract_array_data(indicators_timeseries, "bb_lower", analysis_periods),
                    "cci": extract_array_data(indicators_timeseries, "cci_20", analysis_periods)
                }
            }
            
            return timeframe_data
            
        except Exception as e:
            logger.error(f"{symbol} {timeframe} 데이터 수집 실패: {e}")
            return None
    
    def create_simple_analysis_prompt(self, multi_data: Dict, agent_strategy: str) -> str:
        """테이블 형태의 간결한 AI 분석용 프롬프트 생성"""
        try:
            if not multi_data or not multi_data.get("timeframe_data"):
                logger.error("멀티 데이터가 없어서 프롬프트 생성 불가")
                return ""
            
            # 기본 정보
            symbol = multi_data.get("symbol", "UNKNOWN")
            symbol_display = multi_data.get("symbol_display", symbol)
            timeframes_used = multi_data.get("timeframes_used", [])
            
            if not timeframes_used:
                logger.error("사용된 시간봉이 없습니다")
                return ""
            
            # 첫 번째 시간봉 데이터 사용 (보통 15m)
            timeframe = timeframes_used[0]
            timeframe_data = multi_data["timeframe_data"].get(timeframe)
            
            if not timeframe_data:
                logger.error(f"{timeframe} 시간봉 데이터가 없습니다")
                return ""
            
            # 테이블 데이터 생성
            table_data = self._create_table_data(timeframe_data, timeframe)
            
            if not table_data:
                logger.error("테이블 데이터 생성 실패")
                return ""
            
            # 프롬프트 구성
            prompt_parts = [
                f"분석 대상: {symbol} ({symbol_display})",
                f"최신 10개 캔들 데이터 ({timeframe}봉):",
                "",
                table_data,
                "",
                f"전략: {agent_strategy}",
                "",
                "위 테이블 데이터를 분석하여 다음 JSON 형식으로 응답하세요:",
                "",
                """{
        "recommendation": "BUY|SELL|HOLD",
        "confidence": 0.75,
        "analysis": "상세한 분석 내용",
        "reasons": ["근거 1", "근거 2", "근거 3"],
        "target_price": 120.50,
        "stop_loss": 115.00,
        "risk_level": "LOW|MEDIUM|HIGH"
    }""",
                "",
                "- 테이블의 최신 데이터(마지막 행)가 현재 상황입니다",
                "- 시간 순서대로 트렌드를 분석하세요",
                "- JSON 형식을 정확히 지켜주세요"
            ]
            
            final_prompt = "\n".join(prompt_parts)
            logger.info(f"{symbol} 테이블 형태 프롬프트 생성 완료: {len(final_prompt)} 문자")
            
            return final_prompt
            
        except Exception as e:
            logger.error(f"테이블 프롬프트 생성 실패: {e}")
            return ""

    def _create_table_data(self, timeframe_data: Dict, timeframe: str) -> str:
        """테이블 형태의 데이터 생성"""
        try:
            data_arrays = timeframe_data.get("data_arrays", {})
            
            # 필요한 데이터 배열들
            prices = data_arrays.get('prices', [])
            volumes = data_arrays.get('volumes', [])
            rsi = data_arrays.get('rsi', [])
            macd = data_arrays.get('macd', [])
            macd_signal = data_arrays.get('macd_signal', [])
            ma_20 = data_arrays.get('ma_20', [])
            ma_50 = data_arrays.get('ma_50', [])
            bb_upper = data_arrays.get('bb_upper', [])
            bb_lower = data_arrays.get('bb_lower', [])
            cci = data_arrays.get('cci', [])
            
            if not prices:
                logger.error("가격 데이터가 없습니다")
                return ""
            
            # 최신 10개만 선택
            data_length = min(len(prices), 10)
            start_idx = max(0, len(prices) - data_length)
            
            # 테이블 헤더
            table_lines = [
                "시간   | 종가   | 거래량  | RSI | MACD | 신호선 | MA20  | MA50  | BB상단 | BB하단 | CCI"
            ]
            
            # 현재 시간을 기준으로 역순 계산
            from datetime import datetime, timedelta
            current_time = datetime.now()
            
            # 테이블 데이터 행들
            for i in range(start_idx, len(prices)):
                try:
                    # 시간 계산 (15분 간격으로 역순)
                    minutes_ago = (len(prices) - 1 - i) * 15
                    row_time = current_time - timedelta(minutes=minutes_ago)
                    time_str = row_time.strftime("%H:%M")
                    
                    # 각 값들 안전하게 추출
                    def safe_get(arr, idx, default=0):
                        try:
                            if idx < len(arr) and arr[idx] is not None:
                                return arr[idx]
                            return default
                        except:
                            return default
                    
                    def format_volume(vol):
                        """거래량 포맷팅 (K, M 단위)"""
                        if vol >= 1000000:
                            return f"{vol/1000000:.1f}M"
                        elif vol >= 1000:
                            return f"{vol/1000:.0f}K"
                        else:
                            return f"{vol:.0f}"
                    
                    def format_price(price):
                        """가격 포맷팅 (정수)"""
                        return f"{price:.0f}" if price else "0"
                    
                    def format_indicator(val):
                        """지표 포맷팅 (소수점 1자리)"""
                        return f"{val:.1f}" if val is not None else "0.0"
                    
                    # 각 열 데이터
                    close_price = safe_get(prices, i)
                    volume = safe_get(volumes, i)
                    rsi_val = safe_get(rsi, i)
                    macd_val = safe_get(macd, i)
                    signal_val = safe_get(macd_signal, i)
                    ma20_val = safe_get(ma_20, i)
                    ma50_val = safe_get(ma_50, i)
                    bb_up_val = safe_get(bb_upper, i)
                    bb_low_val = safe_get(bb_lower, i)
                    cci_val = safe_get(cci, i)
                    
                    # 테이블 행 생성 (고정 폭으로 정렬)
                    row = f"{time_str:<6} | {format_price(close_price):<6} | {format_volume(volume):<7} | {format_indicator(rsi_val):<3} | {format_indicator(macd_val):<4} | {format_indicator(signal_val):<4} | {format_price(ma20_val):<5} | {format_price(ma50_val):<5} | {format_price(bb_up_val):<6} | {format_price(bb_low_val):<6} | {format_indicator(cci_val):<3}"
                    
                    table_lines.append(row)
                    
                except Exception as e:
                    logger.warning(f"테이블 행 생성 실패 (인덱스 {i}): {e}")
                    continue
            
            if len(table_lines) <= 1:  # 헤더만 있는 경우
                logger.error("테이블 데이터 행이 없습니다")
                return ""
            
            return "\n".join(table_lines)
            
        except Exception as e:
            logger.error(f"테이블 데이터 생성 실패: {e}")
            return ""


class MarketAnalyzer:
    """통합 시장 분석 클래스"""
    
    def __init__(self):
        self.data_collector = DataCollector()
        self.technical_analyzer = TechnicalAnalyzer()
        self.multi_analyzer = MultiTimeframeAnalyzer()
        logger.info("통합 시장 분석기 초기화 완료")
    
    def start_data_collection(self):
        """데이터 수집 시작"""
        return self.data_collector.start_collection()
    
    def stop_data_collection(self):
        """데이터 수집 중지"""
        return self.data_collector.stop_collection()
    
    def update_active_symbols(self, symbols: List[str]):
        """활성 심볼 업데이트"""
        return self.data_collector.update_active_symbols(symbols)
    
    def get_active_symbols(self) -> List[str]:
        """활성 심볼 목록"""
        return self.data_collector.get_active_symbols()
    
    def ensure_recent_data(self, symbol: str, hours_back: int = 24) -> bool:
        """최신 데이터 확보"""
        return self.data_collector.ensure_recent_data_for_symbol(symbol, hours_back)
    
    def get_market_data(self, symbol: str) -> Dict:
        """현재 시장 데이터"""
        return self.data_collector.get_current_market_data(symbol)
    
    def get_technical_signals(self, symbol: str, timeframe: str, analysis_periods: int = 50) -> Dict:
        """기술적 신호"""
        return self.technical_analyzer.get_trading_signals(symbol, timeframe, analysis_periods)
    
    def get_multi_timeframe_data(self, symbol: str, timeframes: List[str], analysis_periods: int = 50) -> Dict:
        """멀티 타임프레임 데이터"""
        return self.multi_analyzer.collect_multi_timeframe_data(symbol, timeframes, analysis_periods)
    
    def create_ai_prompt(self, multi_data: Dict, strategy: str) -> str:
        """AI 분석용 프롬프트 생성"""
        return self.multi_analyzer.create_simple_analysis_prompt(multi_data, strategy)
    
    def check_connection(self) -> bool:
        """거래소 연결 확인"""
        return self.data_collector.check_connection()


# 전역 인스턴스
market_analyzer = MarketAnalyzer()

# market_analyzer.py에서 기존 SignalDetector 클래스를 이것으로 완전히 교체하세요

class SignalDetector:
    """개선된 기술적 지표 기반 시그널 감지 클래스"""
    
    def __init__(self):
        self.signal_history = {}  # 시그널 중복 방지용
        self.signal_cooldown_minutes = 60  # 같은 심볼 재분석 최소 간격 (분) - 1시간으로 증가
        logger.info("개선된 시그널 감지기 초기화 완료")
    
    def detect_signals_for_symbol(self, symbol: str, timeframe: str = "5m") -> List[Dict]:
        """특정 심볼의 시그널 감지 - 심볼당 한 번만 분석"""
        try:
            symbol = normalize_symbol(symbol)
            
            # 쿨다운 체크 - 심볼 단위로
            signal_key = f"{symbol}_ANALYSIS"
            if signal_key in self.signal_history:
                last_time = self.signal_history[signal_key]
                time_diff = (datetime.now() - last_time).total_seconds() / 60
                if time_diff < self.signal_cooldown_minutes:
                    logger.debug(f"{symbol} 분석 쿨다운 중 ({time_diff:.1f}분 < {self.signal_cooldown_minutes}분)")
                    return []
            
            # 캔들 데이터 조회 (크로스오버 감지를 위해 더 많은 데이터 필요)
            df = db.get_candles(symbol, timeframe, limit=200)
            if df.empty or len(df) < 100:
                logger.debug(f"{symbol} {timeframe}: 시그널 분석을 위한 데이터 부족 (현재: {len(df)}개)")
                return []
            
            # 기술적 지표 계산
            analyzer = TechnicalAnalyzer()
            indicators_data = analyzer.calculate_all_indicators_timeseries(df, periods=100)
            if not indicators_data or not indicators_data.get('current'):
                logger.debug(f"{symbol} {timeframe}: 기술적 지표 계산 실패")
                return []
            
            current_indicators = indicators_data['current']
            timeseries_indicators = indicators_data['timeseries']
            current_price = df['close'].iloc[-1]
            
            # 모든 시그널 감지
            detected_signals = []
            
            # 1. 이동평균 크로스오버 (실제 크로스 감지)
            ma_signals = self._detect_real_ma_crossover(timeseries_indicators, symbol)
            detected_signals.extend(ma_signals)
            
            # 2. MACD 크로스오버 (실제 크로스 감지)
            macd_signals = self._detect_real_macd_crossover(timeseries_indicators, symbol)
            detected_signals.extend(macd_signals)
            
            # 3. RSI 전환 신호 (단순 임계값이 아닌 추세 변화)
            rsi_signals = self._detect_rsi_reversal(timeseries_indicators, symbol)
            detected_signals.extend(rsi_signals)
            
            # 4. 볼린저 밴드 스퀴즈 및 브레이크아웃
            bb_signals = self._detect_bollinger_breakout(timeseries_indicators, current_price, symbol)
            detected_signals.extend(bb_signals)
            
            # 5. 거래량 + 가격 급등/급락
            volume_signals = self._detect_volume_price_surge(df, symbol)
            detected_signals.extend(volume_signals)
            
            # 6. CCI 전환 신호
            cci_signals = self._detect_cci_reversal(timeseries_indicators, symbol)
            detected_signals.extend(cci_signals)
            
            # 7. 다중 지표 합의 신호
            consensus_signals = self._detect_multi_indicator_consensus(current_indicators, symbol)
            detected_signals.extend(consensus_signals)
            
            # 유효한 시그널이 있으면 쿨다운 업데이트
            if detected_signals:
                self.signal_history[signal_key] = datetime.now()
                
                # 시그널 강도별 필터링 (MEDIUM 이상만)
                filtered_signals = [s for s in detected_signals if s.get('strength') in ['MEDIUM', 'HIGH', 'VERY_HIGH']]
                
                if filtered_signals:
                    # 가장 강한 시그널들만 선택 (최대 3개)
                    filtered_signals.sort(key=lambda x: self._get_strength_score(x.get('strength', 'LOW')), reverse=True)
                    final_signals = filtered_signals[:3]
                    
                    signal_types = [s['type'] for s in final_signals]
                    logger.info(f"🚨 {symbol} 유효 시그널 감지: {signal_types}")
                    
                    return final_signals
            
            return []
            
        except Exception as e:
            logger.error(f"{symbol} 시그널 감지 실패: {e}")
            return []
    
    def _get_strength_score(self, strength: str) -> int:
        """시그널 강도를 점수로 변환"""
        strength_scores = {
            'VERY_HIGH': 4,
            'HIGH': 3,
            'MEDIUM': 2,
            'LOW': 1
        }
        return strength_scores.get(strength, 1)
    
    def _detect_real_ma_crossover(self, timeseries: Dict, symbol: str) -> List[Dict]:
        """실제 이동평균 크로스오버 감지"""
        signals = []
        
        ma_20_series = timeseries.get('ma_20', [])
        ma_50_series = timeseries.get('ma_50', [])
        
        if len(ma_20_series) < 5 or len(ma_50_series) < 5:
            return signals
        
        try:
            # 최근 5개 데이터로 크로스오버 확인
            recent_20 = [x for x in ma_20_series[-5:] if x is not None]
            recent_50 = [x for x in ma_50_series[-5:] if x is not None]
            
            if len(recent_20) < 4 or len(recent_50) < 4:
                return signals
            
            # 크로스오버 감지: 이전에는 반대였다가 최근에 바뀐 경우
            prev_diff = recent_20[-2] - recent_50[-2]  # 이전 차이
            curr_diff = recent_20[-1] - recent_50[-1]  # 현재 차이
            
            # 골든 크로스 (MA20이 MA50을 아래에서 위로 뚫고 올라감)
            if prev_diff <= 0 and curr_diff > 0:
                # 강도 계산 (차이가 클수록 강한 신호)
                diff_pct = abs(curr_diff) / recent_50[-1] * 100
                strength = 'VERY_HIGH' if diff_pct > 1.0 else 'HIGH' if diff_pct > 0.5 else 'MEDIUM'
                
                signals.append({
                    'symbol': symbol,
                    'type': 'GOLDEN_CROSS',
                    'strength': strength,
                    'value': diff_pct,
                    'direction': 'BUY',
                    'description': f'골든 크로스 돌파 (MA20: ${recent_20[-1]:.4f}, MA50: ${recent_50[-1]:.4f})',
                    'priority': 4 if strength == 'VERY_HIGH' else 3
                })
            
            # 데드 크로스 (MA20이 MA50을 위에서 아래로 뚫고 내려감)
            elif prev_diff >= 0 and curr_diff < 0:
                diff_pct = abs(curr_diff) / recent_50[-1] * 100
                strength = 'VERY_HIGH' if diff_pct > 1.0 else 'HIGH' if diff_pct > 0.5 else 'MEDIUM'
                
                signals.append({
                    'symbol': symbol,
                    'type': 'DEAD_CROSS',
                    'strength': strength,
                    'value': diff_pct,
                    'direction': 'SELL',
                    'description': f'데드 크로스 돌파 (MA20: ${recent_20[-1]:.4f}, MA50: ${recent_50[-1]:.4f})',
                    'priority': 4 if strength == 'VERY_HIGH' else 3
                })
                
        except Exception as e:
            logger.debug(f"이동평균 크로스오버 감지 중 오류: {e}")
        
        return signals
    
    def _detect_real_macd_crossover(self, timeseries: Dict, symbol: str) -> List[Dict]:
        """실제 MACD 크로스오버 감지"""
        signals = []
        
        macd_series = timeseries.get('macd', [])
        signal_series = timeseries.get('macd_signal', [])
        
        if len(macd_series) < 5 or len(signal_series) < 5:
            return signals
        
        try:
            # 최근 5개 데이터
            recent_macd = [x for x in macd_series[-5:] if x is not None]
            recent_signal = [x for x in signal_series[-5:] if x is not None]
            
            if len(recent_macd) < 4 or len(recent_signal) < 4:
                return signals
            
            # 크로스오버 감지
            prev_diff = recent_macd[-2] - recent_signal[-2]
            curr_diff = recent_macd[-1] - recent_signal[-1]
            
            # MACD 상향 돌파 (강세 전환)
            if prev_diff <= 0 and curr_diff > 0:
                strength = 'HIGH' if abs(curr_diff) > 0.001 else 'MEDIUM'
                signals.append({
                    'symbol': symbol,
                    'type': 'MACD_BULLISH_CROSS',
                    'strength': strength,
                    'value': abs(curr_diff),
                    'direction': 'BUY',
                    'description': f'MACD 강세 돌파 ({recent_macd[-1]:.4f} > {recent_signal[-1]:.4f})',
                    'priority': 3 if strength == 'HIGH' else 2
                })
            
            # MACD 하향 돌파 (약세 전환)
            elif prev_diff >= 0 and curr_diff < 0:
                strength = 'HIGH' if abs(curr_diff) > 0.001 else 'MEDIUM'
                signals.append({
                    'symbol': symbol,
                    'type': 'MACD_BEARISH_CROSS',
                    'strength': strength,
                    'value': abs(curr_diff),
                    'direction': 'SELL',
                    'description': f'MACD 약세 돌파 ({recent_macd[-1]:.4f} < {recent_signal[-1]:.4f})',
                    'priority': 3 if strength == 'HIGH' else 2
                })
                
        except Exception as e:
            logger.debug(f"MACD 크로스오버 감지 중 오류: {e}")
        
        return signals
    
    def _detect_rsi_reversal(self, timeseries: Dict, symbol: str) -> List[Dict]:
        """RSI 전환 신호 감지 (단순 임계값이 아닌 추세 변화)"""
        signals = []
        
        rsi_series = timeseries.get('rsi_14', [])
        if len(rsi_series) < 10:
            return signals
        
        try:
            # 최근 10개 RSI 값
            recent_rsi = [x for x in rsi_series[-10:] if x is not None]
            if len(recent_rsi) < 8:
                return signals
            
            current_rsi = recent_rsi[-1]
            prev_rsi_trend = sum(recent_rsi[-4:-1]) / 3  # 이전 3개 평균
            
            # RSI 과매도에서 반등 신호
            if current_rsi <= 35 and current_rsi > prev_rsi_trend:  # 과매도 구간에서 상승 전환
                strength = 'VERY_HIGH' if current_rsi <= 25 else 'HIGH' if current_rsi <= 30 else 'MEDIUM'
                signals.append({
                    'symbol': symbol,
                    'type': 'RSI_OVERSOLD_REVERSAL',
                    'strength': strength,
                    'value': current_rsi,
                    'direction': 'BUY',
                    'description': f'RSI 과매도 반전 ({current_rsi:.1f} ↗)',
                    'priority': 4 if strength == 'VERY_HIGH' else 3
                })
            
            # RSI 과매수에서 하락 신호
            elif current_rsi >= 65 and current_rsi < prev_rsi_trend:  # 과매수 구간에서 하락 전환
                strength = 'VERY_HIGH' if current_rsi >= 75 else 'HIGH' if current_rsi >= 70 else 'MEDIUM'
                signals.append({
                    'symbol': symbol,
                    'type': 'RSI_OVERBOUGHT_REVERSAL',
                    'strength': strength,
                    'value': current_rsi,
                    'direction': 'SELL',
                    'description': f'RSI 과매수 반전 ({current_rsi:.1f} ↘)',
                    'priority': 4 if strength == 'VERY_HIGH' else 3
                })
                
        except Exception as e:
            logger.debug(f"RSI 전환 신호 감지 중 오류: {e}")
        
        return signals
    
    def _detect_bollinger_breakout(self, timeseries: Dict, current_price: float, symbol: str) -> List[Dict]:
        """볼린저 밴드 스퀴즈 후 브레이크아웃 감지"""
        signals = []
        
        bb_upper_series = timeseries.get('bb_upper', [])
        bb_lower_series = timeseries.get('bb_lower', [])
        bb_middle_series = timeseries.get('bb_middle', [])
        
        if len(bb_upper_series) < 20 or len(bb_lower_series) < 20:
            return signals
        
        try:
            # 최근 20개 데이터
            recent_upper = [x for x in bb_upper_series[-20:] if x is not None]
            recent_lower = [x for x in bb_lower_series[-20:] if x is not None]
            recent_middle = [x for x in bb_middle_series[-20:] if x is not None]
            
            if len(recent_upper) < 15 or len(recent_lower) < 15:
                return signals
            
            # 밴드폭 계산 (스퀴즈 감지)
            current_width = (recent_upper[-1] - recent_lower[-1]) / recent_middle[-1] * 100
            avg_width = sum([(recent_upper[i] - recent_lower[i]) / recent_middle[i] * 100 
                           for i in range(-10, -1)]) / 9
            
            # 스퀴즈 후 확장 (브레이크아웃)
            if current_width > avg_width * 1.2:  # 밴드폭이 20% 이상 확장
                # 상향 브레이크아웃
                if current_price > recent_upper[-2]:  # 이전 상단을 돌파
                    signals.append({
                        'symbol': symbol,
                        'type': 'BB_UPWARD_BREAKOUT',
                        'strength': 'HIGH',
                        'value': (current_price - recent_upper[-2]) / recent_upper[-2] * 100,
                        'direction': 'BUY',
                        'description': f'볼린저 밴드 상향 돌파 (${current_price:.4f} > ${recent_upper[-2]:.4f})',
                        'priority': 3
                    })
                
                # 하향 브레이크아웃
                elif current_price < recent_lower[-2]:  # 이전 하단을 돌파
                    signals.append({
                        'symbol': symbol,
                        'type': 'BB_DOWNWARD_BREAKOUT',
                        'strength': 'HIGH',
                        'value': (recent_lower[-2] - current_price) / recent_lower[-2] * 100,
                        'direction': 'SELL',
                        'description': f'볼린저 밴드 하향 돌파 (${current_price:.4f} < ${recent_lower[-2]:.4f})',
                        'priority': 3
                    })
                    
        except Exception as e:
            logger.debug(f"볼린저 밴드 브레이크아웃 감지 중 오류: {e}")
        
        return signals
    
    def _detect_volume_price_surge(self, df: pd.DataFrame, symbol: str) -> List[Dict]:
        """거래량 + 가격 급등/급락 동반 신호"""
        signals = []
        
        if len(df) < 30:
            return signals
        
        try:
            # 최근 30개 데이터
            recent_df = df.tail(30)
            current_volume = recent_df['volume'].iloc[-1]
            current_price = recent_df['close'].iloc[-1]
            prev_price = recent_df['close'].iloc[-2]
            
            # 평균 거래량 (최근 20개)
            avg_volume = recent_df['volume'].iloc[-21:-1].mean()
            
            # 거래량이 평균의 2.5배 이상 + 가격 변화가 2% 이상
            if current_volume > avg_volume * 2.5:
                price_change_pct = (current_price - prev_price) / prev_price * 100
                
                # 급등 (거래량 + 가격 상승)
                if price_change_pct > 2.0:
                    strength = 'VERY_HIGH' if price_change_pct > 5.0 else 'HIGH'
                    signals.append({
                        'symbol': symbol,
                        'type': 'VOLUME_PRICE_SURGE_UP',
                        'strength': strength,
                        'value': price_change_pct,
                        'direction': 'BUY',
                        'description': f'거래량 급증 + 급등 ({price_change_pct:+.1f}%, 거래량 {current_volume/avg_volume:.1f}배)',
                        'priority': 4 if strength == 'VERY_HIGH' else 3
                    })
                
                # 급락 (거래량 + 가격 하락)
                elif price_change_pct < -2.0:
                    strength = 'VERY_HIGH' if price_change_pct < -5.0 else 'HIGH'
                    signals.append({
                        'symbol': symbol,
                        'type': 'VOLUME_PRICE_SURGE_DOWN',
                        'strength': strength,
                        'value': abs(price_change_pct),
                        'direction': 'SELL',
                        'description': f'거래량 급증 + 급락 ({price_change_pct:+.1f}%, 거래량 {current_volume/avg_volume:.1f}배)',
                        'priority': 4 if strength == 'VERY_HIGH' else 3
                    })
                    
        except Exception as e:
            logger.debug(f"거래량-가격 급변 감지 중 오류: {e}")
        
        return signals
    
    def _detect_cci_reversal(self, timeseries: Dict, symbol: str) -> List[Dict]:
        """CCI 전환 신호 감지"""
        signals = []
        
        cci_series = timeseries.get('cci_20', [])
        if len(cci_series) < 10:
            return signals
        
        try:
            # 최근 10개 CCI 값
            recent_cci = [x for x in cci_series[-10:] if x is not None]
            if len(recent_cci) < 8:
                return signals
            
            current_cci = recent_cci[-1]
            prev_cci_avg = sum(recent_cci[-4:-1]) / 3
            
            # CCI 과매도에서 반전
            if current_cci <= -80 and current_cci > prev_cci_avg:
                strength = 'HIGH' if current_cci <= -120 else 'MEDIUM'
                signals.append({
                    'symbol': symbol,
                    'type': 'CCI_OVERSOLD_REVERSAL',
                    'strength': strength,
                    'value': current_cci,
                    'direction': 'BUY',
                    'description': f'CCI 과매도 반전 ({current_cci:.1f} ↗)',
                    'priority': 3 if strength == 'HIGH' else 2
                })
            
            # CCI 과매수에서 반전
            elif current_cci >= 80 and current_cci < prev_cci_avg:
                strength = 'HIGH' if current_cci >= 120 else 'MEDIUM'
                signals.append({
                    'symbol': symbol,
                    'type': 'CCI_OVERBOUGHT_REVERSAL',
                    'strength': strength,
                    'value': current_cci,
                    'direction': 'SELL',
                    'description': f'CCI 과매수 반전 ({current_cci:.1f} ↘)',
                    'priority': 3 if strength == 'HIGH' else 2
                })
                
        except Exception as e:
            logger.debug(f"CCI 전환 신호 감지 중 오류: {e}")
        
        return signals
    
    def _detect_multi_indicator_consensus(self, indicators: Dict, symbol: str) -> List[Dict]:
        """다중 지표 합의 신호 감지"""
        signals = []
        
        try:
            bullish_count = 0
            bearish_count = 0
            
            # RSI 체크
            rsi = indicators.get('rsi_14')
            if rsi:
                if rsi < 40:
                    bullish_count += 1
                elif rsi > 60:
                    bearish_count += 1
            
            # MACD 체크
            macd = indicators.get('macd')
            macd_signal = indicators.get('macd_signal')
            if macd and macd_signal:
                if macd > macd_signal:
                    bullish_count += 1
                else:
                    bearish_count += 1
            
            # 이동평균 체크
            ma_20 = indicators.get('ma_20')
            ma_50 = indicators.get('ma_50')
            if ma_20 and ma_50:
                if ma_20 > ma_50:
                    bullish_count += 1
                else:
                    bearish_count += 1
            
            # CCI 체크
            cci = indicators.get('cci_20')
            if cci:
                if cci < -50:
                    bullish_count += 1
                elif cci > 50:
                    bearish_count += 1
            
            # 강한 합의 (3개 이상 지표가 같은 방향)
            if bullish_count >= 3:
                strength = 'VERY_HIGH' if bullish_count >= 4 else 'HIGH'
                signals.append({
                    'symbol': symbol,
                    'type': 'MULTI_INDICATOR_BULLISH',
                    'strength': strength,
                    'value': bullish_count,
                    'direction': 'BUY',
                    'description': f'다중 지표 강세 합의 ({bullish_count}개 지표)',
                    'priority': 4 if strength == 'VERY_HIGH' else 3
                })
            
            elif bearish_count >= 3:
                strength = 'VERY_HIGH' if bearish_count >= 4 else 'HIGH'
                signals.append({
                    'symbol': symbol,
                    'type': 'MULTI_INDICATOR_BEARISH',
                    'strength': strength,
                    'value': bearish_count,
                    'direction': 'SELL',
                    'description': f'다중 지표 약세 합의 ({bearish_count}개 지표)',
                    'priority': 4 if strength == 'VERY_HIGH' else 3
                })
                
        except Exception as e:
            logger.debug(f"다중 지표 합의 감지 중 오류: {e}")
        
        return signals
    
    def detect_signals_for_all_symbols(self, symbols: List[str], timeframe: str = "5m") -> Dict[str, List[Dict]]:
        """모든 심볼의 시그널 감지 - 심볼당 한 번만"""
        all_signals = {}
        
        for symbol in symbols:
            try:
                signals = self.detect_signals_for_symbol(symbol, timeframe)
                if signals:
                    all_signals[symbol] = signals
                    logger.info(f"📊 {symbol}: {len(signals)}개 시그널 감지")
            except Exception as e:
                logger.error(f"{symbol} 시그널 감지 실패: {e}")
        
        return all_signals
    
    def get_signal_summary(self, all_signals: Dict[str, List[Dict]]) -> Dict:
        """시그널 요약 정보"""
        total_signals = sum(len(signals) for signals in all_signals.values())
        signal_types = {}
        high_priority_count = 0
        very_high_strength_count = 0
        
        for symbol, signals in all_signals.items():
            for signal in signals:
                signal_type = signal['type']
                signal_types[signal_type] = signal_types.get(signal_type, 0) + 1
                
                if signal.get('priority', 1) >= 3:
                    high_priority_count += 1
                    
                if signal.get('strength') == 'VERY_HIGH':
                    very_high_strength_count += 1
        
        return {
            'total_signals': total_signals,
            'symbols_with_signals': len(all_signals),
            'signal_types': signal_types,
            'high_priority_signals': high_priority_count,
            'very_high_strength_signals': very_high_strength_count,
            'timestamp': datetime.now().isoformat()
        }

def initialize_historical_data(symbols: List[str] = None, days: int = 5):
    """초기 과거 데이터 수집"""
    logger.info("멀티 심볼 초기 과거 데이터 수집 시작")
    
    if not market_analyzer.check_connection():
        logger.error("거래소 연결 실패")
        return False
    
    if symbols:
        market_analyzer.update_active_symbols(symbols)
    
    # 과거 데이터 수집
    results = {}
    active_symbols = market_analyzer.get_active_symbols()
    
    for symbol in active_symbols:
        results[symbol] = {}
        symbol_display = get_symbol_display_name(symbol)
        logger.info(f"🔄 {symbol} ({symbol_display}) 과거 데이터 수집 시작...")
        
        for timeframe in TIMEFRAMES:
            try:
                success = market_analyzer.data_collector.fetch_historical_data(symbol, timeframe, days)
                results[symbol][timeframe] = success
                
                if success:
                    logger.info(f"✅ {symbol} {timeframe}: 완료")
                else:
                    logger.error(f"❌ {symbol} {timeframe}: 실패")
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"❌ {symbol} {timeframe}: {e}")
                results[symbol][timeframe] = False
        
        time.sleep(2)  # 심볼 간 간격
    
    # 결과 요약
    total_success = sum(sum(timeframes.values()) for timeframes in results.values())
    total_attempts = len(active_symbols) * len(TIMEFRAMES)
    
    logger.info(f"📊 전체 과거 데이터 수집 완료: {total_success}/{total_attempts} 성공")
    
    return results
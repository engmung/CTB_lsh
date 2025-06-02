import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os
import threading
from config import DATABASE_PATH, DEFAULT_SYMBOL, normalize_symbol, logger

class Database:
    def __init__(self):
        self.db_path = DATABASE_PATH
        self._lock = threading.Lock()
        # 데이터 디렉토리 생성
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_database()
    
    def get_connection(self):
        """데이터베이스 연결 반환"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
    
    def _convert_to_datetime(self, timestamp):
        """Timestamp를 datetime으로 변환"""
        if isinstance(timestamp, str):
            return pd.to_datetime(timestamp).to_pydatetime()
        elif hasattr(timestamp, 'to_pydatetime'):
            return timestamp.to_pydatetime()
        elif isinstance(timestamp, datetime):
            return timestamp
        else:
            return pd.to_datetime(timestamp).to_pydatetime()
    
    def init_database(self):
        """데이터베이스 테이블 초기화"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 캔들 데이터 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS candles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        timeframe TEXT NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume REAL NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, timestamp, timeframe)
                    )
                """)
                
                # 현재가 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS current_price (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        price REAL NOT NULL,
                        volume_24h REAL,
                        change_24h REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # AI 분석 결과 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ai_analysis (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        agent_name TEXT,
                        recommendation TEXT NOT NULL,
                        confidence REAL NOT NULL,
                        analysis TEXT NOT NULL,
                        target_price REAL,
                        stop_loss REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 기술적 지표 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS technical_indicators (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        timeframe TEXT NOT NULL,
                        rsi_14 REAL,
                        ma_20 REAL,
                        ma_50 REAL,
                        macd REAL,
                        macd_signal REAL,
                        bb_upper REAL,
                        bb_middle REAL,
                        bb_lower REAL,
                        cci_20 REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, timestamp, timeframe)
                    )
                """)
                
                conn.commit()
                logger.info("데이터베이스 초기화 완료")
    
    def insert_candle(self, symbol: str, timestamp, timeframe: str, ohlcv: Dict):
        """캔들 데이터 삽입"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    dt_timestamp = self._convert_to_datetime(timestamp)
                    symbol = normalize_symbol(symbol)
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO candles 
                        (symbol, timestamp, timeframe, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        dt_timestamp,
                        timeframe,
                        float(ohlcv['open']),
                        float(ohlcv['high']),
                        float(ohlcv['low']),
                        float(ohlcv['close']),
                        float(ohlcv['volume'])
                    ))
                    conn.commit()
                    return True
                except Exception as e:
                    logger.error(f"캔들 데이터 삽입 실패 ({symbol}): {e}")
                    return False
    
    def insert_current_price(self, symbol: str, price_data: Dict):
        """현재가 데이터 삽입"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    symbol = normalize_symbol(symbol)
                    
                    cursor.execute("""
                        INSERT INTO current_price 
                        (symbol, timestamp, price, volume_24h, change_24h)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        datetime.now(),
                        float(price_data['price']),
                        float(price_data.get('volume_24h', 0)),
                        float(price_data.get('change_24h', 0))
                    ))
                    conn.commit()
                    return True
                except Exception as e:
                    logger.error(f"현재가 데이터 삽입 실패 ({symbol}): {e}")
                    return False
    
    def insert_technical_indicators(self, symbol: str, timestamp, timeframe: str, indicators: Dict):
        """기술적 지표 삽입"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    dt_timestamp = self._convert_to_datetime(timestamp)
                    symbol = normalize_symbol(symbol)
                    
                    def safe_float(value):
                        if value is None or pd.isna(value):
                            return None
                        return float(value)
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO technical_indicators 
                        (symbol, timestamp, timeframe, rsi_14, ma_20, ma_50, macd, macd_signal, bb_upper, bb_middle, bb_lower, cci_20)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        dt_timestamp,
                        timeframe,
                        safe_float(indicators.get('rsi_14')),
                        safe_float(indicators.get('ma_20')),
                        safe_float(indicators.get('ma_50')),
                        safe_float(indicators.get('macd')),
                        safe_float(indicators.get('macd_signal')),
                        safe_float(indicators.get('bb_upper')),
                        safe_float(indicators.get('bb_middle')),
                        safe_float(indicators.get('bb_lower')),
                        safe_float(indicators.get('cci_20'))
                    ))
                    conn.commit()
                    return True
                except Exception as e:
                    logger.error(f"기술적 지표 삽입 실패 ({symbol}): {e}")
                    return False
                
    
    def insert_ai_analysis(self, analysis_data: Dict):
        """AI 분석 결과 삽입"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    symbol = normalize_symbol(analysis_data.get('symbol', DEFAULT_SYMBOL))
                    
                    cursor.execute("""
                        INSERT INTO ai_analysis 
                        (symbol, timestamp, agent_name, recommendation, confidence, analysis, target_price, stop_loss)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        datetime.now(),
                        analysis_data.get('agent_name'),
                        analysis_data['recommendation'],
                        float(analysis_data['confidence']),
                        analysis_data['analysis'],
                        float(analysis_data.get('target_price', 0)) if analysis_data.get('target_price') else None,
                        float(analysis_data.get('stop_loss', 0)) if analysis_data.get('stop_loss') else None
                    ))
                    conn.commit()
                    return cursor.lastrowid
                except Exception as e:
                    logger.error(f"AI 분석 결과 삽입 실패: {e}")
                    return None
    
    def get_candles(self, symbol: str, timeframe: str, limit: int = 100) -> pd.DataFrame:
        """캔들 데이터 조회"""
        with self.get_connection() as conn:
            symbol = normalize_symbol(symbol)
            
            try:
                # 실제 데이터 조회 - 최신 데이터부터
                query = """
                    SELECT timestamp, open, high, low, close, volume 
                    FROM candles 
                    WHERE symbol = ? AND timeframe = ? 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """
                
                df = pd.read_sql_query(query, conn, params=(symbol, timeframe, limit))
                
                if df.empty:
                    logger.warning(f"{symbol} {timeframe} 캔들 데이터가 없습니다")
                    return df
                
                # 타임스탬프 처리
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp').reset_index(drop=True)  # 시간순 정렬
                
                # 더 자세한 로깅
                latest_time = df['timestamp'].iloc[-1]
                oldest_time = df['timestamp'].iloc[0]
                time_span = latest_time - oldest_time
                
                logger.debug(f"{symbol} {timeframe} 캔들 {len(df)}개 조회 완료 "
                            f"(기간: {oldest_time.strftime('%m-%d %H:%M')} ~ {latest_time.strftime('%m-%d %H:%M')}, "
                            f"범위: {time_span.total_seconds()/3600:.1f}시간)")
                
                return df
                
            except Exception as e:
                logger.error(f"{symbol} {timeframe} 캔들 조회 실패: {e}")
                return pd.DataFrame()
    
    def get_current_price(self, symbol: str = None) -> Optional[Dict]:
        """최신 현재가 조회"""
        if symbol is None:
            symbol = DEFAULT_SYMBOL
        
        symbol = normalize_symbol(symbol)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT price, volume_24h, change_24h, timestamp 
                FROM current_price 
                WHERE symbol = ?
                ORDER BY timestamp DESC 
                LIMIT 1
            """, (symbol,))
            row = cursor.fetchone()
            if row:
                return {
                    'symbol': symbol,
                    'price': row[0],
                    'volume_24h': row[1],
                    'change_24h': row[2],
                    'timestamp': row[3]
                }
            return None
    
    def get_technical_indicators(self, symbol: str, timeframe: str, limit: int = 50) -> pd.DataFrame:
        """기술적 지표 조회"""
        with self.get_connection() as conn:
            symbol = normalize_symbol(symbol)
            
            query = """
                SELECT * FROM technical_indicators 
                WHERE symbol = ? AND timeframe = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(symbol, timeframe, limit))
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp').reset_index(drop=True)
            return df
    
    def get_ai_analysis_history(self, symbol: str = None, limit: int = 10) -> List[Dict]:
        """AI 분석 히스토리 조회"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if symbol:
                symbol = normalize_symbol(symbol)
                cursor.execute("""
                    SELECT * FROM ai_analysis 
                    WHERE symbol = ?
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (symbol, limit))
            else:
                cursor.execute("""
                    SELECT * FROM ai_analysis 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (limit,))
            
            columns = [description[0] for description in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            return results
    
    def get_available_symbols(self) -> List[str]:
        """데이터베이스에 저장된 모든 심볼 목록"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT DISTINCT symbol FROM candles ORDER BY symbol")
                symbols = [row[0] for row in cursor.fetchall()]
                return symbols
            except Exception as e:
                logger.error(f"사용 가능한 심볼 조회 실패: {e}")
                return []
            
    # database.py의 Database 클래스에서 init_database 메서드를 다음으로 완전히 교체하세요:

    def init_database(self):
        """데이터베이스 테이블 초기화"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 캔들 데이터 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS candles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        timeframe TEXT NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume REAL NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, timestamp, timeframe)
                    )
                """)
                
                # 현재가 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS current_price (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        price REAL NOT NULL,
                        volume_24h REAL,
                        change_24h REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # AI 분석 결과 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ai_analysis (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        agent_name TEXT,
                        recommendation TEXT NOT NULL,
                        confidence REAL NOT NULL,
                        analysis TEXT NOT NULL,
                        target_price REAL,
                        stop_loss REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 기술적 지표 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS technical_indicators (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        timestamp DATETIME NOT NULL,
                        timeframe TEXT NOT NULL,
                        rsi_14 REAL,
                        ma_20 REAL,
                        ma_50 REAL,
                        macd REAL,
                        macd_signal REAL,
                        bb_upper REAL,
                        bb_middle REAL,
                        bb_lower REAL,
                        cci_20 REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, timestamp, timeframe)
                    )
                """)
                
                # 가상 거래 기록 테이블 (새로 추가)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS virtual_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        action TEXT NOT NULL,
                        direction TEXT,
                        price REAL NOT NULL,
                        size REAL,
                        leverage REAL DEFAULT 1.0,
                        invested_amount REAL,
                        realized_pnl REAL,
                        target_price REAL,
                        stop_loss REAL,
                        exit_reason TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 총괄 에이전트 결정 기록 테이블 (새로 추가)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS master_decisions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL DEFAULT 'SOL/USDT',
                        trading_decision TEXT NOT NULL,
                        confidence REAL NOT NULL,
                        direction TEXT,
                        leverage REAL,
                        target_price REAL,
                        stop_loss REAL,
                        reasoning TEXT,
                        risk_assessment TEXT,
                        market_timing TEXT,
                        expected_return REAL,
                        current_price REAL,
                        portfolio_balance REAL,
                        market_sentiment REAL,
                        execution_success BOOLEAN,
                        individual_analysis_id INTEGER,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
                logger.info("데이터베이스 초기화 완료 (가상 거래 테이블 포함)")

    def insert_virtual_trade(self, trade_data: Dict):
        """가상 거래 기록 삽입"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO virtual_trades 
                        (symbol, action, direction, price, size, leverage, invested_amount, 
                        realized_pnl, target_price, stop_loss, exit_reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        trade_data.get('symbol', DEFAULT_SYMBOL),
                        trade_data['action'],
                        trade_data.get('direction'),
                        float(trade_data['price']),
                        float(trade_data.get('size', 0)),
                        float(trade_data.get('leverage', 1.0)),
                        float(trade_data.get('invested_amount', 0)),
                        float(trade_data.get('realized_pnl', 0)),
                        float(trade_data.get('target_price', 0)) if trade_data.get('target_price') else None,
                        float(trade_data.get('stop_loss', 0)) if trade_data.get('stop_loss') else None,
                        trade_data.get('exit_reason')
                    ))
                    conn.commit()
                    return cursor.lastrowid
                except Exception as e:
                    logger.error(f"가상 거래 기록 삽입 실패: {e}")
                    return None

    def insert_master_decision(self, decision_data: Dict):
        """총괄 에이전트 결정 기록 삽입"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    portfolio_status = decision_data.get('portfolio_status', {})
                    market_sentiment = decision_data.get('market_sentiment', {})
                    execution_result = decision_data.get('execution_result', {})
                    
                    cursor.execute("""
                        INSERT INTO master_decisions 
                        (symbol, trading_decision, confidence, direction, leverage, target_price, 
                        stop_loss, reasoning, risk_assessment, market_timing, expected_return,
                        current_price, portfolio_balance, market_sentiment, execution_success,
                        individual_analysis_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        decision_data.get('symbol', DEFAULT_SYMBOL),
                        decision_data['trading_decision'],
                        float(decision_data['confidence']),
                        decision_data.get('direction'),
                        float(decision_data.get('leverage', 1.0)),
                        float(decision_data.get('target_price', 0)) if decision_data.get('target_price') else None,
                        float(decision_data.get('stop_loss', 0)) if decision_data.get('stop_loss') else None,
                        decision_data.get('reasoning'),
                        decision_data.get('risk_assessment'),
                        decision_data.get('market_timing'),
                        float(decision_data.get('expected_return', 0)),
                        float(decision_data.get('current_price', 0)),
                        float(portfolio_status.get('current_balance', 0)),
                        float(market_sentiment.get('combined_sentiment', 50)),
                        execution_result.get('success', False),
                        decision_data.get('individual_analysis_id')
                    ))
                    conn.commit()
                    return cursor.lastrowid
                except Exception as e:
                    logger.error(f"총괄 결정 기록 삽입 실패: {e}")
                    return None

    def get_virtual_trades_history(self, limit: int = 20) -> List[Dict]:
        """가상 거래 히스토리 조회"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT * FROM virtual_trades 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (limit,))
                
                columns = [description[0] for description in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                return results
            except Exception as e:
                logger.error(f"가상 거래 히스토리 조회 실패: {e}")
                return []

    def get_master_decisions_history(self, limit: int = 20) -> List[Dict]:
        """총괄 결정 히스토리 조회"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT * FROM master_decisions 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (limit,))
                
                columns = [description[0] for description in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                return results
            except Exception as e:
                logger.error(f"총괄 결정 히스토리 조회 실패: {e}")
                return []

    def get_portfolio_statistics(self) -> Dict:
        """포트폴리오 통계 조회"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 전체 거래 수
                cursor.execute("SELECT COUNT(*) FROM virtual_trades WHERE action = 'EXIT'")
                total_trades = cursor.fetchone()[0]
                
                # 수익 거래 수
                cursor.execute("SELECT COUNT(*) FROM virtual_trades WHERE action = 'EXIT' AND realized_pnl > 0")
                profitable_trades = cursor.fetchone()[0]
                
                # 총 손익
                cursor.execute("SELECT SUM(realized_pnl) FROM virtual_trades WHERE action = 'EXIT'")
                total_pnl_result = cursor.fetchone()[0]
                total_pnl = total_pnl_result if total_pnl_result else 0.0
                
                # 승률 계산
                win_rate = (profitable_trades / total_trades * 100) if total_trades > 0 else 0.0
                
                return {
                    'total_trades': total_trades,
                    'profitable_trades': profitable_trades,
                    'losing_trades': total_trades - profitable_trades,
                    'win_rate': win_rate,
                    'total_pnl': total_pnl,
                    'average_pnl': total_pnl / total_trades if total_trades > 0 else 0.0
                }
            except Exception as e:
                logger.error(f"포트폴리오 통계 조회 실패: {e}")
                return {
                    'total_trades': 0,
                    'profitable_trades': 0,
                    'losing_trades': 0,
                    'win_rate': 0.0,
                    'total_pnl': 0.0,
                    'average_pnl': 0.0
                }

# 전역 데이터베이스 인스턴스
db = Database()
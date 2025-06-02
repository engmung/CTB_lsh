from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, List, Optional
import threading
import schedule
import time
import uvicorn
import asyncio
from datetime import datetime, timedelta

# 프로젝트 모듈 임포트
from config import DEFAULT_SYMBOL, TIMEFRAMES, SCHEDULER_INTERVAL_MINUTES, get_symbol_display_name, normalize_symbol, logger
from database import db
from market_analyzer import market_analyzer, initialize_historical_data
from ai_system import ai_system
from trading_engine import trading_engine, risk_manager, position_manager
from notion_integration import notion_config, notion_logger
from master_agent import master_agent
from virtual_portfolio import virtual_portfolio
from market_data import market_data_collector
from position_monitor import position_monitor


# FastAPI 앱 생성
app = FastAPI(
    title="Trading Bot API",
    description="AI 기반 멀티 심볼 암호화폐 트레이딩 봇 (시그널 기반)",
    version="2.0.0",
    docs_url="/docs",  # 이 줄이 있는지 확인
    redoc_url="/redoc"  # 이 줄도 확인
)

# 데이터 수집 상태
collection_status = {
    "running": False,
    "started_at": None,
    "errors": []
}

# 스케줄러 상태
scheduler_status = {
    "running": False,
    "started_at": None,
    "mode": "signal_based",
    "signal_check_interval": 5,
    "verification_interval": 15,
    "analysis_count": 0,
    "verification_count": 0,
    "signal_detection_count": 0,
    "last_run": None,
    "last_signal_check": None,
    "errors": []
}


class AgentAnalysisRequest(BaseModel):
    agent_name: str
    analysis_periods: int = 50


class SignalBasedScheduler:
    """개선된 시그널 기반 분석 스케줄러 클래스 - 정각 기준 실행"""
    
    def __init__(self):
        self.running = False
        self.thread = None
        self.analysis_count = 0
        self.verification_count = 0
        self.signal_detection_count = 0
        self.data_collection_count = 0
        self.last_run = None
        self.last_signal_check = None
        self.last_data_collection = None
        self.errors = []
        self.signal_detector = None
        
        # 정각 기준 실행 시간 설정
        self.data_collection_schedule = {
            '5m': [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56],  # 5분마다
            '15m': [1, 16, 31, 46],  # 15분마다
            '1h': [1]  # 매시 1분
        }
        
        # 시그널 체크는 데이터 수집 2분 후 (안전 마진)
        self.signal_check_schedule = {
            '5m': [3, 8, 13, 18, 23, 28, 33, 38, 43, 48, 53, 58],  # 5분마다 + 2분
            '15m': [3, 18, 33, 48],  # 15분마다 + 2분
            '1h': [3]  # 매시 3분
        }
        
        # 검증은 15분마다
        self.verification_schedule = [3, 18, 33, 48]
    
    def start_scheduler(self):
        """시간 동기화된 스케줄러 시작"""
        if self.running:
            logger.warning("시그널 기반 스케줄러가 이미 실행 중입니다")
            return False
        
        if not ai_system.is_available():
            logger.error("AI 분석기를 사용할 수 없어 스케줄러를 시작할 수 없습니다")
            return False
        
        if not notion_logger.is_available():
            logger.error("노션 연동이 불가능하여 스케줄러를 시작할 수 없습니다")
            return False
        
        # SignalDetector 초기화
        try:
            from market_analyzer import SignalDetector
            self.signal_detector = SignalDetector()
            logger.info("시그널 감지기 초기화 완료")
        except Exception as e:
            logger.error(f"시그널 감지기 초기화 실패: {e}")
            return False
        
        # 정각 기준 스케줄 등록
        self._register_synchronized_schedules()
        
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        
        # 전역 상태 업데이트
        global scheduler_status
        scheduler_status.update({
            "running": True,
            "started_at": datetime.now().isoformat(),
            "mode": "synchronized_signal_based",
            "data_collection_schedule": self.data_collection_schedule,
            "signal_check_schedule": self.signal_check_schedule,
            "verification_schedule": self.verification_schedule,
            "errors": []
        })
        
        logger.info(f"🕒 === 시간 동기화 스케줄러 시작 ===")
        logger.info(f"📊 데이터 수집: 5분({len(self.data_collection_schedule['5m'])}회/시), 15분({len(self.data_collection_schedule['15m'])}회/시), 1시간({len(self.data_collection_schedule['1h'])}회/시)")
        logger.info(f"🚨 시그널 체크: 5분({len(self.signal_check_schedule['5m'])}회/시), 15분({len(self.signal_check_schedule['15m'])}회/시), 1시간({len(self.signal_check_schedule['1h'])}회/시)")
        logger.info(f"🔍 검증: {len(self.verification_schedule)}회/시")
        
        # 다음 실행 시간 표시
        self._log_next_execution_times()
        
        return True
    
    def _register_synchronized_schedules(self):
        """정각 기준 동기화된 스케줄 등록"""
        logger.info("정각 기준 스케줄 등록 중...")
        
        # 데이터 수집 스케줄
        for timeframe, minutes in self.data_collection_schedule.items():
            for minute in minutes:
                schedule.every().hour.at(f":{minute:02d}").do(
                    self._data_collection_job, timeframe
                ).tag(f"data_{timeframe}")
        
        # 시그널 체크 스케줄 (통합)
        for minute in self.signal_check_schedule['5m']:  # 가장 빈번한 5분 스케줄 사용
            schedule.every().hour.at(f":{minute:02d}").do(
                self._signal_detection_job
            ).tag("signal_check")
        
        # 검증 스케줄
        for minute in self.verification_schedule:
            schedule.every().hour.at(f":{minute:02d}").do(
                self._verification_job
            ).tag("verification")
        
        logger.info(f"총 {len(schedule.get_jobs())}개 정각 기준 작업 등록 완료")
    
    def stop_scheduler(self):
        """스케줄러 중지"""
        self.running = False
        schedule.clear()
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        # 전역 상태 업데이트
        global scheduler_status
        scheduler_status.update({
            "running": False,
            "started_at": None,
            "mode": "stopped"
        })
        
        logger.info("시간 동기화 스케줄러 중지")
    
    def _run_scheduler(self):
        """스케줄러 메인 루프"""
        logger.info("스케줄러 메인 루프 시작 - 다음 정각 실행 대기 중...")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(10)  # 10초마다 체크 (더 정확한 타이밍)
            except Exception as e:
                logger.error(f"스케줄러 실행 중 오류: {e}")
                self.errors.append(f"{datetime.now()}: {str(e)}")
                scheduler_status["errors"] = self.errors[-5:]  # 최근 5개만 유지
                time.sleep(60)
    
    def _data_collection_job(self, timeframe: str):
        """데이터 수집 작업 (정각 기준)"""
        current_time = datetime.now()
        logger.info(f"📊 === {current_time.strftime('%H:%M')} {timeframe} 데이터 수집 시작 ===")
        
        try:
            self.last_data_collection = current_time
            scheduler_status["last_data_collection"] = self.last_data_collection.isoformat()
            
            # 활성화된 심볼들 조회
            if not notion_config.is_available():
                logger.error("노션 설정 관리자를 사용할 수 없습니다")
                return
            
            active_symbols = notion_config.get_all_symbols()
            if not active_symbols:
                logger.warning("활성화된 심볼이 없습니다")
                return
            
            logger.info(f"데이터 수집 대상 심볼 {len(active_symbols)}개: {active_symbols}")
            
            # 각 심볼별로 해당 시간봉 데이터 수집
            success_count = 0
            total_symbols = len(active_symbols)
            
            for symbol in active_symbols:
                try:
                    # 최신 데이터 확보 (마지막 2시간 분량)
                    success = market_analyzer.ensure_recent_data(symbol, hours_back=2)
                    if success:
                        success_count += 1
                        logger.debug(f"✅ {symbol} {timeframe} 데이터 수집 성공")
                    else:
                        logger.warning(f"❌ {symbol} {timeframe} 데이터 수집 실패")
                    
                    time.sleep(0.5)  # 심볼 간 간격
                    
                except Exception as e:
                    logger.error(f"❌ {symbol} {timeframe} 데이터 수집 중 오류: {e}")
            
            self.data_collection_count += 1
            scheduler_status["data_collection_count"] = self.data_collection_count
            
            logger.info(f"✅ === {timeframe} 데이터 수집 완료: {success_count}/{total_symbols} 성공 ===")
            
        except Exception as e:
            logger.error(f"{timeframe} 데이터 수집 작업 실행 중 오류: {e}")
            self.errors.append(f"{datetime.now()}: {str(e)}")
            scheduler_status["errors"] = self.errors[-5:]
    
    def _signal_detection_job(self):
        """시그널 감지 작업 (정각 기준)"""
        current_time = datetime.now()
        logger.info(f"🔍 === {current_time.strftime('%H:%M')} 시그널 감지 시작 ===")
        
        try:
            self.last_signal_check = current_time
            scheduler_status["last_signal_check"] = self.last_signal_check.isoformat()
            
            # 활성화된 심볼들 조회
            if not notion_config.is_available():
                logger.error("노션 설정 관리자를 사용할 수 없습니다")
                return
            
            active_symbols = notion_config.get_all_symbols()
            if not active_symbols:
                logger.warning("활성화된 심볼이 없습니다")
                return
            
            logger.info(f"시그널 감지 대상 심볼 {len(active_symbols)}개: {active_symbols}")
            
            # 모든 심볼의 시그널 감지
            all_signals = self.signal_detector.detect_signals_for_all_symbols(active_symbols)
            
            self.signal_detection_count += 1
            scheduler_status["signal_detection_count"] = self.signal_detection_count
            
            if not all_signals:
                logger.info("감지된 시그널이 없습니다")
                return
            
            # 시그널 요약
            signal_summary = self.signal_detector.get_signal_summary(all_signals)
            logger.info(f"📊 시그널 감지 요약: {signal_summary['total_signals']}개 시그널, "
                       f"{signal_summary['symbols_with_signals']}개 심볼, "
                       f"고우선순위: {signal_summary['high_priority_signals']}개")
            
            # 시그널 기반 분석 실행
            analysis_results = self._execute_signal_based_analyses(all_signals)
            
            # 카운터 업데이트
            self.analysis_count += analysis_results['success_count']
            scheduler_status["analysis_count"] = self.analysis_count
            
            logger.info(f"✅ === 시그널 기반 분석 완료: {analysis_results['success_count']}개 성공, {analysis_results['failure_count']}개 실패 ===")
            
        except Exception as e:
            logger.error(f"시그널 감지 작업 실행 중 오류: {e}")
            self.errors.append(f"{datetime.now()}: {str(e)}")
            scheduler_status["errors"] = self.errors[-5:]
    
    def _verification_job(self):
        """분석 결과 검증 작업 (정각 기준)"""
        current_time = datetime.now()
        logger.info(f"🔍 === {current_time.strftime('%H:%M')} 검증 시작 ===")
        
        try:
            verification_results = self._verify_previous_analyses()
            
            self.verification_count += verification_results['verified_count']
            scheduler_status["verification_count"] = self.verification_count
            
            logger.info(f"✅ === 검증 완료: {verification_results['verified_count']}개 "
                       f"(성공: {verification_results['success_count']}, 실패: {verification_results['failure_count']}) ===")
            
        except Exception as e:
            logger.error(f"검증 작업 실행 중 오류: {e}")
            self.errors.append(f"{datetime.now()}: {str(e)}")
            scheduler_status["errors"] = self.errors[-5:]
    
    def _log_next_execution_times(self):
        """다음 실행 시간들 로깅"""
        try:
            current_time = datetime.now()
            
            # 다음 데이터 수집 시간
            next_data_5m = self._get_next_execution_time(self.data_collection_schedule['5m'])
            next_data_15m = self._get_next_execution_time(self.data_collection_schedule['15m'])
            next_data_1h = self._get_next_execution_time(self.data_collection_schedule['1h'])
            
            # 다음 시그널 체크 시간
            next_signal = self._get_next_execution_time(self.signal_check_schedule['5m'])
            
            # 다음 검증 시간
            next_verification = self._get_next_execution_time(self.verification_schedule)
            
            logger.info(f"⏰ 다음 실행 시간:")
            logger.info(f"  📊 5분 데이터: {next_data_5m.strftime('%H:%M')}")
            logger.info(f"  📊 15분 데이터: {next_data_15m.strftime('%H:%M')}")
            logger.info(f"  📊 1시간 데이터: {next_data_1h.strftime('%H:%M')}")
            logger.info(f"  🚨 시그널 체크: {next_signal.strftime('%H:%M')}")
            logger.info(f"  🔍 검증: {next_verification.strftime('%H:%M')}")
            
        except Exception as e:
            logger.warning(f"다음 실행 시간 로깅 실패: {e}")
    
    def _get_next_execution_time(self, minute_list: List[int]) -> datetime:
        """다음 실행 시간 계산"""
        current_time = datetime.now()
        current_minute = current_time.minute
        
        # 현재 시간 이후의 다음 실행 분 찾기
        next_minute = None
        for minute in sorted(minute_list):
            if minute > current_minute:
                next_minute = minute
                break
        
        if next_minute is None:
            # 다음 시간의 첫 번째 실행 분
            next_minute = min(minute_list)
            next_time = current_time.replace(minute=next_minute, second=0, microsecond=0) + timedelta(hours=1)
        else:
            next_time = current_time.replace(minute=next_minute, second=0, microsecond=0)
        
        return next_time
    
    def wait_for_next_sync_point(self):
        """다음 동기화 지점까지 대기"""
        current_time = datetime.now()
        
        # 모든 실행 시간 중 가장 가까운 시간 찾기
        all_minutes = set()
        all_minutes.update(self.data_collection_schedule['5m'])
        all_minutes.update(self.signal_check_schedule['5m'])
        all_minutes.update(self.verification_schedule)
        
        next_exec_time = self._get_next_execution_time(list(all_minutes))
        wait_seconds = (next_exec_time - current_time).total_seconds()
        
        if wait_seconds > 0:
            logger.info(f"⏳ 다음 동기화 지점까지 대기: {next_exec_time.strftime('%H:%M')} ({wait_seconds:.0f}초)")
            time.sleep(min(wait_seconds, 300))  # 최대 5분만 대기
    
    # 기존 메서드들은 그대로 유지 (코드가 너무 길어져서 생략)
    def _execute_signal_based_analyses(self, all_signals: Dict[str, List[Dict]]) -> Dict:
        """시그널 기반 분석 실행 - 개별 분석 후 총괄 에이전트 호출"""
        analysis_results = {
            "success_count": 0,
            "failure_count": 0,
            "total_symbols": 0,
            "master_decisions": 0,
            "trading_executions": 0,
            "details": []
        }
        
        try:
            # 심볼별로 그룹화 (이미 심볼별로 되어 있음)
            analysis_results["total_symbols"] = len(all_signals)
            
            # 각 심볼에 대해 한 번씩만 분석 실행
            for symbol, signals in all_signals.items():
                try:
                    logger.info(f"🚨 {symbol} 시그널 기반 분석 시작 - {len(signals)}개 시그널 감지")
                    
                    # 해당 심볼을 분석하는 에이전트들 조회
                    agents_for_symbol = notion_config.get_agents_by_symbol(symbol)
                    
                    if not agents_for_symbol:
                        logger.warning(f"{symbol}을 분석하는 에이전트가 없습니다")
                        analysis_results["failure_count"] += 1
                        
                        analysis_results["details"].append({
                            "symbol": symbol,
                            "signals": [s['type'] for s in signals],
                            "signal_count": len(signals),
                            "success": False,
                            "error": "해당 심볼을 분석하는 에이전트 없음"
                        })
                        continue
                    
                    # 첫 번째 에이전트로 분석 (향후 에이전트 선택 로직 개선 가능)
                    agent_info = agents_for_symbol[0]
                    agent_name = agent_info['name']
                    
                    # 시그널 정보 요약 (분석에 포함할 컨텍스트)
                    signal_context = self._create_signal_context(signals)
                    
                    logger.info(f"🤖 {agent_name} 에이전트로 {symbol} 분석 시작...")
                    logger.info(f"📊 감지된 시그널: {[s['type'] for s in signals]}")
                    
                    # AI 분석 수행
                    analysis_result = ai_system.analyze_with_agent(agent_name, analysis_periods=50)
                    
                    if analysis_result and not analysis_result.get("error"):
                        # 현재가 조회
                        current_price_data = db.get_current_price(symbol)
                        current_price = current_price_data['price'] if current_price_data else 0
                        
                        # 분석 결과에 모든 시그널 정보 추가
                        analysis_result['triggered_signals'] = {
                            'count': len(signals),
                            'signals': signals,
                            'summary': signal_context,
                            'strongest_signal': max(signals, key=lambda x: self._get_signal_priority_score(x))
                        }
                        
                        # 노션에 개별 분석 결과 저장
                        individual_page_id = notion_logger.create_analysis_page(analysis_result, current_price)
                        
                        if individual_page_id:
                            analysis_results["success_count"] += 1
                            logger.info(f"✅ {agent_name} ({symbol}): {analysis_result['recommendation']} "
                                    f"(신뢰도: {analysis_result['confidence']:.1%}) - "
                                    f"시그널 {len(signals)}개 기반")
                            
                            # 🔥 여기가 핵심: 총괄 에이전트 호출
                            logger.info(f"🎯 총괄 에이전트 호출: {symbol}")
                            
                            if master_agent.is_available():
                                master_decision = master_agent.make_trading_decision(
                                    analysis_result, 
                                    analysis_result['triggered_signals']
                                )
                                
                                if master_decision:
                                    analysis_results["master_decisions"] += 1
                                    
                                    # 총괄 결정 노션 페이지 생성
                                    trading_page_id = notion_logger.create_trading_decision_page(
                                        master_decision, 
                                        analysis_result
                                    )
                                    
                                    # 실제 매매가 실행된 경우 카운트
                                    execution_result = master_decision.get('execution_result', {})
                                    if execution_result.get('success') and execution_result.get('action') in ['ENTER', 'EXIT']:
                                        analysis_results["trading_executions"] += 1
                                    
                                    logger.info(f"🏆 총괄 결정 완료: {symbol} -> {master_decision.get('trading_decision', 'UNKNOWN')}")
                                    
                                    analysis_results["details"].append({
                                        "agent_name": agent_name,
                                        "symbol": symbol,
                                        "signals": [s['type'] for s in signals],
                                        "signal_count": len(signals),
                                        "strongest_signal": signals[0]['type'] if signals else None,
                                        "success": True,
                                        "recommendation": analysis_result.get('recommendation'),
                                        "confidence": analysis_result.get('confidence'),
                                        "individual_page_id": individual_page_id,
                                        "master_decision": master_decision.get('trading_decision'),
                                        "master_confidence": master_decision.get('confidence'),
                                        "trading_page_id": trading_page_id,
                                        "execution_success": execution_result.get('success', False),
                                        "execution_action": execution_result.get('action', 'NONE')
                                    })
                                else:
                                    logger.error(f"❌ 총괄 에이전트 결정 실패: {symbol}")
                                    analysis_results["details"].append({
                                        "agent_name": agent_name,
                                        "symbol": symbol,
                                        "signals": [s['type'] for s in signals],
                                        "signal_count": len(signals),
                                        "success": True,
                                        "individual_page_id": individual_page_id,
                                        "master_decision_error": "총괄 에이전트 결정 실패"
                                    })
                            else:
                                logger.warning(f"⚠️ 총괄 에이전트 사용 불가: {symbol}")
                                analysis_results["details"].append({
                                    "agent_name": agent_name,
                                    "symbol": symbol,
                                    "signals": [s['type'] for s in signals],
                                    "signal_count": len(signals),
                                    "success": True,
                                    "individual_page_id": individual_page_id,
                                    "master_decision_error": "총괄 에이전트 사용 불가"
                                })
                        else:
                            analysis_results["failure_count"] += 1
                            logger.error(f"❌ {agent_name} ({symbol}): 분석 완료했으나 노션 저장 실패")
                            
                            analysis_results["details"].append({
                                "agent_name": agent_name,
                                "symbol": symbol,
                                "signals": [s['type'] for s in signals],
                                "signal_count": len(signals),
                                "success": False,
                                "error": "노션 개별 분석 페이지 저장 실패"
                            })
                    else:
                        analysis_results["failure_count"] += 1
                        error_msg = analysis_result.get("error", "알 수 없는 오류") if analysis_result else "분석 결과 없음"
                        logger.error(f"❌ {agent_name} ({symbol}): 분석 실패 - {error_msg}")
                        
                        analysis_results["details"].append({
                            "agent_name": agent_name,
                            "symbol": symbol,
                            "signals": [s['type'] for s in signals],
                            "signal_count": len(signals),
                            "success": False,
                            "error": error_msg
                        })
                    
                    # 심볼 간 간격 (API 제한 고려)
                    time.sleep(3)
                    
                except Exception as e:
                    analysis_results["failure_count"] += 1
                    logger.error(f"❌ {symbol} 시그널 기반 분석 중 오류: {e}")
                    
                    analysis_results["details"].append({
                        "symbol": symbol,
                        "signals": [s['type'] for s in signals] if signals else [],
                        "signal_count": len(signals) if signals else 0,
                        "success": False,
                        "error": str(e)
                    })
            
            # 최종 요약 로그
            logger.info(f"🎯 === 시그널 기반 분석 최종 완료 ===")
            logger.info(f"📊 개별 분석: {analysis_results['success_count']}개 성공, {analysis_results['failure_count']}개 실패")
            logger.info(f"🤖 총괄 결정: {analysis_results['master_decisions']}개 완료")
            logger.info(f"⚙️ 매매 실행: {analysis_results['trading_executions']}개 완료")
            
        except Exception as e:
            logger.error(f"시그널 기반 분석 과정에서 오류: {e}")
        
        return analysis_results
    
    def _create_signal_context(self, signals: List[Dict]) -> str:
        """시그널들을 분석용 컨텍스트로 변환"""
        if not signals:
            return "시그널 없음"
        
        # 시그널을 강도별로 분류
        very_high = [s for s in signals if s.get('strength') == 'VERY_HIGH']
        high = [s for s in signals if s.get('strength') == 'HIGH']
        medium = [s for s in signals if s.get('strength') == 'MEDIUM']
        
        context_parts = []
        
        if very_high:
            context_parts.append(f"매우 강한 시그널: {[s['type'] for s in very_high]}")
        if high:
            context_parts.append(f"강한 시그널: {[s['type'] for s in high]}")
        if medium:
            context_parts.append(f"중간 시그널: {[s['type'] for s in medium]}")
        
        # 방향성 분석
        buy_signals = [s for s in signals if s.get('direction') == 'BUY']
        sell_signals = [s for s in signals if s.get('direction') == 'SELL']
        
        if len(buy_signals) > len(sell_signals):
            direction_bias = f"강세 편향 ({len(buy_signals)}개 vs {len(sell_signals)}개)"
        elif len(sell_signals) > len(buy_signals):
            direction_bias = f"약세 편향 ({len(sell_signals)}개 vs {len(buy_signals)}개)"
        else:
            direction_bias = "중립적"
        
        context_parts.append(f"방향성: {direction_bias}")
        
        return " | ".join(context_parts)

    def _get_signal_priority_score(self, signal: Dict) -> int:
        """시그널 우선순위 점수 계산"""
        strength_scores = {
            'VERY_HIGH': 4,
            'HIGH': 3,
            'MEDIUM': 2,
            'LOW': 1
        }
        
        strength_score = strength_scores.get(signal.get('strength', 'LOW'), 1)
        priority_score = signal.get('priority', 1)
        
        return strength_score * priority_score
    
    def _verify_previous_analyses(self) -> Dict:
        """이전 분석 결과 검증"""
        logger.info(f"🔍 15분 전 분석 결과 검증 시작...")
        
        verification_results = {
            "verified_count": 0,
            "success_count": 0,
            "failure_count": 0,
            "details": []
        }
        
        try:
            # 노션에서 검증 대기 중인 분석들 조회
            pending_analyses = notion_logger.get_pending_verifications(minutes_ago=15)
            
            if not pending_analyses:
                logger.info("검증할 분석 결과가 없습니다")
                return verification_results
            
            logger.info(f"검증 대기 중인 분석 {len(pending_analyses)}개 발견")
            
            # 각 분석 검증
            for analysis in pending_analyses:
                try:
                    # 해당 분석의 심볼 확인
                    analysis_symbol = analysis.get('symbol', 'SOL/USDT')
                    
                    # 해당 심볼의 현재가 조회
                    current_price_data = db.get_current_price(analysis_symbol)
                    if not current_price_data:
                        logger.error(f"{analysis_symbol} 현재가 조회 실패 - 검증 불가")
                        continue
                    
                    current_price = current_price_data['price']
                    logger.info(f"{analysis_symbol} 현재 가격: ${current_price:.4f}")
                    
                    result = self._verify_single_analysis(analysis, current_price)
                    verification_results["details"].append(result)
                    verification_results["verified_count"] += 1
                    
                    if result["verification_result"] == "성공":
                        verification_results["success_count"] += 1
                    else:
                        verification_results["failure_count"] += 1
                        
                except Exception as e:
                    logger.error(f"개별 분석 검증 실패: {e}")
            
        except Exception as e:
            logger.error(f"분석 검증 과정에서 오류: {e}")
        
        return verification_results
    
    def _verify_single_analysis(self, analysis: Dict, current_price: float) -> Dict:
        """개별 분석 결과 검증"""
        page_id = analysis['page_id']
        recommendation = analysis['recommendation']
        original_price = analysis['original_price']
        target_price = analysis['target_price']
        stop_loss = analysis['stop_loss']
        
        # 검증 로직
        verification_result = self._determine_verification_result(
            recommendation, original_price, current_price, target_price, stop_loss
        )
        
        # 노션 업데이트
        success = notion_logger.update_verification_result(
            page_id, verification_result, current_price, original_price
        )
        
        result_detail = {
            "page_id": page_id,
            "recommendation": recommendation,
            "original_price": original_price,
            "current_price": current_price,
            "target_price": target_price,
            "stop_loss": stop_loss,
            "verification_result": verification_result,
            "update_success": success
        }
        
        if success:
            price_change_pct = ((current_price - original_price) / original_price) * 100
            logger.info(f"✅ 검증 완료: {recommendation} → {verification_result} "
                       f"(${original_price:.4f} → ${current_price:.4f}, {price_change_pct:+.2f}%)")
        else:
            logger.error(f"❌ 검증 업데이트 실패: {page_id}")
        
        return result_detail
    
    def _determine_verification_result(self, recommendation: str, original_price: float, 
                                     current_price: float, target_price: float, stop_loss: float) -> str:
        """분석 결과 검증 로직"""
        try:
            if recommendation == "BUY":
                if current_price >= target_price:
                    return "성공"
                elif current_price <= stop_loss:
                    return "실패"
                else:
                    return "성공" if current_price > original_price else "실패"
                    
            elif recommendation == "SELL":
                if current_price <= target_price:
                    return "성공"
                elif current_price >= stop_loss:
                    return "실패"
                else:
                    return "성공" if current_price < original_price else "실패"
                    
            elif recommendation == "HOLD":
                price_change_pct = abs((current_price - original_price) / original_price) * 100
                return "성공" if price_change_pct <= 2.0 else "실패"
                
            else:
                return "실패"
                
        except Exception as e:
            logger.error(f"검증 결과 판단 중 오류: {e}")
            return "실패"
    
    def run_immediate_signal_detection(self) -> Optional[str]:
        """즉시 시그널 감지 실행 (수동 트리거용)"""
        try:
            logger.info("🚀 즉시 시그널 감지 실행")
            
            if not self.signal_detector:
                from market_analyzer import SignalDetector
                self.signal_detector = SignalDetector()
            
            active_symbols = notion_config.get_all_symbols()
            all_signals = self.signal_detector.detect_signals_for_all_symbols(active_symbols)
            
            if all_signals:
                analysis_results = self._execute_signal_based_analyses(all_signals)
                return f"시그널 감지 및 분석 완료: {analysis_results['success_count']}개 성공, {analysis_results['failure_count']}개 실패"
            else:
                return "감지된 시그널이 없습니다"
                
        except Exception as e:
            logger.error(f"즉시 시그널 감지 실패: {e}")
            return None
    
    def run_immediate_verification(self) -> Optional[str]:
        """즉시 검증 실행 (수동 트리거용)"""
        try:
            logger.info("🔍 즉시 검증 실행")
            verification_results = self._verify_previous_analyses()
            return f"검증 완료: {verification_results['verified_count']}개 (성공: {verification_results['success_count']}, 실패: {verification_results['failure_count']})"
        except Exception as e:
            logger.error(f"즉시 검증 실패: {e}")
            return None
    
    def get_scheduler_status(self) -> Dict:
        """스케줄러 상태 조회"""
        next_data_times = {}
        next_signal_time = None
        next_verification_time = None
        
        if self.running:
            try:
                # 다음 실행 시간들 계산
                next_data_times = {
                    '5m': self._get_next_execution_time(self.data_collection_schedule['5m']).isoformat(),
                    '15m': self._get_next_execution_time(self.data_collection_schedule['15m']).isoformat(),
                    '1h': self._get_next_execution_time(self.data_collection_schedule['1h']).isoformat()
                }
                next_signal_time = self._get_next_execution_time(self.signal_check_schedule['5m']).isoformat()
                next_verification_time = self._get_next_execution_time(self.verification_schedule).isoformat()
            except:
                pass
        
        return {
            "running": self.running,
            "mode": "synchronized_signal_based",
            "data_collection_schedule": self.data_collection_schedule,
            "signal_check_schedule": self.signal_check_schedule,
            "verification_schedule": self.verification_schedule,
            "ai_available": ai_system.is_available(),
            "notion_available": notion_logger.is_available(),
            "agent_management_available": notion_config.is_available(),
            "active_agents": notion_config.get_agent_names() if notion_config.is_available() else [],
            "total_analyses": self.analysis_count,
            "total_verifications": self.verification_count,
            "total_signal_detections": self.signal_detection_count,
            "total_data_collections": self.data_collection_count,
            "last_signal_check": self.last_signal_check.isoformat() if self.last_signal_check else None,
            "last_data_collection": self.last_data_collection.isoformat() if self.last_data_collection else None,
            "next_data_collection_times": next_data_times,
            "next_signal_check": next_signal_time,
            "next_verification": next_verification_time,
            "recent_errors": self.errors[-5:] if self.errors else [],
            "current_time": datetime.now().isoformat()
        }


# 전역 스케줄러 인스턴스
signal_based_scheduler = SignalBasedScheduler()

scheduler_status = {
    "running": False,
    "started_at": None,
    "mode": "synchronized_signal_based",
    "data_collection_schedule": {},
    "signal_check_schedule": {},
    "verification_schedule": [],
    "analysis_count": 0,
    "verification_count": 0,
    "signal_detection_count": 0,
    "data_collection_count": 0,
    "last_run": None,
    "last_signal_check": None,
    "last_data_collection": None,
    "errors": []
}

def start_data_collection():
    """백그라운드 데이터 수집 시작"""
    global collection_status
    
    if collection_status["running"]:
        logger.warning("데이터 수집이 이미 실행 중입니다")
        return
    
    try:
        market_analyzer.start_data_collection()
        collection_status["running"] = True
        collection_status["started_at"] = datetime.now().isoformat()
        collection_status["errors"] = []
        logger.info("백그라운드 데이터 수집 시작")
    except Exception as e:
        logger.error(f"데이터 수집 시작 실패: {e}")
        collection_status["errors"].append(str(e))


def stop_data_collection():
    """백그라운드 데이터 수집 중지"""
    global collection_status
    
    try:
        market_analyzer.stop_data_collection()
        collection_status["running"] = False
        logger.info("백그라운드 데이터 수집 중지")
    except Exception as e:
        logger.error(f"데이터 수집 중지 실패: {e}")


@app.on_event("startup")
async def startup_event():
    """앱 시작 시 초기화 - 시간 동기화 포함"""
    logger.info("Trading Bot API v2.1 시작 (시간 동기화 + 시그널 기반 + 총괄 에이전트)")
    
    try:
        # 1. 데이터베이스 초기화 (가상 거래 테이블 포함)
        db.init_database()
        logger.info("✅ 데이터베이스 초기화 완료")
        
        # 2. 가상 포트폴리오 상태 확인
        portfolio_status = virtual_portfolio.get_portfolio_status()
        logger.info(f"💼 가상 포트폴리오 상태: 잔고 ${portfolio_status['current_balance']:.2f}, "
                   f"총자산 ${portfolio_status['total_value']:.2f}, "
                   f"수익률 {portfolio_status['total_return']:+.2f}%")
        
        # 3. 총괄 에이전트 상태 확인
        if master_agent.is_available():
            logger.info("🤖 총괄 에이전트 사용 가능")
        else:
            logger.warning("⚠️ 총괄 에이전트를 사용할 수 없습니다")
        
        # 4. 시장 데이터 수집기 테스트
        try:
            fear_greed = market_data_collector.get_fear_greed_index()
            logger.info(f"📊 공포탐욕지수: {fear_greed['value']} ({fear_greed['value_classification']})")
        except Exception as e:
            logger.warning(f"시장 데이터 수집기 테스트 실패: {e}")
        
        # 5. 노션 에이전트 설정 로드
        if notion_config.is_available():
            success = notion_config.load_all_agents()
            if success:
                agents = notion_config.get_agent_names()
                symbols = notion_config.get_all_symbols()
                logger.info(f"🎯 트레이딩 에이전트 로드 완료: {agents}")
                logger.info(f"📈 분석 대상 심볼: {symbols}")
                
                # 데이터 수집기에 에이전트 심볼들 업데이트
                market_analyzer.update_active_symbols(symbols)
            else:
                logger.warning("❌ 트레이딩 에이전트 로드 실패")
        else:
            logger.warning("⚠️ 노션 설정 관리자를 사용할 수 없습니다")
        
        # 6. 실시간 데이터 수집 시작 (개선된 버전)
        start_data_collection()
        logger.info("📡 개선된 실시간 데이터 수집 시작")
        
        # 7. 초기 긴급 데이터 수집 (동기 실행)
        def emergency_data_collection():
            logger.info("🚨 긴급 초기 데이터 수집 시작...")
            try:
                if notion_config.is_available():
                    symbols = notion_config.get_all_symbols()
                    for symbol in symbols:
                        success = market_analyzer.ensure_recent_data(symbol, hours_back=1)
                        if success:
                            logger.info(f"✅ {symbol} 긴급 데이터 수집 완료")
                        else:
                            logger.warning(f"❌ {symbol} 긴급 데이터 수집 실패")
                        time.sleep(1)  # 심볼 간 간격
                logger.info("✅ 긴급 초기 데이터 수집 완료")
            except Exception as e:
                logger.error(f"긴급 데이터 수집 실패: {e}")
        
        # 긴급 데이터 수집을 별도 스레드에서 실행
        emergency_thread = threading.Thread(target=emergency_data_collection, daemon=True)
        emergency_thread.start()
        
        # 8. 백그라운드 과거 데이터 수집 (더 많은 데이터)
        def background_historical_collection():
            logger.info("🔄 백그라운드에서 과거 데이터 수집 시작...")
            try:
                # 긴급 데이터 수집 완료 대기
                emergency_thread.join(timeout=60)
                
                if notion_config.is_available():
                    symbols = notion_config.get_all_symbols()
                    initialize_historical_data(symbols, days=3)  # 3일로 축소
                else:
                    initialize_historical_data(days=3)
                logger.info("✅ 백그라운드 과거 데이터 수집 완료")
            except Exception as e:
                logger.error(f"백그라운드 데이터 수집 실패: {e}")
        
        historical_thread = threading.Thread(target=background_historical_collection, daemon=True)
        historical_thread.start()
        
        # # 9. 시간 동기화 대기 로직 아직은 주석처리. docs를 이용한 테스트를 위함.
        # current_time = datetime.now()
        # logger.info(f"🕒 현재 시간: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # # 다음 5분 단위 정각까지 대기
        # next_sync_minute = ((current_time.minute // 5) + 1) * 5
        # if next_sync_minute >= 60:
        #     next_sync_time = current_time.replace(hour=current_time.hour + 1, minute=0, second=0, microsecond=0)
        # else:
        #     next_sync_time = current_time.replace(minute=next_sync_minute, second=0, microsecond=0)
        
        # wait_seconds = (next_sync_time - current_time).total_seconds()
        
        # if wait_seconds > 0 and wait_seconds <= 300:  # 최대 5분만 대기
        #     logger.info(f"⏳ 다음 동기화 지점까지 대기: {next_sync_time.strftime('%H:%M')} ({wait_seconds:.0f}초)")
            
        #     # 대기 중에도 시스템 상태 표시
        #     def show_startup_progress():
        #         for i in range(int(wait_seconds), 0, -10):
        #             if i <= 60:
        #                 logger.info(f"🕒 동기화 대기 중... {i}초 남음")
        #             time.sleep(min(10, i))
            
        #     progress_thread = threading.Thread(target=show_startup_progress, daemon=True)
        #     progress_thread.start()
            
        #     # 실제 대기
        #     await asyncio.sleep(wait_seconds)
        #     logger.info(f"✅ 동기화 지점 도달: {datetime.now().strftime('%H:%M:%S')}")
        # else:
        #     logger.info("동기화 대기 시간이 너무 길거나 이미 동기화됨 - 즉시 시작")
        

        # 9. 포지션 모니터링 시스템 시작
        monitor_success = position_monitor.start_monitoring()
        if monitor_success:
            logger.info("🔍 실시간 포지션 모니터링 시작")
        else:
            logger.warning("⚠️ 포지션 모니터링 시작 실패")

        # 10. 시스템 상태 최종 요약
        logger.info("🚀 === 시스템 초기화 완료 ===")
        logger.info(f"📊 개별 분석 에이전트: {'✅' if ai_system.is_available() else '❌'}")
        logger.info(f"🤖 총괄 에이전트: {'✅' if master_agent.is_available() else '❌'}")
        logger.info(f"💼 가상 포트폴리오: ✅")
        logger.info(f"📈 시장 데이터 수집: ✅")
        logger.info(f"📝 노션 연동: {'✅' if notion_logger.is_available() else '❌'}")
        logger.info(f"⚙️ 에이전트 관리: {'✅' if notion_config.is_available() else '❌'}")
        logger.info(f"🕒 시간 동기화: ✅")
        logger.info("웹서버 시작 완료 - 시간 동기화 기반 AI 트레이딩 시스템 활성화")
        
    except Exception as e:
        logger.error(f"❌ 시작 시 오류: {e}")
        # 중요한 오류가 발생해도 서버는 계속 실행되도록 함

@app.on_event("shutdown")
async def shutdown_event():
    """앱 종료 시 정리"""
    logger.info("Trading Bot API 종료")
    stop_data_collection()
    signal_based_scheduler.stop_scheduler()
    position_monitor.stop_monitoring()


# API 엔드포인트들

@app.get("/")
async def root():
    """API 루트"""
    return {
        "message": "Trading Bot API v2.0 (Signal-Based)",
        "version": "2.0.0",
        "mode": "signal_based",
        "timeframes": TIMEFRAMES,
        "features": {
            "multi_timeframe_analysis": True,
            "ai_agents": ai_system.is_available(),
            "notion_logging": notion_logger.is_available(),
            "agent_management": notion_config.is_available(),
            "trading_engine": trading_engine.is_active(),
            "signal_based_scheduler": True,
            "available_agents": notion_config.get_agent_names() if notion_config.is_available() else []
        },
        "timestamp": datetime.now().isoformat()
    }


@app.get("/status")
async def get_status():
    """시스템 상태 조회"""
    try:
        current_price = db.get_current_price(DEFAULT_SYMBOL)
        
        # 각 시간봉별 데이터 상태 확인
        timeframe_status = {}
        for tf in TIMEFRAMES:
            candles = db.get_candles(DEFAULT_SYMBOL, tf, limit=1)
            timeframe_status[tf] = {
                "data_available": not candles.empty,
                "last_update": candles['timestamp'].iloc[-1].isoformat() if not candles.empty else None
            }
        
        current_time = datetime.now()
        
        return {
            "status": "running" if collection_status["running"] else "stopped",
            "mode": "synchronized_signal_based_with_master_agent",
            "system_time": current_time.isoformat(),
            "collection_status": collection_status,
            "scheduler_status": scheduler_status,
            "position_monitor_status": position_monitor.get_monitor_status(),  # 이 줄 추가
            "sync_info": {
                "enabled": True,
                "current_minute": current_time.minute,
                "scheduler_running": signal_based_scheduler.running
            },
            "current_price": current_price,
            "timeframe_status": timeframe_status,
            "ai_available": ai_system.is_available(),
            "master_agent_available": master_agent.is_available(),
            "notion_available": notion_logger.is_available(),
            "agent_system_available": notion_config.is_available(),
            "trading_engine_status": trading_engine.get_status(),
            "portfolio_status": virtual_portfolio.get_portfolio_status(),
            "portfolio_statistics": db.get_portfolio_statistics(),
            "available_agents": notion_config.get_agent_names() if notion_config.is_available() else [],
            "timestamp": current_time.isoformat()
        }

    except Exception as e:
        logger.error(f"상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/agents")
async def get_agents():
    """사용 가능한 트레이딩 에이전트 목록 조회"""
    try:
        if not notion_config.is_available():
            raise HTTPException(status_code=503, detail="에이전트 시스템을 사용할 수 없습니다")
        
        agents = notion_config.get_all_agents()
        agent_list = []
        
        for name, info in agents.items():
            agent_list.append({
                "name": name,
                "symbol": info['symbol'],
                "symbol_display": get_symbol_display_name(info['symbol']),
                "timeframes": info['timeframes'],
                "strategy_preview": info['strategy'][:100] + "..." if len(info['strategy']) > 100 else info['strategy'],
                "is_active": info['is_active']
            })
        
        return {
            "status": "running" if collection_status["running"] else "stopped",
            "mode": "signal_based_with_master_agent",
            "collection_status": collection_status,
            "scheduler_status": scheduler_status,
            "current_price": current_price,
            "timeframe_status": timeframe_status,
            "ai_available": ai_system.is_available(),
            "master_agent_available": master_agent.is_available(),
            "notion_available": notion_logger.is_available(),
            "agent_system_available": notion_config.is_available(),
            "trading_engine_status": trading_engine.get_status(),
            "portfolio_status": virtual_portfolio.get_portfolio_status(),
            "portfolio_statistics": db.get_portfolio_statistics(),
            "available_agents": notion_config.get_agent_names() if notion_config.is_available() else [],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"에이전트 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/symbols")
async def get_symbols():
    """사용 가능한 심볼 목록 조회"""
    try:
        # 에이전트가 사용하는 심볼들
        agent_symbols = notion_config.get_all_symbols() if notion_config.is_available() else []
        
        # 데이터베이스에 저장된 심볼들
        db_symbols = db.get_available_symbols()
        
        # 데이터 수집기의 활성 심볼들
        active_symbols = market_analyzer.get_active_symbols()
        
        symbol_info = []
        all_symbols = list(set(agent_symbols + db_symbols + active_symbols))
        
        for symbol in all_symbols:
            symbol_display = get_symbol_display_name(symbol)
            agents_using = [name for name, info in notion_config.get_all_agents().items() 
                          if info['symbol'] == symbol] if notion_config.is_available() else []
            
            symbol_info.append({
                "symbol": symbol,
                "symbol_display": symbol_display,
                "agents_using": agents_using,
                "agent_count": len(agents_using),
                "has_data": symbol in db_symbols,
                "collecting_data": symbol in active_symbols
            })
        
        return {
            "symbols": symbol_info,
            "total_count": len(symbol_info),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"심볼 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/price/{symbol}")
async def get_symbol_price(symbol: str):
    """특정 심볼의 현재 가격 조회"""
    try:
        normalized_symbol = normalize_symbol(symbol)
        
        # 데이터베이스에서 최신 가격
        price_data = db.get_current_price(normalized_symbol)
        
        if not price_data:
            # 실시간 조회 시도
            market_data = market_analyzer.get_market_data(normalized_symbol)
            if market_data:
                return {
                    "symbol": normalized_symbol,
                    "symbol_display": get_symbol_display_name(normalized_symbol),
                    "price": market_data['price'],
                    "change_24h": market_data.get('change_24h'),
                    "volume_24h": market_data.get('volume_24h'),
                    "timestamp": market_data['timestamp']
                }
            else:
                raise HTTPException(status_code=404, detail=f"{symbol} 가격 데이터를 찾을 수 없습니다")
        
        return {
            "symbol": normalized_symbol,
            "symbol_display": get_symbol_display_name(normalized_symbol),
            "price": price_data['price'],
            "change_24h": price_data['change_24h'],
            "volume_24h": price_data['volume_24h'],
            "timestamp": price_data['timestamp']
        }
        
    except Exception as e:
        logger.error(f"{symbol} 가격 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/agents/reload")
async def reload_agents():
    """에이전트 설정 새로고침"""
    try:
        if not notion_config.is_available():
            raise HTTPException(status_code=503, detail="에이전트 시스템을 사용할 수 없습니다")
        
        success = notion_config.reload_agents()
        
        if success:
            agents = notion_config.get_agent_names()
            # 새로 로드된 심볼들로 데이터 수집기 업데이트
            symbols = notion_config.get_all_symbols()
            market_analyzer.update_active_symbols(symbols)
            
            return {
                "success": True,
                "message": "에이전트 설정이 새로고침되었습니다",
                "loaded_agents": agents,
                "active_symbols": symbols,
                "count": len(agents),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="에이전트 새로고침 실패")
            
    except Exception as e:
        logger.error(f"에이전트 새로고침 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze/agent/{agent_name}")
async def analyze_with_agent(agent_name: str, analysis_periods: int = 50):
    """특정 에이전트로 시장 분석"""
    try:
        if not ai_system.is_available():
            raise HTTPException(status_code=503, detail="AI 분석 기능을 사용할 수 없습니다")
        
        if not notion_config.is_available():
            raise HTTPException(status_code=503, detail="에이전트 시스템을 사용할 수 없습니다")
        
        # 에이전트 존재 확인
        agent_info = notion_config.get_agent(agent_name)
        if not agent_info:
            available_agents = notion_config.get_agent_names()
            raise HTTPException(
                status_code=404, 
                detail=f"에이전트를 찾을 수 없습니다: {agent_name}. 사용 가능한 에이전트: {available_agents}"
            )
        
        # AI 분석 수행
        logger.info(f"🚀 API를 통한 {agent_name} 분석 요청 시작")
        result = ai_system.analyze_with_agent(agent_name, analysis_periods)
        
        if not result:
            raise HTTPException(status_code=500, detail="AI 분석에 실패했습니다")
        
        # 해당 에이전트의 심볼로 현재가 조회
        agent_symbol = agent_info['symbol']
        current_price_data = db.get_current_price(agent_symbol)
        current_price = current_price_data['price'] if current_price_data else 0
        
        # 노션에 저장 (사용 가능한 경우)
        notion_page_id = None
        if notion_logger.is_available():
            logger.info(f"📝 노션 페이지 생성 중...")
            notion_page_id = notion_logger.create_analysis_page(result, current_price)
            if notion_page_id:
                logger.info(f"✅ 노션 페이지 생성 완료: {notion_page_id}")
            else:
                logger.warning("❌ 노션 페이지 생성 실패")
        
        return {
            "success": True,
            "agent_name": agent_name,
            "symbol": agent_symbol,
            "symbol_display": get_symbol_display_name(agent_symbol),
            "timeframes_used": result.get('timeframes_used', []),
            "analysis": result,
            "current_price": current_price,
            "notion_page_id": notion_page_id,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"에이전트 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 중 오류 발생: {str(e)}")


@app.get("/candles/{symbol}/{timeframe}")
async def get_symbol_candles(symbol: str, timeframe: str, limit: int = 100):
    """특정 심볼의 캔들 데이터 조회"""
    try:
        if timeframe not in TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"지원하지 않는 시간봉: {timeframe}. 지원: {TIMEFRAMES}")
        
        if limit > 1000:
            limit = 1000
        
        normalized_symbol = normalize_symbol(symbol)
        symbol_display = get_symbol_display_name(normalized_symbol)
        
        logger.info(f"📊 캔들 데이터 조회: {normalized_symbol} {timeframe} (limit: {limit})")
        
        candles_df = db.get_candles(normalized_symbol, timeframe, limit)
        
        if candles_df.empty:
            logger.warning(f"❌ {normalized_symbol} {timeframe} 캔들 데이터 없음")
            return {
                "symbol": normalized_symbol,
                "symbol_display": symbol_display,
                "timeframe": timeframe,
                "data": [],
                "count": 0,
                "message": "데이터가 없습니다. 데이터 수집을 시도해보세요.",
                "timestamp": datetime.now().isoformat()
            }
        
        # 타임스탬프를 문자열로 변환
        candles_data = []
        for _, row in candles_df.iterrows():
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
                logger.warning(f"캔들 데이터 변환 실패: {e}")
                continue
        
        latest_time = candles_df['timestamp'].iloc[-1]
        latest_time_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(latest_time, 'strftime') else str(latest_time)
        
        logger.info(f"✅ {normalized_symbol} {timeframe} 캔들 {len(candles_data)}개 반환 (최신: {latest_time_str})")
        
        return {
            "symbol": normalized_symbol,
            "symbol_display": symbol_display,
            "timeframe": timeframe,
            "data": candles_data,
            "count": len(candles_data),
            "latest_data_time": latest_time_str,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{symbol} {timeframe} 캔들 데이터 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"캔들 데이터 조회 실패: {str(e)}")


@app.get("/indicators/{symbol}/{timeframe}")
async def get_symbol_technical_indicators(symbol: str, timeframe: str):
    """특정 심볼의 기술적 지표 조회"""
    try:
        if timeframe not in TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"지원하지 않는 시간봉: {timeframe}")
        
        normalized_symbol = normalize_symbol(symbol)
        symbol_display = get_symbol_display_name(normalized_symbol)
        
        signals = market_analyzer.get_technical_signals(normalized_symbol, timeframe, analysis_periods=50)
        
        if not signals:
            return {
                "symbol": normalized_symbol,
                "symbol_display": symbol_display,
                "timeframe": timeframe,
                "message": "기술적 지표 데이터가 없습니다",
                "timestamp": datetime.now().isoformat()
            }
        
        return {
            "symbol": normalized_symbol,
            "symbol_display": symbol_display,
            "timeframe": timeframe,
            "analysis_periods": signals.get('analysis_periods', 50),
            "current_indicators": signals.get('current_indicators', {}),
            "indicators_timeseries": signals.get('indicators_timeseries', {}),
            "recent_candles": signals.get('recent_candles', []),
            "recent_volumes": signals.get('recent_volumes', []),
            "signals": signals.get('signals', {}),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"{symbol} {timeframe} 기술적 지표 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/indicators/multi/{symbol}")
async def get_multi_timeframe_indicators_for_symbol(symbol: str, timeframes: str = "5m,15m,1h,4h", analysis_periods: int = 50):
    """특정 심볼의 멀티 타임프레임 기술적 지표 조회"""
    try:
        normalized_symbol = normalize_symbol(symbol)
        
        # 시간봉 파싱
        timeframe_list = [tf.strip() for tf in timeframes.split(",")]
        
        # 유효한 시간봉인지 확인
        invalid_timeframes = [tf for tf in timeframe_list if tf not in TIMEFRAMES]
        if invalid_timeframes:
            raise HTTPException(status_code=400, detail=f"지원하지 않는 시간봉: {invalid_timeframes}")
        
        # 멀티 타임프레임 데이터 수집
        multi_data = market_analyzer.get_multi_timeframe_data(normalized_symbol, timeframe_list, analysis_periods)
        
        if not multi_data:
            raise HTTPException(status_code=404, detail=f"{symbol} 멀티 타임프레임 데이터를 찾을 수 없습니다")
        
        return {
            "symbol": normalized_symbol,
            "symbol_display": get_symbol_display_name(normalized_symbol),
            "multi_timeframe_data": multi_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{symbol} 멀티 타임프레임 지표 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analysis/history")
async def get_analysis_history(limit: int = 10, symbol: str = None):
    """AI 분석 히스토리 조회"""
    try:
        if limit > 50:
            limit = 50
        
        if symbol:
            normalized_symbol = normalize_symbol(symbol)
            history = db.get_ai_analysis_history(normalized_symbol, limit)
            
            return {
                "symbol": normalized_symbol,
                "symbol_display": get_symbol_display_name(normalized_symbol),
                "history": history,
                "count": len(history),
                "timestamp": datetime.now().isoformat()
            }
        else:
            history = ai_system.get_analysis_history(limit)
            
            return {
                "symbol": "ALL",
                "history": history,
                "count": len(history),
                "timestamp": datetime.now().isoformat()
            }
        
    except Exception as e:
        logger.error(f"분석 히스토리 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data/collect")
async def manual_data_collection(background_tasks: BackgroundTasks, symbols: str = None):
    """수동 데이터 수집"""
    try:
        # 심볼 파싱
        if symbols:
            symbol_list = [s.strip() for s in symbols.split(",")]
        else:
            symbol_list = notion_config.get_all_symbols() if notion_config.is_available() else None
        
        # 백그라운드에서 과거 데이터 수집
        background_tasks.add_task(initialize_historical_data, symbol_list, 5)
        
        return {
            "message": "데이터 수집을 시작했습니다",
            "symbols": symbol_list or ["기본 심볼들"],
            "timeframes": TIMEFRAMES,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"수동 데이터 수집 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 시그널 기반 스케줄러 관련 엔드포인트들

@app.get("/scheduler/status")
async def get_scheduler_status():
    """스케줄러 상태 조회"""
    try:
        status = signal_based_scheduler.get_scheduler_status()
        return {
            "success": True,
            "scheduler_status": status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"스케줄러 상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scheduler/start")
async def start_scheduler():
    """시그널 기반 스케줄러 시작"""
    try:
        success = signal_based_scheduler.start_scheduler()
        if success:
            return {
                "success": True,
                "message": "시그널 기반 스케줄러가 성공적으로 시작되었습니다",
                "status": signal_based_scheduler.get_scheduler_status(),
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "success": False,
                "message": "스케줄러 시작에 실패했습니다",
                "status": signal_based_scheduler.get_scheduler_status(),
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        logger.error(f"스케줄러 시작 API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scheduler/stop")
async def stop_scheduler():
    """스케줄러 중지"""
    try:
        signal_based_scheduler.stop_scheduler()
        return {
            "success": True,
            "message": "스케줄러가 중지되었습니다",
            "status": signal_based_scheduler.get_scheduler_status(),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"스케줄러 중지 API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scheduler/analysis/run")
async def run_immediate_analysis():
    """즉시 시그널 감지 및 분석 실행"""
    try:
        if not signal_based_scheduler.running:
            raise HTTPException(status_code=400, detail="스케줄러가 실행되지 않고 있습니다")
        
        result = signal_based_scheduler.run_immediate_signal_detection()
        
        if result:
            return {
                "success": True,
                "message": "즉시 시그널 감지 및 분석이 완료되었습니다",
                "result": result,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "success": False,
                "message": "즉시 시그널 감지에 실패했습니다",
                "timestamp": datetime.now().isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"즉시 시그널 감지 API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scheduler/verification/run")
async def run_immediate_verification():
    """즉시 검증 실행"""
    try:
        if not signal_based_scheduler.running:
            raise HTTPException(status_code=400, detail="스케줄러가 실행되지 않고 있습니다")
        
        result = signal_based_scheduler.run_immediate_verification()
        
        if result:
            return {
                "success": True,
                "message": "즉시 검증이 완료되었습니다",
                "result": result,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "success": False,
                "message": "즉시 검증에 실패했습니다",
                "timestamp": datetime.now().isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"즉시 검증 API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 시그널 감지 관련 엔드포인트 추가
@app.get("/signals/{symbol}")
async def get_symbol_signals(symbol: str, timeframe: str = "5m"):
    """특정 심볼의 현재 시그널 조회"""
    try:
        if timeframe not in TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"지원하지 않는 시간봉: {timeframe}")
        
        normalized_symbol = normalize_symbol(symbol)
        
        # 시그널 감지기 초기화
        from market_analyzer import SignalDetector
        signal_detector = SignalDetector()
        
        # 시그널 감지
        signals = signal_detector.detect_signals_for_symbol(normalized_symbol, timeframe)
        
        return {
            "symbol": normalized_symbol,
            "symbol_display": get_symbol_display_name(normalized_symbol),
            "timeframe": timeframe,
            "signals": signals,
            "signal_count": len(signals),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"{symbol} 시그널 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/all")
async def get_all_signals(timeframe: str = "5m"):
    """모든 활성 심볼의 시그널 조회"""
    try:
        if timeframe not in TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"지원하지 않는 시간봉: {timeframe}")
        
        if not notion_config.is_available():
            raise HTTPException(status_code=503, detail="에이전트 시스템을 사용할 수 없습니다")
        
        active_symbols = notion_config.get_all_symbols()
        
        # 시그널 감지기 초기화
        from market_analyzer import SignalDetector
        signal_detector = SignalDetector()
        
        # 모든 심볼의 시그널 감지
        all_signals = signal_detector.detect_signals_for_all_symbols(active_symbols, timeframe)
        
        # 시그널 요약
        signal_summary = signal_detector.get_signal_summary(all_signals)
        
        return {
            "timeframe": timeframe,
            "active_symbols": active_symbols,
            "signals_by_symbol": all_signals,
            "summary": signal_summary,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"전체 시그널 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 에러 핸들러
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"error": "엔드포인트를 찾을 수 없습니다", "timestamp": datetime.now().isoformat()}
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"error": "서버 내부 오류", "timestamp": datetime.now().isoformat()}
    )

@app.get("/portfolio/status")
async def get_portfolio_status():
    """가상 포트폴리오 상태 조회"""
    try:
        status = virtual_portfolio.get_portfolio_status()
        return {
            "success": True,
            "portfolio": status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"포트폴리오 상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/portfolio/statistics")
async def get_portfolio_statistics():
    """포트폴리오 통계 조회"""
    try:
        stats = db.get_portfolio_statistics()
        return {
            "success": True,
            "statistics": stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"포트폴리오 통계 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/trades/history")
async def get_trades_history(limit: int = 20):
    """가상 거래 히스토리 조회"""
    try:
        if limit > 100:
            limit = 100
        
        trades = db.get_virtual_trades_history(limit)
        return {
            "success": True,
            "trades": trades,
            "count": len(trades),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"거래 히스토리 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/decisions/history")
async def get_master_decisions_history(limit: int = 20):
    """총괄 에이전트 결정 히스토리 조회"""
    try:
        if limit > 100:
            limit = 100
        
        decisions = db.get_master_decisions_history(limit)
        return {
            "success": True,
            "decisions": decisions,
            "count": len(decisions),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"총괄 결정 히스토리 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/market/sentiment/{symbol}")
async def get_market_sentiment(symbol: str):
    """시장 센티먼트 조회"""
    try:
        normalized_symbol = normalize_symbol(symbol)
        sentiment = market_data_collector.get_market_sentiment(normalized_symbol)
        
        return {
            "success": True,
            "symbol": normalized_symbol,
            "symbol_display": get_symbol_display_name(normalized_symbol),
            "sentiment": sentiment,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"{symbol} 시장 센티먼트 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/portfolio/reset")
async def reset_portfolio():
    """포트폴리오 초기화 (개발/테스트용)"""
    try:
        # 기존 포지션 강제 청산
        if virtual_portfolio.current_position:
            current_price_data = db.get_current_price(virtual_portfolio.current_position['symbol'])
            current_price = current_price_data['price'] if current_price_data else virtual_portfolio.current_position['entry_price']
            virtual_portfolio.exit_position(current_price, "Portfolio Reset")
        
        # 잔고 초기화
        virtual_portfolio.current_balance = virtual_portfolio.initial_balance
        virtual_portfolio.current_position = None
        
        logger.info("포트폴리오 초기화 완료")
        
        return {
            "success": True,
            "message": "포트폴리오가 초기화되었습니다",
            "portfolio": virtual_portfolio.get_portfolio_status(),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"포트폴리오 초기화 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/master/decision/{symbol}")
async def manual_master_decision(symbol: str):
    """수동 총괄 에이전트 결정 실행"""
    try:
        if not master_agent.is_available():
            raise HTTPException(status_code=503, detail="총괄 에이전트를 사용할 수 없습니다")
        
        normalized_symbol = normalize_symbol(symbol)
        
        # 최근 AI 분석 결과 조회
        recent_analysis = db.get_ai_analysis_history(normalized_symbol, 1)
        if not recent_analysis:
            raise HTTPException(status_code=404, detail=f"{symbol}의 최근 분석 결과를 찾을 수 없습니다")
        
        # 분석 결과를 적절한 형태로 변환
        analysis_data = recent_analysis[0]
        individual_analysis = {
            'symbol': normalized_symbol,
            'recommendation': analysis_data['recommendation'],
            'confidence': analysis_data['confidence'],
            'target_price': analysis_data.get('target_price'),
            'stop_loss': analysis_data.get('stop_loss'),
            'analysis': analysis_data['analysis'],
            'reasons': []  # 기본값
        }
        
        # 총괄 에이전트 결정 실행
        master_decision = master_agent.make_trading_decision(individual_analysis)
        
        if not master_decision:
            raise HTTPException(status_code=500, detail="총괄 에이전트 결정 실패")
        
        # 노션 페이지 생성
        trading_page_id = None
        if notion_logger.is_available():
            trading_page_id = notion_logger.create_trading_decision_page(
                master_decision, 
                individual_analysis
            )
        
        return {
            "success": True,
            "symbol": normalized_symbol,
            "symbol_display": get_symbol_display_name(normalized_symbol),
            "master_decision": master_decision,
            "trading_page_id": trading_page_id,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"수동 총괄 결정 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scheduler/sync-info")
async def get_scheduler_sync_info():
    """스케줄러 시간 동기화 정보 조회"""
    try:
        current_time = datetime.now()
        
        # 다음 실행 시간들 계산
        def get_next_minutes(minute_list):
            current_minute = current_time.minute
            next_minute = None
            for minute in sorted(minute_list):
                if minute > current_minute:
                    next_minute = minute
                    break
            if next_minute is None:
                next_minute = min(minute_list)
                return current_time.replace(hour=current_time.hour + 1, minute=next_minute, second=0, microsecond=0)
            else:
                return current_time.replace(minute=next_minute, second=0, microsecond=0)
        
        # 스케줄러 설정
        data_schedule = {
            '5m': [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56],
            '15m': [1, 16, 31, 46],
            '1h': [1]
        }
        signal_schedule = [3, 8, 13, 18, 23, 28, 33, 38, 43, 48, 53, 58]
        verification_schedule = [3, 18, 33, 48]
        
        # 다음 실행 시간들
        next_times = {
            'data_collection': {
                '5m': get_next_minutes(data_schedule['5m']).isoformat(),
                '15m': get_next_minutes(data_schedule['15m']).isoformat(),
                '1h': get_next_minutes(data_schedule['1h']).isoformat()
            },
            'signal_check': get_next_minutes(signal_schedule).isoformat(),
            'verification': get_next_minutes(verification_schedule).isoformat()
        }
        
        return {
            "success": True,
            "current_time": current_time.isoformat(),
            "schedule_config": {
                "data_collection": data_schedule,
                "signal_check": signal_schedule,
                "verification": verification_schedule
            },
            "next_execution_times": next_times,
            "scheduler_running": signal_based_scheduler.running,
            "sync_mode": "enabled",
            "timestamp": current_time.isoformat()
        }
    except Exception as e:
        logger.error(f"스케줄러 동기화 정보 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data/sync-collect")
async def sync_data_collection():
    """시간 동기화 기반 데이터 수집 수동 실행"""
    try:
        if not notion_config.is_available():
            raise HTTPException(status_code=503, detail="에이전트 시스템을 사용할 수 없습니다")
        
        active_symbols = notion_config.get_all_symbols()
        if not active_symbols:
            raise HTTPException(status_code=400, detail="활성화된 심볼이 없습니다")
        
        logger.info(f"🔄 수동 동기화 데이터 수집 시작: {active_symbols}")
        
        results = {}
        success_count = 0
        
        for symbol in active_symbols:
            try:
                # 최신 2시간 데이터 확보
                success = market_analyzer.ensure_recent_data(symbol, hours_back=2)
                results[symbol] = {
                    "success": success,
                    "message": "데이터 수집 완료" if success else "데이터 수집 실패"
                }
                if success:
                    success_count += 1
                
                time.sleep(0.5)  # 심볼 간 간격
                
            except Exception as e:
                results[symbol] = {
                    "success": False,
                    "error": str(e)
                }
        
        return {
            "success": True,
            "message": f"동기화 데이터 수집 완료: {success_count}/{len(active_symbols)} 성공",
            "symbols": active_symbols,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"동기화 데이터 수집 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/system/time-status")
async def get_system_time_status():
    """시스템 시간 상태 및 동기화 정보"""
    try:
        current_time = datetime.now()
        
        # 현재 분이 어떤 스케줄에 해당하는지 확인
        current_minute = current_time.minute
        
        # 데이터 수집 시간인지 확인
        is_data_5m = current_minute in [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56]
        is_data_15m = current_minute in [1, 16, 31, 46]
        is_data_1h = current_minute in [1]
        
        # 시그널 체크 시간인지 확인
        is_signal_check = current_minute in [3, 8, 13, 18, 23, 28, 33, 38, 43, 48, 53, 58]
        
        # 검증 시간인지 확인
        is_verification = current_minute in [3, 18, 33, 48]
        
        # 다음 정각 5분까지의 시간
        next_5min = ((current_minute // 5) + 1) * 5
        if next_5min >= 60:
            next_5min_time = current_time.replace(hour=current_time.hour + 1, minute=0, second=0, microsecond=0)
        else:
            next_5min_time = current_time.replace(minute=next_5min, second=0, microsecond=0)
        
        time_to_next_5min = (next_5min_time - current_time).total_seconds()
        
        return {
            "success": True,
            "system_time": current_time.isoformat(),
            "current_minute": current_minute,
            "current_second": current_time.second,
            "active_schedules": {
                "data_collection_5m": is_data_5m,
                "data_collection_15m": is_data_15m,
                "data_collection_1h": is_data_1h,
                "signal_check": is_signal_check,
                "verification": is_verification
            },
            "next_5min_mark": next_5min_time.isoformat(),
            "seconds_to_next_5min": int(time_to_next_5min),
            "scheduler_status": {
                "running": signal_based_scheduler.running,
                "mode": "synchronized" if signal_based_scheduler.running else "stopped"
            },
            "timestamp": current_time.isoformat()
        }
        
    except Exception as e:
        logger.error(f"시스템 시간 상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scheduler/force-sync")
async def force_scheduler_sync():
    """스케줄러 강제 동기화"""
    try:
        if not signal_based_scheduler.running:
            raise HTTPException(status_code=400, detail="스케줄러가 실행되지 않고 있습니다")
        
        logger.info("🕒 스케줄러 강제 동기화 요청")
        
        # 현재 시간 정보
        current_time = datetime.now()
        
        # 다음 동기화 지점까지의 시간 계산
        signal_based_scheduler.wait_for_next_sync_point()
        
        # 동기화 후 시간
        sync_time = datetime.now()
        
        return {
            "success": True,
            "message": "스케줄러 강제 동기화 완료",
            "before_sync": current_time.isoformat(),
            "after_sync": sync_time.isoformat(),
            "timestamp": sync_time.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"스케줄러 강제 동기화 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
# 포지션 모니터링 관련 API 엔드포인트들

@app.get("/position/monitor/status")
async def get_position_monitor_status():
    """포지션 모니터 상태 조회"""
    try:
        status = position_monitor.get_monitor_status()
        return {
            "success": True,
            "monitor_status": status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"포지션 모니터 상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/position/monitor/start")
async def start_position_monitor():
    """포지션 모니터링 시작"""
    try:
        success = position_monitor.start_monitoring()
        return {
            "success": success,
            "message": "포지션 모니터링이 시작되었습니다" if success else "포지션 모니터링 시작 실패",
            "status": position_monitor.get_monitor_status(),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"포지션 모니터링 시작 API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/position/monitor/stop")
async def stop_position_monitor():
    """포지션 모니터링 중지"""
    try:
        position_monitor.stop_monitoring()
        return {
            "success": True,
            "message": "포지션 모니터링이 중지되었습니다",
            "status": position_monitor.get_monitor_status(),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"포지션 모니터링 중지 API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/position/check")
async def force_position_check():
    """강제 포지션 체크 (테스트용)"""
    try:
        result = position_monitor.force_position_check()
        return {
            "success": True,
            "check_result": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"강제 포지션 체크 API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/position/summary")
async def get_position_summary():
    """현재 포지션 요약 조회"""
    try:
        summary = virtual_portfolio.get_position_summary()
        return {
            "success": True,
            "position_summary": summary,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"포지션 요약 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/position/exit")
async def manual_position_exit(reason: str = "Manual Exit"):
    """수동 포지션 청산"""
    try:
        if not virtual_portfolio.current_position:
            raise HTTPException(status_code=400, detail="청산할 포지션이 없습니다")
        
        symbol = virtual_portfolio.current_position['symbol']
        current_price_data = db.get_current_price(symbol)
        
        if not current_price_data:
            raise HTTPException(status_code=404, detail=f"{symbol} 현재가 조회 실패")
        
        current_price = current_price_data['price']
        exit_info = virtual_portfolio.exit_position(current_price, reason)
        
        if exit_info:
            return {
                "success": True,
                "message": "포지션이 수동으로 청산되었습니다",
                "exit_info": exit_info,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="포지션 청산 실패")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"수동 포지션 청산 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/position/flip/{symbol}")
async def manual_position_flip(symbol: str, direction: str, leverage: float = 2.0):
    """수동 포지션 플립"""
    try:
        if direction not in ['LONG', 'SHORT']:
            raise HTTPException(status_code=400, detail="direction은 LONG 또는 SHORT여야 합니다")
        
        normalized_symbol = normalize_symbol(symbol)
        current_price_data = db.get_current_price(normalized_symbol)
        
        if not current_price_data:
            raise HTTPException(status_code=404, detail=f"{symbol} 현재가 조회 실패")
        
        current_price = current_price_data['price']
        
        # 포지션 플립 실행
        success = virtual_portfolio.enter_position(
            normalized_symbol, direction, current_price, leverage, force_flip=True
        )
        
        if success:
            return {
                "success": True,
                "message": f"포지션 플립 완료: {direction} {leverage}x",
                "symbol": normalized_symbol,
                "symbol_display": get_symbol_display_name(normalized_symbol),
                "new_position": virtual_portfolio.get_position_summary(),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="포지션 플립 실패")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"수동 포지션 플립 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/position/performance")
async def get_position_performance():
    """포지션 성과 분석"""
    try:
        portfolio_status = virtual_portfolio.get_portfolio_status()
        trading_stats = db.get_portfolio_statistics()
        
        # 최근 거래 히스토리
        recent_trades = db.get_virtual_trades_history(10)
        
        # 수익률 분석
        if recent_trades:
            profitable_trades = [t for t in recent_trades if t.get('realized_pnl', 0) > 0]
            losing_trades = [t for t in recent_trades if t.get('realized_pnl', 0) < 0]
            
            avg_win = sum(t.get('realized_pnl', 0) for t in profitable_trades) / len(profitable_trades) if profitable_trades else 0
            avg_loss = sum(t.get('realized_pnl', 0) for t in losing_trades) / len(losing_trades) if losing_trades else 0
            
            profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        else:
            avg_win = avg_loss = profit_factor = 0
        
        performance_data = {
            "portfolio_status": portfolio_status,
            "trading_statistics": trading_stats,
            "recent_trades_count": len(recent_trades),
            "performance_metrics": {
                "average_win": avg_win,
                "average_loss": avg_loss,
                "profit_factor": profit_factor,
                "total_return_percentage": portfolio_status.get('total_return', 0),
                "current_drawdown": max(0, portfolio_status.get('initial_balance', 0) - portfolio_status.get('total_value', 0))
            },
            "recent_trades": recent_trades[:5]  # 최근 5개만
        }
        
        return {
            "success": True,
            "performance": performance_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"포지션 성과 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # 개발 서버 실행
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
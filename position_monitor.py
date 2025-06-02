import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from config import logger, get_symbol_display_name
from database import db
from virtual_portfolio import virtual_portfolio

class PositionMonitor:
    """실시간 포지션 모니터링 시스템"""
    
    def __init__(self, check_interval: int = 10):
        self.check_interval = check_interval  # 체크 간격 (초)
        self.running = False
        self.monitor_thread = None
        self.last_check_time = None
        self.check_count = 0
        self.signal_count = 0
        
        # 분석 재호출 관련 설정
        self.analysis_cooldown = 300  # 5분 쿨다운 (같은 조건으로 재분석 방지)
        self.last_analysis_time = {}
        
        logger.info(f"포지션 모니터 초기화 완료 (체크 간격: {check_interval}초)")
    
    def start_monitoring(self) -> bool:
        """포지션 모니터링 시작"""
        if self.running:
            logger.warning("포지션 모니터링이 이미 실행 중입니다")
            return False
        
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info("🔍 실시간 포지션 모니터링 시작")
        return True
    
    def stop_monitoring(self):
        """포지션 모니터링 중지"""
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            logger.info("포지션 모니터링 중지 신호 전송")
        else:
            logger.info("포지션 모니터링 중지")
    
    def _monitoring_loop(self):
        """모니터링 메인 루프"""
        logger.info("포지션 모니터링 루프 시작")
        
        while self.running:
            try:
                self.last_check_time = datetime.now()
                self.check_count += 1
                
                # 포지션이 있는 경우에만 체크
                if virtual_portfolio.current_position:
                    self._check_position()
                
                # 5분마다 상태 로깅
                if self.check_count % 30 == 0:  # 30 * 10초 = 5분
                    self._log_monitoring_status()
                
            except Exception as e:
                logger.error(f"포지션 모니터링 루프 오류: {e}")
            
            time.sleep(self.check_interval)
        
        logger.info("포지션 모니터링 루프 종료")
    
    def _check_position(self):
        """포지션 상태 체크"""
        try:
            position = virtual_portfolio.current_position
            if not position:
                return
            
            symbol = position['symbol']
            
            # 현재가 조회
            current_price_data = db.get_current_price(symbol)
            if not current_price_data:
                logger.debug(f"{symbol} 현재가 조회 실패")
                return
            
            current_price = current_price_data['price']
            
            # 트레일링 스탑 업데이트
            trailing_updated = virtual_portfolio.update_trailing_stop(current_price)
            if trailing_updated:
                logger.info(f"🔄 트레일링 스탑 업데이트: {symbol} "
                          f"현재가 ${current_price:.4f}, "
                          f"트레일링 스탑 ${position.get('trailing_stop_price', 0):.4f}")
            
            # 포지션 신호 체크
            signals = virtual_portfolio.check_position_signals(current_price)
            
            if signals:
                self.signal_count += 1
                logger.info(f"🚨 포지션 신호 감지: {symbol} - {signals}")
                
                for signal in signals:
                    self._handle_position_signal(signal, current_price)
            
        except Exception as e:
            logger.error(f"포지션 체크 중 오류: {e}")
    
    def _handle_position_signal(self, signal: str, current_price: float):
        """포지션 신호 처리"""
        try:
            position = virtual_portfolio.current_position
            if not position:
                return
            
            symbol = position['symbol']
            symbol_display = get_symbol_display_name(symbol)
            
            if signal == 'TARGET_REACHED':
                logger.info(f"🎯 목표가 도달: {symbol_display} ${current_price:.4f}")
                self._handle_target_reached(current_price)
                
            elif signal == 'TRAILING_STOP':
                logger.info(f"📉 트레일링 스탑 발동: {symbol_display} ${current_price:.4f}")
                self._handle_trailing_stop(current_price)
                
            elif signal == 'PARTIAL_TAKE_PROFIT':
                logger.info(f"💰 부분 익절 조건 도달: {symbol_display} ${current_price:.4f}")
                self._handle_partial_take_profit(current_price)
        
        except Exception as e:
            logger.error(f"포지션 신호 처리 중 오류: {e}")
    
    def _handle_target_reached(self, current_price: float):
        """목표가 도달 처리 - 분석 에이전트 재호출"""
        try:
            position = virtual_portfolio.current_position
            symbol = position['symbol']
            
            # 쿨다운 체크
            if not self._can_request_analysis(symbol, 'TARGET_REACHED'):
                logger.info(f"목표가 도달 분석 쿨다운 중: {symbol}")
                return
            
            logger.info(f"🔍 목표가 도달 - 추가 상승 가능성 분석 요청: {symbol}")
            
            # 분석 에이전트 재호출
            analysis_result = self._request_continue_analysis(current_price)
            
            if analysis_result:
                recommendation = analysis_result.get('recommendation', 'HOLD')
                confidence = analysis_result.get('confidence', 0.0)
                
                logger.info(f"📊 목표가 도달 후 분석 결과: {recommendation} (신뢰도: {confidence:.1%})")
                
                if recommendation == 'SELL' or confidence < 0.6:
                    # 신뢰도가 낮거나 매도 신호면 익절
                    exit_info = virtual_portfolio.exit_position(current_price, "Target Reached - Exit Signal")
                    logger.info(f"✅ 목표가 도달 후 전체 익절: {exit_info}")
                elif recommendation == 'HOLD':
                    # 홀드 신호면 부분 익절 + 트레일링 스탑
                    partial_exit = virtual_portfolio.execute_partial_take_profit(current_price)
                    logger.info(f"📈 목표가 도달 후 부분 익절: {partial_exit}")
                else:  # BUY
                    # 강한 매수 신호면 홀드하고 트레일링 스탑만 적용
                    logger.info(f"🚀 목표가 도달 후 지속 보유 (강한 매수 신호)")
            else:
                # 분석 실패시 기본 부분 익절
                partial_exit = virtual_portfolio.execute_partial_take_profit(current_price)
                logger.info(f"⚠️ 목표가 도달 후 분석 실패 - 기본 부분 익절: {partial_exit}")
            
            # 쿨다운 시간 기록
            self.last_analysis_time[f"{symbol}_TARGET_REACHED"] = datetime.now()
            
        except Exception as e:
            logger.error(f"목표가 도달 처리 중 오류: {e}")
    
    def _handle_trailing_stop(self, current_price: float):
        """트레일링 스탑 처리"""
        try:
            position = virtual_portfolio.current_position
            if not position:
                return
            
            logger.info(f"📉 트레일링 스탑 발동 - 포지션 전체 청산")
            exit_info = virtual_portfolio.exit_position(current_price, "Trailing Stop")
            logger.info(f"✅ 트레일링 스탑 청산 완료: {exit_info}")
            
        except Exception as e:
            logger.error(f"트레일링 스탑 처리 중 오류: {e}")
    
    def _handle_partial_take_profit(self, current_price: float):
        """부분 익절 처리"""
        try:
            position = virtual_portfolio.current_position
            if not position or position.get('partial_profit_taken', False):
                return
            
            logger.info(f"💰 부분 익절 실행")
            partial_exit = virtual_portfolio.execute_partial_take_profit(current_price)
            logger.info(f"✅ 부분 익절 완료: {partial_exit}")
            
        except Exception as e:
            logger.error(f"부분 익절 처리 중 오류: {e}")
    
    def _can_request_analysis(self, symbol: str, signal_type: str) -> bool:
        """분석 요청 가능 여부 체크 (쿨다운)"""
        key = f"{symbol}_{signal_type}"
        last_time = self.last_analysis_time.get(key)
        
        if not last_time:
            return True
        
        time_diff = (datetime.now() - last_time).total_seconds()
        return time_diff >= self.analysis_cooldown
    
    def _request_continue_analysis(self, current_price: float) -> Optional[Dict]:
        """지속 보유 여부 분석 요청"""
        try:
            position = virtual_portfolio.current_position
            if not position:
                return None
            
            symbol = position['symbol']
            
            # 노션에서 해당 심볼의 에이전트 찾기
            from notion_integration import notion_config
            if not notion_config.is_available():
                logger.warning("노션 설정 관리자 사용 불가 - 분석 요청 실패")
                return None
            
            agents_for_symbol = notion_config.get_agents_by_symbol(symbol)
            if not agents_for_symbol:
                logger.warning(f"{symbol}을 분석하는 에이전트가 없습니다")
                return None
            
            # 첫 번째 에이전트로 분석 요청
            agent_info = agents_for_symbol[0]
            agent_name = agent_info['name']
            
            logger.info(f"🤖 {agent_name} 에이전트로 지속 보유 분석 요청")
            
            # AI 분석 시스템 호출
            from ai_system import ai_system
            if not ai_system.is_available():
                logger.warning("AI 분석 시스템 사용 불가")
                return None
            
            # 분석 수행 (기간을 짧게 해서 빠른 분석)
            analysis_result = ai_system.analyze_with_agent(agent_name, analysis_periods=30)
            
            if analysis_result and not analysis_result.get("error"):
                # 노션에 저장 (지속 분석임을 표시)
                from notion_integration import notion_logger
                if notion_logger.is_available():
                    # 분석 결과에 컨텍스트 추가
                    analysis_result['analysis_context'] = 'TARGET_REACHED_CONTINUE_ANALYSIS'
                    analysis_result['triggered_by'] = 'POSITION_MONITOR'
                    analysis_result['position_info'] = virtual_portfolio.get_position_summary()
                    
                    page_id = notion_logger.create_analysis_page(analysis_result, current_price)
                    logger.info(f"📝 지속 분석 노션 페이지 생성: {page_id}")
                
                return analysis_result
            else:
                logger.warning("AI 분석 실패 또는 오류 응답")
                return None
                
        except Exception as e:
            logger.error(f"지속 분석 요청 중 오류: {e}")
            return None
    
    def _log_monitoring_status(self):
        """모니터링 상태 로깅"""
        try:
            current_time = datetime.now()
            uptime = current_time - (self.last_check_time - timedelta(seconds=self.check_interval * self.check_count))
            
            status_info = {
                'running': self.running,
                'uptime': str(uptime).split('.')[0],  # 마이크로초 제거
                'total_checks': self.check_count,
                'signals_detected': self.signal_count,
                'has_position': virtual_portfolio.current_position is not None,
                'last_check': self.last_check_time.strftime('%H:%M:%S')
            }
            
            if virtual_portfolio.current_position:
                position_summary = virtual_portfolio.get_position_summary()
                status_info.update({
                    'position_symbol': position_summary['symbol'],
                    'position_direction': position_summary['direction'],
                    'unrealized_pnl': f"${position_summary['unrealized_pnl']:+.2f}",
                    'holding_duration': position_summary['holding_duration']
                })
            
            logger.info(f"📊 포지션 모니터 상태: {status_info}")
            
        except Exception as e:
            logger.error(f"모니터링 상태 로깅 중 오류: {e}")
    
    def get_monitor_status(self) -> Dict:
        """모니터 상태 조회"""
        return {
            'running': self.running,
            'check_interval': self.check_interval,
            'total_checks': self.check_count,
            'signals_detected': self.signal_count,
            'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
            'analysis_cooldown': self.analysis_cooldown,
            'has_position': virtual_portfolio.current_position is not None,
            'position_summary': virtual_portfolio.get_position_summary() if virtual_portfolio.current_position else None
        }
    
    def force_position_check(self) -> Dict:
        """강제 포지션 체크 (수동 테스트용)"""
        try:
            if not virtual_portfolio.current_position:
                return {'status': 'no_position', 'message': '현재 포지션이 없습니다'}
            
            self._check_position()
            
            return {
                'status': 'checked',
                'message': '포지션 체크 완료',
                'check_count': self.check_count,
                'signals_detected': self.signal_count,
                'position_summary': virtual_portfolio.get_position_summary()
            }
            
        except Exception as e:
            logger.error(f"강제 포지션 체크 실패: {e}")
            return {'status': 'error', 'message': str(e)}


# 전역 인스턴스
position_monitor = PositionMonitor()

if __name__ == "__main__":
    # 테스트
    logger.info("포지션 모니터 테스트 시작")
    
    # 모니터링 시작
    success = position_monitor.start_monitoring()
    logger.info(f"모니터링 시작 결과: {success}")
    
    # 10초 대기
    time.sleep(10)
    
    # 상태 확인
    status = position_monitor.get_monitor_status()
    logger.info(f"모니터 상태: {status}")
    
    # 모니터링 중지
    position_monitor.stop_monitoring()
    
    logger.info("포지션 모니터 테스트 완료")
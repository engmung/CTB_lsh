from typing import Dict, Optional, List
from datetime import datetime
from config import logger, normalize_symbol
from database import db

class VirtualPortfolio:
    """개선된 가상 포트폴리오 관리 클래스"""
    
    def __init__(self, initial_balance: float = 10000.0):
        self.initial_balance = initial_balance
        self.current_balance = initial_balance
        self.current_position = None  # 현재 포지션 (한 번에 하나만)
        
        # 수수료 및 슬리피지 설정
        self.trading_fee_rate = 0.0004  # 0.04% (바이낸스 스팟 기준)
        self.slippage_rate = 0.001      # 0.1% 슬리피지
        
        # 동적 익절 관련 설정
        self.partial_take_profit_ratio = 0.5  # 50% 부분 익절
        self.trailing_stop_ratio = 0.02       # 2% 트레일링 스탑
        
        logger.info(f"개선된 가상 포트폴리오 초기화: {initial_balance} USDT (수수료: {self.trading_fee_rate:.2%}, 슬리피지: {self.slippage_rate:.2%})")
    
    def get_portfolio_status(self) -> Dict:
        """포트폴리오 현재 상태 조회"""
        total_value = self.current_balance
        unrealized_pnl = 0.0
        unrealized_pnl_percentage = 0.0
        
        if self.current_position:
            # 현재 포지션의 시장가 계산
            current_price_data = db.get_current_price(self.current_position['symbol'])
            if current_price_data:
                current_price = current_price_data['price']
                position_value, pnl, pnl_percentage = self._calculate_position_metrics(current_price)
                total_value = self.current_balance + position_value
                unrealized_pnl = pnl
                unrealized_pnl_percentage = pnl_percentage
        
        return {
            'initial_balance': self.initial_balance,
            'current_balance': self.current_balance,
            'total_value': total_value,
            'current_position': self.current_position,
            'unrealized_pnl': unrealized_pnl,
            'unrealized_pnl_percentage': unrealized_pnl_percentage,
            'total_return': ((total_value - self.initial_balance) / self.initial_balance) * 100,
            'has_position': self.current_position is not None,
            'trading_stats': self._get_trading_statistics(),
            'timestamp': datetime.now().isoformat()
        }
    
    def _get_trading_statistics(self) -> Dict:
        """거래 통계 조회"""
        stats = db.get_portfolio_statistics()
        return {
            'total_trades': stats.get('total_trades', 0),
            'win_rate': stats.get('win_rate', 0.0),
            'average_pnl': stats.get('average_pnl', 0.0),
            'profitable_trades': stats.get('profitable_trades', 0),
            'losing_trades': stats.get('losing_trades', 0)
        }
    
    def can_enter_position(self, symbol: str, leverage: float = 1.0) -> bool:
        """포지션 진입 가능 여부 확인"""
        # 현재 포지션이 있으면 포지션 전환으로 처리
        min_balance = 100.0  # 최소 100 USDT 필요
        if self.current_balance < min_balance:
            logger.warning(f"잔고 부족: {self.current_balance} < {min_balance}")
            return False
        
        return True
    
    def enter_position(self, symbol: str, direction: str, entry_price: float, 
                      leverage: float = 1.0, target_price: float = None, 
                      stop_loss: float = None, force_flip: bool = False) -> bool:
        """포지션 진입 (기존 포지션이 있으면 전환)"""
        try:
            symbol = normalize_symbol(symbol)
            
            # 기존 포지션이 있는 경우 처리
            if self.current_position and not force_flip:
                current_symbol = self.current_position['symbol']
                current_direction = self.current_position['direction']
                
                # 같은 심볼, 같은 방향이면 진입 거부
                if current_symbol == symbol and current_direction == direction:
                    logger.info(f"동일한 포지션이 이미 존재: {symbol} {direction}")
                    return False
                
                # 다른 방향이면 포지션 플립
                if current_symbol == symbol and current_direction != direction:
                    return self._flip_position(direction, entry_price, leverage, target_price, stop_loss)
                
                # 다른 심볼이면 기존 포지션 청산 후 새 포지션 진입
                exit_info = self.exit_position(entry_price, "Position Switch")
                logger.info(f"포지션 전환을 위해 기존 포지션 청산: {exit_info}")
            
            if not self.can_enter_position(symbol, leverage):
                return False
            
            # 실제 진입가 계산 (슬리피지 적용)
            actual_entry_price = self._apply_slippage(entry_price, direction, 'ENTER')
            
            # 포지션 크기 계산 (현재 잔고의 95% 사용)
            available_balance = self.current_balance * 0.95
            
            # 수수료 차감
            fee_amount = available_balance * self.trading_fee_rate
            invested_amount = available_balance - fee_amount
            
            # 레버리지 적용한 포지션 크기
            position_size = (invested_amount * leverage) / actual_entry_price
            
            # 포지션 정보 저장
            self.current_position = {
                'symbol': symbol,
                'direction': direction,  # 'LONG' or 'SHORT'
                'entry_price': actual_entry_price,
                'original_entry_price': entry_price,  # 슬리피지 전 가격
                'position_size': position_size,
                'leverage': leverage,
                'invested_amount': invested_amount,
                'target_price': target_price,
                'stop_loss': stop_loss,
                'entry_time': datetime.now().isoformat(),
                'entry_fee': fee_amount,
                'highest_price': actual_entry_price if direction == 'LONG' else 0,
                'lowest_price': actual_entry_price if direction == 'SHORT' else float('inf'),
                'trailing_stop_price': None,
                'partial_profit_taken': False,
                'total_fees_paid': fee_amount
            }
            
            # 잔고에서 사용된 금액 차감
            self.current_balance -= available_balance
            
            # 데이터베이스에 거래 기록
            trade_data = {
                'action': 'ENTER',
                'symbol': symbol,
                'direction': direction,
                'price': actual_entry_price,
                'size': position_size,
                'leverage': leverage,
                'invested_amount': invested_amount,
                'target_price': target_price,
                'stop_loss': stop_loss
            }
            db.insert_virtual_trade(trade_data)
            
            logger.info(f"포지션 진입: {symbol} {direction} {leverage}x "
                       f"진입가: ${actual_entry_price:.4f} (슬리피지 적용), "
                       f"투자금: ${invested_amount:.2f}, 수수료: ${fee_amount:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"포지션 진입 실패: {e}")
            return False
    
    def _flip_position(self, new_direction: str, new_entry_price: float, 
                      leverage: float, target_price: float, stop_loss: float) -> bool:
        """포지션 플립 (기존 포지션 청산 + 반대 방향 진입)"""
        try:
            logger.info(f"포지션 플립 시작: {self.current_position['direction']} → {new_direction}")
            
            # 기존 포지션 청산
            exit_info = self.exit_position(new_entry_price, "Position Flip")
            if not exit_info:
                logger.error("포지션 플립을 위한 청산 실패")
                return False
            
            # 새로운 포지션 진입
            success = self.enter_position(
                self.current_position['symbol'] if self.current_position else exit_info['symbol'],
                new_direction, new_entry_price, leverage, target_price, stop_loss, force_flip=True
            )
            
            if success:
                logger.info(f"포지션 플립 완료: {new_direction} 포지션 진입")
            else:
                logger.error("포지션 플립 실패: 새 포지션 진입 불가")
            
            return success
            
        except Exception as e:
            logger.error(f"포지션 플립 실패: {e}")
            return False
    
    def exit_position(self, exit_price: float, reason: str = "Manual", 
                     partial_ratio: float = 1.0) -> Optional[Dict]:
        """포지션 청산 (부분 청산 지원)"""
        try:
            if not self.current_position:
                logger.warning("청산할 포지션이 없습니다")
                return None
            
            # 실제 청산가 계산 (슬리피지 적용)
            actual_exit_price = self._apply_slippage(exit_price, self.current_position['direction'], 'EXIT')
            
            # 청산할 포지션 크기
            exit_position_size = self.current_position['position_size'] * partial_ratio
            
            # 손익 계산
            position_value, pnl, pnl_percentage = self._calculate_position_metrics(
                actual_exit_price, exit_position_size
            )
            
            # 청산 수수료 계산
            exit_fee = position_value * self.trading_fee_rate
            final_position_value = position_value - exit_fee
            
            # 실현 손익 (수수료 차감)
            realized_pnl = pnl - exit_fee
            
            # 잔고 업데이트
            self.current_balance += final_position_value
            
            # 청산 정보
            exit_info = {
                'symbol': self.current_position['symbol'],
                'direction': self.current_position['direction'],
                'entry_price': self.current_position['entry_price'],
                'exit_price': actual_exit_price,
                'original_exit_price': exit_price,
                'position_size': exit_position_size,
                'leverage': self.current_position['leverage'],
                'invested_amount': self.current_position['invested_amount'] * partial_ratio,
                'realized_pnl': realized_pnl,
                'realized_pnl_percentage': (realized_pnl / (self.current_position['invested_amount'] * partial_ratio)) * 100,
                'exit_reason': reason,
                'holding_duration': self._calculate_holding_duration(),
                'exit_time': datetime.now().isoformat(),
                'exit_fee': exit_fee,
                'total_fees': self.current_position.get('total_fees_paid', 0) + exit_fee,
                'partial_exit': partial_ratio < 1.0,
                'exit_ratio': partial_ratio
            }
            
            # 데이터베이스에 거래 기록
            trade_data = {
                'action': 'EXIT',
                'symbol': self.current_position['symbol'],
                'direction': self.current_position['direction'],
                'price': actual_exit_price,
                'size': exit_position_size,
                'leverage': self.current_position['leverage'],
                'realized_pnl': realized_pnl,
                'exit_reason': reason
            }
            db.insert_virtual_trade(trade_data)
            
            logger.info(f"포지션 청산 ({partial_ratio:.0%}): {self.current_position['symbol']} "
                       f"손익: ${realized_pnl:.2f} ({exit_info['realized_pnl_percentage']:+.2f}%) "
                       f"수수료: ${exit_fee:.2f}, 사유: {reason}")
            
            # 부분 청산인 경우 포지션 크기 조정
            if partial_ratio < 1.0:
                self.current_position['position_size'] *= (1 - partial_ratio)
                self.current_position['invested_amount'] *= (1 - partial_ratio)
                self.current_position['total_fees_paid'] += exit_fee
                
                if reason == "Partial Take Profit":
                    self.current_position['partial_profit_taken'] = True
            else:
                # 완전 청산인 경우 포지션 정보 초기화
                self.current_position = None
            
            return exit_info
            
        except Exception as e:
            logger.error(f"포지션 청산 실패: {e}")
            return None
    
    def _apply_slippage(self, price: float, direction: str, action: str) -> float:
        """슬리피지 적용"""
        if action == 'ENTER':
            if direction == 'LONG':
                return price * (1 + self.slippage_rate)  # 매수시 불리하게
            else:  # SHORT
                return price * (1 - self.slippage_rate)  # 매도시 불리하게
        else:  # EXIT
            if direction == 'LONG':
                return price * (1 - self.slippage_rate)  # 매도시 불리하게
            else:  # SHORT
                return price * (1 + self.slippage_rate)  # 매수시 불리하게
    
    def _calculate_position_metrics(self, current_price: float, position_size: float = None) -> tuple:
        """포지션 메트릭 계산 (포지션 가치, 손익, 손익률)"""
        if not self.current_position:
            return 0.0, 0.0, 0.0
        
        if position_size is None:
            position_size = self.current_position['position_size']
        
        entry_price = self.current_position['entry_price']
        leverage = self.current_position['leverage']
        direction = self.current_position['direction']
        
        # 실제 투자 금액 비율
        position_ratio = position_size / self.current_position['position_size']
        invested_amount = self.current_position['invested_amount'] * position_ratio
        
        # 가격 변동률 계산
        if direction == 'LONG':
            price_change_rate = (current_price - entry_price) / entry_price
        else:  # SHORT
            price_change_rate = (entry_price - current_price) / entry_price
        
        # 레버리지 적용한 손익률
        leveraged_return_rate = price_change_rate * leverage
        
        # 포지션 가치 계산
        position_value = invested_amount * (1 + leveraged_return_rate)
        
        # 청산가 보호 (손실이 투자금의 95%를 넘지 않도록)
        min_value = invested_amount * 0.05
        position_value = max(position_value, min_value)
        
        # 손익 계산
        pnl = position_value - invested_amount
        pnl_percentage = (pnl / invested_amount) * 100 if invested_amount > 0 else 0.0
        
        return position_value, pnl, pnl_percentage
    
    def _calculate_holding_duration(self) -> str:
        """포지션 보유 시간 계산"""
        if not self.current_position:
            return "0분"
        
        entry_time = datetime.fromisoformat(self.current_position['entry_time'].replace('Z', '+00:00'))
        duration = datetime.now() - entry_time.replace(tzinfo=None)
        
        total_minutes = int(duration.total_seconds() / 60)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        days = hours // 24
        hours = hours % 24
        
        if days > 0:
            return f"{days}일 {hours}시간 {minutes}분"
        elif hours > 0:
            return f"{hours}시간 {minutes}분"
        else:
            return f"{minutes}분"
    
    def update_trailing_stop(self, current_price: float) -> bool:
        """트레일링 스탑 업데이트"""
        if not self.current_position:
            return False
        
        direction = self.current_position['direction']
        
        # 최고/최저가 업데이트
        if direction == 'LONG':
            if current_price > self.current_position['highest_price']:
                self.current_position['highest_price'] = current_price
                # 트레일링 스탑 가격 업데이트
                self.current_position['trailing_stop_price'] = current_price * (1 - self.trailing_stop_ratio)
                return True
        else:  # SHORT
            if current_price < self.current_position['lowest_price']:
                self.current_position['lowest_price'] = current_price
                # 트레일링 스탑 가격 업데이트
                self.current_position['trailing_stop_price'] = current_price * (1 + self.trailing_stop_ratio)
                return True
        
        return False
    
    def check_position_signals(self, current_price: float) -> List[str]:
        """포지션 관련 신호 체크"""
        if not self.current_position:
            return []
        
        signals = []
        direction = self.current_position['direction']
        target_price = self.current_position.get('target_price')
        trailing_stop_price = self.current_position.get('trailing_stop_price')
        
        # 목표가 도달 체크
        if target_price:
            if direction == 'LONG' and current_price >= target_price:
                signals.append('TARGET_REACHED')
            elif direction == 'SHORT' and current_price <= target_price:
                signals.append('TARGET_REACHED')
        
        # 트레일링 스탑 체크
        if trailing_stop_price:
            if direction == 'LONG' and current_price <= trailing_stop_price:
                signals.append('TRAILING_STOP')
            elif direction == 'SHORT' and current_price >= trailing_stop_price:
                signals.append('TRAILING_STOP')
        
        # 수익률 기반 부분 익절 체크 (10% 이상 수익시)
        _, pnl, pnl_percentage = self._calculate_position_metrics(current_price)
        if pnl_percentage >= 10.0 and not self.current_position.get('partial_profit_taken', False):
            signals.append('PARTIAL_TAKE_PROFIT')
        
        return signals
    
    def execute_partial_take_profit(self, current_price: float) -> Optional[Dict]:
        """부분 익절 실행"""
        if not self.current_position:
            return None
        
        logger.info(f"부분 익절 실행: {self.partial_take_profit_ratio:.0%}")
        return self.exit_position(current_price, "Partial Take Profit", self.partial_take_profit_ratio)
    
    def get_position_summary(self) -> Dict:
        """포지션 요약 정보"""
        if not self.current_position:
            return {'has_position': False}
        
        current_price_data = db.get_current_price(self.current_position['symbol'])
        current_price = current_price_data['price'] if current_price_data else self.current_position['entry_price']
        
        position_value, pnl, pnl_percentage = self._calculate_position_metrics(current_price)
        
        return {
            'has_position': True,
            'symbol': self.current_position['symbol'],
            'direction': self.current_position['direction'],
            'entry_price': self.current_position['entry_price'],
            'current_price': current_price,
            'leverage': self.current_position['leverage'],
            'position_size': self.current_position['position_size'],
            'invested_amount': self.current_position['invested_amount'],
            'position_value': position_value,
            'unrealized_pnl': pnl,
            'unrealized_pnl_percentage': pnl_percentage,
            'target_price': self.current_position.get('target_price'),
            'trailing_stop_price': self.current_position.get('trailing_stop_price'),
            'holding_duration': self._calculate_holding_duration(),
            'partial_profit_taken': self.current_position.get('partial_profit_taken', False),
            'total_fees_paid': self.current_position.get('total_fees_paid', 0)
        }


# 전역 인스턴스
virtual_portfolio = VirtualPortfolio()

if __name__ == "__main__":
    # 테스트
    logger.info("개선된 가상 포트폴리오 테스트 시작")
    
    status = virtual_portfolio.get_portfolio_status()
    logger.info(f"초기 상태: {status}")
    
    # 테스트 포지션 진입
    success = virtual_portfolio.enter_position("BTC/USDT", "LONG", 45000.0, 2.0, 46000.0, 44000.0)
    logger.info(f"포지션 진입 결과: {success}")
    
    if success:
        status = virtual_portfolio.get_portfolio_status()
        logger.info(f"포지션 진입 후: {status}")
        
        # 트레일링 스탑 테스트
        virtual_portfolio.update_trailing_stop(45500.0)
        logger.info("트레일링 스탑 업데이트 테스트")
        
        # 신호 체크 테스트
        signals = virtual_portfolio.check_position_signals(45500.0)
        logger.info(f"포지션 신호: {signals}")
    
    logger.info("개선된 가상 포트폴리오 테스트 완료")
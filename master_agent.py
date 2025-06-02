import json
from datetime import datetime
from typing import Dict, Optional, List
from config import GEMINI_API_KEY, logger, get_symbol_display_name
from virtual_portfolio import virtual_portfolio
from market_data import market_data_collector
from database import db

try:
    from google import genai
    from google.genai import types
    GOOGLE_GENAI_AVAILABLE = True
except ImportError:
    GOOGLE_GENAI_AVAILABLE = False
    logger.warning("google-genai 패키지를 찾을 수 없습니다. 총괄 에이전트가 비활성화됩니다.")

# 총괄 에이전트 시스템 프롬프트
MASTER_AGENT_PROMPT = """당신은 최고의 트레이딩 총괄 관리자입니다.
개별 분석 에이전트의 결과와 현재 포트폴리오 상태, 시장 상황을 종합하여 최종 매매 결정을 내립니다.

결정 기준:
1. 개별 분석의 신뢰도와 추천 방향
2. 현재 포트폴리오 상태 (포지션 유무, 손익 상황)
3. 시장 센티먼트 (공포탐욕지수, 변동성)
4. 리스크 관리 (손절/목표가 달성)

매매 결정:
- ENTER: 새로운 포지션 진입
- EXIT: 기존 포지션 청산  
- HOLD: 현재 상태 유지

레버리지 결정 (1x ~ 10x):
- 높은 신뢰도 + 좋은 시장 상황 = 높은 레버리지
- 낮은 신뢰도 + 불안한 시장 = 낮은 레버리지

정확한 JSON 형식으로 응답하세요."""


class MasterAgent:
    """총괄 트레이딩 에이전트"""
    
    def __init__(self):
        self.available = self._check_availability()
        if self.available:
            logger.info("총괄 에이전트 초기화 완료")
        else:
            logger.warning("총괄 에이전트를 사용할 수 없습니다")
    
    def _check_availability(self) -> bool:
        """총괄 에이전트 사용 가능 여부 확인"""
        return GOOGLE_GENAI_AVAILABLE and bool(GEMINI_API_KEY)
    
    def is_available(self) -> bool:
        """총괄 에이전트 사용 가능 여부"""
        return self.available
    
    def make_trading_decision(self, individual_analysis: Dict, triggered_signals: Dict = None) -> Optional[Dict]:
        """개별 분석 결과를 받아 최종 매매 결정"""
        if not self.available:
            logger.error("총괄 에이전트를 사용할 수 없습니다")
            return None
        
        try:
            symbol = individual_analysis.get('symbol', 'UNKNOWN')
            symbol_display = get_symbol_display_name(symbol)
            
            logger.info(f"🤖 === 총괄 에이전트 매매 결정 시작: {symbol} ({symbol_display}) ===")
            
            # 1. 현재 포트폴리오 상태 확인
            portfolio_status = virtual_portfolio.get_portfolio_status()
            
            # 2. 시장 센티먼트 확인
            market_sentiment = market_data_collector.get_market_sentiment(symbol)
            
            # 3. 현재가 조회
            current_price_data = db.get_current_price(symbol)
            current_price = current_price_data['price'] if current_price_data else 0
            
            # 4. 기존 포지션 손절/목표가 체크
            position_signal = None
            if portfolio_status['has_position']:
                position_signal = virtual_portfolio.check_stop_loss_target(current_price)
            
            # 5. 총괄 분석용 프롬프트 생성
            decision_prompt = self._create_decision_prompt(
                individual_analysis, 
                portfolio_status, 
                market_sentiment, 
                current_price,
                position_signal,
                triggered_signals
            )
            
            # 6. AI 매매 결정 수행
            logger.info(f"🧠 총괄 AI 분석 실행...")
            master_decision = self._call_master_ai(decision_prompt)
            
            if master_decision.get("error"):
                logger.error(f"총괄 AI 분석 실패: {master_decision['error']}")
                return None
            
            # 7. 결정 결과에 메타데이터 추가
            master_decision.update({
                'symbol': symbol,
                'symbol_display': symbol_display,
                'individual_analysis_id': individual_analysis.get('id'),
                'current_price': current_price,
                'portfolio_status': portfolio_status,
                'market_sentiment': market_sentiment,
                'position_signal': position_signal,
                'triggered_signals': triggered_signals,
                'decision_timestamp': datetime.now().isoformat()
            })
            
            # 8. 매매 실행
            execution_result = self._execute_trading_decision(master_decision)
            master_decision['execution_result'] = execution_result
            
            # 9. 결정 기록 저장
            db.insert_master_decision(master_decision)
            
            decision_action = master_decision.get('trading_decision', 'HOLD')
            confidence = master_decision.get('confidence', 0.0)
            
            logger.info(f"🎯 === 총괄 에이전트 결정 완료: {decision_action} (신뢰도: {confidence:.1%}) ===")
            
            return master_decision
                
        except Exception as e:
            logger.error(f"총괄 에이전트 매매 결정 중 오류: {e}")
            return None
    
    def _create_decision_prompt(self, individual_analysis: Dict, portfolio_status: Dict, 
                              market_sentiment: Dict, current_price: float, 
                              position_signal: str = None, triggered_signals: Dict = None) -> str:
        """총괄 결정용 프롬프트 생성"""
        try:
            symbol = individual_analysis.get('symbol', 'UNKNOWN')
            symbol_display = individual_analysis.get('symbol_display', symbol)
            
            # 개별 분석 정보
            individual_summary = f"""개별 분석 결과:
- 심볼: {symbol} ({symbol_display})
- 추천: {individual_analysis.get('recommendation', 'N/A')}
- 신뢰도: {individual_analysis.get('confidence', 0):.1%}
- 목표가: ${individual_analysis.get('target_price', 0):.4f}
- 손절가: ${individual_analysis.get('stop_loss', 0):.4f}
- 분석 내용: {individual_analysis.get('analysis', 'N/A')[:200]}...
- 주요 근거: {', '.join(individual_analysis.get('reasons', [])[:3])}"""
            
            # 시그널 정보
            signal_summary = ""
            if triggered_signals:
                signal_count = triggered_signals.get('count', 0)
                signal_types = [s.get('type', 'UNKNOWN') for s in triggered_signals.get('signals', [])]
                strongest_signal = triggered_signals.get('strongest_signal', {})
                
                signal_summary = f"""감지된 시그널:
- 시그널 개수: {signal_count}개
- 시그널 타입: {', '.join(signal_types)}
- 주요 시그널: {strongest_signal.get('description', 'N/A')}
- 시그널 강도: {strongest_signal.get('strength', 'N/A')}"""
            
            # 포트폴리오 상태
            portfolio_summary = f"""현재 포트폴리오:
- 현재 잔고: ${portfolio_status.get('current_balance', 0):.2f}
- 총 자산: ${portfolio_status.get('total_value', 0):.2f}
- 수익률: {portfolio_status.get('total_return', 0):+.2f}%
- 포지션 유무: {'있음' if portfolio_status.get('has_position') else '없음'}"""
            
            # 기존 포지션 정보 (있는 경우)
            position_info = ""
            if portfolio_status.get('has_position'):
                pos = portfolio_status.get('current_position', {})
                position_info = f"""기존 포지션:
- 심볼: {pos.get('symbol', 'N/A')}
- 방향: {pos.get('direction', 'N/A')}
- 진입가: ${pos.get('entry_price', 0):.4f}
- 레버리지: {pos.get('leverage', 1)}x
- 미실현 손익: ${portfolio_status.get('unrealized_pnl', 0):+.2f} ({portfolio_status.get('unrealized_pnl_percentage', 0):+.2f}%)
- 목표가: ${pos.get('target_price', 0):.4f}
- 손절가: ${pos.get('stop_loss', 0):.4f}"""
                
                if position_signal:
                    position_info += f"\n- ⚠️ 손절/목표가 신호: {position_signal}"
            
            # 시장 센티먼트
            sentiment_summary = f"""시장 센티먼트:
- 현재가: ${current_price:.4f}
- 종합 센티먼트: {market_sentiment.get('combined_sentiment', 50):.1f} ({market_sentiment.get('sentiment_label', 'Neutral')})
- 공포탐욕지수: {market_sentiment.get('fear_greed_index', {}).get('value', 50)} ({market_sentiment.get('fear_greed_index', {}).get('value_classification', 'Neutral')})
- 변동성: {market_sentiment.get('volatility_data', {}).get('volatility', 0):.2f}% ({market_sentiment.get('volatility_data', {}).get('classification', 'Medium')})
- 센티먼트 추천: {market_sentiment.get('recommendation', 'Neutral')}"""
            
            # 최종 프롬프트
            prompt_parts = [
                MASTER_AGENT_PROMPT,
                "",
                "=== 현재 상황 분석 ===",
                individual_summary,
                "",
                signal_summary,
                "",
                portfolio_summary,
                "",
                position_info,
                "",
                sentiment_summary,
                "",
                "=== 결정 요청 ===",
                "위 정보를 종합하여 다음 JSON 형식으로 매매 결정을 내려주세요:",
                "",
                """{
    "trading_decision": "ENTER|EXIT|HOLD",
    "confidence": 0.85,
    "direction": "LONG|SHORT|null",
    "leverage": 2.5,
    "target_price": 120.50,
    "stop_loss": 115.00,
    "reasoning": "상세한 결정 근거",
    "risk_assessment": "LOW|MEDIUM|HIGH",
    "market_timing": "EXCELLENT|GOOD|NEUTRAL|POOR",
    "expected_return": 8.5
}""",
                "",
                "결정 규칙:",
                "1. 기존 포지션이 있고 손절/목표가 신호가 있으면 EXIT 우선 고려",
                "2. 개별 분석 신뢰도가 70% 미만이면 HOLD 또는 낮은 레버리지",
                "3. 시장 센티먼트가 극단적(20 이하 또는 80 이상)이면 신중한 접근",
                "4. 변동성이 Very High이면 레버리지 최대 3x로 제한",
                "5. 포트폴리오 손실이 -10% 이상이면 보수적 접근"
            ]
            
            final_prompt = "\n".join(prompt_parts)
            logger.info(f"총괄 결정 프롬프트 생성 완료: {len(final_prompt)} 문자")
            
            return final_prompt
            
        except Exception as e:
            logger.error(f"총괄 결정 프롬프트 생성 실패: {e}")
            return ""
    
    def _call_master_ai(self, prompt_text: str) -> Dict:
        """총괄 AI 호출"""
        if not self._check_availability():
            return {"error": "총괄 AI를 사용할 수 없습니다. API 키를 확인하세요."}
        
        try:
            # 클라이언트 초기화
            client = genai.Client(api_key=GEMINI_API_KEY)
            
            contents = [
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_text(text=prompt_text),
                    ],
                ),
            ]
            
            # Structured Output 설정
            generate_content_config = types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=genai.types.Schema(
                    type=genai.types.Type.OBJECT,
                    properties={
                        "trading_decision": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="ENTER, EXIT, 또는 HOLD 중 하나"
                        ),
                        "confidence": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="0.0에서 1.0 사이의 신뢰도"
                        ),
                        "direction": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="LONG, SHORT, 또는 null"
                        ),
                        "leverage": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="1.0에서 10.0 사이의 레버리지"
                        ),
                        "target_price": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="목표 가격"
                        ),
                        "stop_loss": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="손절 가격"
                        ),
                        "reasoning": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="결정 근거"
                        ),
                        "risk_assessment": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="LOW, MEDIUM, 또는 HIGH"
                        ),
                        "market_timing": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="EXCELLENT, GOOD, NEUTRAL, 또는 POOR"
                        ),
                        "expected_return": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="예상 수익률 (%)"
                        )
                    },
                    required=["trading_decision", "confidence", "reasoning"]
                )
            )
            
            logger.info("총괄 AI 호출 시작...")
            
            # API 호출
            response = client.models.generate_content(
                model="gemini-2.5-flash-preview-05-20",
                contents=contents,
                config=generate_content_config
            )
            
            if not response.candidates or not response.candidates[0].content:
                logger.error("총괄 AI 응답에 내용이 없습니다.")
                return {"error": "총괄 AI 분석 응답을 받을 수 없습니다."}
            
            response_text = response.candidates[0].content.parts[0].text
            
            if not response_text or response_text.strip() == "":
                logger.error("총괄 AI 응답 텍스트가 비어있습니다.")
                return {"error": "총괄 AI가 빈 응답을 반환했습니다."}
            
            # JSON 파싱
            try:
                decision_result = json.loads(response_text.strip())
                logger.info("총괄 AI 분석 완료")
                
                # 필수 필드 검증 및 기본값 설정
                if "trading_decision" not in decision_result:
                    decision_result["trading_decision"] = "HOLD"
                if "confidence" not in decision_result:
                    decision_result["confidence"] = 0.5
                if "reasoning" not in decision_result:
                    decision_result["reasoning"] = "결정 근거가 제공되지 않았습니다."
                
                # 레버리지 범위 검증
                leverage = decision_result.get("leverage", 1.0)
                decision_result["leverage"] = max(1.0, min(10.0, leverage))
                
                return decision_result
                
            except json.JSONDecodeError as e:
                logger.error(f"총괄 AI JSON 파싱 오류: {e}")
                return {
                    "error": "JSON 파싱 실패",
                    "trading_decision": "HOLD",
                    "confidence": 0.3,
                    "reasoning": f"API 응답 파싱에 실패했습니다. 원본 응답: {response_text[:500]}"
                }
            
        except Exception as e:
            logger.error(f"총괄 AI 호출 중 오류: {e}")
            return {
                "error": f"총괄 AI 분석 중 오류가 발생했습니다: {str(e)}",
                "trading_decision": "HOLD",
                "confidence": 0.3,
                "reasoning": f"API 호출 중 오류가 발생했습니다: {str(e)}"
            }
    
    def _execute_trading_decision(self, master_decision: Dict) -> Dict:
        """개선된 매매 결정 실행 - 포지션 전환 로직 포함"""
        try:
            decision = master_decision.get('trading_decision', 'HOLD')
            symbol = master_decision.get('symbol', 'UNKNOWN')
            current_price = master_decision.get('current_price', 0)
            direction = master_decision.get('direction', 'LONG')
            leverage = master_decision.get('leverage', 1.0)
            target_price = master_decision.get('target_price')
            stop_loss = master_decision.get('stop_loss')
            
            # 현재 포지션 상태 확인
            portfolio_status = virtual_portfolio.get_portfolio_status()
            has_position = portfolio_status.get('has_position', False)
            current_position = virtual_portfolio.current_position
            
            logger.info(f"🎯 매매 결정 실행: {symbol} {decision} (현재 포지션: {'있음' if has_position else '없음'})")
            
            if decision == "EXIT":
                # 포지션 청산
                if has_position:
                    exit_result = virtual_portfolio.exit_position(current_price, "Master Agent Decision")
                    if exit_result:
                        logger.info(f"✅ 포지션 청산 완료: {symbol} (${exit_result['realized_pnl']:+.2f})")
                        return {
                            'action': 'EXIT',
                            'success': True,
                            'exit_info': exit_result,
                            'message': f"포지션 청산 완료: ${exit_result['realized_pnl']:+.2f} 손익"
                        }
                    else:
                        logger.warning(f"❌ 포지션 청산 실패: {symbol}")
                        return {
                            'action': 'EXIT',
                            'success': False,
                            'error': '포지션 청산 실패'
                        }
                else:
                    logger.info(f"📊 청산할 포지션이 없음: {symbol}")
                    return {
                        'action': 'EXIT',
                        'success': False,
                        'error': '청산할 포지션이 없습니다'
                    }
            
            elif decision == "ENTER":
                # 새 포지션 진입 또는 포지션 전환
                if has_position:
                    current_symbol = current_position['symbol']
                    current_direction = current_position['direction']
                    
                    # 같은 심볼인 경우
                    if current_symbol == symbol:
                        # 같은 방향이면 진입 거부
                        if current_direction == direction:
                            logger.info(f"📊 동일한 포지션이 이미 존재: {symbol} {direction}")
                            return {
                                'action': 'ENTER',
                                'success': False,
                                'error': f'동일한 {direction} 포지션이 이미 존재합니다',
                                'message': f"기존 {direction} 포지션 유지"
                            }
                        
                        # 반대 방향이면 포지션 플립
                        else:
                            logger.info(f"🔄 포지션 플립 실행: {current_direction} → {direction}")
                            enter_result = virtual_portfolio.enter_position(
                                symbol, direction, current_price, leverage, target_price, stop_loss
                            )
                            
                            if enter_result:
                                logger.info(f"✅ 포지션 플립 완료: {symbol} {direction} {leverage}x")
                                return {
                                    'action': 'POSITION_FLIP',
                                    'success': True,
                                    'position_info': {
                                        'symbol': symbol,
                                        'direction': direction,
                                        'entry_price': current_price,
                                        'leverage': leverage,
                                        'target_price': target_price,
                                        'stop_loss': stop_loss,
                                        'previous_direction': current_direction
                                    },
                                    'message': f"포지션 플립 완료: {current_direction} → {direction}"
                                }
                            else:
                                logger.warning(f"❌ 포지션 플립 실패: {symbol}")
                                return {
                                    'action': 'POSITION_FLIP',
                                    'success': False,
                                    'error': '포지션 플립 실패'
                                }
                    
                    # 다른 심볼인 경우 - 기존 포지션 청산 후 새 포지션 진입
                    else:
                        logger.info(f"💱 심볼 전환: {current_symbol} → {symbol}")
                        
                        # 기존 포지션 청산
                        exit_result = virtual_portfolio.exit_position(current_price, "Symbol Switch")
                        if exit_result:
                            logger.info(f"✅ 기존 포지션 청산: {current_symbol} (${exit_result['realized_pnl']:+.2f})")
                        
                        # 새 포지션 진입
                        enter_result = virtual_portfolio.enter_position(
                            symbol, direction, current_price, leverage, target_price, stop_loss
                        )
                        
                        if enter_result:
                            logger.info(f"✅ 새 포지션 진입: {symbol} {direction} {leverage}x")
                            return {
                                'action': 'SYMBOL_SWITCH',
                                'success': True,
                                'exit_info': exit_result,
                                'position_info': {
                                    'symbol': symbol,
                                    'direction': direction,
                                    'entry_price': current_price,
                                    'leverage': leverage,
                                    'target_price': target_price,
                                    'stop_loss': stop_loss
                                },
                                'message': f"심볼 전환 완료: {current_symbol} → {symbol}"
                            }
                        else:
                            logger.warning(f"❌ 새 포지션 진입 실패: {symbol}")
                            return {
                                'action': 'SYMBOL_SWITCH',
                                'success': False,
                                'error': '새 포지션 진입 실패',
                                'exit_info': exit_result
                            }
                
                # 포지션이 없는 경우 - 일반적인 신규 진입
                else:
                    enter_result = virtual_portfolio.enter_position(
                        symbol, direction, current_price, leverage, target_price, stop_loss
                    )
                    
                    if enter_result:
                        logger.info(f"✅ 신규 포지션 진입: {symbol} {direction} {leverage}x")
                        return {
                            'action': 'ENTER',
                            'success': True,
                            'position_info': {
                                'symbol': symbol,
                                'direction': direction,
                                'entry_price': current_price,
                                'leverage': leverage,
                                'target_price': target_price,
                                'stop_loss': stop_loss
                            },
                            'message': f"신규 포지션 진입: {direction} {leverage}x"
                        }
                    else:
                        logger.warning(f"❌ 신규 포지션 진입 실패: {symbol}")
                        return {
                            'action': 'ENTER',
                            'success': False,
                            'error': '신규 포지션 진입 조건 불충족'
                        }
            
            else:  # HOLD
                if has_position:
                    position_summary = virtual_portfolio.get_position_summary()
                    logger.info(f"📊 포지션 유지: {symbol} "
                            f"(미실현 손익: ${position_summary['unrealized_pnl']:+.2f})")
                    return {
                        'action': 'HOLD',
                        'success': True,
                        'message': f"기존 포지션 유지: {position_summary['direction']} (${position_summary['unrealized_pnl']:+.2f})",
                        'position_info': position_summary
                    }
                else:
                    logger.info(f"📊 포지션 없음 - 대기: {symbol}")
                    return {
                        'action': 'HOLD',
                        'success': True,
                        'message': '포지션 없음 - 관망 상태 유지'
                    }
        
        except Exception as e:
            logger.error(f"매매 결정 실행 실패: {e}")
            return {
                'action': 'ERROR',
                'success': False,
                'error': str(e)
            }


# 전역 인스턴스
master_agent = MasterAgent()

if __name__ == "__main__":
    # 테스트 실행
    logger.info("총괄 에이전트 테스트 시작")
    
    if master_agent.is_available():
        logger.info("총괄 에이전트 사용 가능")
        
        # 테스트용 개별 분석 결과
        test_analysis = {
            'symbol': 'BTC/USDT',
            'recommendation': 'BUY',
            'confidence': 0.75,
            'target_price': 46000.0,
            'stop_loss': 44000.0,
            'analysis': '기술적 지표가 강세를 보이고 있습니다.',
            'reasons': ['RSI 과매도 반전', 'MACD 골든크로스', '거래량 급증']
        }
        
        # 매매 결정 테스트
        result = master_agent.make_trading_decision(test_analysis)
        logger.info(f"매매 결정 결과: {result}")
    else:
        logger.error("총괄 에이전트를 사용할 수 없습니다")
    
    logger.info("총괄 에이전트 테스트 완료")
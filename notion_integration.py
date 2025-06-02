import json
from datetime import datetime
from typing import Dict, List, Optional
from notion_client import Client
from config import logger, normalize_symbol, get_symbol_display_name, DEFAULT_SYMBOL

# 환경변수 import - 없는 것들은 None으로 처리
import os
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID')
TRADING_AGENTS_DB_ID = os.getenv('TRADING_AGENTS_DB_ID')
TRADING_DECISIONS_DB_ID = os.getenv('TRADING_DECISIONS_DB_ID')


class NotionConfigManager:
    """노션 설정 관리 클래스 (기존 notion_config_manager.py)"""
    
    def __init__(self):
        self.client = None
        self.trading_agents_db_id = TRADING_AGENTS_DB_ID
        self.agents_cache = {}  # 에이전트 캐시
        self.available = self._check_availability()
        
        if self.available:
            self.client = Client(auth=NOTION_API_KEY)
            logger.info("노션 설정 관리자 초기화 완료")
        else:
            logger.warning("노션 설정 관리자를 사용할 수 없습니다")
    
    def _check_availability(self) -> bool:
        """노션 연동 가능 여부 확인"""
        return bool(NOTION_API_KEY and self.trading_agents_db_id)
    
    def is_available(self) -> bool:
        """노션 연동 가능 여부"""
        return self.available
    
    def load_all_agents(self) -> bool:
        """모든 트레이딩 에이전트 로드"""
        if not self.available:
            logger.error("노션 연동 불가 또는 데이터베이스 ID 미설정")
            return False
        
        try:
            # 모든 활성 에이전트 조회
            response = self.client.databases.query(
                database_id=self.trading_agents_db_id,
                filter={
                    "property": "활성화",
                    "checkbox": {
                        "equals": True
                    }
                }
            )
            
            self.agents_cache = {}
            
            for page in response["results"]:
                try:
                    agent_info = self._parse_agent_page(page)
                    if agent_info:
                        self.agents_cache[agent_info['name']] = agent_info
                        symbol_display = get_symbol_display_name(agent_info['symbol'])
                        logger.info(f"에이전트 로드: {agent_info['name']} (심볼: {agent_info['symbol']} - {symbol_display}, 시간봉: {agent_info['timeframes']})")
                except Exception as e:
                    logger.error(f"에이전트 파싱 실패: {e}")
                    continue
            
            logger.info(f"총 {len(self.agents_cache)}개 에이전트 로드 완료")
            return True
            
        except Exception as e:
            logger.error(f"에이전트 로드 실패: {e}")
            return False
    
    def _parse_agent_page(self, page: Dict) -> Optional[Dict]:
        """노션 페이지에서 에이전트 정보 파싱"""
        try:
            properties = page["properties"]
            
            # 이름 추출
            name_prop = properties.get("이름", {})
            if name_prop.get("type") == "title":
                name = "".join([text["plain_text"] for text in name_prop.get("title", [])])
            else:
                logger.warning("이름 필드를 찾을 수 없습니다")
                return None
            
            # 분석데이터 추출 (멀티셀렉트)
            timeframes_prop = properties.get("분석데이터", {})
            timeframes = []
            if timeframes_prop.get("type") == "multi_select":
                timeframes = [option["name"] for option in timeframes_prop.get("multi_select", [])]
            
            # 전략 추출
            strategy_prop = properties.get("전략", {})
            strategy = ""
            if strategy_prop.get("type") == "rich_text":
                strategy = "".join([text["plain_text"] for text in strategy_prop.get("rich_text", [])])
            
            # 분석코인 추출
            symbol_prop = properties.get("분석코인", {})
            symbol = ""
            if symbol_prop.get("type") == "rich_text":
                symbol = "".join([text["plain_text"] for text in symbol_prop.get("rich_text", [])])
            elif symbol_prop.get("type") == "title":
                symbol = "".join([text["plain_text"] for text in symbol_prop.get("title", [])])
            
            # 심볼 정규화
            if symbol:
                symbol = normalize_symbol(symbol)
            else:
                symbol = DEFAULT_SYMBOL
                logger.info(f"에이전트 {name}: 분석코인이 설정되지 않아 기본값 {DEFAULT_SYMBOL} 사용")
            
            # 활성화 확인
            active_prop = properties.get("활성화", {})
            is_active = active_prop.get("checkbox", False) if active_prop.get("type") == "checkbox" else False
            
            if not name or not strategy or not timeframes:
                logger.warning(f"필수 필드 누락: 이름={name}, 전략길이={len(strategy)}, 시간봉수={len(timeframes)}")
                return None
            
            return {
                "page_id": page["id"],
                "name": name,
                "symbol": symbol,
                "timeframes": timeframes,
                "strategy": strategy,
                "is_active": is_active
            }
            
        except Exception as e:
            logger.error(f"에이전트 페이지 파싱 실패: {e}")
            return None
    
    def get_agent(self, agent_name: str) -> Optional[Dict]:
        """특정 에이전트 정보 조회"""
        return self.agents_cache.get(agent_name)
    
    def get_all_agents(self) -> Dict[str, Dict]:
        """모든 에이전트 정보 조회"""
        return self.agents_cache.copy()
    
    def get_agent_names(self) -> List[str]:
        """사용 가능한 에이전트 이름 목록"""
        return list(self.agents_cache.keys())
    
    def get_agents_by_symbol(self, symbol: str) -> List[Dict]:
        """특정 심볼을 분석하는 에이전트들 조회"""
        symbol = normalize_symbol(symbol)
        agents = []
        
        for agent_info in self.agents_cache.values():
            if agent_info['symbol'] == symbol:
                agents.append(agent_info)
        
        return agents
    
    def get_all_symbols(self) -> List[str]:
        """모든 에이전트가 분석하는 심볼 목록"""
        symbols = set()
        for agent_info in self.agents_cache.values():
            symbols.add(agent_info['symbol'])
        return list(symbols)
    
    def reload_agents(self) -> bool:
        """에이전트 캐시 새로고침"""
        logger.info("에이전트 캐시 새로고침")
        return self.load_all_agents()


class NotionLogger:
    """노션 로거 클래스 (기존 notion_logger.py)"""
    
    def __init__(self):
        self.client = None
        self.analysis_database_id = NOTION_DATABASE_ID  # 기존 분석 결과 DB
        self.trading_database_id = TRADING_DECISIONS_DB_ID  # 총괄 매매 결정 DB
        self.available = self._check_availability()
        
        if self.available:
            self.client = Client(auth=NOTION_API_KEY)
            if self.trading_database_id:
                logger.info("노션 로거 초기화 완료 (분석 결과 + 매매 결정 DB 분리)")
            else:
                logger.info("노션 로거 초기화 완료 (통합 DB 사용)")
                self.trading_database_id = self.analysis_database_id  # 통합 DB 사용
        else:
            logger.warning("노션 로거를 사용할 수 없습니다")
    
    def _check_availability(self) -> bool:
        """노션 연동 가능 여부 확인"""
        return bool(NOTION_API_KEY and NOTION_DATABASE_ID)
    
    def is_available(self) -> bool:
        """노션 연동 가능 여부"""
        return self.available
    
    def create_analysis_page(self, analysis_data: Dict, current_price: float) -> Optional[str]:
        """분석 결과를 노션 페이지로 생성"""
        if not self.available:
            logger.error("노션 연동이 불가능합니다")
            return None
        
        try:
            # 분석 데이터에서 심볼 정보 추출
            symbol = analysis_data.get('symbol', 'UNKNOWN')
            symbol_display = get_symbol_display_name(symbol)
            
            # 제목 생성
            now = datetime.now()
            agent_name = analysis_data.get('agent_name', 'Unknown')
            title = f"{now.strftime('%Y-%m-%d %H:%M')} {agent_name} {symbol_display} 분석"
            
            # 기본 속성들
            properties = {
                "Title": {
                    "title": [
                        {
                            "text": {
                                "content": title
                            }
                        }
                    ]
                }
            }
            
            # 안전하게 속성들 추가
            try:
                properties["분석시간"] = {
                    "date": {
                        "start": now.isoformat()
                    }
                }
                properties["현재가"] = {
                    "number": current_price
                }
                properties["판단"] = {
                    "select": {
                        "name": analysis_data.get('recommendation', 'HOLD')
                    }
                }
                properties["신뢰도"] = {
                    "number": analysis_data.get('confidence', 0.0)
                }
                properties["분석심볼"] = {
                    "rich_text": [
                        {
                            "text": {
                                "content": symbol
                            }
                        }
                    ]
                }
                
                # 목표가와 스탑로스 (있는 경우만)
                if analysis_data.get('target_price'):
                    properties["목표가"] = {
                        "number": float(analysis_data['target_price'])
                    }
                if analysis_data.get('stop_loss'):
                    properties["스탑로스"] = {
                        "number": float(analysis_data['stop_loss'])
                    }
                
            except Exception as e:
                logger.warning(f"속성 추가 중 일부 실패: {e}")
            
            # 페이지 내용 구성
            children = self._create_page_content(analysis_data, current_price, symbol, symbol_display)
            
            # 노션 페이지 생성
            response = self.client.pages.create(
                parent={"database_id": self.analysis_database_id},
                properties=properties,
                children=children
            )
            
            page_id = response["id"]
            logger.info(f"노션 페이지 생성 완료: {page_id} ({symbol_display})")
            return page_id
            
        except Exception as e:
            logger.error(f"노션 페이지 생성 실패: {e}")
            return None

    def _create_page_content(self, analysis_data: Dict, current_price: float, symbol: str, symbol_display: str) -> list:
        """페이지 내용 블록 생성 - 시그널 정보 포함"""
        children = []
        
        # 헤더
        agent_name = analysis_data.get('agent_name', 'Unknown')
        children.append({
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"{symbol} ({symbol_display}) 분석 - {agent_name}"
                        }
                    }
                ]
            }
        })
        
        # 핵심 정보 요약
        timeframes_text = ', '.join(analysis_data.get('timeframes_used', [])) if analysis_data.get('timeframes_used') else '정보 없음'
        target_text = f"${analysis_data.get('target_price', 0):.4f}" if analysis_data.get('target_price') else "미설정"
        stop_text = f"${analysis_data.get('stop_loss', 0):.4f}" if analysis_data.get('stop_loss') else "미설정"
        
        # 시그널 정보 추가
        triggered_signals = analysis_data.get('triggered_signals', {})
        signal_info_text = ""
        
        if triggered_signals:
            signal_count = triggered_signals.get('count', 0)
            signals = triggered_signals.get('signals', [])
            signal_summary = triggered_signals.get('summary', '')
            strongest_signal = triggered_signals.get('strongest_signal', {})
            
            if signal_count > 0:
                signal_types = [s.get('type', 'UNKNOWN') for s in signals]
                signal_info_text = f"""
🚨 **감지된 시그널 ({signal_count}개):**
• 시그널 타입: {', '.join(signal_types)}
• 주요 시그널: {strongest_signal.get('description', 'N/A')}
• 시그널 강도: {strongest_signal.get('strength', 'N/A')}
• 시그널 요약: {signal_summary}

"""
        
        summary_text = f"""{signal_info_text}📊 **분석 요약**
• 심볼: {symbol} ({symbol_display})
• 추천: **{analysis_data.get('recommendation', 'N/A')}** (신뢰도: {analysis_data.get('confidence', 0):.1%})
• 현재가: ${current_price:.4f}
• 목표가: {target_text} | 스탑로스: {stop_text}
• 시간봉: {timeframes_text} | 리스크: {analysis_data.get('risk_level', 'N/A')}"""
        
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": summary_text
                        }
                    }
                ]
            }
        })
        
        # 시그널 상세 정보 (있는 경우)
        if triggered_signals and triggered_signals.get('signals'):
            signals = triggered_signals.get('signals', [])
            
            signal_details = "🎯 **감지된 시그널 상세:**\n"
            for i, signal in enumerate(signals[:5], 1):  # 최대 5개만
                signal_details += f"{i}. **{signal.get('type', 'UNKNOWN')}** "
                signal_details += f"({signal.get('strength', 'N/A')}) - {signal.get('description', 'N/A')}\n"
            
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": signal_details
                            }
                        }
                    ]
                }
            })
        
        # 상세 분석
        analysis_text = analysis_data.get('analysis', '분석 내용이 없습니다.')
        if len(analysis_text) > 1000:  # 너무 길면 자르기
            analysis_text = analysis_text[:1000] + "..."
        
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"📈 **상세 분석**\n{analysis_text}"
                        }
                    }
                ]
            }
        })
        
        # 주요 근거
        if analysis_data.get('reasons'):
            reasons_text = "💡 **주요 근거**\n"
            for i, reason in enumerate(analysis_data['reasons'][:3], 1):  # 최대 3개만
                reasons_text += f"{i}. {reason}\n"
            
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": reasons_text
                            }
                        }
                    ]
                }
            })
        
        # 면책 조항
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "⚠️ 본 분석은 기술적 시그널과 AI에 의해 생성된 것으로 투자 조언이 아닙니다."
                        },
                        "annotations": {
                            "color": "gray"
                        }
                    }
                ]
            }
        })
        
        return children

    def create_trading_decision_page(self, master_decision: Dict, individual_analysis: Dict = None) -> Optional[str]:
        """총괄 에이전트의 매매 결정을 노션 페이지로 생성"""
        if not self.available:
            logger.error("노션 연동이 불가능합니다")
            return None
        
        try:
            # 기본 정보 추출
            symbol = master_decision.get('symbol', 'UNKNOWN')
            symbol_display = master_decision.get('symbol_display', symbol)
            decision = master_decision.get('trading_decision', 'HOLD')
            current_price = master_decision.get('current_price', 0)
            
            # 제목 생성
            now = datetime.now()
            title = f"{now.strftime('%Y-%m-%d %H:%M')} 매매결정 {symbol_display} {decision}"
            
            # 총괄 매매 결정 DB가 별도로 있는지 확인
            use_separate_db = (self.trading_database_id != self.analysis_database_id)
            target_db_id = self.trading_database_id
            
            if use_separate_db:
                # 별도 DB 사용 시 - 매매 결정 전용 속성들
                properties = self._create_trading_decision_properties(master_decision, title, now)
            else:
                # 통합 DB 사용 시 - 기존 분석 결과 속성들과 호환
                properties = self._create_integrated_properties(master_decision, title, now)
            
            # 페이지 내용 구성
            children = self._create_trading_decision_content(master_decision, individual_analysis)
            
            # 노션 페이지 생성
            response = self.client.pages.create(
                parent={"database_id": target_db_id},
                properties=properties,
                children=children
            )
            
            page_id = response["id"]
            db_type = "별도 매매 결정 DB" if use_separate_db else "통합 DB"
            logger.info(f"매매 결정 노션 페이지 생성 완료: {page_id} ({symbol_display} {decision}) - {db_type}")
            return page_id
            
        except Exception as e:
            logger.error(f"매매 결정 노션 페이지 생성 실패: {e}")
            return None

    def _create_trading_decision_properties(self, master_decision: Dict, title: str, now: datetime) -> Dict:
        """매매 결정 전용 DB 속성 생성"""
        symbol = master_decision.get('symbol', 'UNKNOWN')
        decision = master_decision.get('trading_decision', 'HOLD')
        current_price = master_decision.get('current_price', 0)
        
        properties = {
            "Title": {
                "title": [
                    {
                        "text": {
                            "content": title
                        }
                    }
                ]
            }
        }
        
        # 안전하게 속성들 추가
        try:
            properties["결정시간"] = {
                "date": {
                    "start": now.isoformat()
                }
            }
            properties["심볼"] = {
                "rich_text": [
                    {
                        "text": {
                            "content": symbol
                        }
                    }
                ]
            }
            properties["매매결정"] = {
                "select": {
                    "name": decision
                }
            }
            properties["신뢰도"] = {
                "number": master_decision.get('confidence', 0.0)
            }
            properties["현재가"] = {
                "number": current_price
            }
            
            # 추가 정보 (있는 경우만)
            if master_decision.get('direction'):
                properties["방향"] = {
                    "select": {
                        "name": master_decision['direction']
                    }
                }
            if master_decision.get('leverage'):
                properties["레버리지"] = {
                    "number": float(master_decision['leverage'])
                }
            if master_decision.get('target_price'):
                properties["목표가"] = {
                    "number": float(master_decision['target_price'])
                }
            if master_decision.get('stop_loss'):
                properties["손절가"] = {
                    "number": float(master_decision['stop_loss'])
                }
            if master_decision.get('risk_assessment'):
                properties["리스크"] = {
                    "select": {
                        "name": master_decision['risk_assessment']
                    }
                }
            if master_decision.get('market_timing'):
                properties["시장타이밍"] = {
                    "select": {
                        "name": master_decision['market_timing']
                    }
                }
            
            # 실행 결과
            execution_result = master_decision.get('execution_result', {})
            if execution_result.get('success') is not None:
                properties["실행성공"] = {
                    "checkbox": execution_result.get('success', False)
                }
            if execution_result.get('action'):
                properties["실행액션"] = {
                    "select": {
                        "name": execution_result['action']
                    }
                }
            
            # 포트폴리오 정보
            portfolio_status = master_decision.get('portfolio_status', {})
            if portfolio_status.get('current_balance'):
                properties["포트폴리오잔고"] = {
                    "number": float(portfolio_status['current_balance'])
                }
            if portfolio_status.get('total_return'):
                properties["누적수익률"] = {
                    "number": float(portfolio_status['total_return'])
                }
            
            # 시장 센티먼트
            market_sentiment = master_decision.get('market_sentiment', {})
            if market_sentiment.get('combined_sentiment'):
                properties["시장센티먼트"] = {
                    "number": float(market_sentiment['combined_sentiment'])
                }
            
        except Exception as e:
            logger.warning(f"매매 결정 속성 추가 중 일부 실패: {e}")
        
        return properties

    def _create_integrated_properties(self, master_decision: Dict, title: str, now: datetime) -> Dict:
        """통합 DB 사용 시 기존 속성과 호환되는 속성 생성"""
        symbol = master_decision.get('symbol', 'UNKNOWN')
        decision = master_decision.get('trading_decision', 'HOLD')
        current_price = master_decision.get('current_price', 0)
        
        # 기존 분석 결과 DB와 호환되는 속성들만 사용
        properties = {
            "Title": {
                "title": [
                    {
                        "text": {
                            "content": title
                        }
                    }
                ]
            }
        }
        
        try:
            # 기존 DB에서 사용하는 속성들
            properties["분석시간"] = {
                "date": {
                    "start": now.isoformat()
                }
            }
            properties["현재가"] = {
                "number": current_price
            }
            properties["판단"] = {
                "select": {
                    "name": decision
                }
            }
            properties["신뢰도"] = {
                "number": master_decision.get('confidence', 0.0)
            }
            properties["분석심볼"] = {
                "rich_text": [
                    {
                        "text": {
                            "content": symbol
                        }
                    }
                ]
            }
            
            # 목표가와 스탑로스 (있는 경우만)
            if master_decision.get('target_price'):
                properties["목표가"] = {
                    "number": float(master_decision['target_price'])
                }
            if master_decision.get('stop_loss'):
                properties["스탑로스"] = {
                    "number": float(master_decision['stop_loss'])
                }
            
        except Exception as e:
            logger.warning(f"통합 DB 속성 추가 중 일부 실패: {e}")
        
        return properties

    def _create_trading_decision_content(self, master_decision: Dict, individual_analysis: Dict = None) -> list:
        """매매 결정 페이지 내용 블록 생성"""
        children = []
        
        # 기본 정보
        symbol = master_decision.get('symbol', 'UNKNOWN')
        symbol_display = master_decision.get('symbol_display', symbol)
        decision = master_decision.get('trading_decision', 'HOLD')
        confidence = master_decision.get('confidence', 0.0)
        current_price = master_decision.get('current_price', 0)
        
        # 헤더
        children.append({
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"🤖 총괄 에이전트 매매 결정: {symbol_display}"
                        }
                    }
                ]
            }
        })
        
        # 결정 요약
        decision_emoji = {"ENTER": "📈", "EXIT": "📉", "HOLD": "📊"}.get(decision, "📊")
        direction = master_decision.get('direction', '')
        leverage = master_decision.get('leverage', 1.0)
        target_price = master_decision.get('target_price', 0)
        stop_loss = master_decision.get('stop_loss', 0)
        
        direction_text = f" {direction}" if direction else ""
        leverage_text = f" {leverage}x" if decision == "ENTER" else ""
        target_text = f"목표가: ${target_price:.4f}" if target_price else "목표가: 미설정"
        stop_text = f"손절가: ${stop_loss:.4f}" if stop_loss else "손절가: 미설정"
        
        summary_text = f"""{decision_emoji} **최종 결정: {decision}{direction_text}{leverage_text}**
📊 신뢰도: {confidence:.1%}
💰 현재가: ${current_price:.4f}
🎯 {target_text} | 🛑 {stop_text}
⚖️ 리스크: {master_decision.get('risk_assessment', 'N/A')}
⏰ 시장 타이밍: {master_decision.get('market_timing', 'N/A')}"""
        
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": summary_text
                        }
                    }
                ]
            }
        })
        
        # 결정 근거
        reasoning = master_decision.get('reasoning', '결정 근거가 제공되지 않았습니다.')
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"🧠 **결정 근거**\n{reasoning}"
                        }
                    }
                ]
            }
        })
        
        # 포트폴리오 상태
        portfolio_status = master_decision.get('portfolio_status', {})
        if portfolio_status:
            portfolio_text = f"""💼 **포트폴리오 상태**
• 현재 잔고: ${portfolio_status.get('current_balance', 0):.2f}
• 총 자산: ${portfolio_status.get('total_value', 0):.2f}
• 총 수익률: {portfolio_status.get('total_return', 0):+.2f}%
• 포지션 유무: {'있음' if portfolio_status.get('has_position') else '없음'}"""
            
            # 기존 포지션 정보
            if portfolio_status.get('has_position'):
                pos = portfolio_status.get('current_position', {})
                portfolio_text += f"""
• 기존 포지션: {pos.get('symbol', 'N/A')} {pos.get('direction', 'N/A')} {pos.get('leverage', 1)}x
• 진입가: ${pos.get('entry_price', 0):.4f}
• 미실현 손익: ${portfolio_status.get('unrealized_pnl', 0):+.2f} ({portfolio_status.get('unrealized_pnl_percentage', 0):+.2f}%)"""
            
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": portfolio_text
                            }
                        }
                    ]
                }
            })
        
        # 시장 센티먼트
        market_sentiment = master_decision.get('market_sentiment', {})
        if market_sentiment:
            sentiment_text = f"""📈 **시장 센티먼트**
• 종합 센티먼트: {market_sentiment.get('combined_sentiment', 50):.1f} ({market_sentiment.get('sentiment_label', 'Neutral')})
• 공포탐욕지수: {market_sentiment.get('fear_greed_index', {}).get('value', 50)} ({market_sentiment.get('fear_greed_index', {}).get('value_classification', 'Neutral')})
• 변동성: {market_sentiment.get('volatility_data', {}).get('volatility', 0):.2f}% ({market_sentiment.get('volatility_data', {}).get('classification', 'Medium')})
• 추천: {market_sentiment.get('recommendation', 'Neutral')}"""
            
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": sentiment_text
                            }
                        }
                    ]
                }
            })
        
        # 개별 분석 요약 (있는 경우)
        if individual_analysis:
            individual_text = f"""🔍 **개별 분석 요약**
• 추천: {individual_analysis.get('recommendation', 'N/A')}
• 신뢰도: {individual_analysis.get('confidence', 0):.1%}
• 목표가: ${individual_analysis.get('target_price', 0):.4f}
• 손절가: ${individual_analysis.get('stop_loss', 0):.4f}
• 주요 근거: {', '.join(individual_analysis.get('reasons', [])[:3])}"""
            
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": individual_text
                            }
                        }
                    ]
                }
            })
        
        # 시그널 정보 (있는 경우)
        triggered_signals = master_decision.get('triggered_signals', {})
        if triggered_signals and triggered_signals.get('signals'):
            signals = triggered_signals.get('signals', [])
            signal_text = f"""🚨 **감지된 시그널 ({len(signals)}개)**\n"""
            
            for i, signal in enumerate(signals[:3], 1):  # 최대 3개만
                signal_text += f"{i}. {signal.get('type', 'UNKNOWN')} ({signal.get('strength', 'N/A')}) - {signal.get('description', 'N/A')}\n"
            
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": signal_text
                            }
                        }
                    ]
                }
            })
        
        # 실행 결과
        execution_result = master_decision.get('execution_result', {})
        if execution_result:
            action = execution_result.get('action', 'UNKNOWN')
            success = execution_result.get('success', False)
            
            execution_text = f"⚙️ **실행 결과**\n• 액션: {action}\n• 성공: {'✅' if success else '❌'}"
            
            if execution_result.get('exit_info'):
                exit_info = execution_result['exit_info']
                execution_text += f"\n• 실현 손익: ${exit_info.get('realized_pnl', 0):+.2f} ({exit_info.get('realized_pnl_percentage', 0):+.2f}%)"
                execution_text += f"\n• 보유 기간: {exit_info.get('holding_duration', 'N/A')}"
            
            if execution_result.get('position_info'):
                pos_info = execution_result['position_info']
                execution_text += f"\n• 진입 포지션: {pos_info.get('direction', 'N/A')} {pos_info.get('leverage', 1)}x"
            
            if execution_result.get('error'):
                execution_text += f"\n• 오류: {execution_result['error']}"
            
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": execution_text
                            }
                        }
                    ]
                }
            })
        
        # 면책 조항
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "⚠️ 본 매매 결정은 AI에 의해 생성된 가상 트레이딩 결과로 실제 투자 조언이 아닙니다."
                        },
                        "annotations": {
                            "color": "gray"
                        }
                    }
                ]
            }
        })
        
        return children

    def get_pending_verifications(self, minutes_ago: int = 15) -> List[Dict]:
        """검증 대기 중인 분석들 조회 (N분 전)"""
        if not self.available:
            return []
        
        try:
            from datetime import timedelta
            # N분 전 시간 계산
            target_time = datetime.now() - timedelta(minutes=minutes_ago)
            # 범위를 넓게 잡기 (±3분)
            start_time = target_time - timedelta(minutes=3)
            end_time = target_time + timedelta(minutes=3)
            
            # 데이터베이스 쿼리 - 결과가 없고 목표가/스탑로스가 있는 분석들
            response = self.client.databases.query(
                database_id=self.analysis_database_id,
                filter={
                    "and": [
                        {
                            "property": "결과",
                            "select": {
                                "is_empty": True
                            }
                        },
                        {
                            "property": "분석시간",
                            "date": {
                                "after": start_time.isoformat()
                            }
                        },
                        {
                            "property": "분석시간",
                            "date": {
                                "before": end_time.isoformat()
                            }
                        },
                        {
                            "property": "목표가",
                            "number": {
                                "is_not_empty": True
                            }
                        },
                        {
                            "property": "스탑로스",
                            "number": {
                                "is_not_empty": True
                            }
                        }
                    ]
                }
            )
            
            pending_analyses = []
            for page in response["results"]:
                try:
                    properties = page["properties"]
                    
                    # 필요한 정보 추출
                    analysis_info = {
                        "page_id": page["id"],
                        "recommendation": self._extract_select(properties.get("판단")),
                        "original_price": self._extract_number(properties.get("현재가")),
                        "target_price": self._extract_number(properties.get("목표가")),
                        "stop_loss": self._extract_number(properties.get("스탑로스")),
                        "analysis_time": self._extract_date(properties.get("분석시간")),
                        "symbol": self._extract_rich_text(properties.get("분석심볼")) or "SOL/USDT"
                    }
                    
                    # 필수 정보가 있는 경우만 추가
                    if (analysis_info["recommendation"] and 
                        analysis_info["original_price"] and 
                        analysis_info["target_price"] and 
                        analysis_info["stop_loss"]):
                        pending_analyses.append(analysis_info)
                        
                except Exception as e:
                    logger.warning(f"개별 페이지 파싱 실패: {e}")
                    continue
            
            logger.info(f"검증 대기 분석 {len(pending_analyses)}개 발견")
            return pending_analyses
            
        except Exception as e:
            logger.error(f"검증 대기 목록 조회 실패: {e}")
            return []
    
    def update_verification_result(self, page_id: str, result: str, current_price: float, 
                                 original_price: float) -> bool:
        """분석 결과 검증 업데이트"""
        if not self.available:
            logger.error("노션 연동이 불가능합니다")
            return False
        
        try:
            # 업데이트할 속성들
            update_properties = {
                "결과": {
                    "select": {
                        "name": result
                    }
                }
            }
            
            # 페이지 속성 업데이트
            self.client.pages.update(
                page_id=page_id,
                properties=update_properties
            )
            
            logger.info(f"검증 결과 업데이트 완료: {page_id} - {result}")
            return True
            
        except Exception as e:
            logger.error(f"검증 결과 업데이트 실패: {e}")
            return False
    
    def _extract_select(self, prop) -> Optional[str]:
        """셀렉트 속성에서 값 추출"""
        if prop and prop.get("select"):
            return prop["select"].get("name")
        return None
    
    def _extract_number(self, prop) -> Optional[float]:
        """숫자 속성에서 값 추출"""
        if prop and prop.get("number") is not None:
            return float(prop["number"])
        return None
    
    def _extract_date(self, prop) -> Optional[str]:
        """날짜 속성에서 값 추출"""
        if prop and prop.get("date") and prop["date"].get("start"):
            return prop["date"]["start"]
        return None
    
    def _extract_rich_text(self, prop) -> Optional[str]:
        """리치 텍스트 속성에서 값 추출"""
        if prop and prop.get("rich_text"):
            text_parts = []
            for text_obj in prop["rich_text"]:
                if text_obj.get("text") and text_obj["text"].get("content"):
                    text_parts.append(text_obj["text"]["content"])
            return "".join(text_parts) if text_parts else None
        return None


class NotionIntegration:
    """노션 통합 클래스"""
    
    def __init__(self):
        self.config_manager = NotionConfigManager()
        self.logger = NotionLogger()
        logger.info("노션 통합 시스템 초기화 완료")
    
    def is_available(self) -> bool:
        """노션 연동 사용 가능 여부"""
        return self.config_manager.is_available() or self.logger.is_available()
    
    def get_config_manager(self) -> NotionConfigManager:
        """설정 관리자 반환"""
        return self.config_manager
    
    def get_logger(self) -> NotionLogger:
        """로거 반환"""
        return self.logger


# 전역 인스턴스들
notion_config = NotionConfigManager()
notion_logger = NotionLogger()
notion_integration = NotionIntegration()

if __name__ == "__main__":
    # 테스트 실행
    logger.info("노션 통합 시스템 테스트 시작")
    
    if notion_config.is_available():
        success = notion_config.load_all_agents()
        logger.info(f"에이전트 로드 결과: {success}")
        logger.info(f"로드된 에이전트들: {notion_config.get_agent_names()}")
        logger.info(f"분석 대상 심볼들: {notion_config.get_all_symbols()}")
    else:
        logger.error("노션 설정 관리자를 사용할 수 없습니다")
    
    if notion_logger.is_available():
        logger.info("노션 로거 사용 가능")
    else:
        logger.error("노션 로거를 사용할 수 없습니다")
    
    logger.info("노션 통합 시스템 테스트 완료")
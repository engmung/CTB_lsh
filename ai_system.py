import json
from datetime import datetime
from typing import Dict, Optional, List
from config import GEMINI_API_KEY, logger, get_symbol_display_name
from database import db
from market_analyzer import market_analyzer

try:
    from google import genai
    from google.genai import types
    GOOGLE_GENAI_AVAILABLE = True
except ImportError:
    GOOGLE_GENAI_AVAILABLE = False
    logger.warning("google-genai 패키지를 찾을 수 없습니다. AI 분석 기능이 비활성화됩니다.")

# 간소화된 시스템 프롬프트
BASIC_SYSTEM_PROMPT = """당신은 최고의 코인 트레이더입니다. 
제공된 멀티 타임프레임 시장 데이터를 분석하여 전문적인 매매 판단을 제공합니다.

분석 지침:
1. 멀티 타임프레임 데이터의 신호 합의도를 중요하게 고려하세요
2. 다이버전스가 있다면 신중하게 판단하세요
3. 제공된 전략에 따라 분석 관점을 조정하세요
4. 기술적 지표의 시계열 변화를 중점적으로 분석하세요

정확한 JSON 형식으로 응답해주세요."""


class AIAnalyzer:
    """AI 분석 시스템 (기존 ai_analyzer.py를 기반으로 정리)"""
    
    def __init__(self):
        self.available = self._check_availability()
        if self.available:
            logger.info("Gemini AI 초기화 완료")
        else:
            logger.warning("Gemini AI를 사용할 수 없습니다")
    
    def _check_availability(self) -> bool:
        """AI 분석 기능 사용 가능 여부 확인"""
        return GOOGLE_GENAI_AVAILABLE and bool(GEMINI_API_KEY)
    
    def is_available(self) -> bool:
        """AI 분석 기능 사용 가능 여부"""
        return self.available
    
    def analyze_with_agent(self, agent_name: str, analysis_periods: int = 50) -> Optional[Dict]:
        """특정 에이전트로 시장 분석 수행"""
        if not self.available:
            logger.error("AI 분석 기능을 사용할 수 없습니다")
            return None
        
        # 노션에서 에이전트 정보 조회 (다른 모듈에서 가져와야 함)
        from notion_integration import notion_config
        
        agent_info = notion_config.get_agent(agent_name)
        if not agent_info:
            logger.error(f"에이전트를 찾을 수 없습니다: {agent_name}")
            return None
        
        try:
            symbol = agent_info['symbol']
            timeframes = agent_info['timeframes']
            
            logger.info(f"🤖 === {agent_name} 에이전트 분석 시작 ===")
            logger.info(f"📊 분석 대상: {symbol} ({get_symbol_display_name(symbol)})")
            logger.info(f"⏱️ 시간봉: {timeframes}")
            logger.info(f"📈 분석 기간: {analysis_periods}개 캔들")
            
            # 🔥 분석 전 데이터 상태 확인 및 로깅
            logger.info(f"📋 === 분석 전 데이터 상태 점검 ===")
            data_ready = True
            for tf in timeframes:
                candles_df = db.get_candles(symbol, tf, limit=5)
                if candles_df.empty:
                    logger.error(f"❌ {tf}: 데이터 없음")
                    data_ready = False
                else:
                    latest_time = candles_df['timestamp'].iloc[-1]
                    latest_time_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(latest_time, 'strftime') else str(latest_time)
                    time_diff = datetime.now() - latest_time.to_pydatetime()
                    minutes_old = time_diff.total_seconds() / 60
                    
                    if minutes_old > 60:  # 1시간 이상 오래된 데이터
                        logger.warning(f"⚠️ {tf}: 최신 데이터 {latest_time_str} ({minutes_old:.0f}분 전) - 오래됨")
                    else:
                        logger.info(f"✅ {tf}: 최신 데이터 {latest_time_str} ({minutes_old:.0f}분 전)")
            
            if not data_ready:
                logger.error(f"❌ {agent_name}: 필요한 데이터가 부족하여 분석 중단")
                return None
            
            # 멀티 타임프레임 데이터 수집
            logger.info(f"🔄 멀티 타임프레임 데이터 수집 시작...")
            multi_data = market_analyzer.get_multi_timeframe_data(symbol, timeframes, analysis_periods)
            
            if not multi_data:
                logger.error("분석할 시장 데이터가 없습니다")
                return None
            
            # 간소화된 AI 분석용 프롬프트 생성
            logger.info(f"📝 AI 분석용 프롬프트 생성...")
            prompt_text = market_analyzer.create_ai_prompt(multi_data, agent_info['strategy'])
            
            # AI 분석 수행
            logger.info(f"🧠 AI 분석 실행...")
            analysis_result = self._call_gemini_api_structured(prompt_text)
            
            if analysis_result.get("error"):
                logger.error(f"AI 분석 실패: {analysis_result['error']}")
                return None
            
            # 분석 결과에 메타데이터 추가
            analysis_result['symbol'] = symbol
            analysis_result['agent_name'] = agent_name
            analysis_result['agent_page_id'] = agent_info['page_id']
            analysis_result['timeframes_used'] = timeframes
            analysis_result['analysis_periods'] = analysis_periods
            analysis_result['timestamp'] = datetime.now().isoformat()
            
            # 분석 결과 저장
            db.insert_ai_analysis(analysis_result)
            logger.info(f"🎯 === {agent_name} AI 분석 완료: {analysis_result['recommendation']} (신뢰도: {analysis_result['confidence']:.1%}) ===")
            
            return analysis_result
                
        except Exception as e:
            logger.error(f"AI 분석 중 오류: {e}")
            return None
    
    def _call_gemini_api_structured(self, prompt_text: str) -> Dict:
        """Gemini AI API 호출"""
        if not self._check_availability():
            return {"error": "AI 분석 기능을 사용할 수 없습니다. API 키를 확인하세요."}
        
        try:
            # 클라이언트 초기화
            client = genai.Client(api_key=GEMINI_API_KEY)
            
            # 사용자 입력 구성
            user_input = f"{BASIC_SYSTEM_PROMPT}\n\n{prompt_text}"
            
            contents = [
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_text(text=user_input),
                    ],
                ),
            ]
            
            # Structured Output 설정
            generate_content_config = types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=genai.types.Schema(
                    type=genai.types.Type.OBJECT,
                    properties={
                        "recommendation": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="BUY, SELL, 또는 HOLD 중 하나"
                        ),
                        "confidence": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="0.0에서 1.0 사이의 신뢰도"
                        ),
                        "analysis": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="상세한 분석 내용"
                        ),
                        "reasons": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(type=genai.types.Type.STRING),
                            description="분석 근거 리스트"
                        ),
                        "target_price": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="목표 가격"
                        ),
                        "stop_loss": genai.types.Schema(
                            type=genai.types.Type.NUMBER,
                            description="스탑로스 가격"
                        ),
                        "risk_level": genai.types.Schema(
                            type=genai.types.Type.STRING,
                            description="LOW, MEDIUM, 또는 HIGH"
                        )
                    },
                    required=["recommendation", "confidence", "analysis", "reasons"]
                )
            )
            
            logger.info("Gemini API 호출 시작...")
            
            # API 호출
            response = client.models.generate_content(
                model="gemini-2.5-flash-preview-05-20",
                contents=contents,
                config=generate_content_config
            )
            
            if not response.candidates or not response.candidates[0].content:
                logger.error("Gemini API 응답에 내용이 없습니다.")
                return {"error": "AI 분석 응답을 받을 수 없습니다."}
            
            response_text = response.candidates[0].content.parts[0].text
            
            if not response_text or response_text.strip() == "":
                logger.error("응답 텍스트가 비어있습니다.")
                return {"error": "AI가 빈 응답을 반환했습니다."}
            
            # JSON 파싱
            try:
                analysis_result = json.loads(response_text.strip())
                logger.info("AI 분석 완료")
                
                # 필수 필드 검증 및 기본값 설정
                if "recommendation" not in analysis_result:
                    analysis_result["recommendation"] = "HOLD"
                if "confidence" not in analysis_result:
                    analysis_result["confidence"] = 0.5
                if "analysis" not in analysis_result:
                    analysis_result["analysis"] = "분석 내용이 제공되지 않았습니다."
                if "reasons" not in analysis_result:
                    analysis_result["reasons"] = ["분석 근거가 제공되지 않았습니다."]
                
                return analysis_result
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON 파싱 오류: {e}")
                return {
                    "error": "JSON 파싱 실패",
                    "recommendation": "HOLD",
                    "confidence": 0.3,
                    "analysis": f"API 응답 파싱에 실패했습니다. 원본 응답: {response_text[:500]}",
                    "reasons": ["API 응답 파싱 오류"]
                }
            
        except Exception as e:
            logger.error(f"Gemini API 호출 중 오류: {e}")
            return {
                "error": f"AI 분석 중 오류가 발생했습니다: {str(e)}",
                "recommendation": "HOLD",
                "confidence": 0.3,
                "analysis": f"API 호출 중 오류가 발생했습니다: {str(e)}",
                "reasons": ["API 호출 오류"]
            }
    
    def get_analysis_history(self, limit: int = 10) -> List[Dict]:
        """AI 분석 히스토리 조회"""
        return db.get_ai_analysis_history(limit)


class AISystem:
    """AI 시스템 메인 클래스"""
    
    def __init__(self):
        self.analyzer = AIAnalyzer()
        logger.info("AI 시스템 초기화 완료")
    
    def is_available(self) -> bool:
        """AI 시스템 사용 가능 여부"""
        return self.analyzer.is_available()
    
    def analyze_with_agent(self, agent_name: str, analysis_periods: int = 50) -> Optional[Dict]:
        """에이전트 분석 수행"""
        return self.analyzer.analyze_with_agent(agent_name, analysis_periods)
    
    def get_analysis_history(self, limit: int = 10) -> List[Dict]:
        """분석 히스토리 조회"""
        return self.analyzer.get_analysis_history(limit)
    
    def get_available_agents(self) -> List[str]:
        """사용 가능한 에이전트 목록"""
        try:
            from notion_integration import notion_config
            return notion_config.get_agent_names()
        except:
            return []


# 전역 AI 시스템 인스턴스
ai_system = AISystem()

if __name__ == "__main__":
    # 테스트 실행
    logger.info("AI 시스템 테스트 시작")
    
    if ai_system.is_available():
        logger.info("AI 분석 기능 사용 가능")
    else:
        logger.error("AI 분석 기능을 사용할 수 없습니다")
    
    logger.info("AI 시스템 테스트 완료")
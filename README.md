# MTL Port Congestion Monitor

글로벌 50개 항만 실시간 혼잡도 모니터링 시스템

## 아키텍처

```
aisstream.io WebSocket (무료)
        ↓
  collector.py  ← Railway 상시 실행
        ↓
  Supabase PostgreSQL (무료 500MB)
        ↓
  frontend/index.html ← Vercel or 회사 홈페이지 직접 삽입
```

---

## 1단계: aisstream.io API 키 재발급

1. https://aisstream.io 로그인
2. Dashboard → API Keys → 기존 키 삭제 후 새 키 발급
3. 새 키를 메모해두기 (절대 코드에 직접 넣지 말 것)

---

## 2단계: Supabase 설정

1. https://supabase.com 에서 새 프로젝트 생성
2. SQL Editor → `schema.sql` 전체 내용 붙여넣기 → Run
3. Settings → API에서 두 값 메모:
   - **Project URL**: `https://xxxx.supabase.co`
   - **service_role** 키 (비공개, 서버에서만 사용)
   - **anon public** 키 (프론트엔드에서 사용, 공개 OK)

---

## 3단계: Railway 배포

1. https://railway.app → New Project → Deploy from GitHub
   - 이 폴더를 GitHub에 push 후 연결 (collector.py, requirements.txt, railway.toml)
2. Variables 탭에서 환경변수 설정:
   ```
   AISSTREAM_API_KEY  = (1단계에서 발급한 새 키)
   SUPABASE_URL       = (2단계 Project URL)
   SUPABASE_SERVICE_KEY = (2단계 service_role 키)
   ```
3. Deploy → 로그에서 "Connected to aisstream.io" 확인

---

## 4단계: 프론트엔드 배포

### 옵션 A: 회사 홈페이지에 직접 삽입
`frontend/index.html` 상단 CONFIG 수정 후 HTML 파일을 홈페이지에 추가:
```javascript
const CONFIG = {
  SUPABASE_URL:      "https://xxxx.supabase.co",   // 실제 URL
  SUPABASE_ANON_KEY: "eyJhbGci...",               // anon public 키
};
```

### 옵션 B: Vercel 독립 페이지
```bash
cd frontend
npx vercel deploy
```

### 옵션 C: iframe으로 기존 홈페이지에 삽입
```html
<iframe 
  src="https://your-vercel-url.vercel.app" 
  width="100%" 
  height="800px" 
  frameborder="0">
</iframe>
```

---

## 데이터 흐름

1. Railway의 `collector.py`가 **24시간 상시** aisstream.io WebSocket 구독
2. 50개 항만 bounding box 내 AIS 신호를 실시간 수신
3. 선박이 묘박(At Anchor, nav_status=1)이면 메모리에 추적 시작
4. **매 1시간마다** Supabase에 집계 데이터 upsert
   - `port_current` 테이블: 항만별 최신값 1행 (프론트엔드가 조회)
   - `port_history` 테이블: 시계열 누적 (추후 트렌드 분석용)
5. 프론트엔드는 Supabase REST API로 `port_current` 조회 → 지도 렌더링

---

## 한계 및 주의사항

- **TPFS 지수는 근사치**: 실제 묘박 구역 폴리곤 없이 bounding box 사용
- **선박 유형 미구분**: 컨테이너선/벌크선 구분 없이 nav_status 기반
- **첫 데이터**: 수집 시작 후 첫 저장까지 5분 소요, 안정적 데이터는 6~24시간 후
- **Railway 무료 플랜**: 월 $5 Hobby 플랜 권장 (무료는 500시간/월 제한)

---

## 비용

| 항목 | 서비스 | 요금 |
|------|--------|------|
| AIS 데이터 | aisstream.io | 무료 |
| 서버 (24/7) | Railway Hobby | $5/월 |
| 데이터베이스 | Supabase | 무료 (500MB) |
| 프론트엔드 | Vercel | 무료 |
| **합계** | | **$5/월** |

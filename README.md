# MTL Vessel Activity Index (MVAI)

글로벌 컨테이너 항만 73곳의 AIS 신호 기반 선박 활동도 모니터링 시스템.

## 이게 무엇인지 (그리고 무엇이 아닌지)

**MVAI는** 무료 AIS 데이터로 항만별 묘박/접안 선박 수를 집계해 산출하는 자체 활동도 지수입니다.

**MVAI는 다음이 아닙니다:**
- 항만 혼잡도(Port Congestion Index) — 실제 대기시간을 측정하지 않음
- 운임 의사결정 도구 — 산식이 학술적으로 검증되지 않음
- TradLinx PCI / MarineTraffic Port Calls API 등 전문 서비스의 대체재

전문 용도에는 위 서비스들을 사용하시고, MVAI는 내부 참고용으로만 활용하세요.

## 산식
MVAI = min(100, anchored × 6 + berthed × 2)
등급:
VERY_HIGH (75+)   매우 활발
HIGH      (50~75) 활발
MODERATE  (25~50) 보통
LOW       (0~25)  한산

## 아키텍처
aisstream.io WebSocket (무료 AIS)
↓
collector.py  ← GitHub Actions 2시간 cron, 15분 수집
↓
Supabase PostgreSQL
↓
index.html ← 정적 대시보드

## 비용

| 항목 | 서비스 | 요금 |
|---|---|---|
| AIS 데이터 | aisstream.io | 무료 |
| 실행 환경 | GitHub Actions | 무료 (public repo) |
| 데이터베이스 | Supabase | 무료 (500MB) |
| 프론트엔드 | Vercel 또는 정적 호스팅 | 무료 |
| **합계** | | **$0/월** |

## 모니터링 대상 (73개 컨테이너 항만)

권역별 항만 수:
- 한국·일본: 6 (Busan, Incheon, Tokyo 등)
- 중국·홍콩·대만: 10 (Shanghai, Ningbo, HK, Kaohsiung 등)
- 동남아시아: 9 (Singapore, Port Klang 등)
- 남아시아·중동: 9 (Jebel Ali, Nhava Sheva 등)
- 유럽: 10 (Rotterdam, Antwerp, Hamburg 등)
- 북미: 8 (LA, Long Beach, NY/NJ, Houston 등)
- 중남미: 8 (Santos, Manzanillo 등)
- 러시아·CIS: 4 (Saint Petersburg, Vladivostok 등)
- 아프리카·지중해: 9 (Tanger Med, Djibouti 등)

대상 선정 기준: 2024년 컨테이너 처리량(TEU) 및 권역별 대표성.

## 한계

1. **대기시간 미측정** — 묘박 선박 수만 카운트하며, 실제 접안 대기 시간은 별도 측정 안 함
2. **Bounding box 근사** — 실제 묘박지 polygon이 아닌 사각형 영역으로 항만 정의
3. **항만 규모 정규화 없음** — 대형 항만(Shanghai 등)은 절대값 크므로 항상 높게 나타남
4. **무료 AIS 한계** — 위성 AIS 미사용으로 외해 묘박지(연안 30km+)는 누락 가능
5. **ShipType 필터 한계** — Static Message에 ShipType이 누락된 선박은 일부 포함됨
6. **수집 주기** — 2시간 cron, 시각 고정 → 항만별 시간대 편향 존재

## 배포

[기존 배포 가이드 유지 — Supabase 설정, GitHub Secrets, Actions 활성화]

## 인정 (Acknowledgments)

본 시스템의 시각화 컨셉은 TradLinx Port Congestion Index 리포트
(https://www.tradlinx.com)를 참고하여 개발되었습니다. MVAI 산식은 자체 정의된 
간이 지표이며 TradLinx PCI와 동등한 정밀도를 가지지 않습니다.

## 라이선스

[프로젝트 라이선스]

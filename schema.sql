-- ================================================================
-- MTL Port Congestion Monitor — Supabase Schema
-- Supabase SQL Editor에서 실행하세요
-- ================================================================

-- 1. 항만 최신 현황 테이블 (항상 최신값 1행/항만)
CREATE TABLE IF NOT EXISTS port_current (
    port_code        VARCHAR(10)  PRIMARY KEY,
    updated_at       TIMESTAMPTZ  NOT NULL,
    vessels_anchored INTEGER      DEFAULT 0,
    vessels_berthed  INTEGER      DEFAULT 0,
    avg_wait_hours   FLOAT        DEFAULT 0,
    max_wait_hours   FLOAT        DEFAULT 0,
    tpfs             FLOAT        DEFAULT 0,
    level            VARCHAR(20)  DEFAULT 'LOW'
);

-- 2. 시계열 이력 테이블 (매시간 누적)
CREATE TABLE IF NOT EXISTS port_history (
    id               BIGSERIAL    PRIMARY KEY,
    port_code        VARCHAR(10)  NOT NULL,
    snapshot_at      TIMESTAMPTZ  NOT NULL,
    vessels_anchored INTEGER      DEFAULT 0,
    vessels_berthed  INTEGER      DEFAULT 0,
    avg_wait_hours   FLOAT        DEFAULT 0,
    max_wait_hours   FLOAT        DEFAULT 0,
    tpfs             FLOAT        DEFAULT 0,
    level            VARCHAR(20)  DEFAULT 'LOW'
);

CREATE INDEX IF NOT EXISTS idx_history_port_time
    ON port_history (port_code, snapshot_at DESC);

-- 3. RLS 비활성화 (서비스 키로만 쓰기, anon은 읽기만)
ALTER TABLE port_current ENABLE ROW LEVEL SECURITY;
ALTER TABLE port_history  ENABLE ROW LEVEL SECURITY;

-- anon 사용자 읽기 허용 (프론트엔드 공개 API용)
CREATE POLICY "anon_read_current" ON port_current
    FOR SELECT TO anon USING (true);

CREATE POLICY "anon_read_history" ON port_history
    FOR SELECT TO anon USING (true);

-- service_role만 쓰기 허용
CREATE POLICY "service_write_current" ON port_current
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "service_write_history" ON port_history
    FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 4. 초기 데이터 (50개 항만 기본값, 첫 실행 전 UI에서 항만 표시용)
INSERT INTO port_current (port_code, updated_at, level, tpfs)
VALUES
  ('KRPUS','2026-01-01 00:00:00+00','LOW',0),('KRICN','2026-01-01 00:00:00+00','LOW',0),
  ('JPNGO','2026-01-01 00:00:00+00','LOW',0),('JPYOK','2026-01-01 00:00:00+00','LOW',0),
  ('JPTYO','2026-01-01 00:00:00+00','LOW',0),('JPUKB','2026-01-01 00:00:00+00','LOW',0),
  ('CNSHA','2026-01-01 00:00:00+00','LOW',0),('CNQIN','2026-01-01 00:00:00+00','LOW',0),
  ('CNNGB','2026-01-01 00:00:00+00','LOW',0),('CNTXG','2026-01-01 00:00:00+00','LOW',0),
  ('CNYTN','2026-01-01 00:00:00+00','LOW',0),('CNNSA','2026-01-01 00:00:00+00','LOW',0),
  ('CNDLC','2026-01-01 00:00:00+00','LOW',0),('VNTOT','2026-01-01 00:00:00+00','LOW',0),
  ('VNHPH','2026-01-01 00:00:00+00','LOW',0),('THLCH','2026-01-01 00:00:00+00','LOW',0),
  ('SGSIN','2026-01-01 00:00:00+00','LOW',0),('MYLPK','2026-01-01 00:00:00+00','LOW',0),
  ('IDJKT','2026-01-01 00:00:00+00','LOW',0),('IDSUB','2026-01-01 00:00:00+00','LOW',0),
  ('PHMNL','2026-01-01 00:00:00+00','LOW',0),('LKCMB','2026-01-01 00:00:00+00','LOW',0),
  ('AEJEA','2026-01-01 00:00:00+00','LOW',0),('INBOM','2026-01-01 00:00:00+00','LOW',0),
  ('JOAQJ','2026-01-01 00:00:00+00','LOW',0),('ILASH','2026-01-01 00:00:00+00','LOW',0),
  ('NLRTM','2026-01-01 00:00:00+00','LOW',0),('DEHAM','2026-01-01 00:00:00+00','LOW',0),
  ('BEANR','2026-01-01 00:00:00+00','LOW',0),('GBFXT','2026-01-01 00:00:00+00','LOW',0),
  ('FRLEH','2026-01-01 00:00:00+00','LOW',0),('GRPIR','2026-01-01 00:00:00+00','LOW',0),
  ('ESVLC','2026-01-01 00:00:00+00','LOW',0),('ITGOA','2026-01-01 00:00:00+00','LOW',0),
  ('SIKOP','2026-01-01 00:00:00+00','LOW',0),('ESALG','2026-01-01 00:00:00+00','LOW',0),
  ('USLAX','2026-01-01 00:00:00+00','LOW',0),('USLGB','2026-01-01 00:00:00+00','LOW',0),
  ('USNYC','2026-01-01 00:00:00+00','LOW',0),('USSAV','2026-01-01 00:00:00+00','LOW',0),
  ('CAVAN','2026-01-01 00:00:00+00','LOW',0),('USMSY','2026-01-01 00:00:00+00','LOW',0),
  ('RUVVO','2026-01-01 00:00:00+00','LOW',0),('RUNVS','2026-01-01 00:00:00+00','LOW',0),
  ('KZAKT','2026-01-01 00:00:00+00','LOW',0),('MACAS','2026-01-01 00:00:00+00','LOW',0),
  ('KEMBA','2026-01-01 00:00:00+00','LOW',0),('ZADUR','2026-01-01 00:00:00+00','LOW',0),
  ('TZDAR','2026-01-01 00:00:00+00','LOW',0),('EGPSD','2026-01-01 00:00:00+00','LOW',0)
ON CONFLICT (port_code) DO NOTHING;

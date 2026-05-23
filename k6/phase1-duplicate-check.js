/**
 * Phase 1 테스트: 동일 사용자 중복 발급 방지
 *
 * 목표: 같은 userId로 100번 동시 요청 → 정확히 1번만 성공
 *
 * 실행:
 *   k6 run --out influxdb=http://localhost:8086/k6 k6/phase1-duplicate-check.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const successCount = new Counter('duplicate_issue_success');

export const options = {
  scenarios: {
    same_user: {
      executor: 'shared-iterations',
      vus: 100,
      iterations: 100,
      maxDuration: '10s',
    },
  },
};

const COUPON_ID = 1;
const USER_ID   = 9999;  // 고정된 동일 사용자
const BASE_URL  = 'http://localhost:8080';

export default function () {
  const res = http.post(
    `${BASE_URL}/api/coupons/${COUPON_ID}/issue`,
    JSON.stringify({ userId: USER_ID }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, {
    'status is 200 or 409': (r) => r.status === 200 || r.status === 409,
    'no 500 errors':        (r) => r.status !== 500,
  });

  if (res.status === 200) {
    successCount.add(1);
  }
}

export function handleSummary(data) {
  const success = data.metrics['duplicate_issue_success']?.values?.count ?? 0;
  console.log(`\n✅ 성공 횟수: ${success} (기대값: 1)`);
  if (success === 1) {
    console.log('✅ 중복 발급 방지 테스트 통과!');
  } else {
    console.log(`❌ 실패: ${success}번 발급됨 — 동시성 문제 있음`);
  }
  return {};
}

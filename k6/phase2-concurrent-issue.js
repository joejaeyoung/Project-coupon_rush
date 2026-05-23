/**
 * Phase 2 테스트: Redis 기반 1000명 동시 쿠폰 발급
 *
 * 목표: Phase 1과 동일한 조건에서 성능 비교
 *   - 쿠폰 100개, 1000명 동시 요청 → 정확히 100개만 성공
 *   - Phase 1 대비 avg, p(95), RPS 수치 비교
 *
 * 실행:
 *   ./run-k6.sh 1 1000 phase2-concurrent-issue
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const successCount = new Counter('coupon_issue_success');
const failCount    = new Counter('coupon_issue_fail');

const COUPON_ID = __ENV.COUPON_ID || 1;
const USERS     = parseInt(__ENV.USERS || 1000);
const BASE_URL  = 'http://localhost:8080';

export const options = {
  scenarios: {
    concurrent_users: {
      executor: 'shared-iterations',
      vus: USERS,
      iterations: USERS,
      maxDuration: '30s',
    },
  },
  thresholds: {
    // Phase 1: p(95)=2.32s → Phase 2 목표: p(95)<500ms
    http_req_duration: ['p(95)<500'],
    http_req_failed:   ['rate<0.95'],
  },
};

export default function () {
  const userId = __VU;

  const res = http.post(
    `${BASE_URL}/api/coupons/${COUPON_ID}/issue`,
    JSON.stringify({ userId }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, {
    'status 200 (발급 성공)': (r) => r.status === 200,
    'status 409 (소진/중복)': (r) => r.status === 409,
    'status is not 500':      (r) => r.status !== 500,
  });

  if (res.status === 200) successCount.add(1);
  else                    failCount.add(1);
}

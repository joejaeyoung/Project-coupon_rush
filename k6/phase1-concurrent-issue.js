/**
 * Phase 1 테스트: 1000명 동시 쿠폰 발급
 *
 * 목표: 쿠폰 100개, 1000명 동시 요청 → 정확히 100개만 성공
 *
 * 실행:
 *   k6 run --out influxdb=http://localhost:8086/k6 k6/phase1-concurrent-issue.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const successCount = new Counter('coupon_issue_success');
const failCount    = new Counter('coupon_issue_fail');

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
    http_req_duration: ['p(95)<2000'],  // 95%의 요청이 2초 이내
    http_req_failed:   ['rate<0.95'],   // 실패율 95% 미만 (100개 성공, 900개 409)
  },
};

const COUPON_ID   = __ENV.COUPON_ID   || 1;
const USERS       = parseInt(__ENV.USERS || 1000);
const BASE_URL    = 'http://localhost:8080';

export default function () {
  const userId = __VU;  // VU 번호를 userId로 사용 (1~1000)

  const res = http.post(
    `${BASE_URL}/api/coupons/${COUPON_ID}/issue`,
    JSON.stringify({ userId }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  const ok = check(res, {
    'status 200 (발급 성공)': (r) => r.status === 200,
    'status 409 (소진/중복)': (r) => r.status === 409,
    'status is not 500':      (r) => r.status !== 500,
  });

  if (res.status === 200) {
    successCount.add(1);
  } else {
    failCount.add(1);
  }
}

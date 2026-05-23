import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const successCount = new Counter('coupon_issue_success');

const COUPON_ID   = __ENV.COUPON_ID || 1;
const USERS       = parseInt(__ENV.USERS || 100);
const SAME_USER   = 9999;
const BASE_URL    = 'http://localhost:8080';

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
    http_req_failed: ['rate<0.99'],
  },
};

export default function () {
  const res = http.post(
    `${BASE_URL}/api/coupons/${COUPON_ID}/issue`,
    JSON.stringify({ userId: SAME_USER }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, {
    '정확히 1번만 성공': (r) => r.status === 200 || r.status === 409,
  });

  if (res.status === 200) successCount.add(1);
}

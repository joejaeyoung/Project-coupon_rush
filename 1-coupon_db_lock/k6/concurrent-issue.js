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
    http_req_duration: ['p(95)<3000'],
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

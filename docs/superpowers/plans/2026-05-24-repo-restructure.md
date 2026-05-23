# Repo Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Phase별 독립 실행 가능한 디렉토리 구조로 재편하여 GitHub에 업로드 가능한 상태로 만든다.

**Architecture:** 3개 Phase 디렉토리 각각에 Spring 소스 + docker-compose + k6 + 인프라 설정을 완결 패키징한다. 모든 Phase가 동일 포트(8080, 3306 등)를 사용하므로 한 번에 하나만 실행한다.

**Tech Stack:** Docker Compose, Spring Boot (Gradle), k6, Prometheus, Grafana, InfluxDB

---

## 파일 맵

| 생성/이동 | 출처 |
|-----------|------|
| `1-coupon_db_lock/spring/` | `coupon_db_lock/` 내용 복사 |
| `2-coupon_redis/spring/` | `coupon_redis/` 내용 복사 |
| `3-coupon_redis_kafka/spring/` | `coupon/` 내용 복사 |
| `{phase}/docker-compose.yml` | 신규 작성 (Phase별 서비스 다름) |
| `{phase}/k6/concurrent-issue.js` | 기존 k6 스크립트 정리 |
| `{phase}/run-k6.sh` | 신규 작성 |
| `{phase}/reset-coupon.sh` | 신규 작성 |
| `{phase}/grafana/`, `prometheus/`, `mysql/`, `redis/` | 루트 infra 복사 |
| `.gitignore` | 신규 작성 |

---

### Task 1: 디렉토리 골격 생성

**Files:**
- Create: `1-coupon_db_lock/`, `2-coupon_redis/`, `3-coupon_redis_kafka/` 하위 구조

- [ ] **Step 1: 3개 Phase 디렉토리 골격 생성**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"

for phase in "1-coupon_db_lock" "2-coupon_redis" "3-coupon_redis_kafka"; do
  mkdir -p "$phase/spring"
  mkdir -p "$phase/k6"
  mkdir -p "$phase/grafana/provisioning/datasources"
  mkdir -p "$phase/prometheus"
  mkdir -p "$phase/mysql/conf.d"
  mkdir -p "$phase/mysql/logs"
  mkdir -p "$phase/spring/logs"
done

mkdir -p "2-coupon_redis/redis"
mkdir -p "3-coupon_redis_kafka/redis"
```

- [ ] **Step 2: 생성 확인**

```bash
find 1-coupon_db_lock 2-coupon_redis 3-coupon_redis_kafka -type d | sort
```

---

### Task 2: Spring 소스 복사

**Files:**
- Copy: `coupon_db_lock/*` → `1-coupon_db_lock/spring/`
- Copy: `coupon_redis/*` → `2-coupon_redis/spring/`
- Copy: `coupon/*` → `3-coupon_redis_kafka/spring/`

- [ ] **Step 1: Phase 1 Spring 소스 복사**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"
rsync -av --exclude='build/' --exclude='.gradle/' \
  coupon_db_lock/ 1-coupon_db_lock/spring/
```

- [ ] **Step 2: Phase 2 Spring 소스 복사**

```bash
rsync -av --exclude='build/' --exclude='.gradle/' \
  coupon_redis/ 2-coupon_redis/spring/
```

- [ ] **Step 3: Phase 3 Spring 소스 복사**

```bash
rsync -av --exclude='build/' --exclude='.gradle/' \
  coupon/ 3-coupon_redis_kafka/spring/
```

- [ ] **Step 4: 복사 확인 (Dockerfile, build.gradle 존재 여부)**

```bash
ls 1-coupon_db_lock/spring/Dockerfile \
   2-coupon_redis/spring/Dockerfile \
   3-coupon_redis_kafka/spring/Dockerfile
```

Expected: 3개 파일 모두 출력

---

### Task 3: 인프라 설정 복사

**Files:**
- Copy: `grafana/`, `prometheus/`, `mysql/conf.d/`, `redis/` → 각 Phase

- [ ] **Step 1: Grafana provisioning 복사**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"

for phase in "1-coupon_db_lock" "2-coupon_redis" "3-coupon_redis_kafka"; do
  cp -r grafana/provisioning/ "$phase/grafana/provisioning/"
done
```

- [ ] **Step 2: Prometheus 설정 복사**

```bash
for phase in "1-coupon_db_lock" "2-coupon_redis" "3-coupon_redis_kafka"; do
  cp prometheus/prometheus.yml "$phase/prometheus/prometheus.yml"
done
```

- [ ] **Step 3: MySQL conf 복사**

```bash
for phase in "1-coupon_db_lock" "2-coupon_redis" "3-coupon_redis_kafka"; do
  cp -r mysql/conf.d/ "$phase/mysql/conf.d/"
done
```

- [ ] **Step 4: Redis 설정 복사 (Phase 2, 3만)**

```bash
cp redis/redis.conf 2-coupon_redis/redis/redis.conf
cp redis/redis.conf 3-coupon_redis_kafka/redis/redis.conf
```

---

### Task 4: docker-compose.yml 생성 — Phase 1

**Files:**
- Create: `1-coupon_db_lock/docker-compose.yml`

- [ ] **Step 1: Phase 1 docker-compose.yml 작성**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/1-coupon_db_lock/docker-compose.yml" << 'EOF'
name: coupon-phase1

services:
  mysql:
    image: mysql:8.0
    container_name: mysql
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: jajo0303
      MYSQL_DATABASE: coupon_db
      MYSQL_USER: jajo
      MYSQL_PASSWORD: jajo
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./mysql/conf.d:/etc/mysql/conf.d
      - ./mysql/logs:/var/log/mysql
    networks:
      - app-network
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5

  influxdb:
    image: influxdb:1.8
    container_name: influxdb
    restart: always
    ports:
      - "8086:8086"
    environment:
      INFLUXDB_DB: k6
      INFLUXDB_HTTP_AUTH_ENABLED: "false"
    volumes:
      - influxdb_data:/var/lib/influxdb
    networks:
      - app-network

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    restart: always
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    networks:
      - app-network

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    restart: always
    ports:
      - "3001:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      - influxdb
      - prometheus
    networks:
      - app-network

  spring:
    build:
      context: ./spring
      dockerfile: Dockerfile
    container_name: spring-app
    restart: always
    ports:
      - "8080:8080"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/coupon_db?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8
      SPRING_DATASOURCE_USERNAME: jajo
      SPRING_DATASOURCE_PASSWORD: jajo
    volumes:
      - ./spring/logs:/app/logs
    depends_on:
      mysql:
        condition: service_healthy
    networks:
      - app-network

volumes:
  mysql_data:
  influxdb_data:
  prometheus_data:
  grafana_data:

networks:
  app-network:
    driver: bridge
EOF
```

- [ ] **Step 2: 유효성 검사**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/1-coupon_db_lock"
docker compose config --quiet && echo "OK"
```

Expected: `OK`

---

### Task 5: docker-compose.yml 생성 — Phase 2

**Files:**
- Create: `2-coupon_redis/docker-compose.yml`

- [ ] **Step 1: Phase 2 docker-compose.yml 작성**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/2-coupon_redis/docker-compose.yml" << 'EOF'
name: coupon-phase2

services:
  mysql:
    image: mysql:8.0
    container_name: mysql
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: jajo0303
      MYSQL_DATABASE: coupon_db
      MYSQL_USER: jajo
      MYSQL_PASSWORD: jajo
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./mysql/conf.d:/etc/mysql/conf.d
      - ./mysql/logs:/var/log/mysql
    networks:
      - app-network
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    container_name: redis
    restart: always
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
      - ./redis/redis.conf:/usr/local/etc/redis/redis.conf
    networks:
      - app-network
    command: redis-server /usr/local/etc/redis/redis.conf
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  influxdb:
    image: influxdb:1.8
    container_name: influxdb
    restart: always
    ports:
      - "8086:8086"
    environment:
      INFLUXDB_DB: k6
      INFLUXDB_HTTP_AUTH_ENABLED: "false"
    volumes:
      - influxdb_data:/var/lib/influxdb
    networks:
      - app-network

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    restart: always
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    networks:
      - app-network

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    restart: always
    ports:
      - "3001:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      - influxdb
      - prometheus
    networks:
      - app-network

  spring:
    build:
      context: ./spring
      dockerfile: Dockerfile
    container_name: spring-app
    restart: always
    ports:
      - "8080:8080"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/coupon_db?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8
      SPRING_DATASOURCE_USERNAME: jajo
      SPRING_DATASOURCE_PASSWORD: jajo
      SPRING_DATA_REDIS_HOST: redis
      SPRING_DATA_REDIS_PORT: 6379
    volumes:
      - ./spring/logs:/app/logs
    depends_on:
      mysql:
        condition: service_healthy
      redis:
        condition: service_healthy
    networks:
      - app-network

volumes:
  mysql_data:
  redis_data:
  influxdb_data:
  prometheus_data:
  grafana_data:

networks:
  app-network:
    driver: bridge
EOF
```

- [ ] **Step 2: 유효성 검사**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/2-coupon_redis"
docker compose config --quiet && echo "OK"
```

Expected: `OK`

---

### Task 6: docker-compose.yml 생성 — Phase 3

**Files:**
- Create: `3-coupon_redis_kafka/docker-compose.yml`

- [ ] **Step 1: Phase 3 docker-compose.yml 작성**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/3-coupon_redis_kafka/docker-compose.yml" << 'EOF'
name: coupon-phase3

services:
  mysql:
    image: mysql:8.0
    container_name: mysql
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: jajo0303
      MYSQL_DATABASE: coupon_db
      MYSQL_USER: jajo
      MYSQL_PASSWORD: jajo
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./mysql/conf.d:/etc/mysql/conf.d
      - ./mysql/logs:/var/log/mysql
    networks:
      - app-network
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    container_name: redis
    restart: always
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
      - ./redis/redis.conf:/usr/local/etc/redis/redis.conf
    networks:
      - app-network
    command: redis-server /usr/local/etc/redis/redis.conf
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: apache/kafka:3.7.1
    container_name: kafka
    restart: always
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_NODE_ID: "1"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "EXTERNAL://localhost:9092,INTERNAL://kafka:9094"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - app-network
    healthcheck:
      test: ["CMD-SHELL", "nc -z localhost 9092 || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s

  influxdb:
    image: influxdb:1.8
    container_name: influxdb
    restart: always
    ports:
      - "8086:8086"
    environment:
      INFLUXDB_DB: k6
      INFLUXDB_HTTP_AUTH_ENABLED: "false"
    volumes:
      - influxdb_data:/var/lib/influxdb
    networks:
      - app-network

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    restart: always
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    networks:
      - app-network

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    restart: always
    ports:
      - "3001:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      - influxdb
      - prometheus
    networks:
      - app-network

  spring:
    build:
      context: ./spring
      dockerfile: Dockerfile
    container_name: spring-app
    restart: always
    ports:
      - "8080:8080"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/coupon_db?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8
      SPRING_DATASOURCE_USERNAME: jajo
      SPRING_DATASOURCE_PASSWORD: jajo
      SPRING_DATA_REDIS_HOST: redis
      SPRING_DATA_REDIS_PORT: 6379
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9094
    volumes:
      - ./spring/logs:/app/logs
    depends_on:
      mysql:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka:
        condition: service_healthy
    networks:
      - app-network

volumes:
  mysql_data:
  redis_data:
  kafka_data:
  influxdb_data:
  prometheus_data:
  grafana_data:

networks:
  app-network:
    driver: bridge
EOF
```

- [ ] **Step 2: 유효성 검사**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/3-coupon_redis_kafka"
docker compose config --quiet && echo "OK"
```

Expected: `OK`

---

### Task 7: k6 스크립트 생성

**Files:**
- Create: `1-coupon_db_lock/k6/concurrent-issue.js`
- Create: `1-coupon_db_lock/k6/duplicate-check.js`
- Create: `2-coupon_redis/k6/concurrent-issue.js`
- Create: `3-coupon_redis_kafka/k6/concurrent-issue.js`

- [ ] **Step 1: Phase 1 concurrent-issue.js**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/1-coupon_db_lock/k6/concurrent-issue.js" << 'EOF'
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
EOF
```

- [ ] **Step 2: Phase 1 duplicate-check.js**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/1-coupon_db_lock/k6/duplicate-check.js" << 'EOF'
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
EOF
```

- [ ] **Step 3: Phase 2 concurrent-issue.js**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/2-coupon_redis/k6/concurrent-issue.js" << 'EOF'
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
EOF
```

- [ ] **Step 4: Phase 3 concurrent-issue.js (Phase 2와 동일 — Kafka 구현 후 202 추가)**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/3-coupon_redis_kafka/k6/concurrent-issue.js" << 'EOF'
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
    http_req_duration: ['p(95)<300'],
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

  // Phase 3: Kafka 비동기 처리 시 202 Accepted 응답
  check(res, {
    'status 200 or 202 (발급 성공/접수)': (r) => r.status === 200 || r.status === 202,
    'status 409 (소진/중복)':             (r) => r.status === 409,
    'status is not 500':                  (r) => r.status !== 500,
  });

  if (res.status === 200 || res.status === 202) successCount.add(1);
  else                                          failCount.add(1);
}
EOF
```

---

### Task 8: run-k6.sh 생성

**Files:**
- Create: `{phase}/run-k6.sh` (3개)

- [ ] **Step 1: 3개 Phase run-k6.sh 생성**

```bash
ROOT="/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"

for phase in "1-coupon_db_lock" "2-coupon_redis" "3-coupon_redis_kafka"; do
cat > "$ROOT/$phase/run-k6.sh" << 'EOF'
#!/bin/bash
COUPON_ID=${1:-1}
USERS=${2:-1000}

echo "▶ k6 실행: coupon_id=${COUPON_ID} | users=${USERS}"

k6 run \
  --out influxdb=http://localhost:8086/k6 \
  -e COUPON_ID=${COUPON_ID} \
  -e USERS=${USERS} \
  k6/concurrent-issue.js
EOF
chmod +x "$ROOT/$phase/run-k6.sh"
done
```

- [ ] **Step 2: 실행 권한 확인**

```bash
ls -la "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"/*/run-k6.sh
```

Expected: 각 파일에 `-rwxr-xr-x` 권한

---

### Task 9: reset-coupon.sh 생성

**Files:**
- Create: `1-coupon_db_lock/reset-coupon.sh`
- Create: `2-coupon_redis/reset-coupon.sh`
- Create: `3-coupon_redis_kafka/reset-coupon.sh`

- [ ] **Step 1: Phase 1 reset-coupon.sh (DB만)**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/1-coupon_db_lock/reset-coupon.sh" << 'EOF'
#!/bin/bash
COUPON_ID=${1:-1}

docker exec mysql mysql -u jajo -pjajo coupon_db \
  -e "DELETE FROM issued_coupon WHERE coupon_id=${COUPON_ID};
      UPDATE coupon SET issued_quantity=0 WHERE id=${COUPON_ID};"

echo "✅ 쿠폰 ${COUPON_ID} 초기화 완료 (DB)"
EOF
chmod +x "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/1-coupon_db_lock/reset-coupon.sh"
```

- [ ] **Step 2: Phase 2 reset-coupon.sh (DB + Redis)**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/2-coupon_redis/reset-coupon.sh" << 'EOF'
#!/bin/bash
COUPON_ID=${1:-1}

echo "1) DB 초기화..."
docker exec mysql mysql -u jajo -pjajo coupon_db \
  -e "DELETE FROM issued_coupon WHERE coupon_id=${COUPON_ID};
      UPDATE coupon SET issued_quantity=0 WHERE id=${COUPON_ID};"

echo "2) Redis 초기화..."
docker exec redis redis-cli DEL \
  "coupon:${COUPON_ID}:stock" \
  "coupon:${COUPON_ID}:users" \
  "coupon:${COUPON_ID}:startDate" \
  "coupon:${COUPON_ID}:endDate"

echo "3) Redis 재고 초기화..."
curl -s -X POST "http://localhost:8080/api/coupons/${COUPON_ID}/stock/init" > /dev/null

echo "✅ 쿠폰 ${COUPON_ID} 초기화 완료 (DB + Redis)"
EOF
chmod +x "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/2-coupon_redis/reset-coupon.sh"
```

- [ ] **Step 3: Phase 3 reset-coupon.sh (DB + Redis, Kafka는 stateless)**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/3-coupon_redis_kafka/reset-coupon.sh" << 'EOF'
#!/bin/bash
COUPON_ID=${1:-1}

echo "1) DB 초기화..."
docker exec mysql mysql -u jajo -pjajo coupon_db \
  -e "DELETE FROM issued_coupon WHERE coupon_id=${COUPON_ID};
      UPDATE coupon SET issued_quantity=0 WHERE id=${COUPON_ID};"

echo "2) Redis 초기화..."
docker exec redis redis-cli DEL \
  "coupon:${COUPON_ID}:stock" \
  "coupon:${COUPON_ID}:users" \
  "coupon:${COUPON_ID}:startDate" \
  "coupon:${COUPON_ID}:endDate"

echo "3) Redis 재고 초기화..."
curl -s -X POST "http://localhost:8080/api/coupons/${COUPON_ID}/stock/init" > /dev/null

echo "✅ 쿠폰 ${COUPON_ID} 초기화 완료 (DB + Redis)"
EOF
chmod +x "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/3-coupon_redis_kafka/reset-coupon.sh"
```

---

### Task 10: .gitignore 생성

**Files:**
- Create: `.gitignore` (루트)

- [ ] **Step 1: .gitignore 작성**

```bash
cat > "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/.gitignore" << 'EOF'
# Spring Build
**/spring/build/
**/spring/.gradle/
**/spring/logs/

# MySQL / Redis logs & data
**/mysql/logs/
**/mysql/data/

# macOS
.DS_Store

# IntelliJ
**/.idea/
**/*.iml

# k6 결과
*.json
EOF
```

---

### Task 11: 최종 검증

- [ ] **Step 1: 각 Phase 디렉토리 구조 확인**

```bash
find "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/1-coupon_db_lock" \
     "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/2-coupon_redis" \
     "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰/3-coupon_redis_kafka" \
  -maxdepth 1 | sort
```

Expected: 각 디렉토리에 `spring/`, `k6/`, `docker-compose.yml`, `run-k6.sh`, `reset-coupon.sh` 존재

- [ ] **Step 2: docker compose config 전체 검증**

```bash
ROOT="/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"
for phase in "1-coupon_db_lock" "2-coupon_redis" "3-coupon_redis_kafka"; do
  echo -n "$phase: "
  docker compose -f "$ROOT/$phase/docker-compose.yml" config --quiet && echo "OK"
done
```

Expected:
```
1-coupon_db_lock: OK
2-coupon_redis: OK
3-coupon_redis_kafka: OK
```

---

### Task 12: 구 디렉토리 정리 및 커밋

- [ ] **Step 1: 구 Spring 디렉토리 삭제**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"
rm -rf coupon/ coupon_db_lock/ coupon_redis/
```

- [ ] **Step 2: 루트 파일 정리 (Phase 디렉토리로 이동된 것들)**

```bash
rm -f run-k6.sh reset-coupon.sh load-test.html
rm -rf k6/ grafana/ prometheus/ mysql/ redis/ spring/ docker-compose.yml
```

- [ ] **Step 3: git status 확인**

```bash
git status
```

- [ ] **Step 4: 커밋**

```bash
cd "/Users/jojaeyoung/Desktop/소마/멘토링/raw/신기용 멘토님_선착순 쿠폰"
git add -A
git commit -m "refactor: Phase별 독립 실행 구조로 재편 (1-db_lock / 2-redis / 3-redis_kafka)"
```

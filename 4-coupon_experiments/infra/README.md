# 실험 인프라

프로젝트 이름은 `coupon-lab`이다. 기존 실습의 `container_name`이나 기존 서비스의 볼륨을 사용하지 않는다. 호스트 포트는 모두 `127.0.0.1`에만 공개한다. 아래 명령은 `4-coupon_experiments/`에서 실행한다.

```bash
python3 infra/lab.py up
python3 infra/lab.py status
python3 infra/lab.py versions
python3 infra/lab.py logs
python3 infra/lab.py down
```

`down`은 이 프로젝트의 컨테이너와 네트워크만 제거하고 볼륨은 보존한다. `results/infra/events.jsonl`에 모든 실행 결과를 기록하며, 장애·복구 시 컨테이너 로그도 저장한다. 성공과 실패 모두 JSON 한 개를 stdout에 출력하고 실패 시 종료 코드 1을 반환한다.

| 서비스 | 호스트 주소 | 구성 |
| --- | --- | --- |
| MySQL | `127.0.0.1:23306` | DB `coupon_lab`, 계정 `coupon/coupon`, root 비밀번호 `couponroot` |
| Redis primary | `127.0.0.1:26379` | AOF, `appendfsync everysec`, 복제본 1개 |
| Redis replica | `127.0.0.1:26380` | 기본 읽기 전용, AOF |
| Kafka 1/2/3 | `127.0.0.1:29092,29093,29094` | KRaft 3노드, RF=3, minISR=2, unclean election 비활성화 |
| Sentinel 1/2/3 | `127.0.0.1:26381,26382,26383` | 선택 프로필, quorum=2 |
| Redis Cluster 1/2/3 | `127.0.0.1:27379,27380,27381` | 선택 프로필, master 3개, replica 0개 |

Kafka 이미지는 기존 3단계 예제의 `3.7.1`과 달리 로컬에 캐시된 `apache/kafka:3.8.1`을 명시적으로 사용한다. 초기 성능·분포 실험은 브로커마다 JVM heap 256MiB, 컨테이너 한도 768MiB로 실행했다. 264개 토픽의 증거를 보존한 후속 검증에서 kafka1의 heap OOM을 관측하여, 최종 재현 구성은 heap 512MiB·컨테이너 한도 1GiB로 높이고 `-XX:+ExitOnOutOfMemoryError`를 추가했다. 초기 구성은 [compose-measured.yml](../results/compose-measured.yml), 원인과 순차 복구 기록은 [kafka-heap-recovery.json](../results/kafka-heap-recovery.json)에 보존한다. 최종 순서 회귀 검증과 초기 성능 수치는 이 메모리 구성 차이를 구분한다. MySQL은 `mysql:8.0`, Redis는 `redis:7-alpine`이며, 실제 패치 버전과 이미지 digest는 실행 결과에서 확인한다. 이 구성의 처리량은 로컬 Docker 자원을 공유하는 비교 실험 값이다.

## Kafka 장애

```bash
python3 infra/lab.py kafka-stop-leader --topic TOPIC --partition 0
python3 infra/lab.py kafka-start all
```

`kafka-stop-leader`는 실제 topic metadata에서 leader를 찾고 해당 `coupon-lab` 브로커에만 SIGKILL을 보낸다. 리더 번호, 종료 전 metadata, 로그 경로를 반환한다. 브로커 한 대의 종료는 리더 재선출을 검증한다. 이 구성은 broker/controller 역할을 겸하는 KRaft 3노드이므로 두 대 종료 시 controller 과반수와 ISR이 함께 부족해질 수 있다. 따라서 두 노드 종료 결과를 minISR만 독립적으로 검증한 결과로 해석하지 않는다. 프로듀서의 `acks=all` 설정은 애플리케이션에서 별도로 적용해야 한다.

## 복제 지연 중 Redis의 승인된 쓰기 손실

```bash
python3 infra/lab.py redis-freeze-replica
# 여기서 primary에 실제 SET/Lua 예약을 보내고 성공 응답을 기록한다.
python3 infra/lab.py redis-promote
# docker compose -p coupon-lab -f compose.yml exec -T redis-replica redis-cli GET KEY
# 승격된 replica에서 방금 승인된 값/예약의 존재 여부를 확인한다.
python3 infra/lab.py redis-restore
```

`redis-freeze-replica`는 이름과 달리 단순 process pause를 사용하지 않는다. 복제본을 **이 실험의 Docker 네트워크에서 분리**하고, primary의 기존 복제 연결을 종료한 뒤 `connected_slaves=0`을 확인한다. 단순 pause를 쓰면 이미 커널 버퍼에 있던 복제 데이터가 resume 직후 적용되어 손실 실험이 불명확해질 수 있기 때문이다.

`redis-promote`는 primary를 SIGKILL하고, 네트워크가 분리된 복제본을 `REPLICAOF NO ONE`으로 승격한 뒤 네트워크에 다시 연결한다. Docker Desktop에서는 네트워크 재연결 직후 호스트 포트 전달이 늦게 복구될 수 있어 증거 조회는 해당 컨테이너의 `docker compose exec`로 수행한다. 이 실험은 비동기 복제 지연이 발생한 상태를 의도적으로 만든 **수동 승격 실험**이다. Sentinel의 자동 장애 감지 시간을 측정하는 실험과 구분한다. 재연결 뒤 클라이언트도 26380으로 전환해야 하며, 고정된 primary 주소만 쓰는 애플리케이션이 자동으로 전환되는 구성은 아니다.

`redis-restore`는 승격된 복제본을 정본으로 삼아 예전 primary를 먼저 동기화하고, 기존 주소 역할로 되돌린다. 장애 전 primary에만 있던 divergent 데이터가 다시 나타나지 않게 하는 복구 절차다. 복구 중에는 애플리케이션 쓰기를 중단한다. 애플리케이션 데이터 복구·재고 재구축은 이 인프라 스크립트의 범위 밖이며 DB/이벤트 기반 복구 실험이 별도로 수행한다.

## Sentinel 자동 승격

```bash
python3 infra/lab.py sentinel-up
python3 infra/lab.py sentinel-failover
python3 infra/lab.py redis-restore
# 위 전체 흐름을 seed 검증, 승격 후 쓰기, 복구 후 데이터 검증까지 실행:
python3 infra/sentinel-experiments.py
```

Sentinel 3개가 서로를 발견한 다음 primary를 종료하고, replica가 master가 될 때까지의 시간을 기록한다. `redis-restore`와 수동 손실 실험은 Sentinel들을 먼저 중지해 다른 실험의 자동 승격이 개입하지 않게 한다. Sentinel이 반환하는 주소는 Docker 내부 호스트 주소이므로, 이 실험에서는 역할 전환 자체를 확인하며 호스트의 Spring 클라이언트가 Sentinel 주소를 직접 사용하는 구성은 아니다.

## Redis Cluster 슬롯과 hot key

```bash
python3 infra/lab.py cluster-up
```

독립된 3개 master에 16,384개 슬롯을 배분한다. 이 클러스터는 여러 쿠폰을 분산하는 실험용이며 HA 실험용 복제본은 없다. 한 쿠폰의 재고와 중복 사용자 집합을 Lua로 함께 다룰 때에는 `stock:{coupon-id}` / `users:{coupon-id}`처럼 같은 hash tag가 필요하다. 따라서 그 쿠폰의 원자적 연산은 한 master에 남는다. 서로 다른 쿠폰을 여러 master로 분산한 처리량과 비교해야 한다.

클러스터가 공지하는 노드 주소는 Docker 내부 주소다. 호스트에서 직접 실험할 때는 `CLUSTER MYID`로 각 공개 포트를 node ID와 연결하고, 해당 슬롯의 master에 요청하거나 Docker 네트워크 안에서 cluster-aware client를 실행한다. 클러스터 자동 redirect를 호스트에서 그대로 사용하는 구성은 아니다.

```bash
python3 infra/cluster-experiments.py
```

기본 측정은 12,000건, 동시 작업자 18개, 3회 반복이다. 항상 master 3개를 실행한 상태에서 36개 쿠폰 키를 master 1개에 모은 경우와 3개에 배분한 경우를 비교한다. 한 쿠폰 100% 집중 / 균등 분산 / 한 쿠폰 약 90% 집중을 각각 측정한다. 양쪽 모두 같은 연결 수와 Lua(`SISMEMBER` + `GET` + `DECR` + `SADD`)를 사용하며 인위적인 지연이나 CPU 작업은 삽입하지 않는다. 실행 순서는 고정 seed로 섞는다.

`results/redis-cluster.json`에는 실제 `CROSSSLOT` 오류, 같은 hash tag의 Lua 성공, master별 `EVALSHA` 호출 증가량, 처리량과 RTT p95, 총 재고 및 발급 사용자 수 검증을 저장한다. 처리량의 wall time에는 연결 생성과 worker 기동도 포함하며 RTT는 각 명령 송신부터 응답 수신까지 측정한다. HTTP 발급 API의 end-to-end 지연과 구분해야 한다. 클라이언트·Docker 네트워크가 먼저 병목이면 master를 늘려도 처리량 개선이 관측되지 않을 수 있다.

#!/bin/bash
# Redis primary를 죽여 Sentinel failover 동안·뒤에 발급 API가 어떻게 되는지 본다. 앱(bootRun)이 떠 있어야 한다.
#   ./failover.sh        정상 복제 중 failover — 승인된 예약이 살아남는가, 다시 202가 나오기까지 몇 초인가
#   ./failover.sh lag    replica 연결을 끊은 채(복제 지연 흉내) 예약 → failover — 승인된 예약이 사라지는가, DB 총량 가드가 무엇을 막는가
# WAIT 켠 앱(기본)과 끈 앱(--coupon.redis.wait-replicas=0)으로 각각 돌려 비교한다.
# 죽인 노드는 5초 뒤 재기동한다(운영에서 supervisor가 다시 띄우듯). WAIT가 켜져 있으면 replica가 돌아와야 다시 승인된다.
COUPON_ID=1
sentinel()   { docker exec sentinel-1 redis-cli -p 26379 "$@"; }
master_port(){ sentinel SENTINEL get-master-addr-by-name mymaster | tail -1; }
redis()      { docker exec sentinel-1 redis-cli -p "$(master_port)" "$@"; }
state()      { echo "master=127.0.0.1:$(master_port) stock=$(redis GET coupon:$COUPON_ID:stock) users=$(redis SCARD coupon:$COUPON_ID:users)"; }
now()        { perl -MTime::HiRes=time -e 'printf "%.1f", time'; }
issue() {    # "<HTTP 코드> <message>" 한 줄
  local out; out=$(curl -s -w '\n%{http_code}' -X POST "http://localhost:8080/api/coupons/$COUPON_ID/issue" -H 'Content-Type: application/json' -d "{\"userId\":$1}")
  echo "$(echo "$out" | tail -1) $(echo "$out" | head -1 | sed -n 's/.*"message":"\([^"]*\)".*/\1/p')"
}
codes()      { for u in "$@"; do printf '%s ' "$(issue "$u" | cut -d' ' -f1)"; done; }
last_msg()   { issue "$1" | cut -d' ' -f2-; }

./reset-coupon.sh "$COUPON_ID" 10 > /dev/null 2>&1
# 컨테이너 이름과 역할은 failover를 거치며 바뀐다 — Sentinel이 말하는 포트로 지금의 primary/replica 컨테이너를 정한다
if [ "$(master_port)" = 6381 ]; then PRIMARY=redis-primary; P=6381; REPLICA=redis-replica; else PRIMARY=redis-replica; P=6382; REPLICA=redis-primary; fi
if [ "$1" = lag ]; then   # replica를 멈추고 primary 쪽 복제 연결도 끊는다 — 이후 쓰기는 replica의 소켓 버퍼에도 닿지 않는다 (pause만으로는 커널 버퍼가 받아 둔다)
  docker pause "$REPLICA" > /dev/null; redis CLIENT KILL TYPE replica > /dev/null; echo "$REPLICA 정지 + 복제 연결 끊음 — 복제 지연 흉내"
fi
R=$(codes 1 2 3 4 5); echo "1) 사용자 1~5 발급 → $R"; case "$R" in *503*) echo "   ($(last_msg 5))";; esac
echo "   $(state)"

docker kill "$PRIMARY" > /dev/null; T0=$(now); echo "2) $PRIMARY kill"
[ "$1" = lag ] && docker unpause "$REPLICA" > /dev/null

RESTARTED=0; PREV=""
while :; do                                        # 0.2초 간격으로 사용자 6 발급 시도. 응답이 바뀔 때만 출력
  RESP=$(issue 6); CODE=${RESP%% *}; EL=$(echo "$(now) - $T0" | bc)
  [ "$CODE" = 202 ] && break
  [ "$RESP" != "$PREV" ] && printf '   +%4.1fs → %s\n' "$EL" "$RESP"; PREV="$RESP"
  if [ $RESTARTED = 0 ] && [ "$(echo "$EL > 5" | bc)" = 1 ]; then docker start "$PRIMARY" > /dev/null; RESTARTED=1; printf '   +%4.1fs %s 재기동\n' "$EL" "$PRIMARY"; fi
  if [ "$(echo "$EL > 90" | bc)" = 1 ]; then echo "   90초 안에 202가 나오지 않음"; break; fi
  sleep 0.2
done
printf '3) +%4.1fs → %s  다시 승인됨\n' "$EL" "$CODE"
[ $RESTARTED = 0 ] && docker start "$PRIMARY" > /dev/null && echo "   $PRIMARY 재기동"
echo "   $(state)   (정상: stock=4 users=6 · lag: stock=9 users=1 — WAIT가 켜져 있었으면 1~5는 애초에 거절됐고, 꺼져 있었으면 승인됐다가 사라진 것)"
if [ "$1" != lag ]; then
  echo "4) 사용자 1~5 재요청 → $(codes 1 2 3 4 5)   (409면 승인된 예약이 failover를 넘어 살아남았다)"
else
  echo "4) 사용자 11~25 발급 → $(codes $(seq 11 25))"
  sleep 3   # Consumer가 DB에 쓰도록
  R=$(redis SMEMBERS coupon:$COUPON_ID:users | sort -n); D=$(docker exec mysql mysql -u coupon -pcoupon coupon_db --skip-column-names -se "SELECT user_id FROM issued_coupon WHERE coupon_id=$COUPON_ID ORDER BY user_id" 2>/dev/null)
  echo "   Redis 승인자: $(echo $R)"
  echo "   DB 발급자  : $(echo $D)  (total_quantity=10 — 조건부 UPDATE가 그 이상을 막는다)"
  echo "   202를 받았지만 쿠폰이 없는 사용자: $(comm -23 <(echo "$R" | sort) <(echo "$D" | sort) | sort -n | tr '\n' ' ')"
  echo "   쿠폰은 있는데 Redis가 잊은 사용자 : $(comm -13 <(echo "$R" | sort) <(echo "$D" | sort) | sort -n | tr '\n' ' ')"
fi

for i in $(seq 1 30); do [ "$(docker exec "$PRIMARY" redis-cli -p $P ROLE 2>/dev/null | head -1)" = slave ] && break; sleep 1; done
echo "5) $PRIMARY → $(docker exec "$PRIMARY" redis-cli -p $P ROLE | head -1) (Sentinel이 replica로 되돌림, 재기동 후 ${i}초)"

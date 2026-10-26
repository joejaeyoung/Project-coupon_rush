"""Generate the editable SVG diagrams used by the Phase 4 READMEs.

Run from the repository root: python3 docs/architecture/phase4_svg.py
Only Python's standard library is required.
"""

from html import escape
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "assets"
FONT = "-apple-system, BlinkMacSystemFont, 'Apple SD Gothic Neo', 'Noto Sans KR', 'Segoe UI', sans-serif"
INK = "#193044"
MUTED = "#5F7180"
LINE = "#CAD4D8"
TEAL = "#087F79"
TEAL_LIGHT = "#E5F4F0"
BLUE = "#32629B"
BLUE_LIGHT = "#EAF1FB"
VIOLET = "#69529F"
VIOLET_LIGHT = "#F0ECF8"
ORANGE = "#D86742"
ORANGE_LIGHT = "#FFF0E9"


class Diagram:
    def __init__(self, width, height, title, description):
        self.width, self.height = width, height
        self.items = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
            f'<title id="title">{escape(title)}</title>',
            f'<desc id="desc">{escape(description)}</desc>',
            '<defs>'
            '<marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="4.5" orient="auto">'
            '<path d="M0 0 L9 4.5 L0 9 Z" fill="#748792"/></marker>'
            '<marker id="accent-arrow" markerWidth="9" markerHeight="9" refX="8" refY="4.5" orient="auto">'
            '<path d="M0 0 L9 4.5 L0 9 Z" fill="#087F79"/></marker>'
            '</defs>',
            f'<rect width="{width}" height="{height}" rx="24" fill="#F8F7F4"/>',
        ]

    def rect(self, x, y, w, h, fill="#FFFFFF", stroke=LINE, radius=16, stroke_width=1.5):
        self.items.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"/>'
        )

    def text(self, x, y, value, size=18, color=INK, weight=500, anchor="start", spacing=None):
        extra = f' letter-spacing="{spacing}"' if spacing is not None else ""
        self.items.append(
            f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
            f'font-weight="{weight}" fill="{color}" text-anchor="{anchor}"{extra}>'
            f'{escape(value)}</text>'
        )

    def path(self, points, color="#748792", width=2.5, dash=None, arrow=True):
        data = "M" + " L".join(f"{x} {y}" for x, y in points)
        marker = ' marker-end="url(#accent-arrow)"' if arrow and color == TEAL else (' marker-end="url(#arrow)"' if arrow else "")
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        self.items.append(
            f'<path d="{data}" fill="none" stroke="{color}" stroke-width="{width}" '
            f'stroke-linecap="round" stroke-linejoin="round"{dash_attr}{marker}/>'
        )

    def dot(self, x, y, color=TEAL, radius=5):
        self.items.append(f'<circle cx="{x}" cy="{y}" r="{radius}" fill="{color}"/>')

    def header(self, eyebrow, title, subtitle):
        self.text(52, 54, eyebrow, 14, TEAL, 750, spacing=2)
        self.text(52, 101, title, 34, INK, 750)
        self.text(52, 134, subtitle, 17, MUTED, 450)
        self.path([(52, 157), (self.width - 52, 157)], LINE, 1.3, arrow=False)

    def card(self, x, y, w, h, eyebrow, title, lines=(), fill="#FFFFFF", border=LINE, accent=TEAL, title_size=21):
        self.rect(x, y, w, h, fill, border, 16)
        self.rect(x, y, 6, h, accent, accent, 3, 0)
        self.text(x + 20, y + 27, eyebrow, 12, accent, 750, spacing=1.2)
        self.text(x + 20, y + 60, title, title_size, INK, 700)
        for i, line in enumerate(lines):
            self.text(x + 20, y + 88 + i * 23, line, 15, MUTED, 450)

    def pill(self, x, y, w, label, color=TEAL, fill=TEAL_LIGHT):
        self.rect(x, y, w, 29, fill, fill, 14, 0)
        self.text(x + w / 2, y + 20, label, 13, color, 700, "middle")

    def footer(self, number, title, body, color=TEAL):
        y = self.height - 76
        self.rect(52, y - 10, self.width - 104, 56, "#FFFFFF", "#DDE4E4", 12, 1)
        self.pill(68, y + 4, 42, number, color, TEAL_LIGHT if color == TEAL else ORANGE_LIGHT)
        self.text(126, y + 24, title, 16, INK, 700)
        self.text(self.width - 68, y + 24, body, 14, MUTED, 450, "end")

    def save(self, filename):
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / filename).write_text("\n".join(self.items + ["</svg>"]) + "\n", encoding="utf-8")


def architecture():
    d = Diagram(1600, 950, "Coupon Rush Phase 4 architecture", "One Spring Boot process checks Redis, waits for Kafka ACK, then returns 202. Kafka Consumer stores the coupon in MySQL. A separate GET checks the committed DB row.")
    d.header("COUPON RUSH  /  PHASE 04", "접수와 발급 확정은 서로 다른 시점", "왼쪽에서 요청하고, 오른쪽에서 판정·기록합니다. 아래 경로로 최종 발급 여부를 확인합니다.")
    d.rect(52, 190, 220, 550, "#FFFFFF", "#DDE4E4", 20)
    d.text(76, 226, "CLIENT", 14, ORANGE, 750, spacing=1.3)
    d.card(76, 255, 173, 145, "01  REQUEST", "발급 요청", ("POST /issue", "userId 전달"), ORANGE_LIGHT, "#F1C5B3", ORANGE, 20)
    d.card(76, 465, 173, 145, "08  VERIFY", "결과 조회", ("GET /issues/", "users/{userId}"), BLUE_LIGHT, "#C8D9EE", BLUE, 20)

    d.rect(308, 190, 745, 550, "#FFFFFF", "#C8D8D4", 20)
    d.text(334, 226, "SPRING BOOT  ·  한 프로세스", 14, TEAL, 750, spacing=1.2)
    d.card(338, 255, 212, 145, "HTTP  ·  :8080", "CouponService", ("기간 · 재고 선확인", "예약 후 Kafka 발행"), "#FBFCFB", "#CBD9D6", TEAL)
    d.card(605, 255, 212, 145, "REDIS CLIENT", "RedisService", ("MGET → SADD", "DECR → WAIT 1"), TEAL_LIGHT, "#B9DCD4", TEAL)
    d.card(605, 465, 212, 145, "KAFKA PRODUCER", "Producer", ("send().join()", "acks=all 확인"), VIOLET_LIGHT, "#D5C9EB", VIOLET)
    d.card(842, 580, 184, 145, "KAFKA CONSUMER", "Consumer", ("중복 · 총량 검사", "UPDATE → INSERT"), VIOLET_LIGHT, "#D5C9EB", VIOLET, 19)
    d.card(338, 465, 212, 145, "HTTP  ·  :8080", "상태 조회 API", ("DB 발급 기록 조회", "ISSUED / 미확인"), BLUE_LIGHT, "#C8D9EE", BLUE)

    d.rect(1090, 190, 458, 550, "#FFFFFF", "#DDE4E4", 20)
    d.text(1116, 226, "INFRASTRUCTURE  ·  LOCAL COMPOSE", 14, MUTED, 750, spacing=1.2)
    d.card(1120, 255, 398, 145, "REDIS HA", "primary + replica", ("Sentinel ×3이 primary 주소 제공", "예약 후 WAIT로 복제 수신 확인"), TEAL_LIGHT, "#B9DCD4", TEAL)
    d.card(1120, 465, 398, 105, "KAFKA  ·  :9092", "coupon-issue", ("key = couponId · 단일 브로커",), VIOLET_LIGHT, "#D5C9EB", VIOLET)
    d.card(1120, 610, 398, 105, "MYSQL  ·  :3306", "최종 발급 기록", ("coupon / issued_coupon",), BLUE_LIGHT, "#C8D9EE", BLUE)

    d.path([(249, 325), (338, 325)], ORANGE)
    d.text(293, 312, "POST", 12, ORANGE, 700, "middle")
    d.path([(338, 370), (249, 370)], TEAL)
    d.text(293, 364, "202", 12, TEAL, 700, "middle")
    d.path([(550, 325), (605, 325)], TEAL)
    d.path([(817, 325), (1120, 325)], TEAL)
    d.text(963, 310, "복제 확인", 13, TEAL, 700, "middle")
    d.path([(447, 400), (447, 428), (711, 428), (711, 465)], VIOLET)
    d.text(560, 420, "예약 통과", 13, VIOLET, 700, "middle")
    d.path([(817, 521), (1120, 521)], VIOLET)
    d.path([(1120, 548), (1070, 548), (1070, 652), (1026, 652)], VIOLET)
    d.text(1060, 642, "poll", 12, VIOLET, 700, "end")
    d.path([(1026, 685), (1120, 685)], BLUE)
    d.text(1073, 675, "DB 저장", 12, BLUE, 700, "middle")
    d.path([(249, 537), (338, 537)], BLUE)
    d.text(291, 524, "GET", 12, BLUE, 700, "middle")
    d.path([(338, 580), (249, 580)], BLUE)
    d.path([(550, 585), (565, 585), (565, 785), (1300, 785), (1300, 715)], BLUE)
    d.text(875, 776, "issued_coupon 존재 확인", 13, BLUE, 700, "middle")
    d.pill(76, 647, 173, "202 = Kafka 접수", TEAL, TEAL_LIGHT)
    d.pill(338, 647, 212, "200 = DB 조회 결과", BLUE, BLUE_LIGHT)
    d.footer("!", "DB 저장은 HTTP 응답과 독립", "202를 받았어도 최종 발급이 확정된 것은 아닙니다.", ORANGE)
    d.save("phase-4-architecture.svg")


def init():
    d = Diagram(1500, 430, "Phase 4 coupon initialization", "The reset script resets MySQL and Redis data, calls the initialization API, and the API copies coupon quantity and dates from MySQL to Redis.")
    d.header("01  /  PREPARE", "실험 데이터 초기화", "reset-coupon.sh는 기존 실험을 정리한 뒤, DB 쿠폰 값을 Redis에 올립니다.")
    steps = [
        (52, "01  MYSQL", "쿠폰 초기화", ("issued_quantity=0", "issued_coupon 삭제"), BLUE_LIGHT, BLUE),
        (420, "02  REDIS", "이전 키 삭제", ("stock · users", "startDate · endDate"), TEAL_LIGHT, TEAL),
        (788, "03  API", "POST /stock/init", ("MySQL 쿠폰 조회", "총량 · 기간 반환"), "#FFFFFF", INK),
        (1156, "04  REDIS", "MSET 재고·기간", ("stock · startDate", "endDate 저장"), TEAL_LIGHT, TEAL),
    ]
    for x, eyebrow, title, lines, fill, accent in steps:
        d.card(x, 200, 292, 148, eyebrow, title, lines, fill, "#D5DFDF", accent, 20)
    for x in (344, 712, 1080):
        d.path([(x, 274), (x + 66, 274)])
    d.text(750, 389, "초기화 API는 users를 삭제하지 않으며, 진행 중인 발급의 복구 API가 아닙니다.", 15, MUTED, 500, "middle")
    d.save("phase-4-initialize.svg")


def issue():
    d = Diagram(1600, 760, "Phase 4 coupon issue flow", "The issue API checks Redis, reserves inventory, waits for one replica, then waits for Kafka ACK before returning 202. Kafka Consumer later commits the issued coupon to MySQL.")
    d.header("02  /  ISSUE", "Redis가 예약하고 Kafka가 접수를 확인합니다", "앞단에서 거절한 요청은 Kafka로 보내지 않습니다. ACK 뒤에도 DB 저장은 별도로 진행됩니다.")
    columns = [
        (52, "01  CHECK", "MGET", ("기간 · 재고 선확인", "없음 404 / 기간 400", "소진 409"), TEAL_LIGHT, TEAL),
        (310, "02  RESERVE", "SADD", ("사용자 중복 검사", "기존 사용자 409"), TEAL_LIGHT, TEAL),
        (568, "03  RESERVE", "DECR", ("재고 1개 차감", "음수면 보상 → 409"), TEAL_LIGHT, TEAL),
        (826, "04  REPLICATE", "WAIT 1", ("replica 수신 확인", "부족하면 보상 → 503"), TEAL_LIGHT, TEAL),
        (1084, "05  PUBLISH", "Kafka ACK", ("send().join()", "실패하면 보상 → 503"), VIOLET_LIGHT, VIOLET),
        (1342, "06  RESPOND", "202 Accepted", ("접수 확인", "DB 완료는 미확정"), ORANGE_LIGHT, ORANGE),
    ]
    for x, eyebrow, title, lines, fill, accent in columns:
        d.card(x, 222, 206, 160, eyebrow, title, lines, fill, "#D5DFDF", accent, 19)
    for x in (258, 516, 774, 1032, 1290):
        d.path([(x, 302), (x + 52, 302)], TEAL if x < 1032 else VIOLET)
    d.rect(52, 422, 1496, 226, "#FFFFFF", "#DDE4E4", 18)
    d.text(78, 458, "KAFKA ACK 이후  ·  비동기 최종 저장", 15, VIOLET, 750, spacing=1)
    d.card(78, 484, 305, 126, "KAFKA", "coupon-issue", ("key = couponId",), VIOLET_LIGHT, "#D5C9EB", VIOLET, 20)
    d.card(492, 484, 305, 126, "CONSUMER", "중복 · 총량 검사", ("기존 발급 또는 한도 초과면 종료",), VIOLET_LIGHT, "#D5C9EB", VIOLET, 20)
    d.card(906, 484, 305, 126, "MYSQL", "UPDATE → INSERT", ("한 트랜잭션으로 커밋",), BLUE_LIGHT, "#C8D9EE", BLUE, 20)
    d.card(1320, 484, 202, 126, "KAFKA", "offset 커밋", ("DB 처리 뒤",), "#FFFFFF", "#D5DFDF", VIOLET, 20)
    for x in (383, 797, 1211):
        d.path([(x, 547), (x + 109, 547)])
    d.path([(1187, 382), (1187, 407), (230, 407), (230, 484)], VIOLET, 2, arrow=True)
    d.text(750, 698, "Kafka ACK를 못 받았어도 브로커에 기록된 이벤트가 뒤늦게 소비될 수 있습니다.", 16, ORANGE, 600, "middle")
    d.save("phase-4-issue.svg")


def status():
    d = Diagram(1500, 610, "Phase 4 issue result lookup", "After receiving 202, a client calls a separate GET endpoint. The API checks whether an issued_coupon row exists in MySQL and responds ISSUED or NOT_CONFIRMED.")
    d.header("03  /  VERIFY", "클라이언트가 최종 발급 여부를 따로 조회합니다", "POST의 202는 접수 응답입니다. GET은 MySQL에 커밋된 발급 기록을 읽습니다.")
    d.card(52, 214, 315, 170, "CLIENT", "202를 받은 뒤", ("간격을 두고 GET 요청", "couponId=1 · userId=42"), ORANGE_LIGHT, "#F1C5B3", ORANGE)
    d.card(478, 214, 315, 170, "SPRING BOOT", "발급 상태 조회", ("GET /api/coupons/1/", "issues/users/42"), BLUE_LIGHT, "#C8D9EE", BLUE)
    d.card(904, 214, 315, 170, "MYSQL", "issued_coupon", ("coupon_id=1, user_id=42", "존재 여부 조회"), BLUE_LIGHT, "#C8D9EE", BLUE)
    d.path([(367, 298), (478, 298)], BLUE)
    d.path([(793, 298), (904, 298)], BLUE)
    d.path([(1219, 298), (1262, 298)], BLUE)
    d.rect(1262, 213, 186, 170, "#FFFFFF", "#D5DFDF", 16)
    d.pill(1278, 239, 154, "ISSUED", TEAL, TEAL_LIGHT)
    d.pill(1278, 295, 154, "NOT_CONFIRMED", ORANGE, ORANGE_LIGHT)
    d.text(1355, 364, "HTTP 200", 14, MUTED, 600, "middle")
    d.rect(52, 436, 1396, 92, "#FFFFFF", "#DDE4E4", 14)
    d.text(80, 471, "ISSUED", 17, TEAL, 750)
    d.text(190, 471, "DB 발급 기록이 있음", 16, INK, 500)
    d.text(600, 471, "NOT_CONFIRMED", 17, ORANGE, 750)
    d.text(805, 471, "현재 기록 없음 · 처리 중과 실패를 구분하지 못함", 16, INK, 500)
    d.text(80, 505, "정해 둔 횟수만 조회하고 끝까지 기록이 없으면 '결과 확인 불가'로 표시합니다.", 15, MUTED, 450)
    d.save("phase-4-status.svg")


def failover():
    d = Diagram(1500, 660, "Phase 4 Redis Sentinel failover", "When the Redis primary fails, Sentinel promotes the replica and Lettuce reconnects. With WAIT 1, requests still receive 503 until a replica is available again. After the old primary returns as replica, ACKs can succeed and 202 responses resume.")
    d.header("04  /  REDIS HA", "primary 승격과 발급 재개 사이에 공백이 있습니다", "Sentinel이 새 primary를 찾아도 WAIT 1을 통과할 replica가 생겨야 202가 재개됩니다.")
    steps = [
        (52, "01  FAILURE", "primary 중단", ("기존 :6381 종료", "접속 중 요청 503"), ORANGE_LIGHT, ORANGE),
        (343, "02  SENTINEL", "장애 판단", ("3대 감시 · quorum 2", "down-after 2000ms 설정"), VIOLET_LIGHT, VIOLET),
        (634, "03  REDIS", "replica 승격", ("새 primary가 됨", "Lettuce가 다시 연결"), TEAL_LIGHT, TEAL),
        (925, "04  WAIT", "복제본 부재", ("WAIT 1 결과 부족", "예약 보상 후 503"), ORANGE_LIGHT, ORANGE),
        (1216, "05  RECOVERY", "replica 복귀", ("옛 primary가 재합류", "WAIT 성공 → 202"), TEAL_LIGHT, TEAL),
    ]
    for x, eyebrow, title, lines, fill, accent in steps:
        d.card(x, 224, 232, 170, eyebrow, title, lines, fill, "#D5DFDF", accent, 20)
    for x in (284, 575, 866, 1157):
        d.path([(x, 309), (x + 59, 309)])
    d.rect(52, 444, 1396, 115, "#FFFFFF", "#DDE4E4", 16)
    d.pill(80, 467, 188, "승격 완료 ≠ 발급 재개", ORANGE, ORANGE_LIGHT)
    d.text(291, 488, "WAIT는 replica의 수신을 확인합니다. 승격 직후에는 보낼 replica가 없어 503을 반환합니다.", 17, INK, 500)
    d.text(80, 530, "기존 failover 실험: WAIT 1에서 다시 202가 나올 때까지 약 10.5초 (로컬 측정).", 15, MUTED, 450)
    d.save("phase-4-failover.svg")


if __name__ == "__main__":
    architecture()
    init()
    issue()
    status()
    failover()

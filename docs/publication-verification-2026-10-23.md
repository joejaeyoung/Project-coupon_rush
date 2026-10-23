# 게시 전 검증: 2026-10-23

원격 main `0e10e4798ccb973251ba28b1f34ef9e48b0472b5`에 로컬 복구 커밋 `2ff03f3635e72ef2cd804e47af35df188621d9ae`의 실험 모듈을 통합했다. README 충돌은 원격의 측정 조건·응답 의미·실행 방법 정정을 유지하고, 10월 실험과 기존 세 기준선을 구분하는 방식으로 해결했다.

이 통합에서 실험 애플리케이션 코드는 변경하지 않았다. 문서·측정 원본·재현 도구를 함께 게시하며, 과거 측정 결과의 숫자를 새 실행 결과로 바꾸지 않았다.

| 확인 | 결과 |
| --- | --- |
| JDK 17 `./gradlew test bootJar --no-daemon` | 성공, Java 테스트 9개·실패/오류/skip 0 |
| CommitBarrierTest / LabStoreTest / RetryingLookupTest | 3 / 4 / 2개 통과 |
| `python3 -m unittest discover -s tests -v` | 3개 통과 |
| Python 구문 검사 | 13파일 통과 |
| Shell·Gradle wrapper 구문 검사 | 13파일 통과 |
| 복구 검증 기록의 소스 SHA-256 | 30/30 일치 |
| 재빌드 JAR SHA-256 | 복구 후 기록과 일치 |
| README·관련 문서의 상대 링크·앵커 | Markdown 17개에서 191개 정상, 충돌 마커 0개 |

JAR SHA-256: `22f7b792d6b931b2deacfc28ea29adb2dbfc1a61736641406f7f2da684efba8c`.

기준 기록: [복구 검증 JSON](../4-coupon_experiments/results/recovery-2026-10-22/verification.json). 초기 검증 기록과는 `app.py`·`LabStore.java`가 후속 수정으로 다르며, 최신 복구 기록에는 두 변경이 반영돼 있다.

이날 Docker 장애·분포·API 부하 실험은 재실행하지 않았다. 그 결과는 [2026-10-22 실증 보고서](experiments-2026-10-22.md)의 실행 버전·환경 범위를 따른다. Java/Python 회귀 통과를 다중 노드 HA 재검증으로 해석하지 않는다.

실험 원본 CSV·로그와 Windows wrapper의 줄바꿈·공백은 보존했다. 소스·문서의 변경분 공백 검사는 통과했으며, 원본 로그의 후행 공백을 정리해 측정 파일을 바꾸지는 않았다.

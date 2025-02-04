# 🎭 한국 공연 시장 데이터 파이프라인 (KOPIS)

## 📌 프로젝트 개요
이 프로젝트는 **KOPIS API**를 활용하여 한국 공연 시장 데이터를 Snowflake 데이터 웨어하우스에 적재하고 분석하는 파이프라인을 구축하는 것을 목표로 합니다.  
주요 데이터는 **공연별 티켓 판매량, 매출액, 취소율, 공연 일정** 등을 포함하며, 이를 기반으로 다양한 시각적 분석을 진행할 수 있습니다.

---

## 🏗️ **Snowflake 테이블 스키마**
- 공연 통계 테이블을 생성하여 공연 건수, 판매된 티켓 수, 취소된 티켓 수, 티켓 판매액 등의 정보를 저장합니다.
- 원본 JSON 데이터를 저장하는 테이블을 따로 두어 데이터 정합성을 유지합니다.
CREATE TABLE gyoung.kopis_performance_raw (
    prfcnt INT,
    ntssnmrs INT,
    cancelnmrs INT,
    amount BIGINT,
    nmrs INT,
    prfdtcnt INT,
    prfdt DATE, 
    prfprocnt INT
);
---

## ☁️ **S3 스토리지 연동 설정**
- AWS S3 스토리지와 Snowflake를 연동하여 데이터 적재를 자동화합니다.
- Snowflake **Storage Integration**을 설정하여 특정 S3 버킷의 데이터를 Snowflake에서 직접 읽어올 수 있도록 구성합니다.
- Snowflake 스테이지(Stage)를 생성하여 S3 데이터를 쉽게 관리합니다.
- 스테이지 내 파일 목록을 확인하고, 적재 경로를 수정할 수 있습니다.

---

## 📥 **데이터 적재 (ETL)**
- KOPIS API에서 공연 데이터를 가져와 JSON 형식으로 S3에 저장합니다.
- Snowflake의 **COPY INTO** 명령어를 활용하여 S3의 데이터를 Snowflake 테이블로 적재합니다.
- CSV 데이터를 저장하는 테이블과 정제된 데이터를 저장하는 테이블을 구분하여 관리합니다.

---

## 📊 **데이터 확인**
- Snowflake에서 원본 데이터와 정제된 데이터를 조회하여 데이터가 정상적으로 적재되었는지 확인합니다.
- 공연 날짜 기준으로 데이터를 정렬하여 공연 시장의 변화 패턴을 분석할 수 있습니다.
- 필요에 따라 테이블 구조를 수정하여 추가적인 데이터를 저장할 수 있습니다.

---

## 🔄 **업데이트 및 유지보수**
- S3 버킷의 여러 디렉터리에서 데이터를 가져올 수 있도록 Snowflake Storage Integration 설정을 수정합니다.
- 기존 테이블을 삭제하고 새로운 구조로 다시 생성하여 데이터 모델을 개선합니다.
- Snowflake Storage Integration이 올바르게 설정되었는지 확인하고 필요하면 변경합니다.

---

## 📌 **관련 자료**
- [KOPIS API 공식 문서](https://www.kopis.or.kr/)
- [Snowflake 공식 문서](https://docs.snowflake.com/)
- [AWS S3 Storage Integration](https://docs.snowflake.com/en/user-guide/data-load-s3)

---

이 프로젝트는 공연 시장 데이터를 체계적으로 관리하고, 데이터 기반 분석을 통해 인사이트를 도출하는 것을 목표로 합니다.  
🚀 지속적인 개선을 통해 데이터 품질을 높이고 활용성을 극대화할 계획입니다!

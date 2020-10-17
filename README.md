# Spring Boot 1.5.22 용 Batch 연습

### IntelliJ 설정
* Annotation Processors: [v] Enable annotation processing

### 실행 방법:
* VM Options: `-Xms256m -Xmx256m -XX:+UseG1GC`
* Program arguments: --job.name=`jpaPagingJob`
* Active profile: `mysql`

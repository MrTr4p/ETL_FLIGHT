%~d0
cd %~dp0
java -Dtalend.component.manager.m2.repository="%cd%/../lib" -Xms256M -Xmx1024M -cp .;../lib/routines.jar;../lib/log4j-slf4j-impl-2.13.2.jar;../lib/log4j-api-2.13.2.jar;../lib/log4j-core-2.13.2.jar;../lib/jboss-marshalling-2.0.12.Final.jar;../lib/dom4j-2.1.3.jar;../lib/slf4j-api-1.7.29.jar;../lib/postgresql-42.2.14.jar;../lib/crypto-utils-0.31.12.jar;parent_job_0_1.jar;airport_dim_0_1.jar;ticket_dim_0_1.jar;flight_dim_0_1.jar;booking_fact_0_1.jar;weather_dim_0_1.jar;payment_dim_0_1.jar;passenger_dim_0_1.jar;date_dim_0_1.jar; depi.parent_job_0_1.parent_job --context=Default %*

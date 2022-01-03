------------------------------------------
-- 데이터베이스 생성
CREATE DATABASE sql_project DEFAULT CHARACTER SET utf8mb4;

-- 테이블 생성
-- Python에서 query 전송 및 데이터 insert (ipynb 파일의 Python 코드 참조)
CREATE TABLE delivery_count (date date, hour_time int, deliver_type varchar(16), dosi varchar(8), sigu varchar(8), count int, dayweek varchar(16))
CREATE TABLE delivery_weather (dosi varchar(8), sigu varchar(8), date date, hour_time int, rain_type varchar(8), humidity int, precipitation float, temperatur float, wind_speed float, wind_strength varchar(8))

-- 이상치 조회
-- hmd, pcp, temp, wind_spd
select hmd from delivery group by hmd order by hmd limit 10;
select pcp from delivery group by pcp order by pcp limit 10;
select temp from delivery group by temp order by temp limit 10;
select wind_spd from delivery group by wind_spd order by wind_spd limit 10;
-- 이상치 조회 결과:
-- hmd: -998 (3,147건)
-- pcp: -998.9 (226건)
-- temp: -999 (226건)
-- wind_spd: -998.9 (225건)

-- 이상치 제거
-- hmd, wind_spd는 그대로 두고, pcp, temp의 이상치만 제거
delete from delivery
where CAST(pcp AS DECIMAL) = CAST(-998.9 AS DECIMAL);

delete from delivery
where temp = -999;

-- 두 데이터 테이블 self join
-- 두 테이블을 연결시킬 수 있는 컬럼은 구(sigu), 날짜(date), 시간(hour_time) 컬럼이다
-- null은 제외되도록 구, 날짜, 시간 값이 같은 것 기준으로 join
select * 
from delivery_count c, delivery_weather w 
where c.sigu=w.sigu and c.date=w.date and c.hour_time=w.hour_time limit 10;

-- join시킨 테이블에서 필요한 피처들만 가져오기
select c.date, c.hour, c.dlvr_type, c.sigu, c.count, c.dayweek, w.rain_type, w.hmd, w.pcp, w.temp, w.wind_spd, w.wind_str 
from delivery_count c, delivery_weather w 
where c.sigu=w.sigu and c.date=w.date and c.hour=w.hour limit 10;

-- (date 값의 월 부분 파싱) 월(month) 조회
select *, mid(date, 6, 2) as month from delivery;

-- 요일(dayweek) 한글로 변경하여 조회 
select *, (case
when c.dayweek = 'Monday' then '월요일'
when c.dayweek = 'Tuesday' then '화요일'
when c.dayweek = 'Wednesday' then '수요일'
when c.dayweek = 'Thursday' then '목요일'
when c.dayweek = 'Friday' then '금요일'
when c.dayweek = 'Saturday' then '토요일'
when c.dayweek = 'Sunday' then '일요일' end) as dayweek
from delivery_count c;

-- 필요한 데이터셋을 새 테이블로 생성
-- 월(month), 요일(dayweek) 컬럼 추가하여 생성
create table delivery select 
c.date, mid(c.date, 6,2) as month, c.hour, (case
when c.dayweek = 'Monday' then '월요일'
when c.dayweek = 'Tuesday' then '화요일'
when c.dayweek = 'Wednesday' then '수요일'
when c.dayweek = 'Thursday' then '목요일'
when c.dayweek = 'Friday' then '금요일'
when c.dayweek = 'Saturday' then '토요일'
when c.dayweek = 'Sunday' then '일요일' end) as dayweek,
c.dlvr_type, c.sigu, c.count, w.rain_type, w.hmd, w.pcp, w.temp, w.wind_spd, w.wind_str 
from delivery_count c, delivery_weather w 
where c.sigu=w.sigu and c.date=w.date and c.hour=w.hour;

-- 시간당 강우량 범주화
select *, (case
when pcp > 0 and pcp < 1.0 then '0.1-1.0'
when pcp >= 1.0 and pcp < 2.5 then '1.0-2.5'
when pcp >= 2.5 and pcp < 5.0 then '2.5-5.0'
when pcp >= 5.0 and pcp < 10.0 then '5.0-10.0'
when pcp >= 10.0 and pcp < 15.0 then '10.0-15.0'
when pcp >= 15.0 and pcp < 20.0 then '15.0-20.0'
when pcp >= 20.0 and pcp < 30.0 then '20.0-30.0'
when pcp >= 30.0 and pcp < 40.0 then '30.0-40.0'
when pcp >= 40.0 and pcp < 50.0 then '40.0-50.0'
when pcp >= 50.0 and pcp < 70.0 then '50.0-70.0'
when pcp >= 70.0 and pcp < 110.0 then '70.0-110.0'
when pcp >= 110.0 then '110.0 이상' else '0' end) as pcp_g
from delivery;

-- 강우량 범주화한 것 새 컬럼으로 추가
alter table delivery
add column pcp_g varchar(10);

update delivery
set pcp_g = case
when pcp > 0 and pcp < 1.0 then '0.1-1.0'
when pcp >= 1.0 and pcp < 2.5 then '1.0-2.5'
when pcp >= 2.5 and pcp < 5.0 then '2.5-5.0'
when pcp >= 5.0 and pcp < 10.0 then '5.0-10.0'
when pcp >= 10.0 and pcp < 15.0 then '10.0-15.0'
when pcp >= 15.0 and pcp < 20.0 then '15.0-20.0'
when pcp >= 20.0 and pcp < 30.0 then '20.0-30.0'
when pcp >= 30.0 and pcp < 40.0 then '30.0-40.0'
when pcp >= 40.0 and pcp < 50.0 then '40.0-50.0'
when pcp >= 50.0 and pcp < 70.0 then '50.0-70.0'
when pcp >= 70.0 and pcp < 110.0 then '70.0-110.0'
when pcp >= 110.0 then '110.0 이상' else '0' end;

-- 시간당 기온 범주화
select *, (case
when temp >= 0 and temp < 10 then '0-10'
when temp >= 10 and temp < 20 then '10-20'
when temp >= 20 and temp < 30 then '20-30'
when temp >= 30 and temp < 40 then '30-40'
when temp >= 40 then '40 이상'
when temp >= -10 and temp < 0 then '-10-0'
when temp < -10 then '-10 미만' end) as temp_g
from delivery;

-- 기온 범주화한 것 새 컬럼으로 추가
alter table delivery
add column temp_g varchar(10);

update delivery
set temp_g = case
when temp >= 0 and temp < 10 then '0-10'
when temp >= 10 and temp < 20 then '10-20'
when temp >= 20 and temp < 30 then '20-30'
when temp >= 30 and temp < 40 then '30-40'
when temp >= 40 then '40 이상'
when temp >= -10 and temp < 0 then '-10-0'
when temp < -10 then '-10 미만' end;

------------------------------------------
-- EDA

-- 월별 총 배달건수
select month, sum(count)
from delivery
group by month;

-- 월별 시간당 평균 배달건수
select month, avg(count) 
from delivery 
group by month;

-- 일별 총 배달건수
select date, sum(count) 
from delivery
group by date;

-- 시간대별 총/평균 배달건수
select hour, sum(count), avg(count) 
from delivery 
group by hour 
order by sum(count) desc;

-- 기온 카테고리별 총/평균 배달건수
select temp_g, sum(count), avg(count) 
from delivery 
group by temp_g;

-- 강수량 카테고리별 총/평균 배달건수
select pcp_g, sum(count), avg(count) 
from delivery 
group by pcp_g;

-- 날씨별 시간당 평균 배달건수
select rain_type, avg(count) 
from delivery
group by rain_type 
order by avg(count) desc;

-- 바람세기별 시간당 평균 배달건수
select wind_str, avg(count) 
from delivery
group by wind_str 
order by avg(count) desc;

-- 일 평균 기온
select date, avg(temp) 
from delivery 
group by date;

-- 배달 유형별 총 배달건수
select dlvr_type, sum(count) 
from delivery
group by dlvr_type 
order by sum(count) desc;

-- 요일별 총 배달건수
select dayweek, sum(count), avg(count)
from delivery
group by dayweek 
order by sum(count) desc;

-- 구별 총 배달건수
select sigu, sum(count) 
from delivery
group by sigu 
order by sum(count) desc;

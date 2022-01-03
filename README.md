# 배달 데이터 분석
## MySQL 및 Python을 활용한 배달 데이터 전처리 및 분석

### 데이터 소개
- KT 통신 빅데이터 플랫폼 (https://www.bigdata-telecom.kr/)
- 업종 목적지별 배달 주문건수
    - 날짜, 시간, 배달 유형, 시/도, 시/구, 배달건수
    - 총 1,026,046개
    - 이 중 서울 데이터: 167,802개
- 주문지역 기상 정보
    - ID, 도시 코드, 시/도, 시/구, 날짜, 시간, 날씨, 습도, 강우량, 기온, 풍속, 바람 세기, 풍향 방위, 풍향 방위각, 바람 분류  
    - 총 1,606,750개
    - 이 중 서울 데이터: 160,675개

### 프로젝트 과정
1. Python에서 두 데이터셋을 읽고 간략한 정리 후, AWS 서버에 생성한 MySQL DB에 두 데이터셋을 전송하고 저장
2. MySQL에서 데이터 전처리, 가공 및 EDA 진행
3. 처리된 데이터를 Python에서 불러와 EDA 내용 시각화

---

## 데이터 전처리 및 EDA

### 데이터 읽기
- Python으로 아래 두 데이터셋을 읽고 서울시 데이터만 추출하였다.
  - (1) 업종 목적지별 배달 주문건수, (2) 주문지역 기상 정보
- 날짜 컬럼의 데이터 타입을 datetime으로 변경하였다.
- 시간 컬럼의 값 형태 변경하였다.
- 불필요한 컬럼은 제외하였다.

### AWS 서버의 MySQL DB에 데이터 전송 및 저장

**AWS 서버 MySQL DB에 접속 및 연결**
```python
import mysql.connector

# AWS 서버 DB에 접속/연결
remote = mysql.connector.connect(
    host = host,
    port = 3306,
    user = "admin",
    password = password,
    database = "sql_project"
)
```

**업종 목적지별 배달 주문건수 데이터 테이블 생성 및 입력**

```python
# AWS DB에 테이블 생성
cur = remote.cursor()
cur.execute("CREATE TABLE delivery_count (date date, hour_time int, deliver_type varchar(16), dosi varchar(8), sigu varchar(8), count int, dayweek varchar(16))")

# 생성한 테이블에 데이터 입력
sql = """INSERT INTO delivery_count VALUES (%s, %s, %s, %s, %s, %s, %s)"""
cursor = remote.cursor(buffered=True)

for i, row in seoul_deliver_count.iterrows():
    cursor.execute(sql, tuple(row))
    #print(tuple(row))

remote.commit()
```

**주문지역 기상 정보 데이터 테이블 생성 및 입력**

```python
# AWS DB에 테이블 생성
cur = remote.cursor()
cur.execute("CREATE TABLE delivery_weather (dosi varchar(8), sigu varchar(8), date date, hour_time int, rain_type varchar(8), humidity int, precipitation float, temperatur float, wind_speed float, wind_strength varchar(8))")

# 생성한 테이블에 데이터 입력
sql = """INSERT INTO delivery_weather VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
cursor = remote.cursor(buffered=True)

for i, row in seoul_deliver_weather.iterrows():
    cursor.execute(sql, tuple(row))
    #print(tuple(row))

remote.commit()
```

### MySQL 데이터 전처리 및 EDA

**이상치 조회**
```sql
-- hmd, pcp, temp, wind_spd
select hmd from delivery group by hmd order by hmd limit 10;
select pcp from delivery group by pcp order by pcp limit 10;
select temp from delivery group by temp order by temp limit 10;
select wind_spd from delivery group by wind_spd order by wind_spd limit 10;
```
이상치 조회 결과:
- hmd: -998 (3,147건)
- pcp: -998.9 (226건)
- temp: -999 (226건)
- wind_spd: -998.9 (225건)

**이상치 제거**

```sql
-- hmd, wind_spd는 그대로 두고, pcp, temp의 이상치만 제거
delete from delivery
where CAST(pcp AS DECIMAL) = CAST(-998.9 AS DECIMAL);

delete from delivery
where temp = -999;
```

**두 데이터 테이블 병합 및 1차 가공**

- 두 테이블을 연결시킬 수 있는 컬럼은 구(sigu), 날짜(date), 시간(hour_time) 컬럼이다.
- Null은 제외되도록 구, 날짜, 시간 값이 같은 것 기준으로 병합(join)하여 필요한 데이터/컬럼만을 가져와 새 테이블로 생성하였다.
- 이 과정에서, date 값의 월 부분을 파싱하여 월(month) 컬럼을 새로 생성하였고
- 요일(dayweek) 값은 한글로 변경하여 생성하였다.

```sql
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
```

**데이터 2차 가공**
- 시간당 강우량(pcp)과 시간당 기온(temp)은 각각 범위로 구분하여 범주화 한 새로운 컬럼을 생성하였다.

```sql
-- 강우량 범주화하여 새 컬럼으로 추가
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

-- 기온 범주화하여 새 컬럼으로 추가
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
```

### 전처리된 데이터 조회 및 EDA/시각화

- MySQL에서 1차적으로 EDA를 진행하였고,
- 주요 EDA 내용을 Python을 사용하여 시각화 하였다.

```python
import pandas as pd

cursor = remote.cursor(buffered=True)
cursor.execute("select * from delivery")

result = cursor.fetchall()
delivery = pd.DataFrame(result, columns=['date','month','hour','dayweek','dlvr_type','gu','count','rain_type','hmd','pcp','temp','wind_spd','wind_str','pcp_g','temp_g'])
```

**변수간 상관관계**
```python
# 변수간 상관관계
colormap = 'vlag_r' #plt.cm.PuBu
plt.figure(figsize=(6,6))
plt.title("Features Correlation")
mask = np.triu(np.ones_like(delivery.drop(columns='hour').corr(), dtype=np.bool))
sns.heatmap(round(delivery.drop(columns='hour').corr(),4), 
            linewidths=0.1, vmax=1.0, vmin=-1.0, square=True, # square: 정사각형 모양
            mask=mask, cmap=colormap, linecolor='white', annot=True)
plt.yticks(rotation=0)
plt.show();
```

![1](https://user-images.githubusercontent.com/38115693/147908296-578f2779-9f8a-4a6b-8fe4-96ff4e4f5a34.png)

- 변수간 상관관계가 높은 변수들은 없다.


**전체 강우량, 기온, 배달건수 조회**
```python
# 전체 강우량, 기온, 배달건수 데이터 시각화하여 조회
temp = delivery[['date','pcp','temp','count']]

plt.figure(figsize=(20,10))
sns.lineplot(x='date', y='pcp', data=temp, label='pcp')
sns.lineplot(x='date', y='temp', data=temp, label='temp')
sns.lineplot(x='date', y='count', data=temp, label='count')

plt.legend()
plt.show()
```

![2](https://user-images.githubusercontent.com/38115693/147908411-334e42cf-4d46-4c99-ac9e-6d00fbae7e1a.png)

- 1월 ~ 7월까지의 배달건수 추이를 보면, 점차 감소한다. 날이 따듯해질 수록 배달주문이 감소하는 것으라 생각한다.
- 강우량이 높은 시간대엔 배달건수도 증가하는 경향이 보인다.

**월별 총 배달건수**

```python
# 월별 총 배달건수
cursor = remote.cursor(buffered=True)
cursor.execute(
    "select month, sum(count) \
    from delivery \
    group by month")
df = pd.DataFrame(cursor.fetchall(), columns=['month','count'])

sns.barplot(data=df, x='count', y='month', palette='Blues_r');
```
![3](https://user-images.githubusercontent.com/38115693/147908484-8baea441-b683-48f3-8d99-5317c49264e6.png)

- 월별로 보더라도, 날씨가 추운 1월, 2월, 그리고 3월에 배달건수가 가장 많은 것을 볼 수 있다.

**기온별 시간당 평균 배달건수**

```python
# 기온에 따른 시간당 평균 배달건수
cursor = remote.cursor(buffered=True)
cursor.execute(
    "select temp_g, sum(count), avg(count) \
    from delivery \
    group by temp_g")
df = pd.DataFrame(cursor.fetchall(), columns=['temp','count_total' ,'count_avg'])

sns.barplot(data=df.sort_values(by='count_avg', ascending=False), x='count_avg', y='temp', palette='Blues_r');
```
![4](https://user-images.githubusercontent.com/38115693/147908601-02b71f38-2512-4287-97cf-04e702de143f.png)

- 기온별로 보면, 영하 -10도에서 영상 10도 사이일 때, 시간별 평균 배달건수가 가장 높다.
- 영상 10-40도의 경우와 비교하여, 따듯한 때보다는 추운 때에 배달주문이 더 많은 것이라 생각한다.

**강우량별 시간당 평균 배달건수**

```python
# 강우량에 따른 시간당 평균 배달건수
cursor = remote.cursor(buffered=True)
cursor.execute(
    "select pcp_g, sum(count), avg(count) \
    from delivery \
    group by pcp_g")
df = pd.DataFrame(cursor.fetchall(), columns=['pcp','count_total' ,'count_avg'])

sns.barplot(data=df.sort_values(by='count_avg', ascending=False), x='count_avg', y='pcp', palette='Blues_r');
```
- 강우량별로 보면, 시간당 강우량 20-30mm, 2.5-5.0mm, 5.0-10.0mm 등의 순서로 평균 배달건수가 가장 많다.

**날씨별 시간당 평균 배달건수**
```python
# 날씨별 시간당 평균 배달건수
cursor = remote.cursor(buffered=True)
cursor.execute(
    "select rain_type, avg(count) \
    from delivery \
    group by rain_type \
    order by avg(count) desc")
df = pd.DataFrame(cursor.fetchall(), columns=['rain_type','count_avg'])

sns.barplot(data=df.sort_values(by='count_avg', ascending=False), x='count_avg', y='rain_type', palette='Blues_r');
```
![5](https://user-images.githubusercontent.com/38115693/147908837-3b51dabd-b410-427a-aab1-510ad3317b80.png)

- 아무 것도 내리지 않을 때보다, 비 그리고 특히 눈이 내릴 때 시간당 평균 배달건수가 더 많다.

**배달유형별 총 배달건수**
```python
# 배달유형별 총 배달건수
cursor = remote.cursor(buffered=True)
cursor.execute(
    "select dlvr_type, sum(count) \
    from delivery \
    group by dlvr_type \
    order by sum(count) desc")
df = pd.DataFrame(cursor.fetchall(), columns=['dlvr_type','count'])

sns.barplot(data=df, x='count', y='dlvr_type', palette='Blues_r');
```
![6](https://user-images.githubusercontent.com/38115693/147908913-2f7d3db5-7f80-498b-abed-4e5ba7cc6d37.png)

- 배달유형별로는 치킨, 돈까스/일식, 분식, 패스트푸드, 카페/디저트, 한식 등의 순서로 가장 많다.

**요일별 총 배달건수**
```python
# 요일별 총 배달건수
cursor = remote.cursor(buffered=True)
cursor.execute(
    "select dayweek, sum(count), avg(count)\
    from delivery\
    group by dayweek \
    order by sum(count) desc")
df = pd.DataFrame(cursor.fetchall(), columns=['dlvr_type','total_count','avg_count'])

sns.barplot(data=df, x='total_count', y='dlvr_type', palette='Blues_r');
```
![7](https://user-images.githubusercontent.com/38115693/147908961-cedf3d62-ceed-4a3f-a604-650a7d61161a.png)

- 다른 요일에 비해, 금요일과 주말(토툐일, 일요일)에 배달건수가 가장 많다.

---

## 결론

- 날이 추운 1월, 2월, 3월에 다른 달에 비해 배달주문이 더 많으며, 특히 영하 -10도에서 영상 10도 사이에서 시간별 평균 배달건수가 가장 높다.
- 비나 눈이 내릴 때가 아닌 시간과 비교하여 평균 배달주문건수가 더 많은데, 특히 강우량 20-30mm일 때 가장 많은 배달주문이 발생한다.
- 치킨, 돈까스/일식에 대한 배달주문 선호도가 높고, 이어서 분식과 패스트푸드가 높다.
- 금요일과 주말에 가장 많은 배달주문이 발생한다.

select
  date_trunc('month', dep_timestamp) as ym,
  avg(arrdelay) as del
from
  flights
group by
  1

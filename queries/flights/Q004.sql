select
  origin_name,
  dest_name,
  avg(arrdelay)
from
  flights
group by
  origin_name,
  dest_name

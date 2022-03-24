select
  origin_name,
  dest_name,
  avg(arrdelay),
  avg(depdelay),
  avg(arrdelay + depdelay)
from
  flights
group by
  origin_name,
  dest_name

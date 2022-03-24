select
  carrier_name,
  avg(arrdelay)
from
  flights
group by
  carrier_name

select
  carrier_name,
  count(*)
from
  flights
group by
  carrier_name

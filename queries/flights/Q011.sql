select
  uniquecarrier,
  flightnum,
  dep_timestamp,
  dest_lat
from
  flights
where
  origin_name = 'Lambert-St Louis International'
  and flightnum = 586
limit
  5000

SELECT zip, count(*) FROM address
    INNER JOIN (occurred_at
        INNER JOIN crime ON occurred_at.crime_id = crime.id)
        ON address.id = occurred_at.address_id
group by zip
order by zip;
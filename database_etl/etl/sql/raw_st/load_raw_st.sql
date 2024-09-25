insert into st
select
    detection,
    wine,
    vintage,
    sampler,
    samplecode,
    open_date,
    sampled_date,
    added_to_cellartracker,
    notes,
    size
from
    st_loading
anti join
    st_not_in_ct
    on st_loading.samplecode = st_not_in_ct.samplecode
where
    detection = 'raw';

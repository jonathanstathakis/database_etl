create table sequences (
    seq_name varchar primary key,
    dt_run datetime unique,
    seq_desc varchar
);
insert into sequences
with
parsing as (
    select distinct
        seq_name,
        seq_desc,
        seq_name[11] as sep,
        seq_name[-8:-1] as hms,
        seq_name[-19:-10] as ymd,
        case
            when
                seq_name = 'singlesample'
                then
                    null
            else
                cast(concat(ymd, ' ', replace(hms, '-', ':')) as datetime)
        end as dt_run
    from
        chm_loading
)

select
    seq_name,
    dt_run,
    seq_desc
from
    parsing
order by
    dt_run;

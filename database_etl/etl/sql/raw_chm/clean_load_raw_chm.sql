create or replace table chm_loading (
    ch_samplecode varchar primary key,
    st_samplecode varchar unique,
    acq_date datetime unique not null,
    acq_method varchar not null,
    inj_vol float not null,
    seq_name varchar not null,
    seq_desc varchar,
    vialnum varchar,
    originalfilepath varchar,
    id varchar unique not null,
    description varchar,
    );
insert into chm_loading
select
    notebook as ch_samplecode,
    case
        when
            len(notebook) = 4
            and
                notebook[1] = '0'
            and
                (notebook[-1] = '1' or notebook[-1] = '2')
            then
                notebook[2:3]
        when
            notebook = 'mt-diff-bannock-pn'
        then
            'mt-diff-bannockburn-pn'
        when
            notebook = '2021-debortoli-cabernet-merlot_avantor'
        then
            '72'
        when
            notebook = 'stoney-rise-pn_02-21'
        then
            '73'
        when
            notebook = 'crawford-cab_02-21'
        then
            '74'
        when
            notebook = 'hey-malbec_02-21'
        then
            '75'
        when
            notebook = 'koerner-nellucio-02-21'
        then
            '76'
        when
            notebook = 'z3'
        then
            '00'
        when
            notebook = '116'
        then
            'sigurdcb'
        else
            notebook
        end
            as st_samplecode,
    cast(strptime(date::varchar, '%d-%b-%y, %H:%M:%S') as datetime) as acq_date,
    trim(lower(method::varchar)) as acq_method,
    injection_volume::float as inj_vol,
    replace(trim(lower(seq_name::varchar)), 'wines_2023-03-15_11-33-51', '2023-03-15_11:33:51') as seq_name,
    trim(lower(seq_desc::varchar)) as seq_desc,
    vialnum,
    trim(lower(originalfilepath::varchar)) as originalfilepath,
    id::varchar as id,
    trim(lower("desc"::varchar)) as description,
from
    metadata_df;

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

create temp view chm_st_join as
select
    chm.ch_samplecode as samplecode,
    chm.acq_date,
    chm.acq_method,
    chm.inj_vol,
    chm.seq_name,
    chm.seq_desc,
    chm.vialnum,
    chm.originalfilepath,
    chm.id,
    chm.description,
    st.samplecode as st_samplecode
from
    chm_loading as chm
inner join
    st
    on
        chm.st_samplecode = st.samplecode;
select *
from
    chm_st_join;

create table chm (
        samplecode varchar primary key,
        st_samplecode varchar references st(samplecode) not null,
        acq_date datetime unique,
        acq_method varchar not null,
        inj_vol float not null,
        seq_name varchar references sequences(seq_name),
        vialnum varchar not null,
        originalfilepath varchar not null,
        id varchar unique not null,
        description varchar,
    );
insert into chm by name
    select
        samplecode as samplecode,
        st_samplecode as st_samplecode,
        acq_date as acq_date,
        acq_method as acq_method,
        inj_vol as inj_vol,
        seq_name as seq_name,
        vialnum as vialnum,
        originalfilepath as originalfilepath,
        id as id,
        description as description,
    from
        chm_st_join;
drop table chm_loading;
drop view chm_st_join;
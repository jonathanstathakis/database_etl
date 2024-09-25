
create temp view chm_st_join as
select
    chm.ch_runid as runid,
    chm.acq_date,
    chm.acq_method,
    chm.inj_vol,
    chm.seq_name,
    chm.seq_desc,
    chm.vialnum,
    chm.originalfilepath,
    chm.id,
    chm.description,
    st.samplecode as st_samplecode,
    chm.path as path
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
        runid varchar primary key,
        st_samplecode varchar references st(samplecode) not null,
        acq_date datetime unique,
        acq_method varchar not null,
        inj_vol float not null,
        seq_name varchar references sequences(seq_name),
        vialnum varchar not null,
        originalfilepath varchar not null,
        id varchar unique not null,
        description varchar,
        path varchar
    );
insert into chm by name
    select
        runid as runid,
        st_samplecode as st_samplecode,
        acq_date as acq_date,
        acq_method as acq_method,
        inj_vol as inj_vol,
        seq_name as seq_name,
        vialnum as vialnum,
        originalfilepath as originalfilepath,
        id as id,
        description as description,
        path as path,
    from
        chm_st_join;
        
drop table chm_loading;
drop view chm_st_join;
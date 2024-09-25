create table chm_loading (
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
    path varchar
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
        path::varchar
    from
        metadata_df;
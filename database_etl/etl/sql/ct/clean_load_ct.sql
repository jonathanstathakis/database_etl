/*
deprecating pk as primary key in favor of wine, vintage
*/

--create sequence ct_pk_seq start 1;

create or replace table ct (
    --pk integer primary key,
    wine_key varchar unique not null,
    size varchar not null,
    vintage integer not null,
    wine varchar not null,
    locale varchar not null,
    country varchar not null,
    region varchar not null,
    subregion varchar not null,
    appellation varchar not null,
    producer varchar not null,
    type varchar not null,
    color varchar not null,
    category varchar not null,
    varietal varchar not null,
    primary key (wine, vintage)
);
insert into ct
with
ct_loading as (
    select
        --nextval('ct_pk_seq') as pk,
        cast(vintage as integer) as vintage,
        concat(cast(vintage as integer), ' ', trim(lower(wine))) as wine_key,
        trim(lower(size)) as size,
        strip_accents(trim(lower(replace(replace(wine, '''', ''), '"', ''))))
            as wine,
        trim(lower(locale)) as locale,
        trim(lower(country)) as country,
        trim(lower(region)) as region,
        trim(lower(subregion)) as subregion,
        trim(lower(appellation)) as appellation,
        trim(lower(producer)) as producer,
        trim(lower(type)) as type,
        trim(lower(color)) as color,
        trim(lower(category)) as category,
        trim(lower(varietal)) as varietal
    from
        ct_df
),

ct_cleaned_wine as (
    select
        --pk,
        size,
        vintage,
        wine,
        locale,
        country,
        region,
        subregion,
        appellation,
        producer,
        type,
        color,
        category,
        varietal,
        concat(vintage, ' ', wine) as wine_key
    from
        ct_loading
)

select
    --pk,
    wine_key,
    size,
    vintage,
    wine,
    locale,
    country,
    region,
    subregion,
    appellation,
    producer,
    type,
    color,
    category,
    varietal
from
    ct_cleaned_wine;

create temp view st_loading as (
    select
        detection::varchar as detection,
        trim(lower(sampler))::varchar as sampler,
        trim(lower(samplecode))::varchar as samplecode,
        vintage::integer as vintage,
        -- replace null vintages with 9999 so that string slicing operations downstream work
        replace(
            replace(
                strip_accents(trim(lower(wine))), '"', ''
            ), '''', ''
        )::varchar as wine,
        open_date::varchar as open_date,
        sampled_date::varchar as sampled_date,
        coalesce(
            added_to_cellartracker = 'y',
            false
        )::varchar as added_to_cellartracker,
        replace(
            replace(
                strip_accents(trim(lower(notes))), '"', ''
            ), '''', ''
        )::varchar as notes,
        size::varchar as size
    from
        -- set in 'clean_load_raw_st.py'
        read_parquet(getvariable('dirty_st_path'))
);

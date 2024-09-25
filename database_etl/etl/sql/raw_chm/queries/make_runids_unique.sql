with
    new_names as (
        select
            date,
            notebook,
            rank_dense() over (
                partition by notebook order by date) as rank,
            concat(notebook, '_', rank) as new_name
        from
            metadata_df_
        qualify
            rank > 1
                ),
replaced_names as (
select
    coalesce(new.new_name, orig.notebook) as notebook,
    orig.date as date,
    orig.method as method,
    orig.injection_volume as injection_volume,
    orig.seq_name as seq_name,
    orig.seq_desc as seq_desc,
    orig.vialnum as vialnum,
    orig.originalfilepath as originalfilepath,
    orig.id as id,
    orig."desc" as "desc",
    orig.path as path
from
    metadata_df_ orig
left join
    new_names new
on
    orig.date = new.date
and
    orig.notebook = new.notebook)
select
    notebook,
    date,
    method,
    injection_volume,
    seq_name,
    seq_desc,
    vialnum,
    originalfilepath,
    id,
    "desc",
    path
from
    replaced_names 
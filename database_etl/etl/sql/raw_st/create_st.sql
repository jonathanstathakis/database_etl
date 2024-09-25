create table st (
    detection varchar not null,
    wine varchar,
    vintage integer,
    sampler varchar,
    samplecode varchar primary key,
    open_date varchar,
    sampled_date varchar,
    added_to_cellartracker bool,
    notes varchar,
    size float,
    foreign key (wine, vintage) references ct (wine, vintage)
);

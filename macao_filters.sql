-- Does not exist in fc06 and fc21

select 
    fc01.application_number,
    fc01.insured_id,
    fc01.has_decisions_error,
    fc01.decisions_message
from fc01_records fc01
where not exists (
        select 1 from fc06_records fc06
        where fc06.insured_id = fc01.insured_id
    )
  and not exists (
        select 1 from fc21_records fc21
        where fc21.insured_id = fc01.insured_id
    );


-- Does not exist in fc06 but exist in fc21

select 
    fc01.application_number,
    fc01.insured_id,
    fc01.has_decisions_error,
    fc01.decisions_message
from fc01_records fc01
where not exists (
        select 1 from fc06_records fc06
        where fc06.insured_id = fc01.insured_id
    )
  and exists (
        select 1 from fc21_records fc21
        where fc21.insured_id = fc01.insured_id
    );

-- Does not exist in fc21 but exist in fc06

select 
    fc01.application_number,
    fc01.insured_id,
    fc01.has_decisions_error,
    fc01.decisions_message
from fc01_records fc01
where not exists (
        select 1 from fc21_records fc21
        where fc21.insured_id = fc01.insured_id
    )
  and exists (
        select 1 from fc06_records fc06
        where fc06.insured_id = fc01.insured_id
    );



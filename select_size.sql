select
    table_name as `Table`,
    round((data_length + index_length) / 1024 / 1024) as `Size (MB)`
from information_schema.tables
where
    table_name = "patients_automerge"
    or table_name = "patients"
    or table_name = "patients_fn_bidx"
    or table_name = "patients_ln_bidx"
order by (data_length + index_length) desc
;


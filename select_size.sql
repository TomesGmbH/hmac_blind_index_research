select
    table_name as `Table`, (data_length + index_length) / 1024.0 / 1024.0 as `Size (MB)`
from information_schema.tables
where
    table_name = "patients_automerge"
    or table_name = "patients"
    or table_name = "patients_fn_bidx"
    or table_name = "patients_bidx_split"
    or table_name = "patients_bidx_split_unified"
    or table_name = "patients_bidx_unified"
    or table_name = "patients_bidx"
    or table_name = "patients_ln_bidx"
order by (data_length + index_length) desc
;


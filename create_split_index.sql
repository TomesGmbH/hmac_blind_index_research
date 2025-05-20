CREATE TABLE patients_bidx_split (
    pid INT UNSIGNED NOT NULL,
    cid INT UNSIGNED NOT NULL,

    idx_fn_one TINYINT UNSIGNED NOT NULL,
    idx_fn_two SMALLINT UNSIGNED NOT NULL,
    idx_fn_three MEDIUMINT UNSIGNED NOT NULL,
    idx_fn_four MEDIUMINT UNSIGNED NOT NULL,
    idx_fn_five MEDIUMINT UNSIGNED NOT NULL,
    idx_fn_six MEDIUMINT UNSIGNED NOT NULL,

    idx_ln_one TINYINT UNSIGNED NOT NULL,
    idx_ln_two SMALLINT UNSIGNED NOT NULL,
    idx_ln_three MEDIUMINT UNSIGNED NOT NULL,
    idx_ln_four MEDIUMINT UNSIGNED NOT NULL,
    idx_ln_five MEDIUMINT UNSIGNED NOT NULL,
    idx_ln_six MEDIUMINT UNSIGNED NOT NULL,

    CONSTRAINT `fk_bbid_ps` FOREIGN KEY (`pid`) REFERENCES `patients` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_bbid_cids` FOREIGN KEY (`cid`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,


    UNIQUE INDEX `idx_sidx_cid_pid_onefn` (idx_fn_one, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_twofn` (idx_fn_two, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_thrfnee` (idx_fn_three, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_foufnr` (idx_fn_four, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_fivfne` (idx_fn_five, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_sixfn` (idx_fn_six, cid, pid),

    UNIQUE INDEX `idx_sidx_cid_pid_oneln` (idx_ln_one, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_twoln` (idx_ln_two, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_thrlnee` (idx_ln_three, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_foulnr` (idx_ln_four, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_fivlne` (idx_ln_five, cid, pid),
    UNIQUE INDEX `idx_sidx_cid_pid_sixln` (idx_ln_six, cid, pid),

-- to delete all for one patient
    UNIQUE INDEX `idx_idx_pid` (cid, pid)
);


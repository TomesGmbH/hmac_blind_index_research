CREATE TABLE patients_bidx_split_unified (
    pid INT UNSIGNED NOT NULL,
    cid INT UNSIGNED NOT NULL,

    idx_one TINYINT UNSIGNED NOT NULL,
    idx_two SMALLINT UNSIGNED NOT NULL,
    idx_three MEDIUMINT UNSIGNED NOT NULL,
    idx_four MEDIUMINT UNSIGNED NOT NULL,
    idx_five MEDIUMINT UNSIGNED NOT NULL,
    idx_six MEDIUMINT UNSIGNED NOT NULL,

    CONSTRAINT `fk_bbid_pss` FOREIGN KEY (`pid`) REFERENCES `patients` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_bbid_cidss` FOREIGN KEY (`cid`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,


    INDEX `idx_usidx_cid_pid_one` (idx_one, cid, pid),
    INDEX `idx_usidx_cid_pid_two` (idx_two, cid, pid),
    INDEX `idx_usidx_cid_pid_three` (idx_three, cid, pid),
    INDEX `idx_usidx_cid_pid_four` (idx_four, cid, pid),
    INDEX `idx_usidx_cid_pid_five` (idx_five, cid, pid),
    INDEX `idx_usidx_cid_pid_six` (idx_six, cid, pid),

-- to delete all for one patient
    INDEX `idx_idx_pids` (cid, pid)
);


CREATE TABLE patients_ln_bidx (
    pid INT UNSIGNED NOT NULL,
    cid INT NOT NULL,
    ibit TINYINT NOT NULL,
    CONSTRAINT `fk_ln_bid_p` FOREIGN KEY (`pid`) REFERENCES `patients` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_ln_bid_cid` FOREIGN KEY (`cid`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE INDEX `idx_ln_ibit_pid` (ibit, pid),
    UNIQUE INDEX `idx_ln_pid_ibit` (pid, ibit),
    UNIQUE INDEX `idx_ln_ibit_cid_pid` (ibit, cid, pid),
    UNIQUE INDEX `idx_ln_pid_cid_ibit` (pid, cid, ibit),
    UNIQUE INDEX `idx_ln_cid_ibit_pid` (cid, ibit, pid),
    UNIQUE INDEX `idx_ln_ibit_pid_cid` (ibit, pid, cid),
    UNIQUE INDEX `idx_ln_pid_ibit_cid` (pid, ibit, cid),
    UNIQUE INDEX `idx_ln_cid_pid_ibit` (cid, pid, ibit)
);


CREATE TABLE patients_bidx (
    pid INT UNSIGNED NOT NULL,
    cid INT UNSIGNED NOT NULL,
    lnidx MEDIUMINT UNSIGNED NOT NULL,
    fnidx MEDIUMINT UNSIGNED NOT NULL,
    CONSTRAINT `fk_bid_p` FOREIGN KEY (`pid`) REFERENCES `patients` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_bid_cid` FOREIGN KEY (`cid`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,


    UNIQUE INDEX `idx_fnidx_lnidx_cid_pid` (fnidx,lnidx, cid, pid),

-- to delete all for one patient
    index `idx_pid` (pid)
);


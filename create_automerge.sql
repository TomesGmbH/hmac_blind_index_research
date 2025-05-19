CREATE TABLE patients_automerge (
    pid INT UNSIGNED NOT NULL,
    cid INT NOT NULL,
    automerge_full_name_dob BINARY(32) NOT NULL,
    automerge_email_dob BINARY(32) DEFAULT NULL,
    PRIMARY KEY (pid),  
    CONSTRAINT `fk_patient_auto_merge_patient_id` FOREIGN KEY (`pid`) REFERENCES `patients` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_patient_auto_merge_cid` FOREIGN KEY (`cid`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
-- Indexes for optimized searching
-- automerge_index
    INDEX `idx_automerge_email` (cid, automerge_email_dob),
    UNIQUE INDEX `idx_automerge_name` (cid, automerge_full_name_dob),
    INDEX `idx_automerge_hashes` (cid, automerge_full_name_dob, automerge_email_dob)
);


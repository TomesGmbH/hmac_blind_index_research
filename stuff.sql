EXPLAIN
select p.pid from tbo_patients as p 
INNER JOIN tbo_patients_bfn as fb1 ON fb1.pid = p.pid  AND fb1.cid = p.cid
INNER JOIN tbo_patients_bfn as fb2 ON fb2.pid = fb1.pid AND fb2.cid = fb1.cid
INNER JOIN tbo_patients_bfn as fb4 ON fb4.pid = fb2.pid AND fb4.cid = fb2.cid
INNER JOIN tbo_patients_bfn as fb5 ON fb5.pid = fb4.pid AND fb5.cid = fb4.cid 
INNER JOIN tbo_patients_bfn as fb6 ON fb6.pid = fb5.pid AND fb6.cid = fb5.cid
INNER JOIN tbo_patients_bln as lb1 ON lb1.pid = p.pid  AND lb1.cid = p.cid
INNER JOIN tbo_patients_bln as lb2 ON lb2.pid = lb1.pid AND lb2.cid = lb1.cid
INNER JOIN tbo_patients_bln as lb4 ON lb4.pid = lb2.pid AND lb4.cid = lb2.cid
INNER JOIN tbo_patients_bln as lb5 ON lb5.pid = lb4.pid AND lb5.cid = lb4.cid 
INNER JOIN tbo_patients_bln as lb6 ON lb6.pid = lb5.pid AND lb6.cid = lb5.cid
LEFT JOIN tbo_patients_bfn as fb3 ON fb3.pid = fb6.pid AND fb3.cid = fb6.cid AND fb3.ibit = 3
LEFT JOIN tbo_patients_bfn as fb7 ON fb7.pid = fb3.pid AND fb7.cid = fb3.cid AND fb7.ibit = 7
LEFT JOIN tbo_patients_bfn as fb8 ON fb8.pid = fb7.pid AND fb8.cid = fb7.cid AND fb8.ibit = 8
LEFT JOIN tbo_patients_bfn as fb9 ON fb9.pid = fb8.pid AND fb9.cid = fb8.cid AND fb9.ibit = 9
LEFT JOIN tbo_patients_bln as lb3 ON lb3.pid = lb6.pid AND lb3.cid = lb6.cid AND lb3.ibit = 3
LEFT JOIN tbo_patients_bln as lb7 ON lb7.pid = lb3.pid AND lb7.cid = lb3.cid AND lb7.ibit = 7
LEFT JOIN tbo_patients_bln as lb8 ON lb8.pid = lb7.pid AND lb8.cid = lb7.cid AND lb8.ibit = 8
LEFT JOIN tbo_patients_bln as lb9 ON lb9.pid = lb8.pid AND lb9.cid = lb8.cid AND lb9.ibit = 9
WHERE p.cid = 12 AND (
fb1.ibit = 1 AND fb2.ibit = 2 AND fb4.ibit =4 AND fb5.ibit = 5 AND fb6.ibit = 6 
AND fb3.pid is null AND fb7.pid is null AND fb8.pid is null AND fb9.pid is null
) OR (
  lb1.ibit = 1 AND lb2.ibit = 2 AND lb4.ibit =4 AND lb5.ibit = 5 AND lb6.ibit = 6 
  AND lb3.pid is null AND lb7.pid is null AND lb8.pid is null AND lb9.pid is null
);


EXPLAIN
select p.pid from tbo_patients as p 
INNER JOIN tbo_patients_bfn as b1 ON b1.pid = p.pid AND b1.cid = p.cid
INNER JOIN tbo_patients_bfn as b2 ON b2.pid = b1.pid AND b2.cid = b1.cid
INNER JOIN tbo_patients_bfn as b4 ON b4.pid = b2.pid AND b4.cid = b2.cid
INNER JOIN tbo_patients_bfn as b5 ON b5.pid = b4.pid AND b5.cid = b4.cid 
INNER JOIN tbo_patients_bfn as b6 ON b6.pid = b5.pid AND b6.cid = b5.cid
LEFT JOIN tbo_patients_bfn as b3 ON b3.pid = b6.pid AND b3.cid = b6.cid AND b3.ibit = 3
LEFT JOIN tbo_patients_bfn as b7 ON b7.pid = b3.pid AND b7.cid = b3.cid AND b7.ibit = 7
LEFT JOIN tbo_patients_bfn as b8 ON b8.pid = b7.pid AND b8.cid = b7.cid AND b8.ibit = 8
LEFT JOIN tbo_patients_bfn as b9 ON b9.pid = b8.pid AND b9.cid = b8.cid AND b9.ibit = 9
WHERE p.cid = 12 AND 
b1.ibit = 1 AND b2.ibit = 2 AND b4.ibit =4 AND b5.ibit = 5 AND b6.ibit = 6 
AND b3.pid is null AND b7.pid is null AND b8.pid is null AND b9.pid is null
UNION ALL
select p.pid from tbo_patients as p 
INNER JOIN tbo_patients_bln as b1 ON b1.pid = p.pid AND b1.cid = p.cid
INNER JOIN tbo_patients_bln as b2 ON b2.pid = b1.pid AND b2.cid = b1.cid
INNER JOIN tbo_patients_bln as b4 ON b4.pid = b2.pid AND b4.cid = b2.cid
INNER JOIN tbo_patients_bln as b5 ON b5.pid = b4.pid AND b5.cid = b4.cid 
INNER JOIN tbo_patients_bln as b6 ON b6.pid = b5.pid AND b6.cid = b5.cid
LEFT JOIN tbo_patients_bln as b3 ON b3.pid = b6.pid AND b3.cid = b6.cid AND b3.ibit = 3
LEFT JOIN tbo_patients_bln as b7 ON b7.pid = b3.pid AND b7.cid = b3.cid AND b7.ibit = 7
LEFT JOIN tbo_patients_bln as b8 ON b8.pid = b7.pid AND b8.cid = b7.cid AND b8.ibit = 8
LEFT JOIN tbo_patients_bln as b9 ON b9.pid = b8.pid AND b9.cid = b8.cid AND b9.ibit = 9
WHERE p.cid = 12 AND 
b1.ibit = 1 AND b2.ibit = 2 AND b4.ibit =4 AND b5.ibit = 5 AND b6.ibit = 6 
AND b3.pid is null AND b7.pid is null AND b8.pid is null AND b9.pid is null;

EXPLAIN SELECT p.pid FROM tbo_patients as p 
INNER JOIN tbo_patients_bfn AS b1 ON b1.pid = p.pid
INNER JOIN tbo_patients_bfn AS b2 ON b2.pid = b1.pid
INNER JOIN tbo_patients_bfn AS b4 ON b4.pid = b2.pid
INNER JOIN tbo_patients_bfn AS b5 ON b5.pid = b4.pid
INNER JOIN tbo_patients_bfn AS b6 ON b6.pid = b5.pid
LEFT JOIN tbo_patients_bfn AS b3 ON b3.pid = b6.pid AND b3.ibit = 3
LEFT JOIN tbo_patients_bfn AS b7 ON b7.pid = b3.pid AND b7.ibit = 7
LEFT JOIN tbo_patients_bfn AS b8 ON b8.pid = b7.pid AND b8.ibit = 8
LEFT JOIN tbo_patients_bfn AS b9 ON b9.pid = b8.pid AND b9.ibit = 9
WHERE p.cid = 12 AND 
b1.ibit = 1 AND b2.ibit = 2 AND b4.ibit =4 AND b5.ibit = 5 AND b6.ibit = 6 
AND b3.pid is null AND b7.pid is null AND b8.pid is null AND b9.pid is null;

CREATE TABLE IF NOT EXISTS
  tbo_patients ( 
    pid SERIAL PRIMARY KEY,
    cid int,
    index `idx_p_pid_cid` (pid,cid), 
    index `idx_p_cid_pid` (cid,pid) 
  );
CREATE TABLE IF NOT EXISTS
  tbo_patients_bfn (
    pid bigint unsigned NOT NULL,
    cid int NOT NULL,
    ibit tinyint NOT NULL,
    CONSTRAINT `fk_fn_bid_p` FOREIGN KEY (`pid`) REFERENCES `tbo_patients` (`pid`) ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX  `idx_fn_ibit` (ibit),
    INDEX `idx_fn_cid` (cid),
    INDEX `idx_fn_pid` (pid),
    UNIQUE INDEX `idx_fn_ibit_pid` (ibit, pid),
    UNIQUE INDEX `idx_fn_pid_ibit` (pid, ibit),
    UNIQUE INDEX `idx_fn_ibit_cid_pid` (ibit, cid, pid),
    UNIQUE INDEX `idx_fn_pid_cid_ibit` (pid, cid, ibit),
    UNIQUE INDEX `idx_fn_cid_ibit_pid` (cid, ibit, pid),
    UNIQUE INDEX `idx_fn_ibit_pid_cid` (ibit, pid, cid),
    UNIQUE INDEX `idx_fn_pid_ibit_cid` (pid, ibit, cid),
    UNIQUE INDEX `idx_fn_cid_pid_ibit` (cid, pid, ibit)
  );
CREATE TABLE IF NOT EXISTS
  tbo_patients_bln (
    pid bigint unsigned NOT NULL,
    cid int NOT NULL,
    ibit tinyint NOT NULL,
    CONSTRAINT `fk_ln_bid_p` FOREIGN KEY (`pid`) REFERENCES `tbo_patients` (`pid`) ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX  `idx_ln_ibit` (ibit),
    INDEX `idx_ln_cid` (cid),
    INDEX `idx_ln_pid` (pid),
    UNIQUE INDEX `idx_ln_ibit_pid` (ibit, pid),
    UNIQUE INDEX `idx_ln_pid_ibit` (pid, ibit),
    UNIQUE INDEX `idx_ln_ibit_cid_pid` (ibit, cid, pid),
    UNIQUE INDEX `idx_ln_pid_cid_ibit` (pid, cid, ibit),
    UNIQUE INDEX `idx_ln_cid_ibit_pid` (cid, ibit, pid),
    UNIQUE INDEX `idx_ln_ibit_pid_cid` (ibit, pid, cid),
    UNIQUE INDEX `idx_ln_pid_ibit_cid` (pid, ibit, cid),
    UNIQUE INDEX `idx_ln_cid_pid_ibit` (cid, pid, ibit)
  );

INSERT INTO tbo_patients (cid) values (11),(11),(19),(19),(18),(11),(15),(11),(15),(12),(12);

INSERT INTO tbo_patients_bfn (pid, cid, ibit) values 
(1,  11, 0),(1,  11, 1),(1,  11, 3),(1,  11, 8),
(2,  11, 0),(2,  11, 1),(2,  11, 2),(2,  11, 6),(2, 11, 9),
(3,  19, 2),(3,  19, 3),(3,  19, 4),
(4,  19, 1),(4,  11, 8),(4,  11, 2),(4,  11, 6),(4,  11, 3),
(5,  18, 1),(5,  11, 4),(5,  11, 3),(5,  11, 6),(5,  11, 9),
(6,  11, 0),(6,  11, 9),(6,  11, 2),(6,  11, 6),(6,  11, 8),
(7,  15, 3),(7,  11, 1),(7,  11, 4),(7,  11, 6),(7,  11, 9),
(8,  11, 1),(8,  11, 8),(8,  11, 2),(8,  11, 6),(8,  11, 5),
(9,  15, 2),(9,  11, 9),(9,  11, 3),(9,  11, 4),(9,  11, 5),
(10, 12, 1),(10, 11, 4),(10, 11, 5),(10, 11, 6),(10, 11, 9),
(11, 12, 1),(11, 11, 2),(11, 11, 4),(11, 11, 6),(11, 11, 5);

INSERT INTO tbo_patients_bln (pid, cid, ibit) values 
(1,  11, 4),(1,  11, 1),(1,  11, 3),(1,  11, 8),
(2,  11, 4),(2,  11, 1),(2,  11, 2),(2,  11, 6),(2, 11, 9),
(3,  19, 1),(3,  19, 3),(3,  19, 4),
(4,  19, 0),(4,  11, 8),(4,  11, 2),(4,  11, 6),(4,  11, 3),
(5,  18, 0),(5,  11, 4),(5,  11, 3),(5,  11, 6),(5,  11, 9),
(6,  11, 1),(6,  11, 9),(6,  11, 2),(6,  11, 6),(6,  11, 8),
(7,  15, 7),(7,  11, 1),(7,  11, 4),(7,  11, 6),(7,  11, 9),
(8,  11, 9),(8,  11, 8),(8,  11, 2),(8,  11, 6),(8,  11, 5),
(9,  15, 2),(9,  11, 9),(9,  11, 3),(9,  11, 4),(9,  11, 5),
(10, 12, 2),(10, 11, 3),(10, 11, 5),(10, 11, 6),(10, 11, 9),
(11, 12, 1),(11, 11, 2),(11, 11, 4),(11, 11, 6),(11, 11, 8);

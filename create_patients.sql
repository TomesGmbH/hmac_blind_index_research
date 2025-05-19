CREATE TABLE patients (
-- plaintext
    id INT UNSIGNED AUTO_INCREMENT,
    cid INT NOT NULL,
    emr_pid VARCHAR(50) NOT NULL,
    gender ENUM('male','female','unknown','other') default 'unknown' NOT NULL,

-- cle
    dob DATE NOT NULL,
    email VARCHAR(90) DEFAULT NULL,
    phone VARCHAR(20) DEFAULT NULL,
    mobile VARCHAR(20) DEFAULT NULL,

-- e2ee
    first_name VARCHAR(64) NOT NULL,
    last_name VARCHAR(64) NOT NULL,
    title VARCHAR(64) DEFAULT NULL,

    address_country CHAR(3) DEFAULT NULL,
    address_postal_code CHAR(10) DEFAULT NULL,
    address_state VARCHAR(50) DEFAULT NULL,
    address_city VARCHAR(50) DEFAULT NULL,
    address_line VARCHAR(200) DEFAULT NULL,

    gp_name VARCHAR(200) DEFAULT NULL,
    gp_address_country CHAR(3) DEFAULT NULL,
    gp_address_postal_code CHAR(10) DEFAULT NULL,
    gp_address_state VARCHAR(50) DEFAULT NULL,
    gp_address_city VARCHAR(50) DEFAULT NULL,
    gp_address_line VARCHAR(200) DEFAULT NULL,

    insurance_status TINYINT DEFAULT NULL,

-- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

-- Basic indexes for performance
-- for listing patients
    INDEX idx_patients_cid (cid),
-- for updating patient data from the EMR
    INDEX idx_patients_cid_emr_pid (cid, emr_pid),

    CONSTRAINT fk_patients_cid FOREIGN KEY (cid) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE CASCADE
);


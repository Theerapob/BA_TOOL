CREATE TABLE hrEmployee (
    EmployeeID   varchar(36)   COLLATE Thai_CI_AS NOT NULL,
    FirstName    nvarchar(100) COLLATE Thai_CI_AS NOT NULL,
    LastName     nvarchar(100) COLLATE Thai_CI_AS NOT NULL,
    Email        nvarchar(150) NULL,
    Department   nvarchar(100) NULL,
    Position     nvarchar(100) NULL,
    HireDate     date          NULL,
    IsActive     tinyint       NOT NULL DEFAULT 1,

    PRIMARY KEY (EmployeeID)
);

CREATE TABLE hrPayroll_Details (
    PayrollID              uniqueidentifier  NOT NULL DEFAULT NEWID(),
    EmployeeID             varchar(36)       COLLATE Thai_CI_AS NOT NULL,
    BaseSalary             money             NULL,
    TaxRate                decimal(5, 2)     NULL,
    SocialSecurityFund     decimal(18, 4)    NULL,
    OT_Rate                float             NULL,
    PaymentMethod          int               NOT NULL,
    IsTaxExempt            tinyint           DEFAULT 0,
    LastPaymentTimestamp   datetimeoffset(7) NULL,
    PayrollNote            ntext             COLLATE Thai_CI_AS NULL,

    PRIMARY KEY (PayrollID),
    FOREIGN KEY (EmployeeID) REFERENCES hrEmployee(EmployeeID)
);
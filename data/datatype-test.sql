CREATE TABLE hrBranch_Locations (
    BranchID int PRIMARY KEY,
    BranchName nvarchar(200) COLLATE Thai_CI_AS NOT NULL,
    BranchCode varchar(10) COLLATE Thai_CI_AS NULL,
    LocationGPS geography NULL, 
    OrganizationPath hierarchyid NULL, 
    OperatingHours xml NULL, 
    MainContactJSON nvarchar(max) COLLATE Thai_CI_AS NULL,
    RowVersion timestamp NOT NULL 
);
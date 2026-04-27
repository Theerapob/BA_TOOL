CREATE TABLE hrEmployee_Documents (
    DocumentID bigint IDENTITY(1,1) NOT NULL, 
    EmployeeID varchar(36) COLLATE Thai_CI_AS NOT NULL,
    DocumentName nvarchar(255) COLLATE Thai_CI_AS NOT NULL,
    DocumentType char(5) COLLATE Thai_CI_AS NULL, 
    FileContent varbinary(max) NULL, 
    UploadDate datetime2(3) NULL, 
    IsVerified bit NOT NULL DEFAULT 0,
    DigitalSignature image NULL 
);
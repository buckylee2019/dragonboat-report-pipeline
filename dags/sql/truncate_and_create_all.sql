
DROP TABLE IF EXISTS CONTACTS CASCADE;
CREATE TABLE CONTACTS (
   MEMBER_ID VARCHAR(20) NOT NULL PRIMARY KEY,  
   CHNAME VARCHAR(20),    
   ENGNAME VARCHAR(20) NOT NULL,  
   ID VARCHAR(20),      -- Changed to VARCHAR(20)
   BIRTHDAY DATE,         
   PHONE VARCHAR(20),
   IBMER VARCHAR(1),
   COMPETITION VARCHAR(2)
);  

DROP TABLE IF EXISTS attendances CASCADE;
CREATE TABLE attendances (
   MEMBER_ID VARCHAR(20) NOT NULL,
   PRACTICE_DATE DATE NOT NULL,
   CONSTRAINT FK_attendances FOREIGN KEY (MEMBER_ID) REFERENCES CONTACTS(MEMBER_ID),
   PRIMARY KEY (MEMBER_ID, PRACTICE_DATE)
);

DROP TABLE IF EXISTS memberfee CASCADE;
CREATE TABLE memberfee (
   MEMBER_ID VARCHAR(20) NOT NULL,
   PAID_DATE DATE NOT NULL,
   CONSTRAINT FK_memberfee FOREIGN KEY (MEMBER_ID) REFERENCES CONTACTS(MEMBER_ID),
   PRIMARY KEY (MEMBER_ID, PAID_DATE)
);

DROP TABLE IF EXISTS item CASCADE;
CREATE TABLE item (
   EXPENSE_NAME VARCHAR(50) NOT NULL,
   AMOUNT INTEGER NOT NULL,
   MEMBER VARCHAR(20) NOT NULL,
   PRIMARY KEY (EXPENSE_NAME, AMOUNT,MEMBER)
);


DROP TABLE IF EXISTS EXPENSES CASCADE;
CREATE TABLE EXPENSES (
    CHNAME VARCHAR(50), 
    MEMBER_ID VARCHAR(20),
    EXPENSE_NAME VARCHAR(50), 
    QTY FLOAT,
    UPDATE_DATE DATE,
    CONSTRAINT FK_EXPENSES FOREIGN KEY (MEMBER_ID) REFERENCES CONTACTS(MEMBER_ID)
);

DROP TABLE IF EXISTS PAYMENT CASCADE;
CREATE TABLE PAYMENT ( 
   MEMBER_ID VARCHAR(20),
   DESCRIPTION VARCHAR(30),
   PAID_AMOUNT FLOAT, 
   PAYMENT_DATE DATE,
   CONSTRAINT FK_PAYMENT FOREIGN KEY (MEMBER_ID) REFERENCES CONTACTS(MEMBER_ID)
);

DROP TABLE IF EXISTS TIMEOFF CASCADE;
CREATE TABLE TIMEOFF(
    ENGNAME VARCHAR(20) NOT NULL,
    TIMEOFF_DATE DATE,
    REASON VARCHAR(2)
)
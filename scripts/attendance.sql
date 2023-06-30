 SELECT c.MEMBER_ID,c.CHNAME,c.ENGNAME,c.IBMER,a.PRACTICE_DATE
    FROM contacts c
    LEFT join attendances a on c.MEMBER_ID=a.MEMBER_ID
    
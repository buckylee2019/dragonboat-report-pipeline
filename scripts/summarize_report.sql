WITH expense_sum (MEMBER_ID, EXPENSE_NAME, QTY) AS (
    -- CTE query definition
    SELECT MEMBER_ID, EXPENSE_NAME, SUM(QTY)
    FROM EXPENSES
    GROUP BY MEMBER_ID, EXPENSE_NAME
    -- Additional clauses if needed
),
attendance_count (MEMBER_ID, QTY, EXPENSE_NAME) AS (
    -- CTE query definition
    SELECT a.MEMBER_ID as MEMBER_ID, COUNT(a.PRACTICE_DATE) as QTY, '練習費' AS EXPENSE_NAME
    FROM ATTENDANCES a
    GROUP BY a.MEMBER_ID
    -- Additional clauses if needed
),
total_expense AS (
    -- CTE combining expense_sum and attendance_count
    SELECT MEMBER_ID, EXPENSE_NAME, QTY
    FROM expense_sum
    UNION ALL
    SELECT MEMBER_ID, EXPENSE_NAME, QTY
    FROM attendance_count
)
,
sum_report as (
SELECT c.MEMBER_ID,
       c.CHNAME,
       c.ENGNAME,
       c.IBMer,
       CASE
           WHEN m.PAID_DATE IS NOT NULL AND YEAR(m.PAID_DATE) = YEAR(CURRENT_DATE) THEN '社員'
           WHEN m.PAID_DATE IS NULL THEN '非社員'
       END AS membership,
       SUM(CASE 
       when c.ibmer = '1' and i.EXPENSE_NAME = '練習費' then (es.QTY-1) * i.amount
       when c.ibmer = '0' and i.EXPENSE_NAME = '練習費' then es.QTY * i.amount
       else es.qty * i.amount end) AS TOTAL_AMOUNT
       
FROM Contacts c
LEFT JOIN MemberFee m ON c.MEMBER_ID = m.MEMBER_ID
LEFT JOIN total_expense es ON c.MEMBER_ID = es.MEMBER_ID
LEFT JOIN item i ON es.EXPENSE_NAME = i.EXPENSE_NAME
WHERE (m.PAID_DATE IS NOT NULL AND YEAR(m.PAID_DATE) = YEAR(CURRENT_DATE) AND i.member = '社員')
   OR (m.PAID_DATE IS NULL AND i.member = '非社員')
 group by c.MEMBER_ID, c.CHNAME, c.ENGNAME, c.IBMer,m.PAID_DATE
 )
select MA.MEMBER_ID,MA.CHNAME,MA.ENGNAME,MA.IBMer,MA.membership,MA.TOTAL_AMOUNT, SUM(PAID_AMOUNT) AS PAID_AMOUNT
from sum_report MA LEFT JOIN PAYMENT p ON MA.MEMBER_ID = p.MEMBER_ID
GROUP BY MA.MEMBER_ID,MA.CHNAME,MA.ENGNAME,MA.IBMer,MA.membership,MA.TOTAL_AMOUNT

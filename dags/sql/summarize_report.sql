WITH expense_sum AS (
    SELECT MEMBER_ID, EXPENSE_NAME, SUM(QTY) AS QTY
    FROM EXPENSES
    GROUP BY MEMBER_ID, EXPENSE_NAME
),
attendance_count AS (
    SELECT a.MEMBER_ID, COUNT(a.PRACTICE_DATE) AS QTY, '練習費' AS EXPENSE_NAME
    FROM ATTENDANCES a
    GROUP BY a.MEMBER_ID
),
total_expense AS (
    SELECT MEMBER_ID, EXPENSE_NAME, QTY
    FROM expense_sum
    UNION ALL
    SELECT MEMBER_ID, EXPENSE_NAME, QTY
    FROM attendance_count
),
sum_report AS (
    SELECT
        c.MEMBER_ID,
        c.CHNAME,
        c.ENGNAME,
        c.IBMer,
        CASE
            WHEN m.PAID_DATE IS NOT NULL AND EXTRACT(YEAR FROM m.PAID_DATE) = EXTRACT(YEAR FROM CURRENT_DATE) THEN '社員'
            WHEN m.PAID_DATE IS NULL THEN '非社員'
        END AS membership,
        SUM(CASE
            WHEN c.ibmer = '1' AND i.EXPENSE_NAME = '練習費' THEN (es.QTY-1) * i.amount
            WHEN c.ibmer = '0' AND i.EXPENSE_NAME = '練習費' THEN es.QTY * i.amount
            ELSE es.QTY * i.amount
        END) AS TOTAL_AMOUNT
    FROM Contacts c
    LEFT JOIN MemberFee m ON c.MEMBER_ID = m.MEMBER_ID
    LEFT JOIN total_expense es ON c.MEMBER_ID = es.MEMBER_ID
    LEFT JOIN item i ON es.EXPENSE_NAME = i.EXPENSE_NAME
    WHERE (m.PAID_DATE IS NOT NULL AND EXTRACT(YEAR FROM m.PAID_DATE) = EXTRACT(YEAR FROM CURRENT_DATE) AND i.member = '社員')
        OR (m.PAID_DATE IS NULL AND i.member = '非社員')
    GROUP BY c.MEMBER_ID, c.CHNAME, c.ENGNAME, c.IBMer, m.PAID_DATE
)
SELECT
    MA.MEMBER_ID,
    MA.CHNAME,
    MA.ENGNAME,
    MA.IBMer,
    MA.membership,
    MA.TOTAL_AMOUNT,
    SUM(PAID_AMOUNT) AS PAID_AMOUNT
FROM sum_report MA
LEFT JOIN PAYMENT p ON MA.MEMBER_ID = p.MEMBER_ID
GROUP BY MA.MEMBER_ID, MA.CHNAME, MA.ENGNAME, MA.IBMer, MA.membership, MA.TOTAL_AMOUNT;

SELECT company_name, deadline, job_title, recruit_url, crawling_time, 'saramin' AS source_table
FROM (
    SELECT company_name, deadline, job_title, recruit_url, keywords, experience, crawling_time,
           LAG(company_name, 1) OVER (ORDER BY crawling_time DESC) AS prev_company
    FROM saramin
    WHERE (STR_TO_DATE(saramin.deadline, '%Y-%m-%d') > CURDATE() OR saramin.deadline = '채용시 마감')
        AND (saramin.experience = "신입" OR saramin.experience = "무관(신입포함)")
    ORDER BY crawling_time DESC
) AS saramin_data
WHERE ({saramin})
AND crawling_time >= DATE_SUB(CURDATE(), INTERVAL 2 MONTH)
AND (saramin_data.job_title NOT REGEXP 'pm[0-9]' AND saramin_data.job_title NOT REGEXP '[0-9]pm')
AND (company_name != prev_company)
UNION ALL
-- SELECT company_name, deadline, job_title, recruit_url, created_at, 'incruit' AS source_table
-- FROM (
--     SELECT company_name, CASE 
--         WHEN deadline = '-' THEN '상시' 
--         ELSE deadline 
--     END AS deadline, job_title, recruit_url, keywords, experience, created_at,
--            LAG(company_name, 1) OVER (ORDER BY created_at DESC) AS prev_company
--     FROM incruit
--     WHERE (STR_TO_DATE(incruit.deadline, '%Y-%m-%d') > CURDATE() OR incruit.deadline = '상시' OR incruit.deadline = '채용시')
--         AND (incruit.experience LIKE "신입%" OR incruit.experience = "신입/경력(연차무관)")
--     ORDER BY created_at DESC
-- ) AS incruit_data
-- WHERE ({incruit})
-- AND created_at >= DATE_SUB(CURDATE(), INTERVAL 2 MONTH)
-- AND (incruit_data.job_title NOT REGEXP 'pm[0-9]' AND incruit_data.job_title NOT REGEXP '[0-9]pm')
-- AND (company_name != prev_company)
-- UNION ALL
-- SELECT company_name, deadline, job_title, recruit_url, `date`, 'wanted' AS source_table
-- FROM (
--     SELECT company_name, deadline, job_title, recruit_url, keywords, career, `date`,
--            LAG(company_name, 1) OVER (ORDER BY `date` DESC) AS prev_company
--     FROM wanted
--     WHERE (STR_TO_DATE(wanted.deadline, '%Y-%m-%d') > CURDATE() OR wanted.deadline = '상시채용')
--         AND (wanted.career LIKE "신입%")
--     ORDER BY `date` DESC
-- ) AS wanted_data
-- WHERE ({wanted})
-- AND date >= DATE_SUB(CURDATE(), INTERVAL 2 MONTH)
-- AND (wanted_data.job_title NOT REGEXP 'pm[0-9]' AND wanted_data.job_title NOT REGEXP '[0-9]pm')
-- AND (company_name != prev_company)
-- UNION ALL
SELECT company_name, deadline, job_title, recruit_url, `date`, 'jumpit' AS source_table
FROM (
    SELECT company_name, deadline, job_title, recruit_url, keywords, career, `date`,
           LAG(company_name, 1) OVER (ORDER BY `date` DESC) AS prev_company
    FROM jumpit
    WHERE (STR_TO_DATE(jumpit.deadline, '%Y-%m-%d') > CURDATE() OR jumpit.deadline = '상시')
        AND (jumpit.career LIKE "신입%")
    ORDER BY `date` DESC
) AS jumpit_data
WHERE ({jumpit})
AND date >= DATE_SUB(CURDATE(), INTERVAL 2 MONTH)
AND (jumpit_data.job_title NOT REGEXP 'pm[0-9]' AND jumpit_data.job_title NOT REGEXP '[0-9]pm')
AND (company_name != prev_company);
DELETE FROM {{ params.schema }}.{{ params.target_table }}
WHERE created_at IN
(SELECT DISTINCT(created_at) 
FROM {{ params.schema }}.{{ params.stage_table }});
INSERT INTO {{ params.schema }}.{{ params.target_table }}
SELECT * FROM {{ params.schema }}.{{ params.stage_table }};
TRUNCATE TABLE {{ params.schema }}.{{ params.stage_table }};
SELECT DISTINCT c1910."index" as index1910, c1920."index" as index1920 
INTO true_matches_1910_1920
FROM ark_table a1
INNER JOIN ark_table a2
ON a1.pseudo_person = a2.pseudo_person
AND a2.ark_source='ark1920'
AND a1.ark_source='ark1910'
INNER JOIN compiled_1910 c1910
ON c1910.ark1910 = a1.ark 
INNER JOIN compiled_1920 c1920 
ON c1920.ark1920 = a2.ark

SELECT COUNT(*) FROM true_matches_1910_1920


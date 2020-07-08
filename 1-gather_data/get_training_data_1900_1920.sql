/*SELECT x.index1900,x.index1910,true_index_1910
from (*/

SELECT s.ark1900, c1920.ark1920, true_ark_1920

FROM
   (
      SELECT 
         ark1 as ark1900,
         ark2 as ark1920,
         ark2 as true_ark_1920 
      FROM
         ark_cw_1900_1920 tablesample(0.61 percent)
   )
   AS s 
   INNER JOIN 
	compiled_1900 c1900
	ON c1900.ark1900=s.ark1900
   LEFT JOIN
      compares_1900_1920_v2 as c 
      on c1900."index" = c.index1900
	INNER join 
		compiled_1920 c1920 
		ON c1920."index" = c.index1920
where
   c.index1900 is not null
order by c.index1900


/*SELECT x.index1900,x.index1910,true_index_1910
from (*/

SELECT s.ark1910, c1920.ark1920, true_ark_1920
   --c.index1910,
   --c.index1920,
   --true_index_1920 
FROM
   (
      SELECT 
         ark1 as ark1910,
         ark2 as ark1920,
         ark2 as true_ark_1920 
      FROM
         ark_cw_1910_1920 tablesample(0.2 percent)
   )
   AS s 
   INNER JOIN 
	compiled_1910 c1910
	ON c1910.ark1910=s.ark1910
   LEFT JOIN
      compares_1910_1920_v2 as c 
      on c1910."index" = c.index1910
	INNER join 
		compiled_1920 c1920 
		ON c1920."index" = c.index1920
where
   c.index1910 is not null
order by c.index1910


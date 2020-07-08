/*SELECT x.index1900,x.index1910,true_index_1910
from (*/

SELECT s.ark1900, c1910.ark1910, true_ark_1910

FROM
   (
      SELECT 
         ark1 as ark1900,
         ark2 as ark1910,
         ark2 as true_ark_1910 
      FROM
         ark_cw_1900_1910 tablesample(0.31 percent)
   )
   AS s 
   INNER JOIN 
	compiled_1900 c1900
	ON c1900.ark1900=s.ark1900
   LEFT JOIN
      compares_1900_1910_v2 as c 
      on c1900."index" = c.index1900
	INNER join 
		compiled_1910 c1910 
		ON c1910."index" = c.index1910
where
   c.index1900 is not null
order by c.index1900


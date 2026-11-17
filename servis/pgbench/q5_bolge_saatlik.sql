-- Bölgenin saatlik özeti (Toplu 1)
\set b random(1, 7)
SELECT * FROM toplu1
WHERE bolge = (ARRAY['Marmara','Ege','Akdeniz','İç Anadolu','Karadeniz','Doğu Anadolu','Güneydoğu'])[:b]
ORDER BY saat_tr;

USE ovchinnikovada;

SELECT 
    content.userInn as userInn
    , SUM(content.totalsum) as totalsum
FROM kkt_document_json_textfile
GROUP BY content.userInn
ORDER BY totalsum DESC
LIMIT 1;


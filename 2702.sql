SELECT DISTINCT
    fc28.application_number             AS "IDENTIFIANT DOSSIER (BIS)",
    fc21.last_name                      AS "NOM ASSURE",
    fc21.first_name                     AS "PRENOM ASSURE",
    fc21.date_of_birth                  AS "DATE DE NAISSANCE ASSURE",
    fc23.mail_code_type                 AS "CODE COURRIER TYPE",
    fc23.mail_event_id                  AS "IDENTIFIANT COURRIER ENVOYE",
    fc23.mail_event_date                AS "DATE DE CREATION DU COURRIER",
    fc28.status_request                 AS "ETAT DE LA DEMANDE DE RC",
    fc28.supporting_document_id        AS "CODE PIECE JUSTIFICATIVE TYPE",
    fc28.document_status               AS "ETAT DE LA PIECE JUSTIFIC",
    fc28.document_received_date        AS "DATE DE RECEPTION PIECE JUSTIF",
    fc28.clause_id                      AS "IDENTIFIANT CLAUSE ENVOYEE",
    fc27.clause_code                    AS "CODE CLAUSE/PARAGRAPHE TYPE",
    fc26.variable_type_code             AS "CODE DE LA VARIABLE",
    fc26.variable_order_number          AS "NUMERO ORDRE VARIABLE NUMERIQ",
    fc26.float_value                    AS "CONTENU DE LA VARIABLE NUMERIQ",
    fc26.integer_value                  AS "CONTENU DE LA VARIABLE NUMERIQ",
    fc26.date_value                     AS "CONTENU DE LA VARIABLE TYPE DA",
    fc26.text_value                     AS "CONTENU DE LA VARIABLE TEXTE L"
FROM fc28_records fc28
INNER JOIN fc01_records fc01 
    ON fc28.application_number = fc01.application_number
INNER JOIN fc21_records fc21 
    ON fc28.application_number = fc01.application_number 
   AND fc21.insured_id = fc01.insured_id
INNER JOIN fc23_records fc23 
    ON fc23.application_number = fc28.application_number
INNER JOIN fc27_records fc27 
    ON fc27.application_number = fc23.application_number 
   AND fc27.mail_event_id = fc23.mail_event_id
INNER JOIN fc26_records fc26 
    ON fc26.application_number = fc27.application_number 
   AND fc26.mail_event_id = fc27.mail_event_id
WHERE fc23.mail_code_type = 'DRC'
  AND fc28.document_status = 'A'
  AND fc23.mail_event_date BETWEEN '2025-06-27'::DATE - INTERVAL '180 days' AND '2025-06-27'
ORDER BY fc23.mail_event_date;
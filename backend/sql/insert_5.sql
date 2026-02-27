SET maintenance_work_mem = '4GB';
SET synchronous_commit = off;

INSERT INTO pubmed_articles_new (
    pmid, title, abstract, journal, med_abbr, iso_abbr, 
    pubdate, pubmed_pubdate, pmc, issn, e_issn, doi, 
    year, pagination, volume, issue, pub_status, 
    authors, keywords, pub_types, author_mail, 
    author_first, author_last, affiliations, 
    abstract_cn, factor, jcr, zky, title_abstract_vec
) 
SELECT 
    pmid, title, abstract, journal, med_abbr, iso_abbr, 
    pubdate, pubmed_pubdate, pmc, issn, e_issn, doi, 
    year, pagination, volume, issue, pub_status, 
    authors, keywords, pub_types, author_mail, 
    author_first, author_last, affiliations, 
    abstract_cn, factor, jcr, zky, title_abstract_vec
FROM pubmed_articles_old
WHERE year >= 2026;

ANALYZE pubmed_part_future;

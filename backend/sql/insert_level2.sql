-- 1. 调高内存限制，给排序和转换留足空间
SET work_mem = '2GB';
-- 2. 禁用超时限制（搬家耗时较长）
SET statement_timeout = 0;

-- 3. 开始“大搬家”
INSERT INTO pubmed_articles_2 (
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
FROM pubmed_articles_old;

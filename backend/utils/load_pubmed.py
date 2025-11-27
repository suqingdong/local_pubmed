import json
import datetime

import click
import loguru
from dateutil.parser import parse as date_parse

from pubmed_xml import Pubmed_XML_Parser
from impact_factor.core import Factor


def load_pubmed_xml(xml, min_factor=None, n_years=5):
    parser = Pubmed_XML_Parser()
    fa = Factor()
    result = parser.parse(xml)
    for n, article in enumerate(result, 1):
        loguru.logger.debug(f'>>> dealing with: {n}')
        data = article.data

        # 只保留最近 n_years 年的
        if n_years and date_parse(article.pubmed_pubdate) < datetime.datetime.now() - datetime.timedelta(days=n_years*365):
            continue

        # 只保留期刊文章和综述
        if ('Journal Article' not in article.pub_types) and ('Review' not in article.pub_types):
            continue

        # 只保留影响因子大于等于 min_factor 的文章
        if min_factor:
            fa_result = None
            if article.e_issn:
                fa_result = fa.search(article.e_issn, key='eissn')
            elif article.issn:
                fa_result = fa.search(article.issn, key='issn')
            else:
                fa_result = fa.search(article.journal, key='journal')

            if fa_result:
                data['factor'] = fa_result[0]['factor']
                data['jcr'] = fa_result[0]['jcr']
            else:
                data['factor'] = 0
                loguru.logger.debug(f'filter no impact factor for pmid: {article.pmid}')
                continue

            if data['factor'] < min_factor:
                loguru.logger.debug(f'filter low impact factor for pmid: {article.pmid}')
                continue

        data['pubmed_pubdate'] = date_parse(article.pubmed_pubdate).strftime('%F')
        yield data


@click.command(no_args_is_help=True)
@click.argument('input_xmls', nargs=-1)
@click.option('--min-factor', help='min impact factor', type=float, default=1, show_default=True)
@click.option('--n-years', help='max years', type=int, default=5, show_default=True)
@click.option('-l', '--logfile', help='the log file')
def main(input_xmls, min_factor, n_years, logfile):
    if logfile:
        loguru.logger.remove()
        loguru.logger.add(logfile)
    loguru.logger.info(f'>>> min_factor: {min_factor}, n_years: {n_years}')

    for xml in input_xmls:
        out_jl = xml + '.jl'
        with open(out_jl, 'w') as out:
            for data in load_pubmed_xml(xml, min_factor=min_factor, n_years=n_years):
                out.write(json.dumps(data) + '\n')
    loguru.logger.info(f'>>> save file to: {out_jl}')


if __name__ == '__main__':
    main()

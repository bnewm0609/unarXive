"""
Author: Benjamin Newman
Date: 2023-11-17
"""

import json
import sys

from bs4 import BeautifulSoup


def soupify(table_json):
    soup = BeautifulSoup(table_json, "lxml-xml")
    for row in soup.find_all("row"):
        row.name = "tr"
    for cell in soup.find_all("cell"):
        cell.name = "td"
    for tex_math in soup.find_all("texmath"):
        # remove the "texmath"
        tex_math.extract()
    return soup


def pprint_soup(soup):
    from IPython.display import display, HTML

    display(HTML(str(soup)))


# filters:
def has_x(table_soup):
    return "✗" in " ".join(table_soup.strings)


TABLE_FILTERS = [has_x]


def filter_tables(tables_fn, out_fn):
    """filters tables according to TABLE_FILTERS from tables_fn and saves them in out_fn

    Args:
        tables_fn (str, Path): e.g. "arxiv_dump/out_xml/2310.00000-07773.jsonl"
        out_fn (str, Path): "arxiv_dump/out_tables_filtered/2310.00000-07773.jsonl"
    """
    papers_with_valid_tables = []

    with open(tables_fn) as f:
        for line in f:
            paper = json.loads(line)
            filtered_tables = {}
            for key, table in paper["tables"].items():
                if table["table"]:
                    table_soup = soupify(table["table"])

                    exit_early = False
                    for flter in TABLE_FILTERS:
                        if not flter(table_soup):
                            # exit early
                            exit_early = True
                            break

                    if exit_early:
                        continue

                    print(paper["paper_id"], key)
                    pprint_soup(table_soup)
                    filtered_tables[key] = table
                    filtered_tables[key]["html_table"] = str(table_soup)

            if filtered_tables:
                new_paper = {k: v for k, v in paper.items() if k != "tables"}
                new_paper["tables"] = filtered_tables
                papers_with_valid_tables.append(new_paper)

    # save the first num_demo_tables tables in html doc for answering data quality questions
    # and remove duplicates:

    added_tables = set()
    num_demo_tables = 100
    html_file_text = "<body>"
    with open(out_fn, "w") as f:
        for paper_i, paper in enumerate(papers_with_valid_tables):
            if paper_i < num_demo_tables:
                html_file_text += "<div>"
                arxiv_link = f"https://arxiv.org/pdf/{paper['paper_id']}.pdf"
                html_file_text += (
                    f'<p><a href="{arxiv_link}"><h2>{arxiv_link}</h2></a></p>'
                )

            for key, table in paper["tables"].items():
                bs_table = BeautifulSoup(table["html_table"])
                if bs_table.find("table") in added_tables:
                    # remove the duplicate
                    del paper["tables"][key]
                    continue
                else:
                    for sub_table in bs_table.find_all("table"):
                        added_tables.add(sub_table)

                    if paper_i < num_demo_tables:
                        html_file_text += table["html_table"] + "\n<hr>"
                if paper_i < num_demo_tables:
                    html_file_text += "</div>\n"

            f.write(json.dumps(paper) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python filter_tables.py <path/to/tables.json> <out/tables.json>")
        sys.exit()

    tables_fn = sys.argv[1]
    out_fn = sys.argv[2]
    filter_tables(tables_fn, out_fn)

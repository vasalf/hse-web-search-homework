#!/usr/bin/env python3

import abc
import argparse
import base64
import bs4
import codecs
import errno
import json
import multiprocessing as mp
import os
import os.path
import re
import xml.dom.minidom as minidom


class ExtractedDocument:
    def __init__(self, docId, url, content):
        self.docId = docId
        self.url = codecs.decode(base64.b64decode(url), encoding="cp1251")
        self.content = codecs.decode(base64.b64decode(content), encoding="cp1251")
        self.__processContent()

    @staticmethod
    def __visible(element):
        if element.parent.name in ["style", "script", "head", "[document]", "title"]:
            return False
        elif re.match("<!--.*-->", str(element.encode("utf-8"))):
            return False
        return True

    def __processContent(self):
        soup = bs4.BeautifulSoup(self.content, features="lxml")
        if soup.title is not None:
            self.title = soup.title.text
        else:
            self.title = ""
        self.leadsTo = []
        for tag in soup.findAll("a"):
            self.leadsTo.append(str(tag.get("href")))
        elements = filter(ExtractedDocument.__visible, soup.findAll(text=True))
        texts = map(lambda e: str(e) + " ", elements)
        self.content = "".join(texts)

    def write(self, dirName):
        with open(os.path.join(dirName, "{:>07d}.txt".format(self.docId)), "w", encoding="utf8") as content:
            content.write(self.content)
        with open(os.path.join(dirName, "{:>07d}.json".format(self.docId)), "w", encoding="utf8") as meta:
            json.dump({
                "title": self.title,
                "url": self.url,
                "leadsTo": self.leadsTo,
            }, meta, ensure_ascii=False)


class ExtractorBase(abc.ABC):
    RESULT_DIR = "extracted"

    def __init__(self):
        try:
            os.makedirs(ExtractorBase.RESULT_DIR)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    @abc.abstractmethod
    def addDocument(self, path):
        pass


class MiniDOMExtractor(ExtractorBase):
    @staticmethod
    def getText(node):
        rc = []
        for innerNode in node.childNodes:
            if innerNode.nodeType == innerNode.TEXT_NODE:
                rc.append(innerNode.data)
        return ''.join(rc)

    def addDocument(self, path):
        dom = minidom.parse(path)
        for document in dom.getElementsByTagName("document"):
            content = self.getText(document.getElementsByTagName("content")[0])
            url = self.getText(document.getElementsByTagName("docURL")[0])
            docId = self.getText(document.getElementsByTagName("docID")[0])
            extractedDocument = ExtractedDocument(int(docId), url, content)
            extractedDocument.write(ExtractorBase.RESULT_DIR)


def extract(xmlId):
    extractor = MiniDOMExtractor()
    extractor.addDocument("byweb_for_course/byweb.{}.xml".format(xmlId))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--processes", type=int, help="Number of processes to run (default: 4)", default=4)
    args = parser.parse_args()
    p = mp.Pool(args.processes)
    p.map(extract, list(range(10)))


if __name__ == "__main__":
    main()

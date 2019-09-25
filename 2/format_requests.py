import argparse
import xml.dom.minidom as minidom

from pipeline.pipeline import PipedInput
from pipeline.text_processor_stage import TextLemmatizerStage, TextStemmerStage, TextWithHeaderStage

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', "--input", type=str, help="XML to convert reuests",
                        default="web2008_adhoc.xml")
    parser.add_argument('-m', "--mode", type=str, help="How to convert requests",
                        default="stemmas")

    args = parser.parse_args()
    text = ""
    with open(args.input, "r", encoding="cp1251") as file:
        text = "\n".join(file.readlines())

    if args.mode == "lemmas":
        formatter = TextLemmatizerStage()
    elif args.mode == "stemmas":
        formatter = TextStemmerStage()
    else:
        raise

    dom = minidom.parseString(text)
    for token in dom.getElementsByTagName("task"):
        token.firstChild.firstChild.replaceWholeText(formatter.accept(PipedInput(token.firstChild.firstChild.nodeValue, None, None)).get_text())

    with open("output_{}.xml".format(args.mode), 'w', encoding="utf-8") as file:
        file.write(dom.toxml())


if __name__ == "__main__":
    main()

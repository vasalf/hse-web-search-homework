#!/usr/bin/env python3

import argparse
import errno
import os.path
import sys
import time

from pipeline.pipeline import PipelineStage, PipedInput, PipelineImmutableStage, PipelineFeaturesDumper
from pipeline.json_unpacker_stage import JsonUnpackerStage
from pipeline.length_counter_stage import LengthCounterStage
from pipeline.pagerank import PageRankStage
from queries import *


def main():
    timestamp = time.time()

    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--documents", type=int, help="Number of documents to process", default=100)
    parser.add_argument('-i', "--input", type=str, help="Directory to read extracted documents from",
                        default="extracted")
    parser.add_argument('-e', '--encoding', type=str, help="Documents encoding", default='utf8')

    parser.add_argument('-r', '--relevant', type=str, help="Relevance table for queries path",
                        default="or_relevant-minus_table.xml")
    parser.add_argument('-q', '--queries', type=str, help="Queries path",
                        default="web2008_adhoc.xml")

    args = parser.parse_args()

    features = {}
    # ДИМА, ИСПОЛЬЗУЙ ЭТО V
    CreateFeatureDumper = lambda stage: PipelineFeaturesDumper(stage, features)

    stages = [
        #JsonUnpackerStage(),
        #PipelineImmutableStage(LengthCounterStage()),
        #PipelineImmutableStage(PageRankStage()),
    ]
    # Register your pipeline stage here.

    relevant = load_relevant_docs(args.relevant)
    queries = load_queries(args.queries, relevant)

    print("Ready to process {} files in {} directory.".format(args.documents, args.input))

    try:
        os.makedirs("logs")
        os.makedirs("results")
        print("Created directories", file=sys.stderr)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    with open(os.path.join('logs', '{}.txt'.format(timestamp)), 'w') as logs:
        sys.stdout = logs

        doc_id = 0
        documents_counter = 0
        while documents_counter < args.documents:
            text_path = os.path.join(args.input, "{:>07d}.txt".format(doc_id))
            meta_path = os.path.join(args.input, "{:>07d}.json".format(doc_id))

            if os.path.exists(text_path) and os.path.exists(meta_path):
                print("Found files {} and {}. {}/{} processed.".format(text_path, meta_path, documents_counter,
                                                                       args.documents), file=sys.stderr)
                with open(text_path, 'r', encoding=args.encoding) as text_file:
                    with open(meta_path, 'r', encoding=args.encoding) as meta_file:
                        text = "\n".join(text_file.readlines())
                        meta = "\n".join(meta_file.readlines())

                        stage_input = PipedInput(text, meta, doc_id)

                        for consumer in stages:
                            stage_input = consumer.accept(stage_input)
                documents_counter += 1
            doc_id += 1

        for stage in stages:
            stage.dump()


if __name__ == "__main__":
    main()

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
from pipeline.field_match_stage import FieldMatchStage
from pipeline.init_features_stage import InitFeaturesStage
from pipeline.query_length_stage import QueryLengthStage
from pipeline.text_processor_stage import TextProcessorStage, StopwordsFilter
from pipeline.bm25 import BM25Stage
from queries import *
from utils import *


def main():

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

    relevant, irrelevant, docs_to_queries = load_relevant_docs(args.relevant)
    queries = load_queries(args.queries, relevant, irrelevant)

    features = {}
    query_features = {}
    doc_features = {}
    CreateFeatureDumper = lambda stage: PipelineFeaturesDumper(stage, features, query_features, doc_features)

    stages = [
        JsonUnpackerStage(),
        TextProcessorStage(),
        StopwordsFilter(),
        PipelineImmutableStage(CreateFeatureDumper(InitFeaturesStage(queries, docs_to_queries))),
        PipelineImmutableStage(CreateFeatureDumper(LengthCounterStage())),
        PipelineImmutableStage(CreateFeatureDumper(FieldMatchStage(queries, docs_to_queries))),
        PipelineImmutableStage(CreateFeatureDumper(PageRankStage())),
        PipelineImmutableStage(CreateFeatureDumper(QueryLengthStage(queries))),
        PipelineImmutableStage(CreateFeatureDumper(BM25Stage(queries, docs_to_queries))),
    ]
    # Register your pipeline stage here.

    print("Ready to process {} files in {} directory.".format(args.documents, args.input))

    try:
        os.makedirs("logs")
        os.makedirs("results")
        print("Created directories", file=sys.stderr)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    timestamp = time.time()
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

    result = {}

    for doc, queries in docs_to_queries.items():
        if doc not in doc_features:
            continue
        df = doc_features[doc]
        for q in queries:
            qf = query_features[q]
            fs = {}
            fs.update(df)
            fs.update(qf)
            pair = "{}:{}".format(q, doc)
            fs.update(features[pair])
            fs["target"] = int(doc in relevant[q])

            result[pair] = fs

    with open("dataset", 'w') as file:
        for pair, fs in result.items():
            for key, value in fs.items():
                file.write("{}:{} ".format(key, value))
            file.write("\n")


if __name__ == "__main__":
    main()

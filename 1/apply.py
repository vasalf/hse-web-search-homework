#!/usr/bin/env python3

import argparse
import os.path
import sys
import time

from pipeline.pipeline import PipelineStage, PipedInput, PipelineImmutableStage
from pipeline.text_processor_stage import TextProcessorStage
from pipeline.stats_counters import StopwordsCounter, SimpleStats, DictionaryStats
from pipeline.json_unpacker_stage import JsonUnpackerStage


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--documents", type=int, help="Number of documents to process", default=100)
    parser.add_argument('-i', "--input", type=str, help="Directory to read extracted documents from",
                        default="extracted")
    parser.add_argument('-e', '--encoding', type=str, help="Documents encoding", default='utf8')

    args = parser.parse_args()

    print("Ready to process {} files in {} directory.".format(args.documents, args.input))

    stages = [
        # Turns file lines from the input into the list of normalized words.
        TextProcessorStage(),
        JsonUnpackerStage(),
        PipelineImmutableStage(StopwordsCounter()),
        PipelineImmutableStage(DictionaryStats()),
        PipelineImmutableStage(SimpleStats()),
    ]
    # Register your pipeline stage here.

    with open('logs_{}.txt'.format(time.time()), 'w') as logs:
        sys.stdout = logs

        doc_id = 0
        documents_counter = 0
        while documents_counter < args.documents:
            text_path = os.path.join(args.input, "{:>07d}.txt".format(doc_id))
            meta_path = os.path.join(args.input, "{:>07d}.json".format(doc_id))

            if os.path.exists(text_path) and os.path.exists(meta_path):
                print("Found files {} and {}. {}/{} processed.".format(text_path, meta_path, documents_counter,
                                                                       args.documents))
                with open(text_path, 'r', encoding=args.encoding) as text_file:
                    with open(meta_path, 'r', encoding=args.encoding) as meta_file:
                        text = text_file.readlines()
                        meta = meta_file.readlines()

                        stage_input = PipedInput(text, meta, doc_id)

                        for consumer in stages:
                            stage_input = consumer.accept(stage_input)
                documents_counter += 1
            doc_id += 1

        for stage in stages:
            stage.dump()


if __name__ == "__main__":
    main()

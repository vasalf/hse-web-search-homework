import abc


class PipedInput:
    def __init__(self, text, meta, doc_id):
        self.__text = text
        self.__meta = meta
        self.__doc_id = doc_id

    def get_text(self):
        return self.__text

    def get_meta(self):
        return self.__meta

    def get_doc_id(self):
        return self.__doc_id

    def new(self, text=None, meta=None, doc_id=None):
        new_text = self.__text if text is None else text
        new_meta = self.__meta if meta is None else meta
        new_doc_id = self.__doc_id if doc_id is None else doc_id
        return PipedInput(new_text, new_meta, new_doc_id)


class PipelineStage(abc.ABC):
    def accept(self, consumer_input: PipedInput):
        raise NotImplementedError()

    def dump(self):
        raise NotImplementedError()


class PipelineImmutableStage(PipelineStage):
    def __init__(self, stage: PipelineStage):
        self.stage = stage

    def accept(self, consumer_input: PipedInput):
        self.stage.accept(consumer_input)
        return consumer_input

    def dump(self):
        return self.stage.dump()


class PipelineFeaturesDumper(PipelineImmutableStage):
    def __init__(self, stage: PipelineStage, features_dump, query_features_dump, doc_features_dump):
        super().__init__(stage)
        stage.features = features_dump
        stage.query_features = query_features_dump
        stage.doc_features = doc_features_dump
